<#
.SYNOPSIS
    MT5 Extração - instalador automático para Windows.

.DESCRIPTION
    Idempotente (pode ser executado várias vezes):
      1. Localiza ou instala o Python 3.11 (winget; se indisponível, instalador oficial python.org)
      2. Cria o ambiente virtual .venv e instala as dependências (requirements.txt)
      3. Localiza ou baixa e instala o MetaTrader 5 (mt5setup.exe /auto)
      4. Gera config\config.ini e .env (python -m mt5_extracao.bootstrap configure)
      5. Cria atalho na Área de Trabalho e roda o diagnóstico

    Normalmente é iniciado com duplo clique em install.bat.
    Log: logs\install-windows.log

.PARAMETER Yes
    Não faz perguntas (usa os padrões).
.PARAMETER NoMT5
    Não instala o MetaTrader 5.
.PARAMETER NoShortcut
    Não cria o atalho na Área de Trabalho.
.PARAMETER MT5Path
    Pasta de uma instalação existente do MT5 (ex.: a da sua corretora).
.PARAMETER PythonVersion
    Versão do Python a instalar se nenhuma compatível for encontrada.
#>
[CmdletBinding()]
param(
    [switch]$Yes,
    [switch]$NoMT5,
    [switch]$NoShortcut,
    [string]$MT5Path = "",
    [string]$PythonVersion = "3.11.9"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # acelera Invoke-WebRequest
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# --- Versões/URLs (altere aqui, em um único lugar) ---------------------------
$MT5SetupUrl = "https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe"
$PythonMinor = @(3, 11)     # mínimo aceito (coberto pelo CI)
$PythonMaxMinor = 13        # máximo testado (3.13)

# ProviderPath: caminho do sistema de arquivos (UNC \\servidor\... ou unidade), sem prefixo do provedor
$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..\..")).ProviderPath
$LogDir = Join-Path $ProjectRoot "logs"
$CacheDir = Join-Path $env:LOCALAPPDATA "mt5-extracao\cache"
# Definidos depois de localizar o Python, pelo mesmo resolvedor do app (mt5_extracao/bootstrap/paths.py):
# .venv é reutilizado se for do Windows ou estiver ausente; um .venv do Linux na mesma composição
# (dual boot/WSL) é preservado e o Windows usa .venv-windows, com config\config.windows.ini.
$VenvDir = $null
$VenvPy = $null
$ConfigFile = $null
New-Item -ItemType Directory -Force -Path $LogDir, $CacheDir | Out-Null
$LogFile = Join-Path $LogDir "install-windows.log"
try { Start-Transcript -Path $LogFile -Append | Out-Null } catch { }

$script:Step = 0
$TotalSteps = 5
function Write-Step([string]$Message) {
    $script:Step++
    Write-Host ""
    Write-Host "[$script:Step/$TotalSteps] $Message" -ForegroundColor Cyan
}
function Write-Info([string]$Message) { Write-Host "  - $Message" }
function Write-Ok([string]$Message) { Write-Host "  [OK] $Message" -ForegroundColor Green }
function Write-Warn([string]$Message) { Write-Host "  [!] $Message" -ForegroundColor Yellow }

function Confirm-Action([string]$Question) {
    if ($Yes) { return $true }
    $answer = Read-Host "  $Question [S/n]"
    return ([string]::IsNullOrWhiteSpace($answer) -or $answer -match '^[sSyY]')
}

# Executa um programa externo e lança erro se o código de saída for diferente de 0
# Obs.: no Windows PowerShell 5.1, texto em stderr de programas externos vira erro
# quando $ErrorActionPreference = "Stop"; por isso as funções que chamam programas
# externos usam "Continue" localmente e verificam $LASTEXITCODE.
function Invoke-Native([string]$Exe, [string[]]$ArgList, [string]$ErrorMessage) {
    $ErrorActionPreference = "Continue"
    & $Exe @ArgList | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "$ErrorMessage (código $LASTEXITCODE)" }
}

# Executa em silêncio e retorna apenas o código de saída
function Invoke-Quiet([string]$Exe, [string[]]$ArgList) {
    $ErrorActionPreference = "Continue"
    & $Exe @ArgList *> $null
    return $LASTEXITCODE
}

function Get-File([string]$Url, [string]$Destination) {
    if ((Test-Path $Destination) -and ((Get-Item $Destination).Length -gt 0)) {
        Write-Info "Usando arquivo em cache: $Destination"
        return
    }
    $partial = "$Destination.part"
    for ($attempt = 1; $attempt -le 4; $attempt++) {
        try {
            Write-Info "Baixando $Url (tentativa $attempt/4)"
            Invoke-WebRequest -Uri $Url -OutFile $partial -UseBasicParsing
            Move-Item -Force $partial $Destination
            return
        } catch {
            if ($attempt -eq 4) { throw "Falha ao baixar $Url : $($_.Exception.Message)" }
            Start-Sleep -Seconds ([math]::Pow(2, $attempt))
        }
    }
}

# --- 1. Python ----------------------------------------------------------------
function Test-PythonCandidate([string]$Exe, [string[]]$PrefixArgs) {
    # Retorna a versão "3.11.9" se o Python for compatível (com tkinter); senão $null
    $ErrorActionPreference = "Continue"
    try {
        $code = "import sys, tkinter; print('%d.%d.%d' % sys.version_info[:3])"
        $out = & $Exe @($PrefixArgs + @("-c", $code)) 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $out) { return $null }
        $version = ($out | Select-Object -Last 1).ToString().Trim()
        $parts = $version.Split(".")
        $major = [int]$parts[0]; $minor = [int]$parts[1]
        if ($major -eq 3 -and $minor -ge $PythonMinor[1] -and $minor -le $PythonMaxMinor) { return $version }
    } catch { }
    return $null
}

function Find-Python {
    $candidates = @()
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($v in @("3.11", "3.12", "3.13")) {
            $candidates += , @("py", @("-$v"))
        }
    }
    foreach ($v in @("311", "312", "310", "313", "39")) {
        $candidates += , @((Join-Path $env:LOCALAPPDATA "Programs\Python\Python$v\python.exe"), @())
        $candidates += , @((Join-Path $env:ProgramFiles "Python$v\python.exe"), @())
    }
    $inPath = Get-Command python -ErrorAction SilentlyContinue
    # Ignora o atalho da Microsoft Store (WindowsApps), que não é um Python real
    if ($inPath -and ($inPath.Source -notmatch "WindowsApps")) { $candidates += , @($inPath.Source, @()) }

    foreach ($c in $candidates) {
        $exe = $c[0]; $prefix = [string[]]$c[1]
        if ($exe -ne "py" -and -not (Test-Path $exe)) { continue }
        $version = Test-PythonCandidate $exe $prefix
        if ($version) { return @{ Exe = $exe; Args = $prefix; Version = $version } }
    }
    return $null
}

function Install-PythonWinget {
    $short = ($PythonVersion.Split(".")[0..1]) -join "."
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) { return $null }
    Write-Info "Instalando Python $short via winget (somente para o usuário atual)..."
    Invoke-Quiet "winget" @("install", "--id", "Python.Python.$short", "-e", "--scope", "user", "--silent",
        "--accept-package-agreements", "--accept-source-agreements") | Out-Null
    return Find-Python
}

function Test-VirtualizedAppData {
    # Processos iniciados por apps empacotados (MSIX) podem ter %LOCALAPPDATA% redirecionado para
    # Packages\<app>\LocalCache. O Windows Installer (SYSTEM) não enxerga esse destino e um Python
    # instalado ali ficaria invisível fora do app: detectar antes de instalar.
    $probe = "mt5x-probe-" + [guid]::NewGuid().ToString("N")
    $path = Join-Path $env:LOCALAPPDATA $probe
    try {
        New-Item -ItemType Directory -Path $path | Out-Null
        $packages = Join-Path $env:LOCALAPPDATA "Packages"
        $redirected = @(Get-ChildItem -LiteralPath $packages -Directory -ErrorAction SilentlyContinue |
            Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "LocalCache\Local\$probe") })
        if ($redirected.Count -gt 0) { return $redirected[0].Name }
        return $null
    } finally {
        Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    }
}

function Install-Python {
    $package = Test-VirtualizedAppData
    if ($package) {
        throw ("Este terminal foi aberto por um aplicativo empacotado ($package) e o Windows redireciona " +
               "%LOCALAPPDATA% para a pasta privada dele: o Python não pode ser instalado daqui. " +
               "Rode install.bat pelo Explorador de Arquivos ou por um terminal comum do Windows.")
    }
    # Instalador oficial primeiro: assinatura conferida e PATH do usuário inalterado.
    $installer = Join-Path $CacheDir "python-$PythonVersion-amd64.exe"
    try {
        Get-File "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-amd64.exe" $installer
    } catch {
        Write-Warn "Download oficial indisponível ($($_.Exception.Message)); tentando winget."
        $found = Install-PythonWinget
        if ($found) { return $found }
        throw
    }
    $signature = Get-AuthenticodeSignature -LiteralPath $installer
    if ($signature.Status -ne "Valid" -or $signature.SignerCertificate.Subject -notmatch "Python Software Foundation") {
        throw "Instalador do Python sem assinatura válida da Python Software Foundation ($($signature.Status)): $installer. Apague o arquivo e repita."
    }
    Write-Info "Instalando Python $PythonVersion (silencioso, somente usuário atual, sem alterar o PATH)..."
    # PrependPath=0: o app usa o ambiente do projeto; o `python` padrão do usuário não muda.
    $proc = Start-Process -FilePath $installer -Wait -PassThru -ArgumentList @(
        "/quiet", "InstallAllUsers=0", "PrependPath=0", "Include_launcher=0",
        "Include_tcltk=1", "Include_pip=1", "Include_test=0")
    if ($proc.ExitCode -ne 0) { throw "O instalador do Python retornou código $($proc.ExitCode)." }
    $found = Find-Python
    if (-not $found) { throw "Python instalado, mas não localizado. Feche e abra o terminal e rode install.bat novamente." }
    return $found
}

function Resolve-ProjectEnvironment($python) {
    # Fonte única: mt5_extracao/bootstrap/paths.py (somente stdlib), executado em modo isolado.
    $ErrorActionPreference = "Continue"
    $code = "import sys; sys.path.insert(0, sys.argv[1]); from mt5_extracao.bootstrap import paths as p; " +
            "r = sys.argv[1]; e = p.environment_dir(r); f = p.foreign_environment(r); " +
            "print(e); print(p.venv_platform(e) or ''); print(p.config_path(r)); print(f or '')"
    $out = @(& $python.Exe @($python.Args + @("-I", "-c", $code, $ProjectRoot)) 2>$null)
    if ($LASTEXITCODE -ne 0 -or $out.Count -lt 4) { throw "Não foi possível resolver o ambiente do projeto em $ProjectRoot." }
    $script:VenvDir = $out[0].Trim()
    $script:VenvPy = Join-Path $script:VenvDir "Scripts\python.exe"
    $script:ConfigFile = $out[2].Trim()
    if ($out[1].Trim() -eq "mixed") {
        throw "$($script:VenvDir) mistura arquivos de Windows e Linux. Nada foi instalado sobre ele: mova-o para fora da composição e repita."
    }
    $foreign = $out[3].Trim()
    if ($foreign) {
        Write-Info "Ambiente de outro sistema preservado sem alteração: $foreign"
        Write-Info "Este Windows usa $($script:VenvDir) e $($script:ConfigFile)"
    }
}

function Initialize-PythonEnv {
    Write-Step "Python e ambiente virtual"
    $python = Find-Python
    if ($python) {
        Write-Ok "Python $($python.Version) encontrado"
    } else {
        Write-Warn "Nenhum Python 3.11-3.13 com tkinter encontrado."
        $python = Install-Python
        Write-Ok "Python $($python.Version) instalado"
    }
    Resolve-ProjectEnvironment $python
    if (-not (Test-Path -LiteralPath $VenvPy)) {
        Invoke-Native $python.Exe ($python.Args + @("-m", "venv", $VenvDir)) "Falha ao criar o ambiente virtual"
        Write-Ok "Ambiente virtual criado em $VenvDir"
    } else {
        Write-Ok "Ambiente virtual já existe ($VenvDir)"
    }

    Write-Step "Dependências Python"
    Invoke-Native $VenvPy @("-m", "pip", "install", "--upgrade", "pip", "--quiet", "--disable-pip-version-check") "Falha ao atualizar o pip"
    Write-Info "Instalando dependências (pode levar alguns minutos)..."
    Invoke-Native $VenvPy @("-m", "pip", "install", "-r", (Join-Path $ProjectRoot "requirements.txt"), "--disable-pip-version-check") "Falha ao instalar requirements.txt"
    # Opcionais um a um: a falha de um (ex.: pandas-ta) não impede os outros
    $failed = @()
    foreach ($line in Get-Content (Join-Path $ProjectRoot "requirements-optional.txt")) {
        $pkg = ($line -split "#")[0].Trim()
        if (-not $pkg) { continue }
        if ((Invoke-Quiet $VenvPy @("-m", "pip", "install", $pkg, "--disable-pip-version-check")) -ne 0) { $failed += $pkg }
    }
    if ($failed.Count -eq 0) { Write-Ok "Dependências opcionais instaladas" }
    else { Write-Warn "Opcionais indisponíveis: $($failed -join ', '). O app funciona sem eles (pandas-ta: indicadores básicos; pyarrow/duckdb: exportação Parquet/DuckDB)" }
    Write-Ok "Dependências instaladas"
}

# --- 3. MetaTrader 5 ------------------------------------------------------------
function Test-MT5Installed {
    return ((Invoke-Quiet $VenvPy @("-m", "mt5_extracao.bootstrap", "locate-mt5")) -eq 0)
}

function Install-MT5 {
    Write-Step "MetaTrader 5"
    if ($MT5Path) {
        if (Test-Path (Join-Path $MT5Path "terminal64.exe")) { Write-Ok "Usando MT5 informado: $MT5Path"; return }
        throw "terminal64.exe não encontrado em '$MT5Path'."
    }
    if (Test-MT5Installed) { Write-Ok "MetaTrader 5 já instalado"; return }
    if ($NoMT5) { Write-Warn "Instalação do MT5 pulada (-NoMT5)"; return }
    if (-not (Confirm-Action "MetaTrader 5 não encontrado. Baixar e instalar agora?")) {
        Write-Warn "Instalação do MT5 pulada pelo usuário"; return
    }
    $setup = Join-Path $CacheDir "mt5setup.exe"
    Get-File $MT5SetupUrl $setup
    Write-Info "Executando o instalador do MT5 em modo automático (/auto)."
    Write-Info "O Windows pode pedir permissão de administrador: clique em 'Sim'."
    Start-Process -FilePath $setup -ArgumentList "/auto" -Wait | Out-Null
    $deadline = (Get-Date).AddMinutes(10)
    while (-not (Test-MT5Installed)) {
        if ((Get-Date) -gt $deadline) { throw "O MT5 não foi instalado em 10 minutos. Execute manualmente: $setup" }
        Start-Sleep -Seconds 5
    }
    Write-Ok "MetaTrader 5 instalado"
}

# --- 4. Configuração ------------------------------------------------------------
function Invoke-Configure {
    Write-Step "Configuração inicial (config\config.ini e .env)"
    $cfgArgs = @("-m", "mt5_extracao.bootstrap", "configure", "--no-doctor")
    if ($Yes) { $cfgArgs += "--non-interactive" }
    if ($MT5Path) { $cfgArgs += @("--mt5-path", $MT5Path) }
    Invoke-Native $VenvPy $cfgArgs "Falha na configuração inicial"
    Write-Ok "Configuração concluída"
}

# --- 5. Atalho e diagnóstico ------------------------------------------------------
function Complete-Install {
    Write-Step "Atalho e diagnóstico"
    if (-not $NoShortcut) {
        try {
            $desktop = [Environment]::GetFolderPath("Desktop")
            $shell = New-Object -ComObject WScript.Shell
            $lnk = $shell.CreateShortcut((Join-Path $desktop "MT5 Extração.lnk"))
            $lnk.TargetPath = Join-Path $ProjectRoot "run.bat"
            $lnk.WorkingDirectory = $ProjectRoot
            $lnk.Description = "Extração de dados do MetaTrader 5"
            $lnk.Save()
            Write-Ok "Atalho 'MT5 Extração' criado na Área de Trabalho"
        } catch {
            Write-Warn "Não foi possível criar o atalho: $($_.Exception.Message)"
        }
    }
    Write-Host ""
    $ErrorActionPreference = "Continue"
    & $VenvPy -m mt5_extracao.bootstrap doctor | Out-Host
    return ($LASTEXITCODE -eq 0)
}

try {
    Write-Host "MT5 Extração - instalação automática (Windows)" -ForegroundColor White
    Write-Info "Projeto: $ProjectRoot"
    Set-Location -LiteralPath $ProjectRoot
    Initialize-PythonEnv
    Install-MT5
    Invoke-Configure
    $healthy = Complete-Install
    Write-Host ""
    if ($healthy) {
        Write-Host "Instalação concluída!" -ForegroundColor Green
    } else {
        Write-Host "Instalação concluída com pendências. Corrija os itens [FALHA] acima e rode install.bat novamente." -ForegroundColor Yellow
    }
    Write-Host "  Iniciar o aplicativo: run.bat (ou atalho 'MT5 Extração' na Área de Trabalho)"
    if ($VenvPy) { Write-Host "  Diagnóstico:          `"$VenvPy`" -m mt5_extracao.bootstrap doctor" }
    Write-Host "  Log da instalação:    $LogFile"
    $exitCode = 0
} catch {
    Write-Host ""
    Write-Host "ERRO: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "  Detalhes em: $LogFile"
    Write-Host "  Você pode rodar install.bat novamente: as etapas concluídas são puladas."
    $exitCode = 1
} finally {
    try { Stop-Transcript | Out-Null } catch { }
}
exit $exitCode
