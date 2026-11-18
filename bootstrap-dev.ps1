# Entrada independente para Windows PowerShell 5.1 ou PowerShell 7.
# Reutiliza a composicao registrada (WORKSPACE\repos\app|hadouken|safety): nunca cria outra
# Workspace, clone ou worktree sobre trabalho existente e nunca altera WIP, indice ou branch.
[CmdletBinding()]
param(
    [string]$Workspace = '',
    [switch]$Yes,
    [switch]$WithMT5
)
$ErrorActionPreference = 'Stop'
$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
function Invoke-Checked([string]$Exe, [string[]]$Arguments) {
    # Windows PowerShell 5.1 trata stderr nativo como erro mesmo com exit0.
    $ErrorActionPreference = 'Continue'
    & $Exe @Arguments | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Falhou: $Exe (codigo $LASTEXITCODE). Estado preservado; corrija e repita." }
}
function Get-FullPath([string]$Path) {
    # Aceita unidade, UNC (\\servidor\...), espacos e acentos; remove barra final.
    return [IO.Path]::GetFullPath($Path).TrimEnd('\', '/')
}
function Find-WindowsPython([string]$App) {
    # Mesmo criterio de mt5_extracao/bootstrap/paths.py e run.bat.
    $dedicated = Join-Path $App '.venv-windows\Scripts\python.exe'
    if (Test-Path -LiteralPath $dedicated) { return $dedicated }
    $primary = Join-Path $App '.venv\Scripts\python.exe'
    if ((Test-Path -LiteralPath $primary) -and -not (Test-Path -LiteralPath (Join-Path $App '.venv\bin'))) { return $primary }
    return $null
}

# Composicao que contem este wrapper (WORKSPACE\repos\app\bootstrap-dev.ps1), se houver.
$Own = $null
if ($PSScriptRoot -and (Split-Path -Leaf $PSScriptRoot) -eq 'app' -and
        (Split-Path -Leaf (Split-Path -Parent $PSScriptRoot)) -eq 'repos') {
    $Own = Get-FullPath (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))
}
if (-not $Workspace) {
    $Workspace = if ($Own) { $Own } else { Join-Path $HOME 'Projects\MT5X' }
}
$Workspace = Get-FullPath $Workspace
if ($Own -and $Workspace -ne $Own) {
    throw "Este wrapper pertence a composicao $Own. Rode-o sem -Workspace ou com essa pasta; nenhuma outra Workspace sera criada."
}
Write-Host "MT5X: um ambiente Python por sistema, tres repositorios, pasta $Workspace"
if (-not $Yes) {
    Write-Host 'Use -Yes para preparar; -WithMT5 tambem instala a plataforma nativa.'
    exit 0
}
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw 'Instale Git ou o App Installer/winget da Microsoft e repita. Nenhum checkout sera apagado.'
    }
    Invoke-Checked 'winget' @('install','--id','Git.Git','--exact','--source','winget','--accept-package-agreements','--accept-source-agreements')
    $env:PATH = [Environment]::GetEnvironmentVariable('PATH','Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH','User')
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'Reabra o PowerShell para carregar o Git e repita.' }
}
$App = Join-Path $Workspace 'repos\app'
if (-not (Test-Path -LiteralPath $App)) {
    # Somente host novo: a pasta app ainda nao existe.
    New-Item -ItemType Directory -Force (Join-Path $Workspace 'repos') | Out-Null
    Invoke-Checked 'git' @('clone','--config','core.autocrlf=false','--branch','codex/novo-host-20261024','https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git',$App)
}
$ErrorActionPreference = 'Continue'
$Top = & git -C $App rev-parse --show-toplevel 2>$null
$TopCode = $LASTEXITCODE
$Origin = & git -C $App remote get-url origin 2>$null
$OriginCode = $LASTEXITCODE
$Dirty = @(& git -C $App status --porcelain 2>$null)
$DirtyCode = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
if ($TopCode -ne 0 -or (Get-FullPath $Top) -ne (Get-FullPath $App)) {
    throw "$App nao e a raiz do clone do App. Nada foi alterado; corrija a composicao registrada."
}
if ($OriginCode -ne 0 -or $Origin -notmatch '^https://github\.com/Misael-art/MISA_MT5-Extra-o-de-Dados(\.git)?$') {
    throw "Origem inesperada em $App. Nada foi alterado; confira o registro da composicao."
}
if ($DirtyCode -ne 0) { throw "git status falhou em $App; nada foi alterado." }
if ($Dirty.Count -gt 0) {
    Write-Host "App com $($Dirty.Count) alteracoes locais (WIP): preservadas sem reset, clean, stage ou troca de branch."
}
$Setup = Join-Path $App 'scripts\setup_development.py'
if (-not (Test-Path -LiteralPath $Setup)) {
    throw 'Checkout sem scripts\setup_development.py. Atualize a branch registrada no proprio checkout, preservando o WIP.'
}
$Python = Find-WindowsPython $App
if (-not $Python -or $WithMT5) {
    $Installer = Join-Path $App 'scripts\install\windows.ps1'
    $InstallArgs = @('-NoProfile','-ExecutionPolicy','Bypass','-File',$Installer,'-Yes','-NoShortcut')
    if (-not $WithMT5) { $InstallArgs += '-NoMT5' }
    Invoke-Checked 'powershell' $InstallArgs
    $Python = Find-WindowsPython $App
    if (-not $Python) { throw 'O instalador terminou sem ambiente Windows do projeto; veja logs\install-windows.log.' }
}
if (-not (Get-Command 'g++' -ErrorAction SilentlyContinue)) {
    $Packages = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
    $Compiler = Get-ChildItem -LiteralPath $Packages -Directory -Filter 'BrechtSanders.WinLibs.POSIX.UCRT_*' -ErrorAction SilentlyContinue |
        ForEach-Object { Get-ChildItem -LiteralPath $_.FullName -Filter 'g++.exe' -File -Recurse } | Select-Object -First 1
    if ($Compiler) { $env:PATH = $Compiler.DirectoryName + ';' + $env:PATH }
}
if (-not (Get-Command 'g++' -ErrorAction SilentlyContinue)) {
    # Pacote WinLibs tem centenas de MB: download acima de 70 MB e preparado para o usuario.
    Write-Host ''
    Write-Host 'Toolchain C++ (g++) ausente. O pacote WinLibs excede 70 MB e nao e baixado automaticamente.'
    Write-Host 'Instale-o (somente usuario atual) e rode este wrapper de novo:'
    Write-Host '  winget install --id BrechtSanders.WinLibs.POSIX.UCRT --exact --source winget --scope user'
    Write-Host "Ambiente Python ja preparado: $Python"
    exit 3
}
Invoke-Checked $Python @($Setup,'--workspace',$Workspace)
