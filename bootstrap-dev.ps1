# Entrada independente para Windows PowerShell 5.1 ou PowerShell 7.
[CmdletBinding()]
param(
    [string]$Workspace = (Join-Path $HOME 'Projects\MT5X'),
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
    if ($LASTEXITCODE -ne 0) { throw "Falhou: $Exe (codigo $LASTEXITCODE). Preserve o estado e repita apos corrigir." }
}
Write-Host "MT5X: um ambiente Python, tres repositorios, pasta $Workspace"
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
if (-not (Test-Path $App)) {
    New-Item -ItemType Directory -Force (Join-Path $Workspace 'repos') | Out-Null
    Invoke-Checked 'git' @('clone','--config','core.autocrlf=false','--branch','codex/novo-host-20261024','https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git',$App)
}
$Top = & git -C $App rev-parse --show-toplevel
if ($LASTEXITCODE -ne 0 -or [IO.Path]::GetFullPath($Top) -ne [IO.Path]::GetFullPath($App)) { throw 'A pasta app nao e a raiz do clone esperado.' }
$Origin = & git -C $App remote get-url origin
if ($LASTEXITCODE -ne 0 -or $Origin -notmatch '^https://github\.com/Misael-art/MISA_MT5-Extra-o-de-Dados(\.git)?$') { throw 'Origem diferente; escolha outra Workspace.' }
$Dirty = & git -C $App status --porcelain
if ($LASTEXITCODE -ne 0 -or $Dirty) { throw 'App com alteracoes: preserve o WIP e escolha outra Workspace. Nao use reset/clean.' }
$Setup = Join-Path $App 'scripts\setup_development.py'
if (-not (Test-Path $Setup)) { throw 'Checkout sem wrapper; obtenha a branch codex/novo-host-20261024 em outra pasta.' }
$Python = Join-Path $App '.venv\Scripts\python.exe'
if (-not (Test-Path $Python) -or $WithMT5) {
    $Installer = Join-Path $App 'scripts\install\windows.ps1'
    $InstallArgs = @('-NoProfile','-ExecutionPolicy','Bypass','-File',$Installer,'-Yes')
    if (-not $WithMT5) { $InstallArgs += '-NoMT5' }
    Invoke-Checked 'powershell' $InstallArgs
}
if (-not (Get-Command 'g++' -ErrorAction SilentlyContinue)) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) { throw 'Instale g++ (WinLibs/MinGW) e repita, ou habilite winget.' }
    Invoke-Checked 'winget' @('install','--id','BrechtSanders.WinLibs.POSIX.UCRT','--exact','--source','winget','--scope','user','--accept-package-agreements','--accept-source-agreements')
    $Packages = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
    $Compiler = Get-ChildItem $Packages -Directory -Filter 'BrechtSanders.WinLibs.POSIX.UCRT_*' -ErrorAction SilentlyContinue |
        ForEach-Object { Get-ChildItem $_.FullName -Filter 'g++.exe' -File -Recurse } | Select-Object -First 1
    if ($Compiler) { $env:PATH = $Compiler.DirectoryName + ';' + $env:PATH }
    if (-not (Get-Command 'g++' -ErrorAction SilentlyContinue)) { throw 'Reabra o terminal para carregar o g++ e repita.' }
}
Invoke-Checked $Python @($Setup,'--workspace',$Workspace)
