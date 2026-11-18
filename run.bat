@echo off
rem Inicia o MT5 Extracao (use depois de install.bat).
rem pushd aceita pasta local, com espacos/acentos e UNC (\\servidor\...): mapeia uma unidade temporaria.
setlocal
pushd "%~dp0" || (
    echo Nao foi possivel abrir a pasta do projeto: %~dp0
    exit /b 1
)
rem Mesmo criterio de mt5_extracao/bootstrap/paths.py: .venv-windows quando o .venv e de outro sistema.
set "VENV_PY="
if exist ".venv-windows\Scripts\python.exe" set "VENV_PY=.venv-windows\Scripts\python.exe"
if not defined VENV_PY if exist ".venv\Scripts\python.exe" if not exist ".venv\bin\" set "VENV_PY=.venv\Scripts\python.exe"
if not defined VENV_PY (
    echo O ambiente Windows ainda nao foi instalado. Execute primeiro: install.bat
    popd
    pause
    exit /b 1
)
"%VENV_PY%" -I -c "import sys; sys.path.insert(0, '.'); from mt5_extracao.bootstrap import paths; sys.exit(0 if paths.config_path('.').is_file() else 1)"
if errorlevel 1 "%VENV_PY%" -m mt5_extracao.bootstrap configure
rem run.bat --cli <comando> ...  -> linha de comando (mt5x)
if /I "%~1"=="--cli" (
    shift
    goto :cli
)
"%VENV_PY%" app.py %*
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
    echo.
    echo O aplicativo terminou com erro. Veja a pasta logs\ ou rode:
    echo   "%VENV_PY%" -m mt5_extracao.bootstrap doctor
    pause
)
popd
exit /b %RC%

:cli
set "CLI_ARGS="
:collect
if "%~1"=="" goto :runcli
set CLI_ARGS=%CLI_ARGS% %1
shift
goto :collect
:runcli
rem Consultas locais (mt5_extracao/launch_policy.py) nao abrem o terminal MT5, como no run.sh.
"%VENV_PY%" -m mt5_extracao.launch_policy %CLI_ARGS% >nul 2>nul
if not errorlevel 1 set "MT5X_LOCAL_ONLY=1"
"%VENV_PY%" -m mt5_extracao.cli %CLI_ARGS%
set "RC=%ERRORLEVEL%"
popd
exit /b %RC%
