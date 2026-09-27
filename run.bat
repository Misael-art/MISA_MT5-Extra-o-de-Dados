@echo off
rem Inicia o MT5 Extracao (use depois de install.bat).
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo O ambiente ainda nao foi instalado. Execute primeiro: install.bat
    pause
    exit /b 1
)
if not exist "config\config.ini" ".venv\Scripts\python.exe" -m mt5_extracao.bootstrap configure
rem run.bat --cli <comando> ...  -> linha de comando (mt5x)
if /I "%~1"=="--cli" (
    shift
    goto :cli
)
".venv\Scripts\python.exe" app.py %*
if errorlevel 1 (
    echo.
    echo O aplicativo terminou com erro. Veja a pasta logs\ ou rode:
    echo   .venv\Scripts\python -m mt5_extracao.bootstrap doctor
    pause
)
exit /b %ERRORLEVEL%

:cli
set "CLI_ARGS="
:collect
if "%~1"=="" goto :runcli
set CLI_ARGS=%CLI_ARGS% %1
shift
goto :collect
:runcli
".venv\Scripts\python.exe" -m mt5_extracao.cli %CLI_ARGS%
exit /b %ERRORLEVEL%
