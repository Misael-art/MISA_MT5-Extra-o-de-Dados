@echo off
rem Inicia o MT5 Extracao (use depois de install.bat).
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
    echo O ambiente ainda nao foi instalado. Execute primeiro: install.bat
    pause
    exit /b 1
)
if not exist "config\config.ini" ".venv\Scripts\python.exe" -m mt5_extracao.bootstrap configure
".venv\Scripts\python.exe" app.py %*
if errorlevel 1 (
    echo.
    echo O aplicativo terminou com erro. Veja a pasta logs\ ou rode:
    echo   .venv\Scripts\python -m mt5_extracao.bootstrap doctor
    pause
)
