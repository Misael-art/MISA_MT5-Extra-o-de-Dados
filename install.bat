@echo off
rem Instalacao automatica no Windows: basta dar duplo clique neste arquivo.
rem Opcoes: install.bat -Yes -NoMT5 -NoShortcut -MT5Path "C:\Program Files\Sua Corretora MT5"
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\install\windows.ps1" %*
set "RC=%ERRORLEVEL%"
echo.
pause
exit /b %RC%
