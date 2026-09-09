@echo off
setlocal
cd /d "%~dp0"
set "KABU_MODE=production"
call "%~dp0run_kabu_trader.bat"
exit /b %ERRORLEVEL%
