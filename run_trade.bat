@echo off
setlocal
cd /d "%~dp0"
set "KABU_MODE=production"
set "KABU_CONSOLE_LOG=1"
call "%~dp0run_kabu_trader.bat"
exit /b %ERRORLEVEL%
