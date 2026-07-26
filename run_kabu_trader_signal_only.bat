@echo off
chcp 65001 >nul 2>nul
cd /d "%~dp0"
echo [%date% %time%] kabu_trader start (signal-only) >> "%~dp0kabu_trader_scheduler.log"
"C:\Users\phabc\AppData\Local\Programs\Python\Python311\python.exe" -u "%~dp0kabu_trader.py" --production --signal-only >> "%~dp0kabu_trader_scheduler.log" 2>&1
echo [%date% %time%] kabu_trader exit code=%ERRORLEVEL% >> "%~dp0kabu_trader_scheduler.log"
