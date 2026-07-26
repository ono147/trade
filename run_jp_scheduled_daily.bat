@echo off
chcp 65001 >nul 2>nul
cd /d "%~dp0"
echo [%date% %time%] jp_scheduled_param_search daily start >> "%~dp0jp_scheduled_param_search.log"
"C:\Users\phabc\AppData\Local\Programs\Python\Python311\python.exe" -u "%~dp0jp_scheduled_param_search.py" --mode daily --apply-kabu-config >> "%~dp0jp_scheduled_param_search.log" 2>&1
echo [%date% %time%] jp_scheduled_param_search exit code=%ERRORLEVEL% >> "%~dp0jp_scheduled_param_search.log"
