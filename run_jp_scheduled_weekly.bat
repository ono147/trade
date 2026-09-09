@echo off
setlocal
chcp 65001 >nul 2>nul
cd /d "%~dp0"

set "PYTHON_EXE=%~dp0venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"

set "LOG_PATH=%~dp0jp_scheduled_param_search.log"
echo [%date% %time%] jp_scheduled_param_search weekly start >> "%LOG_PATH%"

if not exist "%PYTHON_EXE%" (
  echo [%date% %time%] ERROR python not found: "%PYTHON_EXE%" >> "%LOG_PATH%"
  exit /b 2
)

"%PYTHON_EXE%" -u "%~dp0jp_scheduled_param_search.py" --mode weekly --apply-kabu-config >> "%LOG_PATH%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] jp_scheduled_param_search exit code=%EXIT_CODE% >> "%LOG_PATH%"
exit /b %EXIT_CODE%
