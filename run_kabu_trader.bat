@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>nul
cd /d "%~dp0"

rem =========================================================
rem Kabu live trading launcher (Windows Task Scheduler ready)
rem - Python/Config existence check
rem - Config JSON syntax check
rem - kabu station API port readiness check
rem - Duplicate process guard
rem - UTF-8 log append
rem =========================================================

set "PYTHON_EXE=%~dp0venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=%LOCALAPPDATA%\Programs\Python\Python311\python.exe"

set "CONFIG_PATH=%~dp0kabu_config.json"
set "MAIN_LOG=%~dp0kabu_trader_scheduler.log"
set "CHECK_LOG=%~dp0kabu_trader_precheck.log"
set "KABU_HOST=127.0.0.1"
set "KABU_PORT=18080"
set "PORT_RETRIES=20"
set "PORT_WAIT_SEC=3"

set "MODE=%KABU_MODE%"
if "%MODE%"=="" set "MODE=production"

echo [%date% %time%] precheck start >> "%CHECK_LOG%"

if not exist "%PYTHON_EXE%" (
  echo [%date% %time%] ERROR python not found: "%PYTHON_EXE%" >> "%CHECK_LOG%"
  exit /b 2
)

if not exist "%CONFIG_PATH%" (
  echo [%date% %time%] ERROR config not found: "%CONFIG_PATH%" >> "%CHECK_LOG%"
  exit /b 3
)

rem JSON syntax validation
"%PYTHON_EXE%" -c "import json,sys; json.load(open(r'%CONFIG_PATH%',encoding='utf-8')); print('config-ok')" >> "%CHECK_LOG%" 2>&1
if errorlevel 1 (
  echo [%date% %time%] ERROR invalid JSON in config >> "%CHECK_LOG%"
  exit /b 4
)

rem Duplicate run guard: skip if kabu_trader.py production is already running
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$p = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'kabu_trader.py' -and $_.CommandLine -match '--production' }; if($p){ exit 0 } else { exit 1 }" >nul 2>&1
if not errorlevel 1 (
  echo [%date% %time%] WARN already running, skip start >> "%CHECK_LOG%"
  exit /b 0
)

call :wait_for_kabu_port
if errorlevel 1 (
  echo [%date% %time%] ERROR kabu API port check failed (%KABU_HOST%:%KABU_PORT%) >> "%CHECK_LOG%"
  exit /b 5
)

echo [%date% %time%] kabu_trader start mode=%MODE% >> "%MAIN_LOG%"
if /I "%MODE%"=="signal-only" (
  "%PYTHON_EXE%" -u "%~dp0kabu_trader.py" --production --signal-only --config "%CONFIG_PATH%" >> "%MAIN_LOG%" 2>&1
) else if /I "%MODE%"=="dry-run" (
  "%PYTHON_EXE%" -u "%~dp0kabu_trader.py" --dry-run --config "%CONFIG_PATH%" >> "%MAIN_LOG%" 2>&1
) else (
  "%PYTHON_EXE%" -u "%~dp0kabu_trader.py" --production --config "%CONFIG_PATH%" >> "%MAIN_LOG%" 2>&1
)
set "EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] kabu_trader exit code=%EXIT_CODE% >> "%MAIN_LOG%"
exit /b %EXIT_CODE%

:wait_for_kabu_port
set /a TRY=1
:port_loop
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$c = New-Object Net.Sockets.TcpClient; try { $c.Connect('%KABU_HOST%', %KABU_PORT%); $c.Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 (
  echo [%date% %time%] port ready: %KABU_HOST%:%KABU_PORT% >> "%CHECK_LOG%"
  exit /b 0
)

if %TRY% GEQ %PORT_RETRIES% exit /b 1
echo [%date% %time%] waiting port (%TRY%/%PORT_RETRIES%) %KABU_HOST%:%KABU_PORT% >> "%CHECK_LOG%"
timeout /t %PORT_WAIT_SEC% /nobreak >nul
set /a TRY+=1
goto :port_loop
