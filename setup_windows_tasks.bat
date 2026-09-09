@echo off
setlocal
chcp 65001 >nul 2>nul
cd /d "%~dp0"

rem =========================================================
rem Windows Task Scheduler auto-setup for trading operations
rem Run once (as the same user that will trade).
rem =========================================================

set "BASE=%~dp0"
set "TASK_PREFIX=TradeBot"

set "TASK_DAILY=%TASK_PREFIX%\\JP Param Search Daily"
set "TASK_SIGNAL=%TASK_PREFIX%\\Kabu Signal Check"
set "TASK_LIVE=%TASK_PREFIX%\\Kabu Live Trader"
set "TASK_WEEKLY=%TASK_PREFIX%\\JP Param Search Weekly"

set "CMD_DAILY=\"%BASE%run_jp_scheduled_daily.bat\""
set "CMD_SIGNAL=\"%BASE%run_trade_signal_only.bat\""
set "CMD_LIVE=\"%BASE%run_trade.bat\""
set "CMD_WEEKLY=\"%BASE%run_jp_scheduled_weekly.bat\""

echo.
echo [1/4] Register daily parameter search (weekdays 06:00)
schtasks /Create /TN "%TASK_DAILY%" /TR %CMD_DAILY% /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 06:00 /F >nul
if errorlevel 1 goto :error

echo [2/4] Register signal-only health check (weekdays 08:55)
schtasks /Create /TN "%TASK_SIGNAL%" /TR %CMD_SIGNAL% /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 08:55 /F >nul
if errorlevel 1 goto :error

echo [3/4] Register live trading start (weekdays 09:00)
schtasks /Create /TN "%TASK_LIVE%" /TR %CMD_LIVE% /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 09:00 /F >nul
if errorlevel 1 goto :error

echo [4/4] Register weekly wide search (Saturday 09:00)
schtasks /Create /TN "%TASK_WEEKLY%" /TR %CMD_WEEKLY% /SC WEEKLY /D SAT /ST 09:00 /F >nul
if errorlevel 1 goto :error

echo.
echo Setup completed successfully.
echo.
echo Created tasks:
echo   %TASK_DAILY%
echo   %TASK_SIGNAL%
echo   %TASK_LIVE%
echo   %TASK_WEEKLY%
echo.
echo You can verify with:
echo   schtasks /Query /TN "%TASK_PREFIX%\\*" /FO LIST /V
exit /b 0

:error
echo.
echo ERROR: failed to create one or more scheduled tasks.
echo Run this batch in a Command Prompt with enough permissions.
exit /b 1
