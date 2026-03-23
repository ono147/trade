@echo off
cd /d "%~dp0"
call venv\Scripts\activate.bat
set PYTHONIOENCODING=utf-8
python virtual_bot.py >> virtual_bot.log 2>&1
