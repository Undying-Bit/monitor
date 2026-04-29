@echo off
setlocal

cd /d "%~dp0"
echo Running: .\.venv\Scripts\python.exe main.py --telegram-catchup-last-days 7 %*
echo.
.\.venv\Scripts\python.exe main.py --telegram-catchup-last-days 7 %*
echo.
echo Exit code: %ERRORLEVEL%
pause
