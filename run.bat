@echo off
setlocal

cd /d "%~dp0"
echo Running: .\.venv\Scripts\python.exe main.py
echo.
.\.venv\Scripts\python.exe main.py
echo.
echo Exit code: %ERRORLEVEL%
pause