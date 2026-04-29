@echo off
setlocal EnableExtensions

rem Resolve source as one level above this script directory (...\Code)
for %%I in ("%~dp0..") do set "SRC=%%~fI"
set "DST=\\192.168.68.115\Gavin\Code"

echo Source: "%SRC%"
echo Target: "%DST%"
echo.

robocopy "%SRC%" "%DST%" /MIR /XO /FFT /COPY:DAT /DCOPY:DAT /R:2 /W:3 /MT:4
set "RC=%ERRORLEVEL%"

rem Robocopy: 0-7 = success states, 8+ = failure
if %RC% LSS 8 (
    echo.
    echo Sync completed successfully. Robocopy exit code: %RC%
    exit /b 0
) else (
    echo.
    echo Sync failed. Robocopy exit code: %RC%
    exit /b %RC%
)
