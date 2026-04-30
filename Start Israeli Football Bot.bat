@echo off
setlocal
title Israeli Football Bot

set "BOT_DIR=F:\OneDrive\Desktop\bottest\israeli-football-bot\israeli-football-bot"

cd /d "%BOT_DIR%" || (
    echo Could not open bot folder:
    echo %BOT_DIR%
    echo.
    pause
    exit /b 1
)

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_CMD=.venv\Scripts\python.exe"
    goto run_bot
)

where py >nul 2>&1
if "%ERRORLEVEL%"=="0" (
    set "PYTHON_CMD=py -3"
    goto run_bot
)

where python >nul 2>&1
if "%ERRORLEVEL%"=="0" (
    set "PYTHON_CMD=python"
    goto run_bot
)

echo Python was not found.
echo Install Python, or add Python to PATH, then run this launcher again.
echo.
pause
exit /b 1

:run_bot
echo Starting Israeli Football Bot...
echo Folder: %CD%
echo Python: %PYTHON_CMD%
echo.

%PYTHON_CMD% bot.py
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%EXIT_CODE%"=="0" (
    echo Bot stopped with error code %EXIT_CODE%.
) else (
    echo Bot stopped.
)
echo.
pause
exit /b %EXIT_CODE%
