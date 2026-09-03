@echo off
title Smart Hybrid Big Data Pipeline

cd /d "%~dp0"

echo.
echo ==========================================
echo        SMART BIG DATA PIPELINE
echo ==========================================
echo.

echo [1] Activating Virtual Environment...
call "%~dp0.venv\Scripts\activate.bat"

if errorlevel 1 (
    echo.
    echo ERROR: Virtual environment could not be activated.
    pause
    exit /b 1
)

echo [OK] Virtual Environment Activated
echo.

set "CSV_FILE="

for %%F in ("%~dp0data\*.csv") do (
    if not defined CSV_FILE set "CSV_FILE=%%~fF"
)

if not defined CSV_FILE (
    echo ERROR: No CSV file found inside data folder.
    echo.
    echo Put your CSV file inside:
    echo %~dp0data
    pause
    exit /b 1
)

echo [2] Dataset Found:
echo     %CSV_FILE%
echo.

echo [3] Starting Pipeline...
echo ==========================================
echo.

python "%~dp0src\main.py" --file "%CSV_FILE%"

echo.
echo ==========================================
echo        PIPELINE FINISHED
echo ==========================================
echo.

pause