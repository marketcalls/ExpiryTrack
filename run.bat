@echo off
REM ExpiryTrack Runner for Windows
REM This script sets proper encoding for Windows terminals

chcp 65001 > nul
set PYTHONIOENCODING=utf-8

echo.
echo ExpiryTrack - Expired Contract Data Collector
echo ==============================================
echo.

if "%1"=="" (
    python main.py --help
) else (
    python main.py %*
)