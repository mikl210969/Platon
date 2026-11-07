@echo off
title Binance Data Recorder - Running...
color 0B

echo ============================================================
echo  STARTING BINANCE/BYBIT DATA RECORDER
echo  Script: background_recorder_v2.py
echo  Time: %DATE% %TIME%
echo ============================================================
echo.

:: 1. Переходим в корень проекта Platon
cd /C C:\Users\user\PLATO31\platform\Platon

:: 2. Активируем виртуальное окружение Python
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
) else (
    echo [ERROR] Virtual environment 'venv' not found in D:\platform\Platon
    pause
    exit /b 1
)

:: 3. Запускаем скрипт рекордера
:: Используем pythonw.exe если хочешь скрыть консоль полностью, 
:: но лучше оставить python.exe, чтобы видеть ошибки, если они будут.
echo Starting recorder... Check console for logs.
python tools\background_recorder_v2.py

:: 4. Если скрипт завершился (упал или был остановлен), окно останется открытым
echo.
echo ============================================================
echo  RECORDER STOPPED OR CRASHED.
echo  Press any key to close this window.
echo ============================================================
pause >nul