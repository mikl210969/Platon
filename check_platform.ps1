# Пакет команд для анализа состояния платформы Platon V2

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " АНАЛИЗ ПЛАТФОРМЫ PLATON V2" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 1. Проверка, запущена ли платформа
Write-Host "`n[1/6] Проверка процесса Python..." -ForegroundColor Yellow
$pythonProcess = Get-Process python -ErrorAction SilentlyContinue
if ($pythonProcess) {
    Write-Host "✅ Платформа работает (PID: $($pythonProcess.Id))" -ForegroundColor Green
} else {
    Write-Host "❌ Платформа не запущена" -ForegroundColor Red
}

# 2. Последние логи
Write-Host "`n[2/6] Последние события (лог)..." -ForegroundColor Yellow
if (Test-Path "platform_run.log") {
    Get-Content platform_run.log -Tail 10 -Encoding UTF8 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
} else {
    Write-Host "  Лог не найден" -ForegroundColor Red
}

# 3. Статистика паспортов
Write-Host "`n[3/6] Статистика паспортов..." -ForegroundColor Yellow
if (Test-Path "data\passports\*.json") {
    $passports = Get-ChildItem data\passports\*.json
    Write-Host "  Всего паспортов: $($passports.Count)" -ForegroundColor Green
    
    # Группировка по статусам
    $statusGroups = $passports | ForEach-Object { 
        $json = Get-Content $_.FullName | ConvertFrom-Json
        $json.status
    } | Group-Object
    
    Write-Host "  По статусам:" -ForegroundColor Cyan
    $statusGroups | ForEach-Object { Write-Host "    $($_.Name): $($_.Count)" }
    
    # Общий P&L
    $totalPnL = ($passports | ForEach-Object { 
        $json = Get-Content $_.FullName | ConvertFrom-Json
        $json.net_pnl
    } | Measure-Object -Sum).Sum
    
    Write-Host "  Общий P&L: $([math]::Round($totalPnL, 2)) USDT" -ForegroundColor $(if ($totalPnL -ge 0) { "Green" } else { "Red" })
} else {
    Write-Host "  Паспорта не найдены" -ForegroundColor Red
}

# 4. Проверка позиции и ордеров на бирже
Write-Host "`n[4/6] Проверка позиции и ордеров..." -ForegroundColor Yellow
Write-Host "  Открой браузер: https://demo-trading.bybit.com/" -ForegroundColor Cyan
Write-Host "  Проверь:" -ForegroundColor Cyan
Write-Host "    - Вкладка 'Позиции' (текущая позиция)" -ForegroundColor Gray
Write-Host "    - Вкладка 'Открытые ордера' (TP1, TP2)" -ForegroundColor Gray
Write-Host "    - Вкладка 'Условные' (SL)" -ForegroundColor Gray

# 5. Метрики платформы
Write-Host "`n[5/6] Метрики платформы..." -ForegroundColor Yellow
try {
    python monitor.py 2>&1 | Select-Object -Skip 2 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
} catch {
    Write-Host "  Ошибка запуска monitor.py: $_" -ForegroundColor Red
}

# 6. Размер лог-файла и паспортов
Write-Host "`n[6/6] Использование диска..." -ForegroundColor Yellow
if (Test-Path "platform_run.log") {
    $logSize = (Get-Item platform_run.log).Length / 1MB
    Write-Host "  Лог: $([math]::Round($logSize, 2)) MB" -ForegroundColor Green
} else {
    Write-Host "  Лог не найден" -ForegroundColor Red
}

if (Test-Path "data\passports") {
    $passportCount = (Get-ChildItem data\passports\*.json).Count
    $passportSize = (Get-ChildItem data\passports\*.json | Measure-Object Length -Sum).Sum / 1MB
    Write-Host "  Паспорта: $passportCount файлов, $([math]::Round($passportSize, 2)) MB" -ForegroundColor Green
} else {
    Write-Host "  Папка passports не найдена" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " АНАЛИЗ ЗАВЕРШЕН" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan