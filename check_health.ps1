Write-Host "`n=== PLATO HEALTH CHECK ===" -ForegroundColor Cyan
Write-Host "Время: $(Get-Date)`n" -ForegroundColor Yellow

# 1. Ошибки
$errors = Select-String -Path "plato.log" -Pattern "ERROR" | Measure-Object
Write-Host "❌ Ошибки в логе: $($errors.Count)" -ForegroundColor $(if ($errors.Count -gt 10) { "Red" } else { "Green" })

# 2. ERROR_STATE
$errorStates = Select-String -Path "plato.log" -Pattern "-> ERROR_STATE" | Measure-Object
Write-Host "🚫 ERROR_STATE: $($errorStates.Count)" -ForegroundColor $(if ($errorStates.Count -gt 5) { "Red" } else { "Green" })

# 3. Активные зоны
$zones = Get-ChildItem "logs\active_zones_*.json" -ErrorAction SilentlyContinue | Measure-Object
Write-Host "📊 Активных файлов зон: $($zones.Count)" -ForegroundColor Cyan

# 4. Размер autopsy
$autopsyLines = Get-Content "logs\strategy_autopsy.jsonl" -ErrorAction SilentlyContinue | Measure-Object
Write-Host "📝 Записей в autopsy: $($autopsyLines.Count)" -ForegroundColor $(if ($autopsyLines.Count -gt 1000) { "Yellow" } else { "Green" })

# 5. RECON работает
$recon = Select-String -Path "plato.log" -Pattern "RECON:PERIODIC" | Measure-Object
Write-Host "🛰️ RECON сверок: $($recon.Count)" -ForegroundColor Green

Write-Host "`n=== END CHECK ===`n" -ForegroundColor Cyan