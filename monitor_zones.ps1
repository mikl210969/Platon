# Monitor Zones & Delta -实时监控脚本
# Показывает создание зон, откаты и накопление дельты в реальном времени

$autopsyFile = "logs\strategy_autopsy.jsonl"
$zonesFile = "logs\active_zones_SOLUSDT.json"

Write-Host "🔍 Мониторинг зон и дельты запущен..." -ForegroundColor Cyan
Write-Host "Нажми Ctrl+C для остановки`n" -ForegroundColor Yellow

# Отслеживаем изменения в файле аутопсии
$lastPosition = 0

while ($true) {
    if (Test-Path $autopsyFile) {
        $file = Get-Item $autopsyFile
        if ($file.Length -gt $lastPosition) {
            $content = Get-Content $autopsyFile -Tail 1
            $json = $content | ConvertFrom-Json
            
            # Цветовая кодировка событий
            switch ($json.event) {
                "ZONE_DETECTED_MEDIAN_BASED" {
                    Write-Host "🎯 [ZONE DETECTED] $($json.zone_id) | $($json.side) @ $($json.price) | Vol: $($json.volume) > Req: $($json.required_vol)" -ForegroundColor Green
                }
                "ZONE_DEPARTED" {
                    Write-Host "📉 [ZONE DEPARTED] $($json.zone_id) | Цена откатила до: $($json.min_price)" -ForegroundColor Yellow
                }
                "SIGNAL_GENERATED" {
                    Write-Host "🚀 [SIGNAL] $($json.zone_id) | $($json.side) @ $($json.entry) | Delta: $($json.delta_sum) | SL: $($json.sl) | TP1: $($json.tp1) | TP2: $($json.tp2)" -ForegroundColor Magenta
                }
                "SIGNAL_REJECTED" {
                    Write-Host "❌ [REJECTED] $($json.zone_id) | Причина: $($json.reason)" -ForegroundColor Red
                }
                "ZONE_INVALIDATED" {
                    Write-Host "💀 [INVALIDATED] $($json.zone_id) | Причина: $($json.reason)" -ForegroundColor DarkRed
                }
                default {
                    Write-Host "ℹ️  [$($json.event)] $($json.zone_id)" -ForegroundColor Gray
                }
            }
            
            $lastPosition = $file.Length
        }
    }
    
    Start-Sleep -Milliseconds 500
}