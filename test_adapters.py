"""
Тест адаптеров: REST и Private WS.
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корень в путь, чтобы импортировать core и adapters
sys.path.append(str(Path(__file__).parent))

from core.config import Config
from core.event_bus import EventBus
from adapters.bybit_rest import BybitRestClient
from adapters.bybit_private_ws import BybitPrivateWSClient

async def main():
    print("="*60)
    print("ТЕСТ АДАПТЕРОВ (REST + UDS)")
    print("="*60)
    
    config = Config("config.yaml")
    bus = EventBus()
    
    # Настройка URL
    use_demo = config.get("bybit.use_demo")
    if use_demo:
        rest_url = "https://api-demo.bybit.com"
        ws_url = "wss://stream-demo.bybit.com/v5/private"
    else:
        rest_url = "https://api-testnet.bybit.com"
        ws_url = "wss://stream-testnet.bybit.com/v5/private"

    api_key = config.get("bybit.api_key")
    api_secret = config.get("bybit.api_secret")

    # 1. Тест REST
    print("\n[1/3] Тест REST клиента...")
    rest = BybitRestClient(api_key, api_secret, rest_url)
    try:
        balance = await rest.get_balance()
        equity = balance['list'][0]['totalEquity']
        print(f"✅ REST работает. Баланс: {equity} USDT")
        
        ticker = await rest.get_ticker("linear", "SOLUSDT")
        price = ticker['list'][0]['lastPrice']
        print(f"✅ Цена SOLUSDT: {price}")
    except Exception as e:
        print(f"❌ REST ошибка: {e}")
        return

    # 2. Тест UDS (Private WS)
    print("\n[2/3] Тест UDS (Private WS)...")
    
    # Подписчик на события UDS
    async def on_order_update(data):
        print(f" [UDS] Получено обновление ордера: {len(data)} записей")
        
    bus.subscribe("UDS:ORDER_UPDATE", on_order_update)
    
    uds = BybitPrivateWSClient(api_key, api_secret, ws_url, bus)
    
    # Запускаем UDS в фоне
    uds_task = asyncio.create_task(uds.connect())
    
    # Ждем 10 секунд, чтобы поймать хотя бы одно событие (например, heartbeat или initial snapshot)
    print("⏳ Ожидание событий от биржи (10 сек)...")
    await asyncio.sleep(10)
    
    # 3. Остановка
    print("\n[3/3] Остановка...")
    await uds.stop()
    await rest.close()
    
    print("\n✅ ТЕСТ АДАПТЕРОВ ЗАВЕРШЕН")

if __name__ == "__main__":
    asyncio.run(main())