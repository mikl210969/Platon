"""
Тест исполнения: Паспорт + OrderManager + PositionMonitor.
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from core.config import Config
from core.event_bus import EventBus
from core.logger import setup_logger
from adapters.bybit_rest import BybitRestClient
from execution.passport import TradePassport
from execution.order_manager import OrderManager
from execution.position_monitor import PositionMonitor

async def main():
    logger = setup_logger("plato", "test_execution.log", "INFO")
    logger.info("=== НАЧАЛО ТЕСТА ИСПОЛНЕНИЯ ===")
    
    config = Config("config.yaml")
    bus = EventBus()
    
    use_demo = config.get("bybit.use_demo")
    rest_url = "https://api-demo.bybit.com" if use_demo else "https://api-testnet.bybit.com"
    
    rest = BybitRestClient(
        config.get("bybit.api_key"),
        config.get("bybit.api_secret"),
        rest_url
    )
    
    om = OrderManager(rest, category="linear")
    monitor = PositionMonitor(bus, om)
    
    # 1. Создаем тестовый паспорт
    logger.info("Создание тестового паспорта...")
    passport = TradePassport(
        symbol="SOLUSDT",
        side="Buy",
        qty=0.1,
        entry_price=100.0,
        tp1_price=101.0,
        tp2_price=102.0,
        sl_price=99.0
    )
    
    # 2. Имитируем получение сигнала извне
    await bus.publish("EXEC:NEW_TRADE", passport)
    
    # 3. Размещаем входной ордер (по текущей рыночной цене минус 5%, чтобы он не исполнился сразу, или по рынку)
    # Для теста возьмем цену сильно ниже рынка, чтобы ордер просто повис в стакане
    ticker = await rest.get_ticker("linear", "SOLUSDT")
    current_price = float(ticker['list'][0]['lastPrice'])
    test_price = str(round(current_price * 0.9, 2)) # На 10% ниже рынка
    
    logger.info(f"Размещение входа по цене {test_price} (рынок: {current_price})...")
    success = await om.place_entry_order(passport, test_price)
    
    if success:
        logger.info("✅ Входной ордер успешно размещен. Проверьте веб-интерфейс Bybit.")
        logger.info("⏳ Жду 5 секунд, затем отменяю ордер для чистоты...")
        await asyncio.sleep(5)
        
        # Отменяем ордер, чтобы не висел
        if passport.entry_order_id:
            await rest.cancel_order("linear", passport.symbol, passport.entry_order_id)
            logger.info("✅ Тестовый ордер отменен.")
    else:
        logger.error("❌ Не удалось разместить тестовый ордер.")
        
    await rest.close()
    logger.info("=== ТЕСТ ИСПОЛНЕНИЯ ЗАВЕРШЕН ===")

if __name__ == "__main__":
    asyncio.run(main())