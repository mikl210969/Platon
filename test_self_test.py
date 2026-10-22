"""
Тест модуля самопроверки.
Запускает SelfTest на 3 минуты (должен пройти 1-2 теста).
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from core.config import Config
from core.event_bus import EventBus
from core.logger import setup_logger
from adapters.bybit_rest import BybitRestClient
from execution.order_manager import OrderManager
from execution.self_test import SelfTest

async def main():
    logger = setup_logger("plato", "test_self_test.log", "INFO")
    logger.info("=== НАЧАЛО ТЕСТА САМОПРОВЕРКИ ===")
    
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
    
    # Создаем SelfTest с интервалом 60 секунд (для быстрого теста)
    self_test = SelfTest(
        event_bus=bus,
        order_manager=om,
        rest_client=rest,
        interval_seconds=60,  # Уменьшено для теста
        test_qty=0.1,
        price_offset_percent=10.0,
        symbol="SOLUSDT",
        category="linear"
    )
    
    # Запускаем SelfTest в фоне
    test_task = asyncio.create_task(self_test.start())
    
    # Ждем 150 секунд (должно пройти 2 теста)
    logger.info("⏳ Ожидание 150 секунд...")
    await asyncio.sleep(150)
    
    # Останавливаем
    await self_test.stop()
    await rest.close()
    
    logger.info("=== ТЕСТ САМОПРОВЕРКИ ЗАВЕРШЕН ===")

if __name__ == "__main__":
    asyncio.run(main())