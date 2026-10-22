"""
Тест ядра: EventBus, Config, Logger.
"""

import asyncio
from core.event_bus import EventBus
from core.config import Config
from core.logger import setup_logger


async def test_event_bus():
    """Тест шины событий."""
    print("\n" + "="*60)
    print("ТЕСТ EVENT BUS")
    print("="*60)
    
    bus = EventBus()
    
    # Подписчик
    async def on_test_event(data):
        print(f"✅ Получено событие: {data}")
    
    bus.subscribe("TEST_EVENT", on_test_event)
    
    # Публикация
    await bus.publish("TEST_EVENT", {"message": "Привет от ядра!"})
    
    print(f"✅ История событий: {len(bus.get_log())} записей")


def test_config():
    """Тест загрузки конфига."""
    print("\n" + "="*60)
    print("ТЕСТ CONFIG")
    print("="*60)
    
    try:
        config = Config("config.yaml")
        
        api_key = config.get("bybit.api_key")
        symbol = config.get("trading.symbol")
        risk = config.get("trading.risk.base_risk_usdt")
        
        print(f"✅ API Key: {api_key}")
        print(f"✅ Symbol: {symbol}")
        print(f"✅ Risk: {risk} USDT")
        
    except Exception as e:
        print(f"❌ Ошибка загрузки конфига: {e}")


def test_logger():
    """Тест логгера."""
    print("\n" + "="*60)
    print("ТЕСТ LOGGER")
    print("="*60)
    
    logger = setup_logger("test", "test.log", "INFO")
    logger.info("Это тестовое сообщение")
    logger.warning("Это предупреждение")
    
    print("✅ Логгер настроен. Проверь файл test.log")


async def main():
    print("\n" + "="*60)
    print("ТЕСТ ЯДРА PLATO V2")
    print("="*60)
    
    test_config()
    test_logger()
    await test_event_bus()
    
    print("\n" + "="*60)
    print("ТЕСТ ЯДРА ЗАВЕРШЕН")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())