import pytest
from unittest.mock import MagicMock, AsyncMock
from execution.position_monitor import PositionMonitor
from execution.passport import TradePassport, SignalSource
from datetime import datetime, timezone

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_monitor():
    """Создает PositionMonitor с заглушками для тестирования."""
    mock_config = {}
    mock_bus = MagicMock()
    mock_bus.subscribe = MagicMock()  # Игнорируем подписки при инициализации
    
    mock_om = MagicMock()
    mock_storage = MagicMock()
    mock_storage.save = MagicMock()
    
    monitor = PositionMonitor(
        config=mock_config,
        event_bus=mock_bus,
        order_manager=mock_om,
        storage=mock_storage
    )
    return monitor


async def test_zombie_passport_cleanup_on_rejection(setup_monitor):
    """
    Тест проверяет, что паспорт удаляется из active_passports 
    при переходе в статус ORDER_REJECTED.
    """
    monitor = setup_monitor
    
    passport = TradePassport(
        passport_id="PASS_ZOMBIE_TEST_001",
        symbol="LINKUSDT",
        side="LONG",
        entry_price=11.0,
        position_size=10.0,
        created_at=datetime.now(timezone.utc)
    )
    
    # 1. Добавляем паспорт в мониторинг (имитация EXEC:NEW_TRADE)
    monitor._on_new_trade(passport)
    
    # Проверяем, что паспорт добавлен
    assert "PASS_ZOMBIE_TEST_001" in monitor.active_passports
    assert "PASS_ZOMBIE_TEST_001" in monitor.managers

    # 2. Имитируем обработку отклонения ордера
    # (Этот метод мы добавим в position_monitor.py на следующем шаге)
    await monitor._handle_order_rejected(passport.passport_id, "Qty invalid or POSITION_EXISTS")
    
    # 3. ПРОВЕРКА: паспорт должен быть полностью удален
    assert "PASS_ZOMBIE_TEST_001" not in monitor.active_passports, "Паспорт остался в active_passports (ЗОМБИ!)"
    assert "PASS_ZOMBIE_TEST_001" not in monitor.managers, "Менеджер остался в памяти (ЗОМБИ!)"
    
    # Проверяем, что состояние было сохранено
    monitor.storage.save.assert_called()
    
    print("\n✅ ИСПРАВЛЕНИЕ ПОДТВЕРЖДЕНО: Зомби-паспорт корректно удален из памяти при ORDER_REJECTED.")