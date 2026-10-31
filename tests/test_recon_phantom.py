import pytest
from unittest.mock import AsyncMock, MagicMock, call
from datetime import datetime, timezone
from execution.recon_engine import ReconciliationEngine
from execution.passport import TradePassport, SignalSource

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_phantom_scenario():
    """Настраивает сценарий: на бирже есть позиция, но активных паспортов нет."""
    
    # Мок REST-клиента
    mock_rest = AsyncMock()
    
    # Позиция на бирже (фантом)
    phantom_position = [
        {"symbol": "LINKUSDT", "size": "10.0", "side": "Buy", "avgPrice": "11.177"}
    ]
    
    # Настраиваем возврат позиции и пустой список ордеров
    mock_rest.get_positions.return_value = phantom_position
    mock_rest.get_open_orders.return_value = []
    
    # Мок OrderManager
    mock_om = AsyncMock()
    mock_om.place_protective_orders = AsyncMock(return_value=True)
    
    # Мок хранилища
    mock_storage = MagicMock()
    mock_storage.save = MagicMock()
    
    # Нет активных паспортов (именно это делает позицию "фантомом")
    def get_active():
        return {}
    
    def get_manager(p):
        return MagicMock()

    return {
        "mock_rest": mock_rest,
        "mock_om": mock_om,
        "mock_storage": mock_storage,
        "get_active": get_active,
        "get_manager": get_manager,
        "phantom_position": phantom_position
    }


async def test_phantom_requires_two_confirmations(setup_phantom_scenario):
    """
    Тест проверяет, что фантом усыновляется только после 2 подтверждений.
    
    Сценарий:
    - Цикл 1: Биржа возвращает позицию, но активных паспортов нет.
      ОЖИДАНИЕ: Фантом НЕ усыновляется (ждём подтверждения).
    - Цикл 2: Биржа снова возвращает ту же позицию.
      ОЖИДАНИЕ: Фантом усыновляется, создаётся паспорт.
    """
    scenario = setup_phantom_scenario
    
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=scenario["mock_rest"],
        storage=scenario["mock_storage"],
        get_active_passports=scenario["get_active"],
        get_manager=scenario["get_manager"],
        order_manager=scenario["mock_om"],
        symbol="LINKUSDT"
    )

    # === ЦИКЛ 1: Позиция появилась впервые ===
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: place_protective_orders НЕ вызван (фантом ещё не усыновлён)
    scenario["mock_om"].place_protective_orders.assert_not_called()
    
    # Проверяем, что счётчик увеличился
    phantom_key = "LINKUSDT_LONG_11.177"
    assert recon._phantom_cycles.get(phantom_key) == 1, "Счётчик фантома должен быть 1 после первого цикла"
    
    print("\n⏳ Цикл 1: Фантом обнаружен, но усыновление отложено (1/2 подтверждения)")

    # === ЦИКЛ 2: Позиция подтверждена ===
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: place_protective_orders вызван (фантом усыновлён)
    scenario["mock_om"].place_protective_orders.assert_called_once()
    
    # Проверяем, что счётчик сброшен после усыновления
    assert phantom_key not in recon._phantom_cycles, "Счётчик должен быть сброшен после усыновления"
    
    print("✅ Цикл 2: Фантом подтверждён и усыновлён")


async def test_phantom_deduplication(setup_phantom_scenario):
    """
    Тест проверяет дедупликацию: если фантом уже усыновлён,
    повторное появление той же позиции не создаёт новый паспорт.
    """
    scenario = setup_phantom_scenario
    
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=scenario["mock_rest"],
        storage=scenario["mock_storage"],
        get_active_passports=scenario["get_active"],
        get_manager=scenario["get_manager"],
        order_manager=scenario["mock_om"],
        symbol="LINKUSDT"
    )

    # Усыновляем фантом (2 цикла)
    await recon.reconcile(reason="PERIODIC")
    await recon.reconcile(reason="PERIODIC")
    
    # Сбрасываем счётчик вызовов
    scenario["mock_om"].place_protective_orders.reset_mock()
    
    # === ЦИКЛ 3: Та же позиция снова ===
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: place_protective_orders НЕ вызван (фантом уже обработан)
    scenario["mock_om"].place_protective_orders.assert_not_called()
    
    print("\n✅ Дедупликация работает: повторный фантом игнорируется")


async def test_phantom_reset_on_position_vanish(setup_phantom_scenario):
    """
    Тест проверяет, что если позиция исчезает до 2-го подтверждения,
    счётчик сбрасывается и фантом не усыновляется.
    """
    scenario = setup_phantom_scenario
    
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=scenario["mock_rest"],
        storage=scenario["mock_storage"],
        get_active_passports=scenario["get_active"],
        get_manager=scenario["get_manager"],
        order_manager=scenario["mock_om"],
        symbol="LINKUSDT"
    )

    # Цикл 1: Позиция есть
    await recon.reconcile(reason="PERIODIC")
    assert recon._phantom_cycles.get("LINKUSDT_LONG_11.177") == 1
    
    # Цикл 2: Позиция ИСЧЕЗЛА (мигание Demo-биржи)
    scenario["mock_rest"].get_positions.return_value = []
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: Счётчик сброшен, фантом не усыновлён
    assert "LINKUSDT_LONG_11.177" not in recon._phantom_cycles
    scenario["mock_om"].place_protective_orders.assert_not_called()
    
    print("\n✅ Счётчик сброшен при исчезновении позиции: ложный фантом проигнорирован")