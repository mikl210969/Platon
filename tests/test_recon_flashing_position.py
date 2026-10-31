import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from execution.recon_engine import ReconciliationEngine
from execution.passport import TradePassport, SignalSource
from execution.passport_manager import PassportManager

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_flashing_scenario():
    passport = TradePassport(
        passport_id="PASS_FLASH_TEST_001",
        symbol="LINKUSDT",
        side="LONG",
        entry_price=11.177,
        position_size=10.0,
        filled_qty=10.0,
        created_at=datetime(2026, 10, 30, 17, 32, 29, tzinfo=timezone.utc),
        signal_source=SignalSource.ADAPTIVE_ZONE
    )
    
    mock_mgr = MagicMock(spec=PassportManager)
    mock_mgr.current_state.return_value = "POSITION_OPEN"
    mock_mgr.is_active.return_value = True
    mock_mgr.get_state_age_seconds.return_value = 120.0
    mock_mgr.log_recon = MagicMock()
    mock_mgr.force_close = MagicMock()
    
    def get_manager(p): return mock_mgr
    def get_active(): return {passport.passport_id: passport}

    mock_rest = AsyncMock()
    mock_storage = MagicMock()
    mock_storage.save = MagicMock()

    return {
        "passport": passport,
        "mock_mgr": mock_mgr,
        "get_manager": get_manager,
        "get_active": get_active,
        "mock_rest": mock_rest,
        "mock_storage": mock_storage
    }


async def test_fixed_behavior_ignores_single_flash(setup_flashing_scenario):
    """
    Тест проверяет ИСПРАВЛЕННОЕ поведение:
    Цикл 1: Биржа возвращает pos_size=0.0 -> RECON ждет, НЕ финализирует.
    Цикл 2: Биржа возвращает pos_size=10.0 -> RECON сбрасывает счетчик, паспорт остается живым.
    """
    scenario = setup_flashing_scenario
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=scenario["mock_rest"],
        storage=scenario["mock_storage"],
        get_active_passports=scenario["get_active"],
        get_manager=scenario["get_manager"],
        order_manager=None,
        symbol="LINKUSDT"
    )

    # === ЦИКЛ 1: ГЛЮК БИРЖИ (pos_size = 0.0) ===
    scenario["mock_rest"].get_positions.return_value = [
        {"symbol": "LINKUSDT", "size": "0.0", "side": "Buy", "avgPrice": "11.177"}
    ]
    scenario["mock_rest"].get_open_orders.return_value = []
    
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: force_close НЕ должен быть вызван при первом сбое
    scenario["mock_mgr"].force_close.assert_not_called()
    assert recon._zero_position_cycles.get("PASS_FLASH_TEST_001") == 1

    # === ЦИКЛ 2: ПОЗИЦИЯ ВЕРНУЛАСЬ (pos_size = 10.0) ===
    scenario["mock_rest"].get_positions.return_value = [
        {"symbol": "LINKUSDT", "size": "10.0", "side": "Buy", "avgPrice": "11.177"}
    ]
    # Сбрасываем мок вызовов, чтобы проверить только второй цикл
    scenario["mock_mgr"].force_close.reset_mock()
    
    await recon.reconcile(reason="PERIODIC")
    
    # ОЖИДАНИЕ: force_close всё еще НЕ вызван, счетчик сброшен
    scenario["mock_mgr"].force_close.assert_not_called()
    assert "PASS_FLASH_TEST_001" not in recon._zero_position_cycles
    
    print("\n✅ ИСПРАВЛЕНИЕ ПОДТВЕРЖДЕНО: RECON игнорирует единичный сбой pos_size=0.0 и ждет подтверждения.")