import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from execution.recon_engine import ReconciliationEngine
from execution.passport import TradePassport, SignalSource

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_cross_contamination_scenario():
    """
    Сценарий: RECON для SOLUSDT пытается обработать паспорт ETHUSDT.
    Это и есть баг перекрёстного загрязнения.
    """
    # Паспорт ETH (НЕ должен быть тронут RECON'ом для SOL)
    eth_passport = TradePassport(
        passport_id="PASS_ETH_001",
        symbol="ETHUSDT",  # <-- ЭТО ETH!
        side="LONG",
        entry_price=2241.16,
        position_size=10.0,
        filled_qty=0.0,  # <-- ИЗМЕНЕНО: 0.0 чтобы сработала синхронизация
        created_at=datetime(2026, 10, 31, 3, 33, 13, tzinfo=timezone.utc),
        signal_source=SignalSource.ADAPTIVE_ZONE
    )
    
    mock_mgr = MagicMock()
    mock_mgr.current_state.return_value = "POSITION_OPEN"
    mock_mgr.is_active.return_value = True
    mock_mgr.get_state_age_seconds.return_value = 120.0
    mock_mgr.log_recon = MagicMock()
    mock_mgr.force_close = MagicMock()
    
    def get_manager(p): return mock_mgr
    def get_active(): return {eth_passport.passport_id: eth_passport}

    mock_rest = AsyncMock()
    # RECON для SOL получает позицию SOL с ценой 96.57
    mock_rest.get_positions.return_value = [
        {"symbol": "SOLUSDT", "size": "10.0", "side": "Buy", "avgPrice": "96.57"}
    ]
    mock_rest.get_open_orders.return_value = []
    
    mock_storage = MagicMock()
    mock_storage.save = MagicMock()

    return {
        "eth_passport": eth_passport,
        "mock_mgr": mock_mgr,
        "get_manager": get_manager,
        "get_active": get_active,
        "mock_rest": mock_rest,
        "mock_storage": mock_storage
    }


async def test_recon_does_not_contaminate_other_symbols(setup_cross_contamination_scenario):
    """
    Тест проверяет, что RECON для SOLUSDT НЕ трогает паспорт ETHUSDT.
    
    До исправления: RECON перезаписывает entry_price паспорта ETH на 96.57 (цена SOL).
    После исправления: паспорт ETH остаётся нетронутым.
    """
    scenario = setup_cross_contamination_scenario
    
    # Создаём RECON для SOLUSDT (не для ETH!)
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=scenario["mock_rest"],
        storage=scenario["mock_storage"],
        get_active_passports=scenario["get_active"],
        get_manager=scenario["get_manager"],
        order_manager=None,
        symbol="SOLUSDT"  # <-- RECON для SOL
    )

    # Запоминаем исходную цену входа ETH
    original_entry_price = scenario["eth_passport"].entry_price  # 2241.16

    # Вызываем reconcile
    await recon.reconcile(reason="PERIODIC")

    # === ПРОВЕРКА ===
    # Цена входа ETH НЕ должна измениться!
    assert scenario["eth_passport"].entry_price == original_entry_price, \
        f"Паспорт ETH загрязнён: entry_price изменился с {original_entry_price} на {scenario['eth_passport'].entry_price}"
    
    # force_close НЕ должен быть вызван для чужого паспорта
    scenario["mock_mgr"].force_close.assert_not_called()
    
    print(f"\n✅ ИСПРАВЛЕНИЕ ПОДТВЕРЖДЕНО: RECON для SOLUSDT не тронул паспорт ETHUSDT (entry_price={original_entry_price})")