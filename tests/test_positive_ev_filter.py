import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timezone, timedelta
from analytics.strategy import AdaptiveZoneV1, AnomalyZone

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_low_profit_zone():
    """
    Создает зону, где расстояние до TP1 слишком мало для покрытия комиссий.
    Entry: 2250.0, TP1: 2250.5 (разница всего 0.5$).
    При размере 10 ETH комиссия съест всю прибыль.
    """
    zone = AnomalyZone(
        id="TEST_EV_ZONE",
        price=2250.0,
        side="LONG",
        state="DEPARTED",
        min_price_after=2249.0,
        max_price_after=None,
        volume=5000.0,
        timestamp=(datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
        candles_buffer=[{"delta": -10.0}],
        rejection_logged=False
    )
    
    config = {
        "trading.structure_validation": {
            "wall_volume_multiple": 2.0,
            "sl_buffer_atr5m": 1.0,
            "tp1_risk_multiple": 1.5, # Стандартный множитель
            "tp2_risk_multiple": 3.0,
            "min_retracement_atr_multiplier": 1.5,
            "departure_max_bars": 24
        },
        "trading.risk.commission_rate": 0.0004 # 0.04%
    }
    
    current_snapshot = {"atr_5m": 0.1, "atr": 0.5} # Маленький ATR
    
    thresholds = MagicMock()
    thresholds.wall_median_bid = 500.0
    thresholds.wall_median_ask = 500.0
    thresholds.wall_threshold = 500.0
    thresholds.delta_threshold_long = -5.0
    
    return {
        "zone": zone,
        "config": config,
        "current_snapshot": current_snapshot,
        "thresholds": thresholds
    }


async def test_positive_ev_filter_blocks_unprofitable_trade(setup_low_profit_zone):
    """
    Проверяет, что сигнал блокируется, если Gross Profit < Commission * 1.5
    """
    scenario = setup_low_profit_zone
    
    strategy = AdaptiveZoneV1(
        symbol="ETHUSDT",
        config=scenario["config"],
        event_bus=AsyncMock(),
        rest_client=AsyncMock()
    )
    strategy.current_snapshot = scenario["current_snapshot"]
    strategy.exit_logic = "structure"
    strategy._get_futures_price = AsyncMock(return_value=2250.0)
    
    current_price = 2250.0
    current_delta = -10.0
    
    signal = await strategy._check_return_and_entry(
        scenario["zone"],
        current_price,
        current_delta,
        scenario["thresholds"]
    )
    
    # Сигнал ДОЛЖЕН быть заблокирован
    assert signal is None, \
        f"🚨 ОШИБКА: Сигнал прошел фильтр EV, хотя прибыль не покрывает комиссии! " \
        f"Signal: {signal}"
    
    print("\n✅ ИСПРАВЛЕНИЕ ПОДТВЕРЖДЕНО: Positive EV Filter заблокировал убыточную сделку")