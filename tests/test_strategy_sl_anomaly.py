import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timezone, timedelta
from analytics.strategy import AdaptiveZoneV1, AnomalyZone

pytestmark = pytest.mark.asyncio


@pytest.fixture
def setup_long_zone_departed():
    """
    Создает зону LONG в состоянии DEPARTED с устаревшим min_price_after.
    Настроена так, чтобы пройти все проверки (возраст, спред, объем, дельта),
    но упасть на Sanity Check (SL >= Entry).
    """
    zone = AnomalyZone(
        id="TEST_ZONE_001",
        price=2254.00,
        side="LONG",
        state="DEPARTED",
        min_price_after=2253.50,  # Устаревший минимум (цена потом упала до 2251.49)
        max_price_after=None,
        volume=5000.0,            # Большой объем для прохождения проверки стены
        timestamp=(datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
        candles_buffer=[{"delta": -10.0}], # Отрицательная дельта для прохождения фильтра
        rejection_logged=False
    )
    
    config = {
        "trading.structure_validation": {
            "wall_volume_multiple": 2.0,
            "sl_buffer_atr5m": 1.0,
            "tp1_risk_multiple": 1.5,
            "tp2_risk_multiple": 3.0,
            "min_retracement_atr_multiplier": 1.5,
            "departure_max_bars": 24
        }
    }
    
    current_snapshot = {"atr_5m": 0.3, "atr": 0.5}
    
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


async def test_sl_anomaly_long_departed_zone(setup_long_zone_departed):
    """
    Проверяет, что Sanity Check блокирует создание сигнала, 
    если рассчитанный SL оказывается выше цены входа для LONG.
    """
    scenario = setup_long_zone_departed
    
    strategy = AdaptiveZoneV1(
        symbol="ETHUSDT",
        config=scenario["config"],
        event_bus=AsyncMock(),
        rest_client=AsyncMock()
    )
    strategy.current_snapshot = scenario["current_snapshot"]
    strategy.exit_logic = "structure"
    strategy._get_futures_price = AsyncMock(return_value=2251.49)
    
    current_price = 2251.49
    current_delta = -10.0
    
    signal = await strategy._check_return_and_entry(
        scenario["zone"],
        current_price,
        current_delta,
        scenario["thresholds"]
    )
    
    # ГЛАВНАЯ ПРОВЕРКА: Сигнал НЕ должен быть сгенерирован
    assert signal is None, \
        f"🚨 ОШИБКА: Сигнал был сгенерирован несмотря на аномальный SL! " \
        f"SL={signal.get('sl_price')}, Entry={signal.get('entry_price')}"
    
    print("\n✅ ИСПРАВЛЕНИЕ ПОДТВЕРЖДЕНО: Sanity Check заблокировал сигнал с SL >= Entry")