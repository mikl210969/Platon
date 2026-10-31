import pytest
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from execution.recon_engine import ReconciliationEngine
from execution.passport import TradePassport, SignalSource

pytestmark = pytest.mark.asyncio


@pytest.fixture
def real_link_data():
    """Загружает реальные исторические данные LINKUSDT."""
    # ✅ ИСПРАВЛЕННЫЙ ПУТЬ (латиницей)
    data_path = Path(r"C:\Users\ongul\YandexDisk\data_new\LINKUSDT")
    
    if not data_path.exists():
        pytest.skip(f"Папка с данными не найдена: {data_path}")
    
    # Ищем файлы с данными (ищем по любым CSV файлам)
    all_csv_files = list(data_path.glob("*.csv"))
    
    if not all_csv_files:
        pytest.skip(f"CSV файлы не найдены в {data_path}")
    
    # Разделяем на trades и orderbook по имени файла
    trade_files = [f for f in all_csv_files if "trade" in f.name.lower()]
    ob_files = [f for f in all_csv_files if "orderbook" in f.name.lower() or "ob" in f.name.lower()]
    
    if not trade_files:
        pytest.skip("Файлы trades не найдены")
    
    # Загружаем первый файл trades
    trades_df = pd.read_csv(trade_files[0])
    
    print(f"\n📊 Загружено данных:")
    print(f"   Trades: {len(trades_df)} записей из {trade_files[0].name}")
    if ob_files:
        print(f"   OrderBook: {len(ob_files)} файлов")
    
    return {
        "trades": trades_df,
        "data_path": data_path
    }


async def test_recon_with_real_link_trades(real_link_data):
    """
    Интеграционный тест: RECON обрабатывает реальную сделку из истории.
    
    Сценарий:
    1. Создаем паспорт для входа по цене, которая есть в истории trades.
    2. Имитируем, что позиция закрылась (берем цену из последующей сделки).
    3. Проверяем, что RECON корректно рассчитывает PnL на основе реальных данных.
    """
    
    trades_df = real_link_data["trades"]
    
    # Берем первую сделку как "вход"
    entry_trade = trades_df.iloc[0]
    entry_price = float(entry_trade["price"])
    entry_time_ms = int(entry_trade["timestamp_ms"])
    entry_time = datetime.fromtimestamp(entry_time_ms / 1000, tz=timezone.utc)
    
    # Берем последнюю сделку как "выход" (для простоты)
    exit_trade = trades_df.iloc[-1]
    exit_price = float(exit_trade["price"])
    exit_time_ms = int(exit_trade["timestamp_ms"])
    
    # Создаем паспорт
    passport = TradePassport(
        passport_id="PASS_REAL_DATA_TEST",
        symbol="LINKUSDT",
        side="LONG",
        entry_price=entry_price,
        position_size=10.0,
        created_at=entry_time,
        signal_source=SignalSource.ADAPTIVE_ZONE
    )
    
    # Создаем моки
    mock_rest = AsyncMock()
    mock_storage = MagicMock()
    mock_manager = MagicMock()
    
    # Настраиваем мок истории PnL (эмулируем ответ биржи)
    # Bybit возвращает PnL и комиссию
    expected_pnl = (exit_price - entry_price) * 10.0  # Для LONG
    expected_fee = abs(exit_price * 10.0 * 0.00055)  # 0.055% комиссия
    
    mock_rest.get_closed_pnl_history.return_value = [
        {
            "symbol": "LINKUSDT",
            "avgEntryPrice": entry_price,
            "closedTime": exit_time_ms,
            "closedPnl": expected_pnl,
            "cumFee": expected_fee
        }
    ]
    
    # Создаем RECON
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=mock_rest,
        storage=mock_storage,
        get_active_passports=lambda: {},
        get_manager=lambda p: mock_manager,
        order_manager=None,
        symbol="LINKUSDT"
    )
    
    # Вызываем финализацию
    await recon._finalize_from_history(passport, mock_manager, "INTEGRATION_TEST")
    
    # Проверяем результаты
    assert passport.exit_reason == "RECON_FLAT"
    assert passport.exit_price == entry_price  # Цена из истории
    
    # PnL должен быть близок к расчетному (с учетом комиссии)
    expected_realized = expected_pnl - expected_fee
    assert passport.realized_pnl == pytest.approx(expected_realized, rel=1e-3)
    
    print(f"\n✅ Интеграционный тест прошел!")
    print(f"   Вход: {entry_price} @ {entry_time}")
    print(f"   Выход: {exit_price} @ {datetime.fromtimestamp(exit_time_ms/1000, tz=timezone.utc)}")
    print(f"   PnL: {passport.realized_pnl:.4f} USDT")