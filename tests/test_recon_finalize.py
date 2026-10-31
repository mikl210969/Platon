import pytest
from unittest.mock import AsyncMock
from datetime import datetime, timezone
from execution.recon_engine import ReconciliationEngine

# Автоматический режим для asyncio
pytestmark = pytest.mark.asyncio


async def test_finalize_with_matching_history(mock_rest_client, mock_storage, mock_manager, sample_passport_link):
    """Сценарий А: История содержит релевантную запись (цена совпадает). PnL должен примениться."""
    
    # 1. Настраиваем мок истории биржи (совпадающая цена входа 11.177)
    mock_rest_client.get_closed_pnl_history.return_value = [
        {
            "symbol": "LINKUSDT",
            "avgEntryPrice": 11.177,
            "closedTime": int(datetime(2026, 10, 30, 17, 33, 33, tzinfo=timezone.utc).timestamp() * 1000),
            "closedPnl": 0.047,
            "cumFee": 0.06
        }
    ]

    # 2. Создаем экземпляр RECON с моками
    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=mock_rest_client,
        storage=mock_storage,
        get_active_passports=lambda: {},
        get_manager=lambda p: mock_manager,
        order_manager=None,
        symbol="LINKUSDT"
    )

    # 3. Вызываем тестируемый метод
    await recon._finalize_from_history(sample_passport_link, mock_manager, "TEST_REASON")

    # 4. Проверяем результаты
    assert sample_passport_link.exit_reason == "RECON_FLAT"
    # Ожидаемый PnL: closedPnl (0.047) - cumFee (0.06) = -0.013
    assert sample_passport_link.realized_pnl == pytest.approx(-0.013, rel=1e-3)
    mock_manager.force_close.assert_called_once()
    mock_storage.save.assert_called_once()


async def test_finalize_ignores_unrelated_history(mock_rest_client, mock_storage, mock_manager, sample_passport_link):
    """Сценарий Б: История содержит запись с ДРУГОЙ ценой входа (например, от ETH). PnL НЕ должен примениться."""
    
    # 1. Настраиваем мок истории биржи (цена входа 2240.0 - явно не LINKUSDT)
    mock_rest_client.get_closed_pnl_history.return_value = [
        {
            "symbol": "ETHUSDT",
            "avgEntryPrice": 2240.0,  
            "closedTime": int(datetime(2026, 10, 30, 17, 33, 33, tzinfo=timezone.utc).timestamp() * 1000),
            "closedPnl": -32.37,       # Тот самый "призрачный" PnL из прошлых логов
            "cumFee": 0.5
        }
    ]

    recon = ReconciliationEngine(
        event_bus=AsyncMock(),
        rest=mock_rest_client,
        storage=mock_storage,
        get_active_passports=lambda: {},
        get_manager=lambda p: mock_manager,
        order_manager=None,
        symbol="LINKUSDT"
    )

    # Запоминаем исходное состояние PnL
    initial_pnl = sample_passport_link.realized_pnl

    # 2. Вызываем метод
    await recon._finalize_from_history(sample_passport_link, mock_manager, "TEST_REASON")

    # 3. Проверяем, что PnL НЕ изменился (или остался 0.0, так как релевантная запись не найдена)
    assert sample_passport_link.realized_pnl == initial_pnl  # Должен остаться 0.0
    assert sample_passport_link.exit_reason == "RECON_FLAT"
    mock_manager.force_close.assert_called_once()