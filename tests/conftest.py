import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from execution.passport import TradePassport, SignalSource



@pytest.fixture
def mock_rest_client():
    """Фейковый REST-клиент биржи."""
    client = AsyncMock()
    client.get_closed_pnl_history = AsyncMock(return_value=[])
    return client

@pytest.fixture
def mock_storage():
    """Фейковое хранилище паспортов."""
    storage = MagicMock()
    storage.save = MagicMock()
    return storage

@pytest.fixture
def mock_manager():
    """Фейковый менеджер состояний паспорта."""
    mgr = MagicMock()
    mgr.log_recon = MagicMock()
    mgr.force_close = MagicMock()
    return mgr

@pytest.fixture
def sample_passport_link():
    """Реальный объект паспорта для тестов (LINKUSDT LONG)."""
    return TradePassport(
        passport_id="PASS_TEST_001",
        symbol="LINKUSDT",
        side="LONG",
        entry_price=11.177,
        position_size=10.0,
        tp1_price=11.236,
        tp2_price=11.293,
        created_at=datetime(2026, 10, 30, 17, 32, 29, tzinfo=timezone.utc),
        signal_source=SignalSource.ADAPTIVE_ZONE
    )