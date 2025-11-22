import sys
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

# --- MOCKING EXTERNAL DEPENDENCIES ---
mock_db = MagicMock()
mock_redis = MagicMock()
mock_config = MagicMock()

sys.modules["shared_db_clients.postgres_client"] = MagicMock(PostgresClient=MagicMock)
sys.modules["shared_db_clients.redis_client"] = MagicMock(CustomRedisClient=MagicMock)
sys.modules["shared_config.config"] = MagicMock(settings=mock_config)
sys.modules["shared_exchange_clients.public.base_client"] = MagicMock(AbstractJanitorClient=MagicMock)

# Now we can safe import
from trading_engine_core.ohlc.manager import OhlcManager  # noqa: E402


@pytest.fixture
def manager():
    # Setup Config Mock
    mock_config.backfill.resolutions = ["1"]
    mock_config.backfill.bootstrap_target_candles = 100
    mock_config.backfill.ohlc_backfill_whitelist = ["BTC-PERP"]
    mock_config.exchanges = {"deribit": MagicMock()}

    # DB Client Mock
    db = MagicMock()
    db.get_pool = AsyncMock()
    db.fetch_latest_ohlc_timestamp = AsyncMock()
    db.bulk_upsert_ohlc = AsyncMock()
    # Mock the helper specifically to return a real timedelta
    db._parse_resolution_to_timedelta.return_value = timedelta(minutes=1)

    redis = AsyncMock()
    return OhlcManager(db, redis)


@pytest.mark.asyncio
async def test_discover_work_whitelist_empty(manager):
    manager.backfill_whitelist = []
    await manager.discover_and_queue_work("deribit")
    manager.db.get_pool.assert_not_called()


@pytest.mark.asyncio
async def test_discover_work_logic(manager):
    # Setup DB responses
    conn = AsyncMock()
    manager.db.get_pool.return_value = conn
    conn.fetchrow.return_value = {"market_type": "future"}

    # Case 1: Bootstrap (No latest tick)
    manager.db.fetch_latest_ohlc_timestamp.return_value = None

    await manager.discover_and_queue_work("deribit")
    manager.redis.enqueue_ohlc_work.assert_awaited()
    args = manager.redis.enqueue_ohlc_work.call_args[0][0]
    assert args["type"] == "BOOTSTRAP"


@pytest.mark.asyncio
async def test_perform_fetch(manager):
    client = AsyncMock()

    # Mock Data - Returns tick 2000 which matches end_ts, stopping loop
    tv_data = {"ticks": [2000], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]}
    # Use side_effect to return data once, then empty (to break loop if logic persists)
    client.get_historical_ohlc.side_effect = [tv_data, {}]

    work_item = {
        "exchange": "ex",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 0,
        "end_ts": 2000,
        "market_type": "spot",
    }

    await manager._perform_paginated_ohlc_fetch(work_item, client)

    manager.db.bulk_upsert_ohlc.assert_awaited()
