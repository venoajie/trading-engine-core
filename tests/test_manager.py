import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# --- MOCKING EXTERNAL DEPENDENCIES ---
# Since trading-engine-core doesn't actually depend on shared-db-clients/config
# in pyproject.toml, we must mock them BEFORE importing the manager.

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

    db = AsyncMock()
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
    manager.db.get_pool.return_value = conn  # conn is usually acquired from pool

    # Mock fetchrow (Instrument details)
    conn.fetchrow.return_value = {"market_type": "future"}

    # Mock parse resolution
    from datetime import timedelta

    manager.db._parse_resolution_to_timedelta.return_value = timedelta(minutes=1)

    # Case 1: Bootstrap (No latest tick)
    manager.db.fetch_latest_ohlc_timestamp.return_value = None

    await manager.discover_and_queue_work("deribit")
    manager.redis.enqueue_ohlc_work.assert_awaited()
    args = manager.redis.enqueue_ohlc_work.call_args[0][0]
    assert args["type"] == "BOOTSTRAP"


@pytest.mark.asyncio
async def test_perform_fetch(manager):
    # Mock client
    client = AsyncMock()

    # Mock Data
    tv_data = {"ticks": [1000], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]}
    client.get_historical_ohlc.return_value = tv_data

    work_item = {
        "exchange": "ex",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 0,
        "end_ts": 2000,
        "market_type": "spot",
    }

    # Mock DB resolution parsing
    from datetime import timedelta

    manager.db._parse_resolution_to_timedelta.return_value = timedelta(minutes=1)

    await manager._perform_paginated_ohlc_fetch(work_item, client)

    manager.db.bulk_upsert_ohlc.assert_awaited()
