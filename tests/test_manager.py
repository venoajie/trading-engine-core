import asyncio
import sys
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.mark.asyncio
async def test_run_worker_success(manager):
    """Test worker processing a valid task."""
    # Mock Redis Dequeue to return item then None (to exit loop? No, loop is infinite)
    # We need to throw CancelledError to stop the infinite loop cleanly.

    valid_item = {
        "type": "BOOTSTRAP",
        "exchange": "deribit",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 1,
        "end_ts": 2,
    }

    # Redis queue sequence: [Item, CancelledError]
    manager.redis.dequeue_ohlc_work.side_effect = [
        valid_item,
        None,  # Sleep branch
        asyncio.CancelledError(),
    ]

    # Mock Client Map
    mock_client_cls = MagicMock()
    mock_client_instance = AsyncMock()
    mock_client_cls.return_value = mock_client_instance
    client_map = {"deribit": mock_client_cls}

    # Mock Config
    manager.exchanges = {"deribit": {}}  # Ensure config exists (though we mocked sys module)
    # Note: We mocked sys.modules["shared_config.config"].settings.exchanges in fixture

    # Run
    with pytest.raises(asyncio.CancelledError):
        # We patch asyncio.sleep to avoid waiting
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, client_map)

    mock_client_instance.connect.assert_awaited()
    mock_client_instance.get_historical_ohlc.assert_awaited()


@pytest.mark.asyncio
async def test_run_worker_bad_item(manager):
    """Test worker handling invalid items."""
    manager.redis.dequeue_ohlc_work.side_effect = [
        "NotADict",  # Invalid Type
        {"partial": "dict"},  # Missing fields
        asyncio.CancelledError(),
    ]

    with pytest.raises(asyncio.CancelledError):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, {})

    # Should not crash, just log errors


@pytest.mark.asyncio
async def test_run_worker_client_errors(manager):
    """Test worker handling missing client/config or exceptions."""
    item = {
        "type": "BOOTSTRAP",
        "exchange": "unknown",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 1,
        "end_ts": 2,
    }

    manager.redis.dequeue_ohlc_work.side_effect = [
        item,  # Unknown Exchange -> Log Error
        asyncio.CancelledError(),
    ]

    with pytest.raises(asyncio.CancelledError):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, {})


@pytest.mark.asyncio
async def test_fetch_empty_response(manager):
    """Test fetch logic when API returns empty data."""
    client = AsyncMock()
    # Return empty dict then valid to stop loop (via side effect or logic?)
    # Actually the loop logic: current_start_ts < end_ts.
    # If empty, it increments current_start_ts + 1.

    # 1. Empty response
    # 2. Response causing transform to return empty
    # 3. Valid response (to end it)

    client.get_historical_ohlc.side_effect = [
        {},  # Empty response
        {"bad": "data"},  # Valid JSON but transformer returns []
        {"ticks": [2000], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]},  # Finish
    ]

    work_item = {
        "exchange": "ex",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 0,
        "end_ts": 2000,
        "market_type": "spot",
    }

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await manager._perform_paginated_ohlc_fetch(work_item, client)

    # Should have called fetch 3 times
    assert client.get_historical_ohlc.call_count == 3
