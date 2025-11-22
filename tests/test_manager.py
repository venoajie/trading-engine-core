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

from trading_engine_core.ohlc.manager import OhlcManager  # noqa: E402


@pytest.fixture
def manager():
    mock_config.backfill.resolutions = ["1"]
    mock_config.backfill.bootstrap_target_candles = 100
    mock_config.backfill.ohlc_backfill_whitelist = ["BTC-PERP"]

    # Setup Exchanges as Mock Objects with model_dump
    mock_exch_config = MagicMock()
    mock_exch_config.model_dump.return_value = {}
    mock_config.exchanges = {"deribit": mock_exch_config}

    db = MagicMock()
    db.get_pool = AsyncMock()
    db.fetch_latest_ohlc_timestamp = AsyncMock()
    db.bulk_upsert_ohlc = AsyncMock()
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
    conn = AsyncMock()
    manager.db.get_pool.return_value = conn
    conn.fetchrow.return_value = {"market_type": "future"}
    manager.db.fetch_latest_ohlc_timestamp.return_value = None

    await manager.discover_and_queue_work("deribit")
    manager.redis.enqueue_ohlc_work.assert_awaited()
    args = manager.redis.enqueue_ohlc_work.call_args[0][0]
    assert args["type"] == "BOOTSTRAP"


@pytest.mark.asyncio
async def test_perform_fetch_success(manager):
    """Test success path with loop termination."""
    client = AsyncMock()
    # Return data that covers the range to stop the loop
    tv_data = {"ticks": [2000], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]}
    client.get_historical_ohlc.return_value = tv_data

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
    valid_item = {
        "type": "BOOTSTRAP",
        "exchange": "deribit",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 1,
        "end_ts": 2,
        "market_type": "spot",
    }

    manager.redis.dequeue_ohlc_work.side_effect = [valid_item, None, asyncio.CancelledError()]

    mock_client_cls = MagicMock()
    mock_client_instance = AsyncMock()
    mock_client_cls.return_value = mock_client_instance
    client_map = {"deribit": mock_client_cls}

    with pytest.raises(asyncio.CancelledError):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, client_map)

    mock_client_instance.connect.assert_awaited()


@pytest.mark.asyncio
async def test_run_worker_bad_item(manager):
    manager.redis.dequeue_ohlc_work.side_effect = ["NotADict", {"partial": "dict"}, asyncio.CancelledError()]

    with pytest.raises(asyncio.CancelledError):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, {})


@pytest.mark.asyncio
async def test_run_worker_client_errors(manager):
    item = {
        "type": "BOOTSTRAP",
        "exchange": "unknown",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 1,
        "end_ts": 2,
    }

    manager.redis.dequeue_ohlc_work.side_effect = [item, asyncio.CancelledError()]

    with pytest.raises(asyncio.CancelledError):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await manager.run_worker(1, {})


@pytest.mark.asyncio
async def test_fetch_empty_response(manager):
    client = AsyncMock()

    # We set resolution to 1 minute.
    # Chunk size ~ 1000 mins = 60,000,000 ms.
    # To force multiple calls, end_ts must be > 60,000,000.
    # Let's set end_ts to allow 3 chunks.

    # However, if response is empty, code does: current_start_ts = current_end_ts + 1
    # This effectively skips the chunk.

    # Scenario:
    # 1. Call 1: Empty -> Advance
    # 2. Call 2: Empty -> Advance
    # 3. Call 3: Success (covers rest) -> Finish

    client.get_historical_ohlc.side_effect = [
        {},  # Empty
        {"bad": "data"},  # Transformer returns []
        {"ticks": [300000000], "open": [1], "high": [1], "low": [1], "close": [1], "volume": [1]},
    ]

    # Make range large enough to not finish in one step if we simply advanced 1ms?
    # Logic: if empty, start = end + 1. This jumps the whole chunk.
    # So regardless of range size, one empty response consumes one chunk.
    # We just need end_ts to be large enough for 3 chunks.

    # 1 min * 1000 * 1000 = 60,000,000 ms per chunk.
    # Total range = 180,000,000

    work_item = {
        "exchange": "ex",
        "instrument": "i",
        "resolution": "1",
        "start_ts": 0,
        "end_ts": 180000000,
        "market_type": "spot",
    }

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await manager._perform_paginated_ohlc_fetch(work_item, client)

    assert client.get_historical_ohlc.call_count == 3
