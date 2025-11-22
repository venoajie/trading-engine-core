# src/trading_engine_core/ohlc/manager.py

import asyncio
from datetime import UTC, datetime
from typing import Any

from loguru import logger as log
from shared_config.config import settings
from shared_db_clients.postgres_client import PostgresClient
from shared_db_clients.redis_client import CustomRedisClient
from shared_exchange_clients.public.base_client import AbstractJanitorClient

# [NEW] Import the centralized transformer.
from trading_engine_core.ohlc.transformer import transform_tv_data_to_ohlc_models


class OhlcManager:
    def __init__(self, db_client: PostgresClient, redis_client: CustomRedisClient):
        self.db = db_client
        self.redis = redis_client
        if not settings.backfill:
            raise ValueError("Backfill settings not configured.")
        self.resolutions = settings.backfill.resolutions
        self.target_candles = settings.backfill.bootstrap_target_candles
        self.backfill_whitelist = settings.backfill.ohlc_backfill_whitelist

    async def discover_and_queue_work(self, exchange_name: str):
        log.info(f"[{exchange_name}] Starting OHLC work discovery...")
        if not self.backfill_whitelist:
            log.warning(f"[{exchange_name}] OHLC backfill whitelist is empty. Skipping.")
            return

        now_utc = datetime.now(UTC)
        tasks_added = 0
        log.info(f"[{exchange_name}] Checking against whitelist: {self.backfill_whitelist}")
        conn = await self.db.get_pool()

        for instrument_name in self.backfill_whitelist:
            instrument_details = await conn.fetchrow(
                "SELECT market_type FROM instruments WHERE exchange = $1 AND instrument_name = $2",
                exchange_name,
                instrument_name,
            )
            if not instrument_details:
                log.warning(f"Instrument '{instrument_name}' not in DB for '{exchange_name}'. Skipping.")
                continue

            market_type = instrument_details["market_type"]
            for res_str in self.resolutions:
                res_str = str(res_str)
                res_td = self.db._parse_resolution_to_timedelta(res_str)
                latest_db_tick = await self.db.fetch_latest_ohlc_timestamp(exchange_name, instrument_name, res_td)
                work_item = None
                if latest_db_tick is None:
                    end_ts = int(now_utc.timestamp() * 1000)
                    start_ts = int((now_utc - (self.target_candles * res_td)).timestamp() * 1000)
                    work_item = {
                        "type": "BOOTSTRAP",
                        "exchange": exchange_name,
                        "instrument": instrument_name,
                        "market_type": market_type,
                        "resolution": res_str,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                    }
                else:
                    next_expected_tick = latest_db_tick + res_td
                    if next_expected_tick < now_utc:
                        start_ts = int(next_expected_tick.timestamp() * 1000)
                        end_ts = int(now_utc.timestamp() * 1000)
                        work_item = {
                            "type": "GAP_FILL",
                            "exchange": exchange_name,
                            "instrument": instrument_name,
                            "market_type": market_type,
                            "resolution": res_str,
                            "start_ts": start_ts,
                            "end_ts": end_ts,
                        }
                if work_item:
                    await self.redis.enqueue_ohlc_work(work_item)
                    tasks_added += 1
        log.success(f"[{exchange_name}] OHLC work discovery complete. Enqueued {tasks_added} tasks.")

    async def run_worker(self, worker_id: int, client_map: dict[str, type[AbstractJanitorClient]]):
        log.info(f"[Worker-{worker_id}] Starting...")
        api_client: AbstractJanitorClient | None = None
        try:
            while True:
                work_item = await self.redis.dequeue_ohlc_work()
                if work_item is None:
                    await asyncio.sleep(5)
                    continue
                if not isinstance(work_item, dict):
                    log.error(f"Invalid work item type: {type(work_item)}. Skipping.")
                    continue

                work_type = work_item.get("type", "UNKNOWN")
                required_fields = ["exchange", "instrument", "resolution", "start_ts", "end_ts"]
                if not all(field in work_item for field in required_fields):
                    log.error(f"Invalid work item: Missing fields. Item: {work_item}")
                    continue

                log.info(
                    f"[Worker-{worker_id}] Processing task: {work_type} for "
                    f"{work_item.get('instrument')} ({work_item.get('resolution')})"
                )
                try:
                    exchange_name = work_item["exchange"]
                    client_class = client_map.get(exchange_name)
                    if not client_class:
                        log.error(f"No client found for exchange '{exchange_name}'. Skipping.")
                        continue

                    exchange_config = settings.exchanges.get(exchange_name)
                    if not exchange_config:
                        log.error(f"No config found for exchange '{exchange_name}'. Skipping.")
                        continue

                    api_client = client_class(exchange_config.model_dump())
                    await api_client.connect()
                    await self._perform_paginated_ohlc_fetch(work_item, api_client)
                    await api_client.close()
                    api_client = None
                except Exception as e:
                    log.error(f"[Worker-{worker_id}] Failed to process task: {work_item}. Error: {e}", exc_info=True)
                    await self.redis.enqueue_failed_ohlc_work(work_item)
                    if api_client:
                        await api_client.close()
                        api_client = None
        except asyncio.CancelledError:
            log.info(f"[Worker-{worker_id}] Cancelled.")
        finally:
            if api_client:
                await api_client.close()

    async def _perform_paginated_ohlc_fetch(self, work_item: dict[str, Any], api_client: AbstractJanitorClient):
        exchange, instrument, res_str, start_ts, end_ts, market_type = (
            work_item["exchange"],
            work_item["instrument"],
            str(work_item["resolution"]),
            work_item["start_ts"],
            work_item["end_ts"],
            work_item["market_type"],
        )
        log.info(f"Executing paginated fetch for {instrument} ({res_str}) from {start_ts} to {end_ts}")

        current_start_ts = start_ts
        total_records_upserted = 0
        resolution_td = self.db._parse_resolution_to_timedelta(res_str)
        # Calculate chunk size based on 1000 candles per request
        chunk_advance_ms = int(1000 * resolution_td.total_seconds() * 1000)

        while current_start_ts < end_ts:
            current_end_ts = min(end_ts, current_start_ts + chunk_advance_ms)
            log.debug(f"Fetching chunk for {instrument}: {current_start_ts} -> {current_end_ts}")

            response = await api_client.get_historical_ohlc(
                instrument, current_start_ts, current_end_ts, res_str, market_type
            )

            if not response or not response.get("ticks"):
                log.warning(f"API call for {instrument} returned no data for this chunk. Advancing.")
                current_start_ts = current_end_ts + 1
                await asyncio.sleep(0.2)
                continue

            # [REFACTORED] Use the centralized transformer to get validated Pydantic models.
            ohlc_models = transform_tv_data_to_ohlc_models(response, exchange, instrument, res_str)

            if not ohlc_models:
                log.debug(f"No new records transformed for {instrument}. Advancing.")
                current_start_ts = current_end_ts + 1
                await asyncio.sleep(0.2)
                continue

            # [REFACTORED] Convert models to dictionaries only at the point of persistence.
            records_for_db = [m.model_dump() for m in ohlc_models]
            await self.db.bulk_upsert_ohlc(records_for_db)
            total_records_upserted += len(records_for_db)

            # [REFACTORED] Get the last tick from the model attribute, not a dictionary key.
            last_tick_ms = ohlc_models[-1].tick
            current_start_ts = last_tick_ms + 1
            await asyncio.sleep(0.2)

        log.success(
            f"Paginated fetch for {instrument} ({res_str}) complete. Upserted {total_records_upserted} records."
        )
