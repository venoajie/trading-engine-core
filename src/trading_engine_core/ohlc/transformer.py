# src/trading_engine_core/ohlc/transformer.py

# --- Built Ins  ---
from typing import Any

# --- Installed  ---
from loguru import logger as log

# --- Local Application Imports ---
from trading_engine_core.models import OHLCModel


def transform_tv_data_to_ohlc_models(
    tv_data: dict[str, Any],
    exchange_name: str,
    instrument_name: str,
    resolution_str: str,
) -> list[OHLCModel]:
    """
    Transforms TradingView-style chart data into a list of canonical, validated
    OHLCModel Pydantic objects. This is the single source of truth for this transformation.
    """
    records: list[OHLCModel] = []
    try:
        if not isinstance(tv_data, dict):
            log.error(f"Invalid TV data type for {instrument_name}: {type(tv_data)}")
            return []

        ticks = tv_data.get("ticks", [])
        opens = tv_data.get("open", [])
        highs = tv_data.get("high", [])
        lows = tv_data.get("low", [])
        closes = tv_data.get("close", [])
        volumes = tv_data.get("volume", [])

        if not all(isinstance(lst, list) for lst in [ticks, opens, highs, lows, closes, volumes]):
            log.error(f"API returned non-list data for {instrument_name}. Skipping chunk.")
            return []

        if not (len(ticks) == len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)):
            log.error(f"Mismatched OHLC array lengths for {instrument_name}. Ticks: {len(ticks)}, Opens: {len(opens)}, etc. Skipping chunk.")
            return []

        for i, tick_ms in enumerate(ticks):
            # Instantiate the Pydantic model directly to enforce the contract.
            model = OHLCModel(
                exchange=exchange_name,
                instrument_name=instrument_name,
                resolution=resolution_str,
                tick=tick_ms,
                open=opens[i],
                high=highs[i],
                low=lows[i],
                close=closes[i],
                volume=volumes[i],
            )
            records.append(model)

    except (TypeError, IndexError, ValueError) as e:
        log.error(f"Error processing API data into OHLCModel for {instrument_name}: {e}", exc_info=True)
        return []

    return records
