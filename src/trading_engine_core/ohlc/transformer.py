
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
    Transforms TradingView-style chart data (Columnar/Dict of Lists) into OHLCModels.
    
    Updated for Microstructure Alpha:
    - Parses 'taker_buy_volume' and 'taker_sell_volume' arrays if present.
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
        
        # Optional Microstructure Arrays
        taker_buys = tv_data.get("taker_buy_volume", [])
        taker_sells = tv_data.get("taker_sell_volume", [])

        # Validate Standard Fields
        mandatory_lists = [ticks, opens, highs, lows, closes, volumes]
        if not all(isinstance(lst, list) for lst in mandatory_lists):
            log.error(f"API returned non-list data for {instrument_name}. Skipping chunk.")
            return []

        length = len(ticks)
        if not all(len(lst) == length for lst in mandatory_lists):
            log.error(f"Mismatched OHLC array lengths for {instrument_name}. Skipping chunk.")
            return []
            
        # Check if Microstructure data matches length
        has_micro_data = (len(taker_buys) == length and len(taker_sells) == length)

        for i in range(length):
            model = OHLCModel(
                exchange=exchange_name,
                instrument_name=instrument_name,
                resolution=resolution_str,
                tick=int(ticks[i]),
                open=float(opens[i]),
                high=float(highs[i]),
                low=float(lows[i]),
                close=float(closes[i]),
                volume=float(volumes[i]),
                # Default to 0.0 if not present (e.g. Deribit API)
                taker_buy_volume=float(taker_buys[i]) if has_micro_data else 0.0,
                taker_sell_volume=float(taker_sells[i]) if has_micro_data else 0.0
            )
            records.append(model)

    except (TypeError, IndexError, ValueError) as e:
        log.error(f"Error processing TV data for {instrument_name}: {e}", exc_info=True)
        return []

    return records


def transform_canonical_list_to_ohlc_models(
    data: list[dict[str, Any]]
) -> list[OHLCModel]:
    """
    Transforms a list of canonical dictionaries (Row-based) into OHLCModels.
    Used by PublicClients (e.g., Binance) and Backfill workers.
    """
    records: list[OHLCModel] = []
    for item in data:
        try:
            model = OHLCModel(
                exchange=item["exchange"],
                instrument_name=item["instrument_name"],
                resolution=item["resolution"],
                tick=item["tick"],
                open=item["open"],
                high=item["high"],
                low=item["low"],
                close=item["close"],
                volume=item["volume"],
                # Keys injected by Client logic (e.g. BinancePublicClient)
                # Defaults to 0.0 for safety
                taker_buy_volume=item.get("taker_buy_volume", 0.0),
                taker_sell_volume=item.get("taker_sell_volume", 0.0)
            )
            records.append(model)
        except (KeyError, ValueError) as e:
            log.error(f"Failed to transform row to OHLCModel: {e}. Row: {item}")
            continue
            
    return records