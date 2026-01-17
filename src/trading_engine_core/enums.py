# src/trading_engine_core/enums.py

from enum import Enum

class MarketType(str, Enum):
    """
    The canonical, internal representation of all market types.
    This is the Single Source of Truth for the entire system.
    """

    SPOT = "spot"
    SPOT_TICKER_24H = "spot_ticker_24h"
    
    LINEAR_FUTURES = "linear_futures"
    LINEAR_FUTURES_COMBO = "linear_futures_combo"
    LINEAR_OPTIONS = "linear_options"
    LINEAR_OPTIONS_COMBO = "linear_options_combo"
    INVERSE_FUTURES = "inverse_futures"
    INVERSE_FUTURES_COMBO = "inverse_futures_combo"
    INVERSE_OPTIONS = "inverse_options"
    INVERSE_OPTIONS_COMBO = "inverse_options_combo"
    UNKNOWN = "unknown"


class StorageMode(str, Enum):
    """Defines the authoritative storage backend for an asset's market data."""

    PERSISTENT = "POSTGRES"
    EPHEMERAL = "REDIS_BUFFER"
    UNKNOWN = "UNKNOWN"