# src\trading_engine_core\enums.py

# --- Built Ins  ---
from enum import Enum


class MarketType(str, Enum):
    """
    The canonical, internal representation of all market types.
    This is the Single Source of Truth for the entire system.
    """

    SPOT = "spot"
    LINEAR_FUTURES = "linear_futures"
    LINEAR_FUTURES_COMBO = "linear_futures_combo"
    LINEAR_OPTIONS = "linear_options"
    LINEAR_OPTIONS_COMBO = "linear_options_combo"
    INVERSE_FUTURES = "inverse_futures"
    INVERSE_FUTURES_COMBO = "inverse_futures_combo"
    INVERSE_OPTIONS = "inverse_options"
    INVERSE_OPTIONS_COMBO = "inverse_options_combo"
    UNKNOWN = "unknown"
