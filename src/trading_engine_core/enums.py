# src\trading_engine_core\enums.py
from enum import Enum

class MarketType(str, Enum):
    """
    The canonical, internal representation of all market types.
    This is the Single Source of Truth for the entire system.
    """
    SPOT = "spot"
    LINEAR_FUTURES = "linear_futures"
    INVERSE_FUTURES = "inverse_futures"
    INVERSE_OPTIONS = "inverse_options"
    UNKNOWN = "unknown"