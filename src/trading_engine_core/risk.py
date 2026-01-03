# src/trading_engine_core/risk.py

# --- Installed  ---

from pydantic import BaseModel


class RiskManagementSettings(BaseModel):
    """
    A shared data contract for global risk management parameters.
    """

    max_order_notional_usd: float
    max_position_notional_usd: float
    price_deviation_tolerance_pct: float
    excluded_from_hedge: list[str]
