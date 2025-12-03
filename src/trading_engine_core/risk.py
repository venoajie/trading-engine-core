# --- Installed  ---
from pydantic import BaseModel


class RiskManagementSettings(BaseModel):
    """
    A shared data contract for global risk management parameters.
    Consumed by any service that needs to enforce pre-trade capital safety rules.
    """

    max_order_notional_usd: float
    max_position_notional_usd: float
    price_deviation_tolerance_pct: float
    equity_dust_threshold: float
