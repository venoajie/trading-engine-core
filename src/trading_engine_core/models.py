# trading_engine_core/models.py

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .enums import MarketType

# --- Base Configuration ---


class AppBaseModel(BaseModel):
    """Base model for all application data contracts."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )


# --- Configuration Models (Used by shared-config) ---


class MarketDefinition(AppBaseModel):
    """
    Defines the connection and subscription details for a specific market.
    This is the authoritative model.
    """

    market_id: str = Field(..., description="Unique identifier, e.g., 'deribit-main'.")
    exchange: str = Field(..., description="Exchange name, e.g., 'deribit'.")
    market_type: MarketType

    output_stream_name: str = Field(..., description="The Redis stream for this market's data, e.g., 'market:stream:binance:trades'.")

    mode: str = Field(default="live", description="Operational mode: 'live', 'paper', 'backtest'.")
    symbols: list[str] = Field(default_factory=list, description="Specific symbols to subscribe to.")
    ws_channels: list[str] = Field(default_factory=list, description="Raw WebSocket channel names.")
    ws_base_url: str | None = Field(default=None, description="Hydrated WebSocket URL.")
    rest_base_url: str | None = Field(default=None, description="Hydrated REST API URL.")


# --- Data Stream Models ---


class OHLCModel(AppBaseModel):
    """
    Standard Open-High-Low-Close candle data.
    Updated for Project Microstructure Alpha to include granular volume data.
    """

    tick: int = Field(..., description="Unix timestamp in milliseconds.")
    open: float
    high: float
    low: float
    close: float
    volume: float = Field(..., description="Total volume (taker_buy + taker_sell).")

    # --- Microstructure Metrics ---
    taker_buy_volume: float = Field(default=0.0, description="Volume where buyer was taker (Aggressive Buy).")
    taker_sell_volume: float = Field(default=0.0, description="Volume where seller was taker (Aggressive Sell).")

    open_interest: float | None = None
    instrument_name: str | None = None
    resolution: str | None = None


class StreamMessage(AppBaseModel):
    """Standard wrapper for incoming WebSocket messages."""

    channel: str
    exchange: str
    timestamp: int
    data: dict[str, Any]


# --- Trading Entity Models ---


class InstrumentModel(AppBaseModel):
    """A validated model for a financial instrument."""

    exchange: str
    instrument_name: str
    market_type: str
    instrument_kind: str
    base_asset: str
    quote_asset: str
    settlement_asset: str
    settlement_period: str | None = None
    tick_size: float | None = None
    contract_size: float | None = None
    expiration_timestamp: datetime | None = None
    data: dict[str, Any] = Field(default_factory=dict, description="Raw exchange payload.")


class OrderModel(AppBaseModel):
    """Represents the state of an order in the system."""

    order_id: str
    instrument_name: str
    order_state: str
    direction: str
    price: float
    amount: float
    label: str | None = None
    trade_id: str | None = None
    take_profit: str | None = None
    stop_loss: str | None = None
    timestamp: int
    last_update_timestamp: int
    creation_timestamp: int
    model_config = ConfigDict(extra="allow")


class MarginCalculationResult(AppBaseModel):
    """Output of PME/Risk calculations."""

    initial_margin: float
    maintenance_margin: float
    is_valid: bool
    error_message: str | None = None


# --- Event Sourcing Models (The Immutable Log) ---


class BaseEvent(AppBaseModel):
    """Abstract base for event-sourced activities."""

    pass


class CycleCreatedEvent(BaseEvent):
    strategy_name: str
    instrument_ticker: str
    initial_parameters: dict[str, Any]


class OrderSentEvent(BaseEvent):
    order_id: str
    order_type: Literal["MARKET", "LIMIT", "STOP"]
    side: Literal["BUY", "SELL"]
    quantity: float
    price: float | None = None


class OrderFilledEvent(BaseEvent):
    order_id: str
    fill_price: float
    fill_quantity: float
    commission: float = 0.0
    timestamp: datetime


class CycleStateUpdatedEvent(BaseEvent):
    """
    [NEW] An event representing an internal state transition of a TradeCycle.
    This makes the application's internal "thinking" process an auditable event.
    """

    previous_status: str
    new_status: str
    reason: str


class CycleClosedEvent(BaseEvent):
    reason: str
    final_pnl: float


# --- Notification Models ---


class TradeNotification(AppBaseModel):
    """A structured model for a trade notification."""

    direction: str
    amount: float
    instrument_name: str
    price: float


class SystemAlert(AppBaseModel):
    """A structured model for a system-level alert."""

    component: str
    event: str
    details: str
    severity: Literal["INFO", "WARNING", "CRITICAL"] = "CRITICAL"


class SignalEvent(BaseModel):
    timestamp: float
    strategy_name: str
    symbol: str
    exchange: str
    signal_type: str  # "LONG", "SHORT", "ALERT"
    strength: float  # 0.0 to 1.0 (RVOL / Max_RVOL)
    metadata: dict[str, Any]  # {"rvol": 5.2, "delta": 0.4, "venues": [...]}
