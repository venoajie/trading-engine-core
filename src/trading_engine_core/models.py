# ~/trading-engine-core/src/trading_engine_core/models.py

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from .enums import MarketType

# --- Base Configuration ---


class AppBaseModel(BaseModel):
    """Base model for all application data contracts."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",  # Default to ignoring extra fields to prevent crashes on API updates
    )


# --- Configuration Models (Used by shared-config) ---


class MarketDefinition(AppBaseModel):
    """
    Defines the connection and subscription details for a specific market.
    Merged from local and library versions.
    """

    market_id: str = Field(..., description="Unique identifier, e.g., 'deribit-main'.")
    exchange: str = Field(..., description="Exchange name, e.g., 'deribit'.")
    market_type: MarketType
    mode: str = Field(default="live", description="Operational mode: 'live', 'paper', 'backtest'.")

    # Subscription details (Keep these fields as they are)
    symbols: list[str] = Field(default_factory=list, description="Specific symbols to subscribe to.")
    ws_channels: list[str] = Field(default_factory=list, description="Raw WebSocket channel names.")

    # Hydrated fields (populated at runtime by config loader)
    ws_base_url: str | None = Field(default=None, description="Hydrated WebSocket URL.")
    rest_base_url: str | None = Field(default=None, description="Hydrated REST API URL.")
    

# --- Data Stream Models ---


class OHLCModel(AppBaseModel):
    """Standard Open-High-Low-Close candle data."""

    tick: int = Field(..., description="Unix timestamp in milliseconds.")
    open: float
    high: float
    low: float
    close: float
    volume: float
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
    """
    A validated model for a financial instrument.
    """

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
    """
    Represents the state of an order in the system.
    """

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
