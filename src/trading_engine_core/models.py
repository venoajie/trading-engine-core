
# trading-engine-core/src/trading_engine_core/models.py

from datetime import datetime
from typing import Any, Literal, Optional, List

from pydantic import BaseModel, Field


class MarketDefinition(BaseModel):
    """
    Defines the connection and subscription details for a specific market.
    This is a core data contract used by configuration and service layers.
    """
    market_id: str = Field(..., description="Unique identifier for the market, e.g., 'deribit-main'.")
    exchange: str = Field(..., description="The name of the exchange, e.g., 'deribit'.")
    market_type: Literal["spot", "futures", "options"]
    ws_channels: List[str] = Field(default_factory=list, description="List of WebSocket channels to subscribe to.")
    ws_base_url: Optional[str] = Field(default=None, description="The base WebSocket URL for the exchange (hydrated at runtime).")
    rest_base_url: Optional[str] = Field(default=None, description="The base REST API URL for the exchange (hydrated at runtime).")


# --- Existing Models ---

class BaseEvent(BaseModel):
    """Base model for all trading events."""
    pass

class CycleCreatedEvent(BaseEvent):
    """
    Defines the data stored when a new trading cycle is initiated by the engine.
    This is the first event in any valid cycle.
    """

    strategy_name: str = Field(
        ..., description="The name of the strategy that initiated the cycle."
    )
    instrument_ticker: str = Field(
        ..., description="The ticker symbol of the instrument being traded."
    )
    initial_parameters: dict[str, Any] = Field(
        ..., description="A dictionary of strategy-specific parameters at cycle start."
    )


class OrderSentEvent(BaseEvent):
    """
    Defines the data stored when an order is sent to the broker API.
    """

    order_id: str = Field(
        ...,
        description="The unique identifier for the order, typically from the broker.",
    )
    order_type: Literal["MARKET", "LIMIT", "STOP"] = Field(
        ..., description="The type of order placed."
    )
    side: Literal["BUY", "SELL"] = Field(..., description="The side of the order.")
    quantity: float = Field(
        ..., description="The quantity of the instrument to be traded."
    )
    price: float | None = Field(
        default=None, description="The price for LIMIT or STOP orders."
    )


class OrderFilledEvent(BaseEvent):
    """
    Defines the data stored when an order execution confirmation is received.
    """

    order_id: str = Field(
        ..., description="The unique identifier of the order that was filled."
    )
    fill_price: float = Field(
        ..., description="The average price at which the order was executed."
    )
    fill_quantity: float = Field(..., description="The quantity that was executed.")
    commission: float = Field(
        default=0.0, description="The commission paid for the trade."
    )
    timestamp: datetime = Field(
        ..., description="The exact time of the trade execution from the broker."
    )


class CycleClosedEvent(BaseEvent):
    """
    Defines the data stored when a trading cycle is concluded.
    This is the final event in any valid cycle.
    """

    reason: str = Field(
        ...,
        description=(
            "The reason for closing the cycle (e.g., "
            "'take_profit_hit', 'manual_close')."
        ),
    )
    final_pnl: float = Field(
        ..., description="The final realized profit or loss for the entire cycle."
    )
