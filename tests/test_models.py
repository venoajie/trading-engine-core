from datetime import datetime

import pytest
from pydantic import ValidationError

from trading_engine_core.models import CycleCreatedEvent, MarketDefinition, OHLCModel, OrderFilledEvent

# --- Fixtures ---


@pytest.fixture
def sample_cycle_created():
    return {"strategy_name": "Strat1", "instrument_ticker": "AAPL", "initial_parameters": {"p": 1}}


@pytest.fixture
def sample_order_filled():
    return {
        "order_id": "ord_1",
        "fill_price": 100.0,
        "fill_quantity": 10,
        "commission": 1.0,
        "timestamp": datetime.now(),
    }


# --- Model Tests ---


def test_market_definition():
    """Test MarketDefinition model validation."""
    # Valid
    md = MarketDefinition(market_id="m1", exchange="ex", market_type="spot", symbols=["BTC"], ws_channels=["ch1"])
    assert md.market_id == "m1"

    # Invalid Type
    with pytest.raises(ValidationError):
        MarketDefinition(market_id="m1", exchange="ex", market_type="INVALID")


def test_ohlc_model():
    """Test OHLC Data Contract."""
    data = {"tick": 1600000000, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000}
    ohlc = OHLCModel(**data)
    assert ohlc.close == 105.0


def test_cycle_events(sample_cycle_created, sample_order_filled):
    """Test event sourcing models."""
    # Cycle Created
    ev1 = CycleCreatedEvent(**sample_cycle_created)
    assert ev1.strategy_name == "Strat1"

    # Order Filled
    ev2 = OrderFilledEvent(**sample_order_filled)
    assert ev2.fill_quantity == 10.0

    # Serialization check
    json_str = ev2.model_dump_json()
    assert "ord_1" in json_str
