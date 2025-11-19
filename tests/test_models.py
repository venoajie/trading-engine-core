# tests/test_models.py

import json
from datetime import datetime

import pytest
from pydantic import ValidationError

from trading_engine_core.models import (
    CycleClosedEvent,
    CycleCreatedEvent,
    OrderFilledEvent,
    OrderSentEvent,
)

# --- Test Data Fixtures ---


@pytest.fixture
def sample_cycle_created_data():
    return {
        "strategy_name": "Momentum Scalper",
        "instrument_ticker": "AAPL",
        "initial_parameters": {"timeframe": "1m", "risk_per_trade": 0.01},
    }


@pytest.fixture
def sample_order_sent_data():
    return {
        "order_id": "broker-order-123",
        "order_type": "LIMIT",
        "side": "BUY",
        "quantity": 100.0,
        "price": 150.25,
    }


@pytest.fixture
def sample_order_filled_data():
    return {
        "order_id": "broker-order-123",
        "fill_price": 150.24,
        "fill_quantity": 100.0,
        "commission": 4.95,
        "timestamp": datetime.now(),
    }


@pytest.fixture
def sample_cycle_closed_data():
    return {
        "reason": "take_profit_hit",
        "final_pnl": 250.75,
    }


# --- Success Case Tests (Valid Data) ---


def test_cycle_created_event_success(sample_cycle_created_data):
    """Verify that a CycleCreatedEvent can be instantiated with valid data."""
    event = CycleCreatedEvent(**sample_cycle_created_data)
    assert event.strategy_name == sample_cycle_created_data["strategy_name"]
    assert event.instrument_ticker == sample_cycle_created_data["instrument_ticker"]
    assert event.initial_parameters["risk_per_trade"] == 0.01


def test_order_sent_event_success(sample_order_sent_data):
    """Verify that an OrderSentEvent can be instantiated with valid data."""
    event = OrderSentEvent(**sample_order_sent_data)
    assert event.order_id == sample_order_sent_data["order_id"]
    assert event.order_type == "LIMIT"
    assert event.side == "BUY"
    assert event.price == 150.25


def test_order_filled_event_success(sample_order_filled_data):
    """Verify that an OrderFilledEvent can be instantiated with valid data."""
    event = OrderFilledEvent(**sample_order_filled_data)
    assert event.fill_price == sample_order_filled_data["fill_price"]
    assert event.commission == 4.95
    assert isinstance(event.timestamp, datetime)


def test_cycle_closed_event_success(sample_cycle_closed_data):
    """Verify that a CycleClosedEvent can be instantiated with valid data."""
    event = CycleClosedEvent(**sample_cycle_closed_data)
    assert event.reason == sample_cycle_closed_data["reason"]
    assert event.final_pnl == 250.75


# --- Failure Case Tests (Invalid Data) ---


def test_cycle_created_event_missing_field(sample_cycle_created_data):
    """Verify that a ValidationError is raised if a required field is missing."""
    del sample_cycle_created_data["strategy_name"]
    with pytest.raises(ValidationError):
        CycleCreatedEvent(**sample_cycle_created_data)


def test_order_sent_event_invalid_literal(sample_order_sent_data):
    """Verify that a ValidationError is raised for an invalid Literal value."""
    sample_order_sent_data["side"] = "INVALID_SIDE"
    with pytest.raises(ValidationError) as excinfo:
        OrderSentEvent(**sample_order_sent_data)
    assert "Input should be 'BUY' or 'SELL'" in str(excinfo.value)


# --- Serialization Contract Test ---


def test_order_filled_event_serialization(sample_order_filled_data):
    """
    Verify that the event serializes to a correct JSON string, as this is the
    contract for data being passed to the database repository.
    """
    event = OrderFilledEvent(**sample_order_filled_data)
    json_output = event.model_dump_json()

    # Verify it's a valid JSON string
    data = json.loads(json_output)

    # Verify the content
    assert data["order_id"] == sample_order_filled_data["order_id"]
    assert data["fill_price"] == sample_order_filled_data["fill_price"]
    assert data["commission"] == sample_order_filled_data["commission"]
    # Pydantic serializes datetime to ISO 8601 format string by default
    assert isinstance(data["timestamp"], str)
