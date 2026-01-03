
# tests/trading_engine_core/test_models.py

import pytest
from pydantic import ValidationError

from trading_engine_core.models import OHLCModel, SignalEvent, StreamMessage
from trading_engine_core.enums import MarketType


def test_ohlc_model_instantiation_happy_path():
    """
    Validates that OHLCModel can be created with all required fields
    and that new microstructure fields have correct defaults.
    """
    tick_ms = 1672531200000  # 2023-01-01 00:00:00 UTC

    candle = OHLCModel(
        tick=tick_ms,
        open=100.0,
        high=110.0,
        low=99.0,
        close=105.0,
        volume=1000.0,
        taker_buy_volume=600.0,
        taker_sell_volume=400.0,
        instrument_name="BTC-PERPETUAL",
        resolution="1",
    )

    assert candle.tick == tick_ms
    assert candle.high == 110.0
    assert candle.taker_buy_volume == 600.0
    assert candle.taker_sell_volume == 400.0
    assert candle.instrument_name == "BTC-PERPETUAL"


def test_ohlc_model_default_volume_fields():
    """
    Ensures that if microstructure volumes are not provided, they default to 0.0.
    """
    candle = OHLCModel(
        tick=1672531200000,
        open=100.0,
        high=110.0,
        low=99.0,
        close=105.0,
        volume=1000.0,
    )

    assert candle.taker_buy_volume == 0.0
    assert candle.taker_sell_volume == 0.0


def test_ohlc_model_missing_required_fields():
    """
    Verifies that Pydantic raises a ValidationError if required fields are missing.
    """
    with pytest.raises(ValidationError) as excinfo:
        OHLCModel(
            tick=1672531200000,
            # 'open' is missing
            high=110.0,
            low=99.0,
            close=105.0,
            volume=1000.0,
        )
    # Check that the error message contains the name of the missing field
    assert "open" in str(excinfo.value)


def test_stream_message_model():
    """
    Validates the basic structure of the StreamMessage data contract.
    """
    msg = StreamMessage(
        channel="trade.BTC-PERPETUAL",
        exchange="deribit",
        timestamp=1672531200123,
        data={"price": 50000, "amount": 0.1},
    )
    assert msg.exchange == "deribit"
    assert msg.data["price"] == 50000


def test_signal_event_model():
    """
    Validates the basic structure of the SignalEvent data contract.
    """
    event = SignalEvent(
        timestamp=1672531200.5,
        strategy_name="volumeSpike",
        symbol="BTCUSDT",
        exchange="binance",
        signal_type="LONG",
        strength=0.85,
        metadata={"rvol": 5.2, "source": "test"},
    )
    assert event.strategy_name == "volumeSpike"
    assert event.strength == 0.85
    assert event.metadata["rvol"] == 5.2