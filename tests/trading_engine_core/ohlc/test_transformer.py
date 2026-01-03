
# tests/trading_engine_core/ohlc/test_transformer.py

import pytest
from trading_engine_core.models import OHLCModel
from trading_engine_core.ohlc.transformer import transform_tv_data_to_ohlc_models


@pytest.fixture
def sample_valid_tv_data():
    """Provides a valid TradingView-style data dictionary for testing."""
    return {
        "ticks": [1672531200000, 1672531260000],  # Two 1-minute candles
        "open": [100.0, 105.0],
        "high": [110.0, 108.0],
        "low": [99.0, 104.0],
        "close": [105.0, 107.0],
        "volume": [1000.0, 500.0],
        "status": "ok",
    }


def test_transform_happy_path(sample_valid_tv_data):
    """
    Tests the transformer with ideal, valid data.
    """
    exchange = "binance"
    instrument = "BTCUSDT"
    resolution = "1"

    result = transform_tv_data_to_ohlc_models(
        sample_valid_tv_data, exchange, instrument, resolution
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(item, OHLCModel) for item in result)

    # Validate the first candle
    first_candle = result[0]
    assert first_candle.tick == 1672531200000
    assert first_candle.open == 100.0
    assert first_candle.high == 110.0
    assert first_candle.low == 99.0
    assert first_candle.close == 105.0
    assert first_candle.volume == 1000.0
    # No exchange/instrument on the base model anymore, they are passed separately
    # assert first_candle.exchange == exchange
    # assert first_candle.instrument_name == instrument
    assert first_candle.resolution == resolution


def test_transform_mismatched_lengths(sample_valid_tv_data):
    """
    Tests that the function returns an empty list if array lengths are inconsistent.
    """
    # Make 'volume' array one element shorter
    sample_valid_tv_data["volume"].pop()
    assert len(sample_valid_tv_data["ticks"]) != len(sample_valid_tv_data["volume"])

    result = transform_tv_data_to_ohlc_models(
        sample_valid_tv_data, "binance", "BTCUSDT", "1"
    )

    assert isinstance(result, list)
    assert len(result) == 0


def test_transform_empty_data_lists():
    """
    Tests that the function handles data with empty lists correctly.
    """
    empty_data = {
        "ticks": [], "open": [], "high": [], "low": [], "close": [], "volume": []
    }
    result = transform_tv_data_to_ohlc_models(empty_data, "binance", "BTCUSDT", "1")
    assert result == []


def test_transform_missing_key():
    """
    Tests that the function handles a missing key (e.g., 'volume') gracefully.
    """
    invalid_data = {
        "ticks": [1672531200000],
        "open": [100.0],
        "high": [110.0],
        "low": [99.0],
        "close": [105.0],
        # "volume" key is missing
    }
    # This will cause a TypeError when `tv_data.get("volume", [])` is used
    # on a non-existent key, resulting in None. The list check should handle it.
    result = transform_tv_data_to_ohlc_models(invalid_data, "binance", "BTCUSDT", "1")
    assert result == []


def test_transform_non_list_data():
    """
    Tests that the function returns an empty list if data values are not lists.
    """
    invalid_data = {"ticks": 12345, "open": 100.0}  # Should be lists
    result = transform_tv_data_to_ohlc_models(invalid_data, "binance", "BTCUSDT", "1")
    assert result == []


def test_transform_non_dict_input():
    """
    Tests robustness against the top-level input not being a dictionary.
    """
    result = transform_tv_data_to_ohlc_models(None, "binance", "BTCUSDT", "1")
    assert result == []

    result = transform_tv_data_to_ohlc_models([], "binance", "BTCUSDT", "1")
    assert result == []
