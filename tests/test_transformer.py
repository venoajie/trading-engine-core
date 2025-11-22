from trading_engine_core.ohlc.transformer import transform_tv_data_to_ohlc_models


def test_transformer_success():
    tv_data = {
        "ticks": [1000, 2000],
        "open": [1, 2],
        "high": [3, 4],
        "low": [0, 1],
        "close": [2, 3],
        "volume": [10, 20],
    }
    models = transform_tv_data_to_ohlc_models(tv_data, "ex", "inst", "1m")
    assert len(models) == 2
    assert models[0].tick == 1000
    assert models[1].close == 3


def test_transformer_invalid_input():
    # Not a dict
    assert transform_tv_data_to_ohlc_models("bad", "ex", "i", "1") == []

    # Mismatched lengths
    tv_bad = {
        "ticks": [1],
        "open": [1, 2],  # Length 2
        "high": [1],
        "low": [1],
        "close": [1],
        "volume": [1],
    }
    assert transform_tv_data_to_ohlc_models(tv_bad, "ex", "i", "1") == []


def test_transformer_missing_keys():
    # Missing 'open'
    tv_missing = {"ticks": [1]}
    assert transform_tv_data_to_ohlc_models(tv_missing, "ex", "i", "1") == []


def test_transformer_exception_handling():
    """Test exception inside the loop (e.g. bad data in list)."""
    from trading_engine_core.ohlc.transformer import transform_tv_data_to_ohlc_models

    tv_data = {
        "ticks": [1000],
        "open": ["not_a_float"],  # Will raise validation error in Pydantic
        "high": [1],
        "low": [1],
        "close": [1],
        "volume": [1],
    }

    # Pydantic raising ValidationError inside the loop is caught by
    # except (TypeError, IndexError, ValueError)
    # Wait, ValidationError inherits from ValueError in Pydantic v2? No.
    # We need to check if the code handles Pydantic validation errors.
    # The code catches (TypeError, IndexError, ValueError).
    # Pydantic ValidationError does NOT inherit from these.
    # So this might crash if not handled. Let's see.

    # Actually, "not_a_float" passed to float field might raise a TypeError/ValueError
    # during Pydantic validation or casting?

    # If the transformer fails, it returns [].
    res = transform_tv_data_to_ohlc_models(tv_data, "ex", "i", "1")
    assert res == []
