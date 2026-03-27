import pandas as pd
import pytest
from src.transform import compute_indicators


def test_daily_return():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5),
        "close": [100.0, 102.0, 101.0, 103.0, 105.0],
        "symbol": "TEST"
    })
    result = compute_indicators(df)
    assert round(result["daily_return"].iloc[1], 4) == 0.02


def test_daily_return_first_row_is_null():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5),
        "close": [100.0, 102.0, 101.0, 103.0, 105.0],
        "symbol": "TEST"
    })
    result = compute_indicators(df)
    assert pd.isna(result["daily_return"].iloc[0])

def test_ma_7():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=20),
        "close": [float(i * 2 + 100) for i in range(20)],
        "symbol": "TEST"
    })
    result = compute_indicators(df)
    expected = sum([100, 102, 104, 106, 108, 110, 112]) / 7
    assert round(result["ma_7"].iloc[6], 4) == round(expected, 4)

def test_rsi_not_null_after_14_rows():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=20),
        "close": [float(i * 2 + 100) for i in range(20)],
        "symbol": "TEST"
    })
    result = compute_indicators(df)
    assert pd.notna(result["rsi_14"].iloc[14])
    assert pd.isna(result["rsi_14"].iloc[0])
