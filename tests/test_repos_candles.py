"""Tests for CandleRepository (src/database/repos/candles.py)."""

INSTRUMENT_KEY = "NSE_EQ|RELIANCE"


def _sample_candles():
    return [
        ["2024-03-25T09:15:00", 2800.0, 2850.0, 2790.0, 2840.0, 50000, 0],
        ["2024-03-26T09:15:00", 2840.0, 2880.0, 2830.0, 2870.0, 45000, 0],
        ["2024-03-27T09:15:00", 2870.0, 2900.0, 2860.0, 2890.0, 48000, 0],
    ]


def test_insert_candle_data(tmp_db):
    """Insert candles, verify count."""
    count = tmp_db.candles.insert_candle_data(INSTRUMENT_KEY, _sample_candles())
    assert count == 3


def test_get_candle_data(tmp_db):
    """Insert then retrieve, verify data."""
    tmp_db.candles.insert_candle_data(INSTRUMENT_KEY, _sample_candles())

    data = tmp_db.candles.get_candle_data(INSTRUMENT_KEY)
    assert len(data) == 3
    # Each row should be a dict with OHLCV fields
    first = data[0]
    assert "open" in first
    assert "high" in first
    assert "low" in first
    assert "close" in first
    assert "volume" in first


def test_get_candle_data_with_date_filter(tmp_db):
    """Insert multiple dates, filter by from_date/to_date."""
    tmp_db.candles.insert_candle_data(INSTRUMENT_KEY, _sample_candles())

    # Filter: only 2024-03-26
    data = tmp_db.candles.get_candle_data(
        INSTRUMENT_KEY,
        from_date="2024-03-26T00:00:00",
        to_date="2024-03-26T23:59:59",
    )
    assert len(data) == 1


def test_get_candle_data_count(tmp_db):
    """Insert, verify total count."""
    tmp_db.candles.insert_candle_data(INSTRUMENT_KEY, _sample_candles())

    count = tmp_db.candles.get_candle_data_count(INSTRUMENT_KEY)
    assert count == 3

    # Total without filter
    total = tmp_db.candles.get_candle_data_count()
    assert total == 3


def test_get_candle_analytics_summary(tmp_db):
    """Insert data, verify summary structure."""
    tmp_db.candles.insert_candle_data(INSTRUMENT_KEY, _sample_candles())

    summary = tmp_db.candles.get_candle_analytics_summary()
    assert "total_candles" in summary
    assert "instruments_with_data" in summary
    assert "earliest_date" in summary
    assert "latest_date" in summary
    assert "instruments" in summary
    assert summary["total_candles"] == 3
    assert summary["instruments_with_data"] == 1
