"""Tests for InstrumentMasterRepository (src/database/repos/instrument_master.py)."""


def test_get_instrument_master_count_empty(tmp_db):
    """Empty DB returns 0."""
    count = tmp_db.instrument_master.get_instrument_master_count()
    assert count == 0


def test_get_instrument_master_segments(tmp_db):
    """Empty returns empty list."""
    segments = tmp_db.instrument_master.get_instrument_master_segments()
    assert segments == []


def test_get_instrument_types_by_segment(tmp_db):
    """Empty returns empty list."""
    types = tmp_db.instrument_master.get_instrument_types_by_segment("NSE_FO")
    assert types == []


def test_get_instrument_master_count_with_segment(tmp_db):
    """With segment filter on empty DB returns 0."""
    count = tmp_db.instrument_master.get_instrument_master_count(segment="NSE_EQ")
    assert count == 0

    # Insert some data and verify count with filter
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|TEST1",
                "trading_symbol": "TEST1",
                "name": "Test 1",
                "exchange": "NSE",
                "segment": "NSE_EQ",
                "instrument_type": "EQ",
                "isin": None,
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            },
            {
                "instrument_key": "NSE_FO|TEST2",
                "trading_symbol": "TEST2",
                "name": "Test 2",
                "exchange": "NSE",
                "segment": "NSE_FO",
                "instrument_type": "FUT",
                "isin": None,
                "lot_size": 50,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            },
        ]
    )
    tmp_db.instrument_master.bulk_insert_instrument_master(df)

    eq_count = tmp_db.instrument_master.get_instrument_master_count(segment="NSE_EQ")
    assert eq_count == 1

    fo_count = tmp_db.instrument_master.get_instrument_master_count(segment="NSE_FO")
    assert fo_count == 1

    total = tmp_db.instrument_master.get_instrument_master_count()
    assert total == 2
