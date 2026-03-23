"""
Tests for DatabaseManager — core database operations.
"""

import pandas as pd


def test_init_creates_tables(tmp_db):
    """Verify all core tables exist after init."""
    with tmp_db.get_connection() as conn:
        rows = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        table_names = {row[0] for row in rows}

    expected = {
        "credentials",
        "default_instruments",
        "instruments",
        "expiries",
        "contracts",
        "historical_data",
        "job_status",
        "instrument_master",
        "candle_data",
        "watchlists",
        "watchlist_items",
        "candle_collection_status",
    }
    assert expected.issubset(table_names), f"Missing tables: {expected - table_names}"


def test_save_and_get_credentials(tmp_db):
    """Round-trip save and retrieve credentials."""
    tmp_db.save_credentials("test_key", "test_secret", "http://localhost/callback")
    creds = tmp_db.get_credentials()
    assert creds is not None
    assert creds["api_key"] == "test_key"
    assert creds["api_secret"] == "test_secret"
    assert creds["redirect_uri"] == "http://localhost/callback"


def test_save_token(tmp_db):
    """Token persistence after save."""
    tmp_db.save_credentials("key", "secret")
    tmp_db.save_token("access_token_123", 9999999999.0)
    creds = tmp_db.get_credentials()
    assert creds["access_token"] == "access_token_123"
    assert creds["token_expiry"] == 9999999999.0


def test_setup_default_instruments(tmp_db):
    """Default instruments are created idempotently."""
    tmp_db.setup_default_instruments()
    instruments = tmp_db.get_default_instruments()
    assert len(instruments) >= 6

    # Call again — should not duplicate
    tmp_db.setup_default_instruments()
    instruments2 = tmp_db.get_default_instruments()
    assert len(instruments2) == len(instruments)


def test_get_default_instruments(tmp_db):
    """get_default_instruments returns list of strings."""
    tmp_db.setup_default_instruments()
    instruments = tmp_db.get_default_instruments()
    assert isinstance(instruments, list)
    assert all(isinstance(i, str) for i in instruments)
    assert "NSE_INDEX|Nifty 50" in instruments


def test_add_instrument(tmp_db):
    """Insert and retrieve a custom instrument."""
    new_id = tmp_db.add_instrument("NSE_EQ|RELIANCE", "RELIANCE", priority=50, category="Stock F&O")
    assert new_id is not None
    instruments = tmp_db.get_active_instruments()
    keys = [i["instrument_key"] for i in instruments]
    assert "NSE_EQ|RELIANCE" in keys


def test_toggle_instrument(tmp_db):
    """Toggle instrument active status."""
    new_id = tmp_db.add_instrument("NSE_EQ|TCS", "TCS")
    tmp_db.toggle_instrument(new_id, False)
    instruments = tmp_db.get_active_instruments()
    tcs = next((i for i in instruments if i["instrument_key"] == "NSE_EQ|TCS"), None)
    assert tcs is not None
    assert tcs["is_active"] is False


def test_remove_instrument(tmp_db):
    """Delete instrument by ID."""
    new_id = tmp_db.add_instrument("NSE_EQ|INFY", "INFY")
    tmp_db.remove_instrument(new_id)
    instruments = tmp_db.get_active_instruments()
    keys = [i["instrument_key"] for i in instruments]
    assert "NSE_EQ|INFY" not in keys


def test_insert_expiries(tmp_db):
    """Bulk insert expiry dates."""
    # First insert the instrument
    tmp_db.insert_instrument(
        {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "symbol": "Nifty 50",
        }
    )
    count = tmp_db.insert_expiries("NSE_INDEX|Nifty 50", ["2025-01-30", "2025-02-27", "2025-03-27"])
    assert count == 3


def test_get_pending_expiries(tmp_db):
    """Filter pending expiries by instrument."""
    tmp_db.insert_instrument(
        {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "symbol": "Nifty 50",
        }
    )
    tmp_db.insert_expiries("NSE_INDEX|Nifty 50", ["2025-01-30", "2025-02-27"])
    pending = tmp_db.get_pending_expiries("NSE_INDEX|Nifty 50")
    assert len(pending) == 2
    assert all(p["contracts_fetched"] is False for p in pending)


def test_insert_contracts(tmp_db):
    """Insert contracts and deduplicate on expired_key."""
    tmp_db.insert_instrument(
        {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "symbol": "Nifty 50",
        }
    )
    contracts = [
        {
            "instrument_key": "NSE_FO|NIFTY25130CE",
            "underlying_key": "NSE_INDEX|Nifty 50",
            "expiry": "2025-01-30",
            "instrument_type": "CE",
            "strike_price": 25000,
            "trading_symbol": "NIFTY25JAN25000CE",
        },
    ]
    count1 = tmp_db.insert_contracts(contracts)
    assert count1 == 1

    # Insert same contract again — should be skipped (INSERT OR IGNORE)
    tmp_db.insert_contracts(contracts)
    # May return 1 due to DuckDB INSERT OR IGNORE behavior, but no duplicate
    with tmp_db.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    assert total == 1


def test_get_summary_stats(tmp_db):
    """Summary stats returns dict with expected keys."""
    stats = tmp_db.get_summary_stats()
    expected_keys = {
        "total_instruments",
        "total_expiries",
        "total_contracts",
        "total_candles",
        "pending_expiries",
        "pending_contracts",
        "master_instruments",
        "total_candle_data",
    }
    assert expected_keys.issubset(stats.keys())
    assert all(isinstance(v, int) for v in stats.values())


def test_bulk_insert_instrument_master(tmp_db):
    """DataFrame insert into instrument_master."""
    df = pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|INE002A01018",
                "trading_symbol": "RELIANCE",
                "name": "Reliance Industries",
                "exchange": "NSE",
                "segment": "NSE_EQ",
                "instrument_type": "EQ",
                "isin": "INE002A01018",
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            },
        ]
    )
    count = tmp_db.bulk_insert_instrument_master(df)
    assert count == 1


def test_get_instrument_master_segments(tmp_db):
    """Returns segment list from instrument_master."""
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
        ]
    )
    tmp_db.bulk_insert_instrument_master(df)
    segments = tmp_db.get_instrument_master_segments()
    assert len(segments) >= 1
    assert any(s["segment"] == "NSE_EQ" for s in segments)


def test_get_instrument_types_by_segment(tmp_db):
    """Returns distinct types for a segment."""
    df = pd.DataFrame(
        [
            {
                "instrument_key": f"NSE_FO|TEST_{t}",
                "trading_symbol": f"TEST_{t}",
                "name": f"Test {t}",
                "exchange": "NSE",
                "segment": "NSE_FO",
                "instrument_type": t,
                "isin": None,
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            }
            for t in ["FUT", "CE", "PE"]
        ]
    )
    tmp_db.bulk_insert_instrument_master(df)
    types = tmp_db.get_instrument_types_by_segment("NSE_FO")
    assert set(types) == {"CE", "FUT", "PE"}
