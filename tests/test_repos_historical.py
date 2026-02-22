"""Tests for HistoricalDataRepository (src/database/repos/historical.py)."""

EXPIRED_KEY = "NSE_FO|NIFTY24MAR25000CE"


def _setup_contract(tmp_db):
    """Helper: insert instrument + contract so historical data can reference it."""
    tmp_db.insert_instrument({"instrument_key": "NSE_INDEX|Nifty 50", "symbol": "Nifty 50"})
    tmp_db.contracts.insert_contracts(
        [
            {
                "instrument_key": EXPIRED_KEY,
                "underlying_key": "NSE_INDEX|Nifty 50",
                "expiry": "2024-03-28",
                "instrument_type": "CE",
                "strike_price": 25000,
                "trading_symbol": "NIFTY24MAR25000CE",
                "lot_size": 50,
                "tick_size": 0.05,
                "exchange_token": "12345",
            }
        ]
    )


def _sample_candles():
    return [
        ["2024-03-28T09:15:00", 100.0, 105.0, 99.0, 103.0, 1000, 500],
        ["2024-03-28T09:30:00", 103.0, 107.0, 102.0, 106.0, 800, 450],
        ["2024-03-28T09:45:00", 106.0, 110.0, 105.0, 109.0, 1200, 600],
    ]


def test_insert_historical_data(tmp_db):
    """Insert candles list, verify count returned."""
    _setup_contract(tmp_db)
    candles = _sample_candles()
    count = tmp_db.historical.insert_historical_data(EXPIRED_KEY, candles)
    assert count == 3


def test_insert_marks_data_fetched(tmp_db):
    """Insert data, verify contract marked data_fetched=TRUE."""
    _setup_contract(tmp_db)
    tmp_db.historical.insert_historical_data(EXPIRED_KEY, _sample_candles())

    with tmp_db.get_connection() as conn:
        row = conn.execute(
            "SELECT data_fetched FROM contracts WHERE expired_instrument_key = ?",
            (EXPIRED_KEY,),
        ).fetchone()
    assert row[0] is True


def test_mark_contract_no_data(tmp_db):
    """Mark as no_data, verify flags."""
    _setup_contract(tmp_db)
    tmp_db.historical.mark_contract_no_data(EXPIRED_KEY)

    with tmp_db.get_connection() as conn:
        row = conn.execute(
            "SELECT data_fetched, no_data FROM contracts WHERE expired_instrument_key = ?",
            (EXPIRED_KEY,),
        ).fetchone()
    assert row[0] is True  # data_fetched
    assert row[1] is True  # no_data


def test_get_historical_data(tmp_db):
    """Insert then retrieve, verify ordering."""
    _setup_contract(tmp_db)
    tmp_db.historical.insert_historical_data(EXPIRED_KEY, _sample_candles())

    data = tmp_db.historical.get_historical_data(EXPIRED_KEY)
    assert len(data) == 3
    # Verify ascending timestamp order
    timestamps = [row[0] for row in data]
    assert timestamps == sorted(timestamps)


def test_get_historical_data_count(tmp_db):
    """Insert, verify count matches."""
    _setup_contract(tmp_db)
    tmp_db.historical.insert_historical_data(EXPIRED_KEY, _sample_candles())

    count = tmp_db.historical.get_historical_data_count(EXPIRED_KEY)
    assert count == 3

    # Total count (no filter) should also be 3
    total = tmp_db.historical.get_historical_data_count()
    assert total == 3
