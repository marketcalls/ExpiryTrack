"""Tests for ContractRepository (src/database/repos/contracts.py)."""


def _insert_base_instrument(tmp_db):
    """Helper: insert the base instrument needed for expiries and contracts."""
    tmp_db.insert_instrument({"instrument_key": "NSE_INDEX|Nifty 50", "symbol": "Nifty 50"})


def _make_contract(
    expired_key="NSE_FO|NIFTY24MAR25000CE",
    underlying_key="NSE_INDEX|Nifty 50",
    expiry="2024-03-28",
    instrument_type="CE",
    strike_price=25000,
    trading_symbol="NIFTY24MAR25000CE",
):
    return {
        "instrument_key": expired_key,
        "underlying_key": underlying_key,
        "expiry": expiry,
        "instrument_type": instrument_type,
        "strike_price": strike_price,
        "trading_symbol": trading_symbol,
        "lot_size": 50,
        "tick_size": 0.05,
        "exchange_token": "12345",
    }


def test_insert_expiries(tmp_db):
    """Insert expiries, verify count."""
    _insert_base_instrument(tmp_db)
    count = tmp_db.contracts.insert_expiries("NSE_INDEX|Nifty 50", ["2024-03-28", "2024-04-25", "2024-05-30"])
    assert count == 3


def test_insert_expiries_dedup(tmp_db):
    """Insert same expiries twice, second call adds 0."""
    _insert_base_instrument(tmp_db)
    dates = ["2024-03-28", "2024-04-25"]

    count1 = tmp_db.contracts.insert_expiries("NSE_INDEX|Nifty 50", dates)
    assert count1 == 2

    count2 = tmp_db.contracts.insert_expiries("NSE_INDEX|Nifty 50", dates)
    assert count2 == 0


def test_get_pending_expiries(tmp_db):
    """Insert expiry, verify pending, mark fetched, verify empty."""
    _insert_base_instrument(tmp_db)
    tmp_db.contracts.insert_expiries("NSE_INDEX|Nifty 50", ["2024-03-28"])

    pending = tmp_db.contracts.get_pending_expiries("NSE_INDEX|Nifty 50")
    assert len(pending) == 1
    assert str(pending[0]["expiry_date"]) == "2024-03-28"

    tmp_db.contracts.mark_expiry_contracts_fetched("NSE_INDEX|Nifty 50", "2024-03-28")

    pending_after = tmp_db.contracts.get_pending_expiries("NSE_INDEX|Nifty 50")
    assert len(pending_after) == 0


def test_insert_contracts(tmp_db):
    """Insert contracts with required fields, verify count."""
    _insert_base_instrument(tmp_db)
    contracts = [_make_contract()]
    count = tmp_db.contracts.insert_contracts(contracts)
    assert count == 1

    with tmp_db.get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    assert total == 1


def test_get_pending_contracts(tmp_db):
    """Insert contract with data_fetched=FALSE, fetch pending."""
    _insert_base_instrument(tmp_db)
    tmp_db.contracts.insert_contracts([_make_contract()])

    pending = tmp_db.contracts.get_pending_contracts()
    assert len(pending) == 1
    assert pending[0]["data_fetched"] is False


def test_get_fetched_keys(tmp_db):
    """Insert contract, mark as fetched via historical insert, verify in fetched set."""
    _insert_base_instrument(tmp_db)
    expired_key = "NSE_FO|NIFTY24MAR25000CE"
    tmp_db.contracts.insert_contracts([_make_contract(expired_key=expired_key)])

    # Mark as fetched by updating directly
    with tmp_db.get_connection() as conn:
        conn.execute(
            "UPDATE contracts SET data_fetched = TRUE WHERE expired_instrument_key = ?",
            (expired_key,),
        )

    fetched = tmp_db.contracts.get_fetched_keys([expired_key])
    assert expired_key in fetched


def test_reset_fetch_attempts(tmp_db):
    """Increment fetch attempts, reset, verify reset."""
    _insert_base_instrument(tmp_db)
    expired_key = "NSE_FO|NIFTY24MAR25000CE"
    tmp_db.contracts.insert_contracts([_make_contract(expired_key=expired_key)])

    # Increment attempts
    tmp_db.contracts.increment_fetch_attempt(expired_key)
    tmp_db.contracts.increment_fetch_attempt(expired_key)

    # Verify attempts > 0
    with tmp_db.get_connection() as conn:
        row = conn.execute(
            "SELECT fetch_attempts FROM contracts WHERE expired_instrument_key = ?",
            (expired_key,),
        ).fetchone()
    assert row[0] == 2

    # Reset
    reset_count = tmp_db.contracts.reset_fetch_attempts()
    assert reset_count == 1

    # Verify reset
    with tmp_db.get_connection() as conn:
        row = conn.execute(
            "SELECT fetch_attempts FROM contracts WHERE expired_instrument_key = ?",
            (expired_key,),
        ).fetchone()
    assert row[0] == 0


def test_mark_expiry_contracts_fetched(tmp_db):
    """Insert expiry, mark as fetched, verify."""
    _insert_base_instrument(tmp_db)
    tmp_db.contracts.insert_expiries("NSE_INDEX|Nifty 50", ["2024-03-28"])

    tmp_db.contracts.mark_expiry_contracts_fetched("NSE_INDEX|Nifty 50", "2024-03-28")

    pending = tmp_db.contracts.get_pending_expiries("NSE_INDEX|Nifty 50")
    assert len(pending) == 0
