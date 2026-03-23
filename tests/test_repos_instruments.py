"""Tests for InstrumentRepository (src/database/repos/instruments.py)."""


def test_setup_default_instruments_idempotent(tmp_db):
    """Call setup twice, same count."""
    tmp_db.instruments.setup_default_instruments()
    instruments1 = tmp_db.instruments.get_active_instruments()

    tmp_db.instruments.setup_default_instruments()
    instruments2 = tmp_db.instruments.get_active_instruments()

    assert len(instruments1) == len(instruments2)
    assert len(instruments1) >= 6


def test_add_instrument(tmp_db):
    """Add new instrument, verify returned ID."""
    new_id = tmp_db.instruments.add_instrument("NSE_EQ|RELIANCE", "RELIANCE", priority=50, category="Stock F&O")
    assert new_id is not None
    assert isinstance(new_id, int)

    instruments = tmp_db.instruments.get_active_instruments()
    keys = [i["instrument_key"] for i in instruments]
    assert "NSE_EQ|RELIANCE" in keys


def test_add_instrument_duplicate(tmp_db):
    """Add same instrument twice, second returns None."""
    first_id = tmp_db.instruments.add_instrument("NSE_EQ|TCS", "TCS")
    assert first_id is not None

    second_id = tmp_db.instruments.add_instrument("NSE_EQ|TCS", "TCS")
    assert second_id is None


def test_toggle_instrument(tmp_db):
    """Add instrument, toggle inactive, verify in active list."""
    new_id = tmp_db.instruments.add_instrument("NSE_EQ|INFY", "INFY")
    assert new_id is not None

    tmp_db.instruments.toggle_instrument(new_id, False)

    instruments = tmp_db.instruments.get_active_instruments()
    infy = next((i for i in instruments if i["instrument_key"] == "NSE_EQ|INFY"), None)
    assert infy is not None
    assert infy["is_active"] is False


def test_remove_instrument(tmp_db):
    """Add instrument, remove it, verify gone."""
    new_id = tmp_db.instruments.add_instrument("NSE_EQ|HDFC", "HDFC")
    assert new_id is not None

    tmp_db.instruments.remove_instrument(new_id)

    instruments = tmp_db.instruments.get_active_instruments()
    keys = [i["instrument_key"] for i in instruments]
    assert "NSE_EQ|HDFC" not in keys


def test_insert_instrument(tmp_db):
    """Insert into instruments table (not default_instruments)."""
    result = tmp_db.instruments.insert_instrument({
        "instrument_key": "NSE_FO|NIFTY",
        "symbol": "NIFTY",
        "name": "Nifty 50",
        "exchange": "NSE",
        "segment": "NSE_FO",
        "underlying_type": "INDEX",
    })
    assert result is True


def test_get_default_instruments(tmp_db):
    """Get default instrument keys."""
    tmp_db.instruments.setup_default_instruments()
    keys = tmp_db.instruments.get_default_instruments()
    assert isinstance(keys, list)
    assert len(keys) >= 1


def test_get_fo_underlying_instruments_empty(tmp_db):
    """No instrument master data → empty result."""
    result = tmp_db.instruments.get_fo_underlying_instruments()
    assert isinstance(result, list)
    assert len(result) == 0


def test_get_fo_available_instruments_empty(tmp_db):
    """No F&O instruments available."""
    result = tmp_db.instruments.get_fo_available_instruments()
    assert isinstance(result, list)


def test_bulk_import_fo_instruments_empty(tmp_db):
    """Bulk import with no data."""
    result = tmp_db.instruments.bulk_import_fo_instruments()
    assert result["added"] == 0
    assert result["total_available"] == 0
