"""Tests for AnalyticsEngine — dashboard summary, charts, storage, cache."""

from src.analytics.engine import AnalyticsCache, AnalyticsEngine


def _seed_data(db):
    """Insert test instruments, contracts, and historical data."""
    with db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES
                ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE'),
                ('NSE_INDEX|Nifty Bank', 'BANKNIFTY', 'Nifty Bank', 'NSE')
        """)
        conn.execute("""
            INSERT INTO contracts (expired_instrument_key, instrument_key, expiry_date,
                                   contract_type, strike_price, trading_symbol,
                                   data_fetched, no_data)
            VALUES
                ('NSE_FO|NIFTY25100CE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25100, 'NIFTY 25100 CE', TRUE, FALSE),
                ('NSE_FO|NIFTY25100PE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'PE', 25100, 'NIFTY 25100 PE', TRUE, FALSE),
                ('NSE_FO|NIFTY25200CE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25200, 'NIFTY 25200 CE', FALSE, FALSE),
                ('NSE_FO|BANKFUT',      'NSE_INDEX|Nifty Bank', '2025-02-27', 'FUT', 0, 'BANKNIFTY FUT', TRUE, TRUE)
        """)
        conn.execute("""
            INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
            VALUES
                ('NSE_FO|NIFTY25100CE', '2025-01-28 09:15:00', 100, 110, 95, 105, 1000, 500),
                ('NSE_FO|NIFTY25100CE', '2025-01-29 09:15:00', 105, 115, 100, 110, 1200, 600),
                ('NSE_FO|NIFTY25100PE', '2025-01-28 09:15:00', 50, 55, 45, 48, 800, 300),
                ('NSE_FO|NIFTY25100PE', '2025-01-29 09:15:00', 48, 52, 40, 42, 900, 350)
        """)


def test_dashboard_summary(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    summary = engine.get_dashboard_summary()
    assert summary["instruments"] == 2
    assert summary["contracts"] == 4
    assert summary["fetched_contracts"] == 3  # 2 CE + 1 FUT
    assert summary["total_candles"] == 4
    assert summary["pending_contracts"] == 1
    assert summary["no_data_contracts"] == 1
    assert summary["coverage_pct"] == 75.0  # 3/4


def test_candles_per_day(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    result = engine.get_candles_per_day()
    assert "labels" in result
    assert "data" in result
    assert len(result["labels"]) == 2  # 2 trading days
    # The data is ordered ascending (reversed)
    assert result["data"][0] == 2  # 2025-01-28: 2 candles
    assert result["data"][1] == 2  # 2025-01-29: 2 candles


def test_candles_per_day_filtered(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    # Filter to a non-existent instrument — should be empty
    result = engine.get_candles_per_day(instrument_key="NONEXISTENT")
    assert result["labels"] == []
    assert result["data"] == []


def test_contracts_by_type(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    result = engine.get_contracts_by_type()
    assert "labels" in result
    assert "data" in result
    # CE: 2, PE: 1, FUT: 1 — sorted by count descending
    assert result["labels"][0] == "CE"
    assert result["data"][0] == 2


def test_contracts_by_instrument(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    result = engine.get_contracts_by_instrument()
    assert "labels" in result
    # NIFTY has 3 contracts, BANKNIFTY has 1
    assert result["labels"][0] == "NIFTY"
    assert result["data"][0] == 3


def test_data_coverage(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    result = engine.get_data_coverage_by_expiry()
    assert "labels" in result
    assert "total" in result
    assert "fetched" in result
    assert len(result["labels"]) >= 1


def test_storage_breakdown(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    result = engine.get_storage_breakdown()
    assert result["instruments"] == 2
    assert result["contracts"] == 4
    assert result["historical_data"] == 4


def test_cache_invalidation(tmp_db):
    _seed_data(tmp_db)
    engine = AnalyticsEngine(tmp_db)
    AnalyticsCache.invalidate_all()

    # First call populates cache
    result1 = engine.get_dashboard_summary()
    assert result1["instruments"] == 2

    # Add more data
    with tmp_db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty IT', 'NIFTYIT', 'Nifty IT', 'NSE')
        """)

    # Cached — should still return 2
    result2 = engine.get_dashboard_summary()
    assert result2["instruments"] == 2

    # Invalidate and re-query — should return 3
    AnalyticsCache.invalidate_all()
    result3 = engine.get_dashboard_summary()
    assert result3["instruments"] == 3
