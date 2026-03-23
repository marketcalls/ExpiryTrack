"""Tests for DataQualityChecker — OHLC integrity, negatives, zero volume, gaps, duplicates, orphans."""

from src.quality.checker import DataQualityChecker


def _seed_clean_data(db):
    """Insert clean test data with no quality issues."""
    with db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE')
        """)
        conn.execute("""
            INSERT INTO contracts (expired_instrument_key, instrument_key, expiry_date,
                                   contract_type, strike_price, trading_symbol, data_fetched)
            VALUES ('NSE_FO|NIFTY25100CE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25100, 'NIFTY 25100 CE', TRUE)
        """)
        # Insert 10 clean candles across 5+ trading days
        for i in range(10):
            day = 20 + (i % 5)
            hour = 9 + (i // 5)
            conn.execute(
                """
                INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    "NSE_FO|NIFTY25100CE",
                    f"2025-01-{day:02d} {hour:02d}:15:00",
                    100 + i,
                    110 + i,
                    95 + i,
                    105 + i,
                    1000 + i * 100,
                    500,
                ],
            )


def test_clean_data_passes(tmp_db):
    """All checks should pass on clean data."""
    _seed_clean_data(tmp_db)
    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    assert report.checks_run == 7
    assert report.error_count == 0
    assert report.passed is True
    assert report.completed_at is not None


def test_ohlc_integrity_violation(tmp_db):
    """Detect when high < open (OHLC integrity violation)."""
    _seed_clean_data(tmp_db)
    with tmp_db.get_connection() as conn:
        conn.execute("""
            INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
            VALUES ('NSE_FO|NIFTY25100CE', '2025-01-30 09:15:00', 200, 150, 100, 180, 500, 0)
        """)

    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    ohlc_violations = [v for v in report.violations if v.check == "ohlc_integrity"]
    assert len(ohlc_violations) >= 1
    assert ohlc_violations[0].severity == "error"


def test_negative_values_detected(tmp_db):
    """Detect negative prices."""
    _seed_clean_data(tmp_db)
    with tmp_db.get_connection() as conn:
        conn.execute("""
            INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
            VALUES ('NSE_FO|NIFTY25100CE', '2025-01-30 10:00:00', -10, 50, -20, 30, 100, 0)
        """)

    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    neg_violations = [v for v in report.violations if v.check == "negative_values"]
    assert len(neg_violations) >= 1
    assert neg_violations[0].severity == "error"


def test_zero_volume_warning(tmp_db):
    """Warn when >20% of candles have zero volume."""
    with tmp_db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE')
        """)
        conn.execute("""
            INSERT INTO contracts (expired_instrument_key, instrument_key, expiry_date,
                                   contract_type, strike_price, trading_symbol, data_fetched)
            VALUES ('NSE_FO|NIFTY25100CE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25100, 'NIFTY 25100 CE', TRUE)
        """)
        # Insert 5 candles, 4 with zero volume (80%)
        for i in range(5):
            vol = 0 if i < 4 else 1000
            conn.execute(
                """
                INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                VALUES (?, ?, 100, 110, 90, 105, ?, 0)
            """,
                [
                    "NSE_FO|NIFTY25100CE",
                    f"2025-01-{20+i:02d} 09:15:00",
                    vol,
                ],
            )

    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    zero_vol = [v for v in report.violations if v.check == "zero_volume"]
    assert len(zero_vol) >= 1
    assert zero_vol[0].severity == "warning"


def test_orphan_contracts_detected(tmp_db):
    """Detect contracts marked as fetched but with no historical data."""
    with tmp_db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE')
        """)
        conn.execute("""
            INSERT INTO contracts (expired_instrument_key, instrument_key, expiry_date,
                                   contract_type, strike_price, trading_symbol, data_fetched)
            VALUES ('NSE_FO|ORPHAN', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25000, 'NIFTY 25000 CE', TRUE)
        """)
        # No historical_data inserted for this contract

    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    orphan_violations = [v for v in report.violations if v.check == "orphan_contracts"]
    assert len(orphan_violations) >= 1
    assert orphan_violations[0].severity == "warning"


def test_report_to_dict(tmp_db):
    """QualityReport.to_dict() returns expected structure."""
    _seed_clean_data(tmp_db)
    checker = DataQualityChecker(tmp_db)
    report = checker.run_all_checks()

    d = report.to_dict()
    assert "checks_run" in d
    assert "checks_passed" in d
    assert "errors" in d
    assert "warnings" in d
    assert "passed" in d
    assert "violations" in d
    assert isinstance(d["violations"], list)
    assert "started_at" in d
    assert "completed_at" in d
