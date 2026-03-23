"""Tests for analytics daily summary refresh."""



class TestAnalyticsSummary:
    def test_refresh_creates_summary_rows(self, tmp_db):
        """Summary refresh should aggregate historical_data into analytics_daily_summary."""
        from src.analytics.summary import refresh_daily_summary

        # Insert test data: instrument, expiry, contract, historical
        with tmp_db.get_connection() as conn:
            conn.execute(
                "INSERT INTO instruments (instrument_key, symbol) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "NIFTY"),
            )
            conn.execute(
                "INSERT INTO expiries (instrument_key, expiry_date) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "2025-01-30"),
            )
            conn.execute(
                """INSERT INTO contracts
                   (expired_instrument_key, instrument_key, trading_symbol, expiry_date, contract_type)
                   VALUES (?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25JAN", "NSE_FO|NIFTY", "NIFTY25JANFUT", "2025-01-30", "FUT"),
            )
            conn.execute(
                """INSERT INTO historical_data
                   (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25JAN", "2025-01-15 09:15:00", 100, 105, 99, 103, 1000, 500),
            )
            conn.execute(
                """INSERT INTO historical_data
                   (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25JAN", "2025-01-15 09:16:00", 103, 106, 102, 105, 800, 600),
            )

        count = refresh_daily_summary(tmp_db, since_date="2025-01-01")
        assert count >= 1

        # Verify summary row exists
        with tmp_db.get_read_connection() as conn:
            row = conn.execute(
                "SELECT candle_count, total_volume FROM analytics_daily_summary WHERE summary_date = '2025-01-15'"
            ).fetchone()
            assert row is not None
            assert row[0] == 2  # 2 candles
            assert row[1] == 1800  # 1000 + 800

    def test_refresh_with_no_data(self, tmp_db):
        """Refresh with no historical data should return 0."""
        from src.analytics.summary import refresh_daily_summary

        count = refresh_daily_summary(tmp_db, since_date="2025-01-01")
        assert count == 0

    def test_refresh_idempotent(self, tmp_db):
        """Running refresh twice should produce same result (DELETE + INSERT)."""
        from src.analytics.summary import refresh_daily_summary

        with tmp_db.get_connection() as conn:
            conn.execute(
                "INSERT INTO instruments (instrument_key, symbol) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "NIFTY"),
            )
            conn.execute(
                "INSERT INTO expiries (instrument_key, expiry_date) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "2025-02-27"),
            )
            conn.execute(
                """INSERT INTO contracts
                   (expired_instrument_key, instrument_key, trading_symbol, expiry_date, contract_type)
                   VALUES (?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25FEB", "NSE_FO|NIFTY", "NIFTY25FEBFUT", "2025-02-27", "FUT"),
            )
            conn.execute(
                """INSERT INTO historical_data
                   (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25FEB", "2025-02-10 09:15:00", 100, 105, 99, 103, 500, 0),
            )

        count1 = refresh_daily_summary(tmp_db, since_date="2025-01-01")
        count2 = refresh_daily_summary(tmp_db, since_date="2025-01-01")
        assert count1 == count2

    def test_refresh_default_since_date(self, tmp_db):
        """Refresh without since_date should default to 30 days ago."""
        from src.analytics.summary import refresh_daily_summary

        count = refresh_daily_summary(tmp_db)
        assert count == 0  # No data, but should not raise
