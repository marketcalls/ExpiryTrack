"""Tests for InstrumentMaster — store, search, segments, staleness."""

from datetime import datetime
from unittest.mock import MagicMock, patch


class TestInstrumentMaster:
    def test_init_with_db(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        assert im.db is tmp_db

    def test_store_instruments_empty(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        count = im._store_instruments([])
        assert count == 0

    def test_store_instruments(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        instruments = [
            {
                "instrument_key": "NSE_EQ|RELIANCE",
                "trading_symbol": "RELIANCE",
                "name": "RELIANCE INDUSTRIES",
                "exchange": "NSE",
                "segment": "NSE_EQ",
                "instrument_type": "EQ",
                "isin": "INE002A01018",
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": "",
            }
        ]
        count = im._store_instruments(instruments)
        assert count >= 1

    def test_search(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        results = im.search("NIFTY")
        assert isinstance(results, list)

    def test_get_segments(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        segments = im.get_segments()
        assert isinstance(segments, list)

    def test_get_last_sync_time_none(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        result = im.get_last_sync_time()
        assert result is None

    def test_is_stale_no_data(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        assert im.is_stale() is True

    def test_is_stale_recent_data(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        # Mock get_last_sync_time to return now
        im.get_last_sync_time = MagicMock(return_value=datetime.now())
        assert im.is_stale(max_age_hours=24) is False

    def test_sync_no_url_configured(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        with patch.object(im, "_download_and_parse") as mock_dl:
            # Fake exchange that doesn't exist in config
            results = im.sync(exchanges=["FAKE_EXCHANGE"])
            mock_dl.assert_not_called()
            assert "FAKE_EXCHANGE" not in results

    def test_get_by_segment(self, tmp_db):
        from src.instruments.master import InstrumentMaster

        im = InstrumentMaster(db_manager=tmp_db)
        results = im.get_by_segment("NSE_EQ")
        assert isinstance(results, list)
