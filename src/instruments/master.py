"""
Instrument Master Management for ExpiryTrack
Downloads, parses, and stores Upstox instrument master files
"""

import gzip
import json
import logging
from datetime import datetime

import httpx
import pandas as pd

from ..config import config
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class InstrumentMaster:
    """Download and manage Upstox instrument master data"""

    def __init__(self, db_manager: DatabaseManager | None = None):
        self.db = db_manager or DatabaseManager()

    def sync(self, exchanges: list[str] | None = None) -> dict[str, int]:
        """
        Download and sync instrument master data from Upstox.

        Args:
            exchanges: List of exchanges to sync (e.g., ['NSE', 'BSE', 'MCX']).
                       Defaults to all available.

        Returns:
            Dict with counts per exchange: {'NSE': 1500, 'BSE': 3000, ...}
        """
        if exchanges is None:
            exchanges = ["NSE", "BSE", "MCX"]

        results = {}
        for exchange in exchanges:
            url = config.INSTRUMENT_MASTER_URLS.get(exchange)
            if not url:
                logger.warning(f"No URL configured for exchange: {exchange}")
                continue

            try:
                instruments = self._download_and_parse(url, exchange)
                if instruments:
                    count = self._store_instruments(instruments)
                    results[exchange] = count
                    logger.info(f"Synced {count} instruments for {exchange}")
                else:
                    results[exchange] = 0
                    logger.warning(f"No instruments found for {exchange}")
            except Exception as e:
                logger.error(f"Failed to sync {exchange}: {e}")
                results[exchange] = -1

        return results

    def _download_and_parse(self, url: str, exchange: str) -> list[dict]:
        """Download and decompress a .json.gz instrument file"""
        logger.info(f"Downloading instrument master for {exchange}...")

        with httpx.Client(timeout=60) as client:
            response = client.get(url)
            response.raise_for_status()

        # Decompress gzip
        raw_data = gzip.decompress(response.content)
        instruments = json.loads(raw_data)

        logger.info(f"Downloaded {len(instruments)} instruments for {exchange}")
        return instruments

    def _store_instruments(self, instruments: list[dict]) -> int:
        """Store instruments in the instrument_master table using bulk insert"""
        if not instruments:
            return 0

        rows = []
        for inst in instruments:
            rows.append(
                {
                    "instrument_key": inst.get("instrument_key", ""),
                    "trading_symbol": inst.get("trading_symbol", ""),
                    "name": inst.get("name", ""),
                    "exchange": inst.get("exchange", ""),
                    "segment": inst.get("segment", ""),
                    "instrument_type": inst.get("instrument_type", ""),
                    "isin": inst.get("isin", ""),
                    "lot_size": inst.get("lot_size"),
                    "tick_size": inst.get("tick_size"),
                    "expiry": inst.get("expiry"),
                    "strike_price": inst.get("strike_price"),
                    "option_type": inst.get("option_type", ""),
                    "last_updated": datetime.now(),
                }
            )

        df = pd.DataFrame(rows)

        # Handle type conversions for DuckDB
        df["lot_size"] = pd.to_numeric(df["lot_size"], errors="coerce").astype("Int64")
        df["tick_size"] = pd.to_numeric(df["tick_size"], errors="coerce")
        df["strike_price"] = pd.to_numeric(df["strike_price"], errors="coerce")

        # Convert expiry to date, coercing errors to NaT
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")

        count = self.db.bulk_insert_instrument_master(df)
        return count

    def search(
        self, query: str, segment: str | None = None, instrument_type: str | None = None, limit: int = 50
    ) -> list[dict]:
        """
        Search instrument master by name/symbol.

        Args:
            query: Search term (matches trading_symbol or name)
            segment: Filter by segment (e.g., 'NSE_EQ')
            instrument_type: Filter by type (e.g., 'EQ', 'FUT', 'CE', 'PE')
            limit: Max results

        Returns:
            List of matching instruments
        """
        return self.db.search_instrument_master(query, segment, instrument_type, limit)

    def get_segments(self) -> list[dict]:
        """Get available segments with instrument counts"""
        return self.db.get_instrument_master_segments()

    def get_by_segment(
        self, segment: str, instrument_type: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """Get instruments for a specific segment"""
        return self.db.get_instruments_by_segment(segment, instrument_type, limit, offset)

    def get_last_sync_time(self) -> datetime | None:
        """Get the timestamp of the last instrument sync"""
        return self.db.get_instrument_master_last_sync()

    def is_stale(self, max_age_hours: int = 24) -> bool:
        """Check if instrument master data is stale"""
        last_sync = self.get_last_sync_time()
        if last_sync is None:
            return True
        age = (datetime.now() - last_sync).total_seconds() / 3600
        return age > max_age_hours
