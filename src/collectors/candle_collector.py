"""
Candle Data Collector for ExpiryTrack
Collects historical OHLCV candle data for any Upstox segment via V3 API
"""

import asyncio
import logging
from datetime import date, timedelta

from ..api.client import UpstoxAPIClient
from ..auth.manager import AuthManager
from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

# Map user-friendly interval strings to V3 API (unit, interval) pairs
INTERVAL_MAP = {
    "1minute": ("minutes", 1),
    "3minute": ("minutes", 3),
    "5minute": ("minutes", 5),
    "10minute": ("minutes", 10),
    "15minute": ("minutes", 15),
    "30minute": ("minutes", 30),
    "1hour": ("hours", 1),
    "1day": ("days", 1),
    "1week": ("weeks", 1),
    "1month": ("months", 1),
}


class CandleCollector:
    """
    Collects historical candle data for instruments via Upstox V3 API.
    Supports all segments: NSE_EQ, BSE_EQ, MCX_FO, etc.
    """

    def __init__(self, auth_manager: AuthManager | None = None, db_manager: DatabaseManager | None = None):
        self.auth_manager = auth_manager or AuthManager()
        self.db = db_manager or DatabaseManager()
        self.api_client = UpstoxAPIClient(self.auth_manager)

        self.stats = {
            "instruments_processed": 0,
            "candles_fetched": 0,
            "errors": 0,
            "skipped": 0,
        }

    async def __aenter__(self):
        await self.api_client.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api_client.close()

    async def collect(
        self,
        instrument_keys: list[str],
        interval: str = "1day",
        from_date: str | None = None,
        to_date: str | None = None,
        batch_size: int = 5,
        incremental: bool = False,
        progress_callback=None,
    ) -> dict:
        """
        Collect historical candle data for a list of instruments.

        Args:
            instrument_keys: List of instrument keys to collect
            interval: Candle interval ('1minute', '5minute', '1day', etc.)
            from_date: Start date (YYYY-MM-DD). Defaults based on interval.
            to_date: End date (YYYY-MM-DD). Defaults to today.
            batch_size: Number of instruments to process concurrently
            progress_callback: Optional callback(instrument_key, current, total, candles)

        Returns:
            Collection statistics dict
        """
        if interval not in INTERVAL_MAP:
            raise ValueError(f"Invalid interval '{interval}'. Valid: {list(INTERVAL_MAP.keys())}")

        unit, interval_val = INTERVAL_MAP[interval]

        if to_date is None:
            to_date = date.today().isoformat()
        if from_date is None:
            days_back = 365 if unit in ("days", "weeks", "months") else 30
            from_date = (date.today() - timedelta(days=days_back)).isoformat()

        # Incremental mode: look up last collected date per instrument
        incremental_dates = {}
        if incremental:
            incremental_dates = self.db.get_last_candle_timestamps(instrument_keys, interval)
            if incremental_dates:
                logger.info(f"Incremental mode: {len(incremental_dates)} instruments have existing data")

        total = len(instrument_keys)
        self.stats = {
            "instruments_processed": 0,
            "candles_fetched": 0,
            "errors": 0,
            "skipped": 0,
        }

        logger.info(
            f"Starting candle collection: {total} instruments, "
            f"interval={interval}, {from_date} to {to_date}"
            f"{' (incremental)' if incremental else ''}"
        )

        # Process in batches
        for i in range(0, total, batch_size):
            batch = instrument_keys[i : i + batch_size]
            tasks = []
            skip_keys = set()
            for key in batch:
                # Compute per-instrument from_date when incremental
                inst_last = incremental_dates.get(key)
                if inst_last:
                    effective_from = (date.fromisoformat(inst_last) + timedelta(days=1)).isoformat()
                else:
                    effective_from = from_date

                if effective_from > to_date:
                    skip_keys.add(key)
                    tasks.append(self._skip_instrument(key))
                else:
                    tasks.append(self._collect_single(key, unit, interval_val, interval, effective_from, to_date))
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for key, result in zip(batch, results, strict=False):
                self.stats["instruments_processed"] += 1
                if key in skip_keys:
                    self.stats["skipped"] += 1
                elif isinstance(result, Exception):
                    self.stats["errors"] += 1
                    logger.error(f"Failed to collect {key}: {result}")
                elif result > 0:
                    self.stats["candles_fetched"] += result
                else:
                    self.stats["skipped"] += 1

                if progress_callback:
                    try:
                        progress_callback(key, self.stats["instruments_processed"], total, result)
                    except Exception:
                        logger.debug("Progress callback failed", exc_info=True)

        logger.info(
            f"Candle collection complete: {self.stats['instruments_processed']} instruments, "
            f"{self.stats['candles_fetched']} candles, {self.stats['errors']} errors"
        )

        return self.stats

    async def _skip_instrument(self, key: str) -> int:
        """Return 0 for instruments already up-to-date (incremental skip)."""
        logger.info(f"Skipping {key}: already up-to-date")
        return 0

    async def _collect_single(
        self, instrument_key: str, unit: str, interval_val: int, interval_str: str, from_date: str, to_date: str
    ) -> int:
        """Collect candles for a single instrument"""
        try:
            candles = await self.api_client.get_historical_candles_v3(
                instrument_key=instrument_key, unit=unit, interval=interval_val, from_date=from_date, to_date=to_date
            )

            if candles:
                count = self.db.insert_candle_data(instrument_key, candles, interval_str)
                return count
            else:
                logger.info(f"No candle data returned for {instrument_key}")
                return 0

        except Exception as e:
            logger.error(f"Error collecting {instrument_key}: {e}")
            raise

    def get_stats(self) -> dict:
        return self.stats.copy()
