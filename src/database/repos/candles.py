"""Candle data repository."""

import logging

import duckdb
import pandas as pd

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

logger = logging.getLogger(__name__)


class CandleRepository(BaseRepository):
    def insert_candle_data(self, instrument_key: str, candles: list[list], interval: str = "1day") -> int:
        if not candles:
            return 0
        with self.get_connection() as conn:
            data_rows = []
            for candle in candles:
                try:
                    data_rows.append(
                        {
                            "instrument_key": instrument_key,
                            "timestamp": candle[0],
                            "open": float(candle[1]),
                            "high": float(candle[2]),
                            "low": float(candle[3]),
                            "close": float(candle[4]),
                            "volume": int(candle[5]) if len(candle) > 5 else 0,
                            "oi": int(candle[6]) if len(candle) > 6 else 0,
                            "interval": interval,
                        }
                    )
                except (ValueError, IndexError) as e:
                    logger.error(f"Failed to parse candle: {e}")
            if not data_rows:
                return 0
            try:
                conn.execute("BEGIN TRANSACTION")
                df = pd.DataFrame(data_rows)  # noqa: F841 — used by DuckDB via SELECT * FROM df
                conn.execute("INSERT OR REPLACE INTO candle_data SELECT * FROM df")
                conn.execute(
                    """
                    INSERT OR REPLACE INTO candle_collection_status
                    (instrument_key, interval, last_collected_date, earliest_date, candle_count, updated_at)
                    VALUES (?, ?, CURRENT_DATE,
                        (SELECT MIN(timestamp::DATE) FROM candle_data WHERE instrument_key = ? AND interval = ?),
                        (SELECT COUNT(*) FROM candle_data WHERE instrument_key = ? AND interval = ?),
                        CURRENT_TIMESTAMP)
                """,
                    (instrument_key, interval, instrument_key, interval, instrument_key, interval),
                )
                conn.commit()
                count = len(data_rows)
                logger.info(f"Inserted {count} candles for {instrument_key} ({interval})")
                return count
            except duckdb.Error as e:
                try:
                    conn.rollback()
                except duckdb.Error:
                    logger.debug("No active transaction to rollback")
                logger.error(f"Failed to insert candle data for {instrument_key}: {e}")
                raise

    def get_candle_data(
        self, instrument_key: str, interval: str = "1day", from_date: str | None = None, to_date: str | None = None
    ) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            conditions = ["instrument_key = ?", "interval = ?"]
            params: list = [instrument_key, interval]
            if from_date:
                conditions.append("timestamp >= ?")
                params.append(from_date)
            if to_date:
                conditions.append("timestamp <= ?")
                params.append(to_date)
            where = " AND ".join(conditions)
            cursor.execute(
                f"""
                SELECT timestamp, open, high, low, close, volume, oi
                FROM candle_data WHERE {where} ORDER BY timestamp
            """,
                params,
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_candle_data_count(self, instrument_key: str | None = None, interval: str | None = None) -> int:
        with self.get_read_connection() as conn:
            conditions = []
            params = []
            if instrument_key:
                conditions.append("instrument_key = ?")
                params.append(instrument_key)
            if interval:
                conditions.append("interval = ?")
                params.append(interval)
            where = " WHERE " + " AND ".join(conditions) if conditions else ""
            result = conn.execute(f"SELECT COUNT(*) FROM candle_data{where}", params).fetchone()
            return result[0] if result else 0

    def get_candle_collection_status(self, segment: str | None = None) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            if segment:
                cursor.execute(
                    """
                    SELECT cs.instrument_key, cs.interval, cs.last_collected_date,
                           cs.earliest_date, cs.candle_count, cs.updated_at,
                           im.trading_symbol, im.name, im.segment
                    FROM candle_collection_status cs
                    LEFT JOIN instrument_master im ON cs.instrument_key = im.instrument_key
                    WHERE im.segment = ? ORDER BY im.trading_symbol
                """,
                    (segment,),
                )
            else:
                cursor.execute("""
                    SELECT cs.instrument_key, cs.interval, cs.last_collected_date,
                           cs.earliest_date, cs.candle_count, cs.updated_at,
                           im.trading_symbol, im.name, im.segment
                    FROM candle_collection_status cs
                    LEFT JOIN instrument_master im ON cs.instrument_key = im.instrument_key
                    ORDER BY im.segment, im.trading_symbol
                """)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_last_candle_timestamps(self, instrument_keys: list[str], interval: str = "1day") -> dict[str, str]:
        if not instrument_keys:
            return {}
        with self.get_read_connection() as conn:
            placeholders = ",".join(["?" for _ in instrument_keys])
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT instrument_key, MAX(timestamp)::DATE as last_date
                FROM candle_data
                WHERE instrument_key IN ({placeholders}) AND interval = ?
                GROUP BY instrument_key
            """,
                instrument_keys + [interval],
            )
            return {row[0]: row[1].isoformat() for row in cursor.fetchall()}

    def get_candle_analytics_summary(self) -> dict:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT COUNT(*) as total_candles,
                       COUNT(DISTINCT instrument_key) as instruments_with_data,
                       MIN(timestamp) as earliest_date, MAX(timestamp) as latest_date
                FROM candle_data
            """)
            summary_row = cursor.fetchone()
            summary = (
                dict_from_row(summary_row)
                if summary_row
                else {
                    "total_candles": 0,
                    "instruments_with_data": 0,
                    "earliest_date": None,
                    "latest_date": None,
                }
            )
            cursor.execute("""
                SELECT cs.instrument_key, cs.interval, cs.last_collected_date,
                       cs.earliest_date, cs.candle_count, cs.updated_at,
                       im.trading_symbol, im.name, im.segment
                FROM candle_collection_status cs
                LEFT JOIN instrument_master im ON cs.instrument_key = im.instrument_key
                ORDER BY cs.updated_at DESC
            """)
            instruments = [dict_from_row(row) for row in cursor.fetchall()]
            return {
                "total_candles": summary.get("total_candles", 0) or 0,
                "instruments_with_data": summary.get("instruments_with_data", 0) or 0,
                "earliest_date": summary.get("earliest_date"),
                "latest_date": summary.get("latest_date"),
                "instruments": instruments,
            }
