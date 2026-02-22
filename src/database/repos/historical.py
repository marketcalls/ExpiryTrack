"""Historical data repository."""

import logging

import duckdb
import pandas as pd

from ..manager import DictCursor
from .base import BaseRepository

logger = logging.getLogger(__name__)


class HistoricalDataRepository(BaseRepository):
    def insert_historical_data(self, expired_instrument_key: str, candles: list[list]) -> int:
        with self.get_connection() as conn:
            data_to_insert = []
            for candle in candles:
                try:
                    data_to_insert.append(
                        {
                            "expired_instrument_key": expired_instrument_key,
                            "timestamp": candle[0],
                            "open": float(candle[1]),
                            "high": float(candle[2]),
                            "low": float(candle[3]),
                            "close": float(candle[4]),
                            "volume": int(candle[5]),
                            "oi": int(candle[6]) if len(candle) > 6 else None,
                        }
                    )
                except (ValueError, IndexError, TypeError) as e:
                    logger.error(f"Failed to parse candle: {e}")

            if data_to_insert:
                try:
                    conn.execute("BEGIN TRANSACTION")
                    df = pd.DataFrame(data_to_insert)  # noqa: F841 — used by DuckDB via SELECT * FROM df
                    conn.execute("INSERT OR REPLACE INTO historical_data SELECT * FROM df")
                    conn.execute(
                        """
                        UPDATE contracts
                        SET data_fetched = TRUE, no_data = FALSE,
                            fetch_attempts = fetch_attempts + 1,
                            last_attempted_at = CURRENT_TIMESTAMP
                        WHERE expired_instrument_key = ?
                    """,
                        (expired_instrument_key,),
                    )
                    conn.commit()
                    count = len(data_to_insert)
                    logger.info(f"Successfully inserted {count} candles for {expired_instrument_key}")
                    return count
                except duckdb.Error as e:
                    logger.error(f"Failed to insert historical data for {expired_instrument_key}: {e}")
                    try:
                        conn.rollback()
                    except duckdb.Error:
                        logger.debug("No active transaction to rollback")
                    raise
            else:
                logger.warning(f"No data to insert for {expired_instrument_key}")
            return 0

    def mark_contract_no_data(self, expired_instrument_key: str) -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE contracts SET data_fetched = TRUE, no_data = TRUE,
                    fetch_attempts = fetch_attempts + 1, last_attempted_at = CURRENT_TIMESTAMP
                WHERE expired_instrument_key = ?
            """,
                (expired_instrument_key,),
            )
            conn.commit()
            logger.info(f"Marked contract {expired_instrument_key} as no_data")

    def get_historical_data(self, expired_instrument_key: str) -> list[list]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT timestamp, open, high, low, close, volume, oi
                FROM historical_data WHERE expired_instrument_key = ? ORDER BY timestamp
            """,
                (expired_instrument_key,),
            )
            return [list(row) for row in cursor.fetchall()]

    def get_historical_data_count(self, expired_instrument_key: str | None = None) -> int:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            if expired_instrument_key:
                cursor.execute(
                    "SELECT COUNT(*) FROM historical_data WHERE expired_instrument_key = ?", (expired_instrument_key,)
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM historical_data")
            return int(cursor.fetchone()[0])
