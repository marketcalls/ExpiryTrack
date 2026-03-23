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
                    # NOTE: do NOT call conn.commit() here — get_connection() commits on exit
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
            # NOTE: do NOT call conn.commit() here — get_connection() commits on exit
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

    def delete_by_instrument(self, instrument_key: str) -> int:
        """Delete all historical data for an instrument. Returns row count."""
        # Escape LIKE wildcards to prevent unintended broad matches
        safe_key = instrument_key.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self.get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                result = conn.execute(
                    "DELETE FROM historical_data WHERE expired_instrument_key LIKE ? || '%' ESCAPE '\\'",
                    (safe_key,),
                ).fetchone()
                count = result[0] if result else 0
                # Also delete from candle_data
                result2 = conn.execute(
                    "DELETE FROM candle_data WHERE instrument_key = ?",
                    (instrument_key,),
                ).fetchone()
                count += result2[0] if result2 else 0
                # NOTE: do NOT call conn.commit() here — get_connection() commits on exit
                logger.info(f"Deleted {count} rows for instrument {instrument_key}")
                return count
            except duckdb.Error:
                conn.rollback()
                raise

    def delete_by_expiry(self, instrument_key: str, expiry_date: str) -> int:
        """Delete historical data for a specific instrument+expiry. Returns row count."""
        with self.get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # Get expired_instrument_keys for this expiry
                keys = conn.execute(
                    "SELECT expired_instrument_key FROM contracts WHERE instrument_key = ? AND expiry = ?",
                    (instrument_key, expiry_date),
                ).fetchall()
                if not keys:
                    # NOTE: get_connection() will commit the empty BEGIN TRANSACTION on exit
                    return 0
                key_list = [k[0] for k in keys]
                placeholders = ",".join(["?"] * len(key_list))
                result = conn.execute(
                    f"DELETE FROM historical_data WHERE expired_instrument_key IN ({placeholders})",
                    key_list,
                ).fetchone()
                count = result[0] if result else 0
                # Reset contracts as unfetched
                conn.execute(
                    f"UPDATE contracts SET data_fetched = FALSE, no_data = FALSE WHERE expired_instrument_key IN ({placeholders})",
                    key_list,
                )
                # NOTE: do NOT call conn.commit() here — get_connection() commits on exit
                logger.info(f"Deleted {count} rows for {instrument_key} expiry {expiry_date}")
                return count
            except duckdb.Error:
                conn.rollback()
                raise

    def delete_by_date_range(self, from_date: str, to_date: str, instrument_key: str | None = None) -> int:
        """Delete historical data within a date range. Returns row count."""
        with self.get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                where = "WHERE CAST(timestamp AS DATE) BETWEEN ? AND ?"
                params: list = [from_date, to_date]
                if instrument_key:
                    safe_key = instrument_key.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                    where += " AND expired_instrument_key LIKE ? || '%' ESCAPE '\\'"
                    params.append(safe_key)
                result = conn.execute(
                    f"DELETE FROM historical_data {where}", params
                ).fetchone()
                count = result[0] if result else 0
                # NOTE: do NOT call conn.commit() here — get_connection() commits on exit
                logger.info(f"Deleted {count} rows for date range {from_date} to {to_date}")
                return count
            except duckdb.Error:
                conn.rollback()
                raise

    def get_storage_estimate(self, instrument_key: str | None = None) -> dict:
        """Get storage estimates (row counts) for data."""
        with self.get_read_connection() as conn:
            where = ""
            params: list = []
            if instrument_key:
                safe_key = instrument_key.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                where = "WHERE expired_instrument_key LIKE ? || '%' ESCAPE '\\'"
                params = [safe_key]

            hist_count = conn.execute(
                f"SELECT COUNT(*) FROM historical_data {where}", params
            ).fetchone()[0]

            candle_where = ""
            candle_params: list = []
            if instrument_key:
                candle_where = "WHERE instrument_key = ?"
                candle_params = [instrument_key]

            candle_count = conn.execute(
                f"SELECT COUNT(*) FROM candle_data {candle_where}", candle_params
            ).fetchone()[0]

            return {
                "historical_rows": hist_count,
                "candle_rows": candle_count,
                "total_rows": hist_count + candle_count,
                "estimated_size_mb": round((hist_count + candle_count) * 100 / 1024 / 1024, 1),
            }

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
