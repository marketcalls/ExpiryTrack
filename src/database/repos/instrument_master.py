"""Instrument master repository."""

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import duckdb

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class InstrumentMasterRepository(BaseRepository):
    def bulk_insert_instrument_master(self, df: "pd.DataFrame") -> int:
        if df.empty:
            return 0
        with self.get_connection() as conn:
            try:
                conn.execute("BEGIN TRANSACTION")
                conn.execute("INSERT OR REPLACE INTO instrument_master SELECT * FROM df")
                conn.commit()
                count = len(df)
                logger.info(f"Bulk inserted {count} instruments into instrument_master")
                return count
            except duckdb.Error as e:
                try:
                    conn.rollback()
                except duckdb.Error:
                    logger.debug("No active transaction to rollback")
                logger.error(f"Failed to bulk insert instrument master: {e}")
                raise

    def search_instrument_master(
        self, query: str, segment: str | None = None, instrument_type: str | None = None, limit: int = 50
    ) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            conditions = ["(trading_symbol ILIKE ? OR name ILIKE ?)"]
            params = [f"%{query}%", f"%{query}%"]
            if segment:
                conditions.append("segment = ?")
                params.append(segment)
            if instrument_type:
                conditions.append("instrument_type = ?")
                params.append(instrument_type)
            where = " AND ".join(conditions)
            cursor.execute(
                f"""
                SELECT instrument_key, trading_symbol, name, exchange, segment,
                       instrument_type, isin, lot_size, tick_size, expiry, strike_price, option_type
                FROM instrument_master WHERE {where} ORDER BY trading_symbol LIMIT ?
            """,
                params + [limit],
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_instrument_master_segments(self) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT segment, COUNT(*) as count, COUNT(DISTINCT instrument_type) as types
                FROM instrument_master GROUP BY segment ORDER BY segment
            """)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_instruments_by_segment(
        self, segment: str, instrument_type: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            conditions = ["segment = ?"]
            params: list = [segment]
            if instrument_type:
                conditions.append("instrument_type = ?")
                params.append(instrument_type)
            where = " AND ".join(conditions)
            cursor.execute(
                f"""
                SELECT instrument_key, trading_symbol, name, exchange, segment,
                       instrument_type, isin, lot_size, tick_size, expiry
                FROM instrument_master WHERE {where} ORDER BY trading_symbol LIMIT ? OFFSET ?
            """,
                params + [limit, offset],
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_instrument_master_last_sync(self) -> datetime | None:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT MAX(last_updated) FROM instrument_master")
            row = cursor.fetchone()
            if row and row[0]:
                val = row[0]
                if isinstance(val, str):
                    return datetime.fromisoformat(val)
                return val  # type: ignore[no-any-return]
            return None

    def get_instrument_types_by_segment(self, segment: str) -> list[str]:
        with self.get_read_connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT instrument_type FROM instrument_master WHERE segment = ? ORDER BY instrument_type",
                (segment,),
            ).fetchall()
            return [row[0] for row in rows if row[0]]

    def get_instrument_keys_by_segment(self, segment: str, instrument_type: str | None = None) -> list[str]:
        with self.get_read_connection() as conn:
            conditions = ["segment = ?"]
            params: list = [segment]
            if instrument_type:
                conditions.append("instrument_type = ?")
                params.append(instrument_type)
            rows = conn.execute(
                f"SELECT instrument_key FROM instrument_master WHERE {' AND '.join(conditions)} ORDER BY trading_symbol",
                params,
            ).fetchall()
            return [row[0] for row in rows]

    def get_instrument_master_count(self, segment: str | None = None, instrument_type: str | None = None) -> int:
        with self.get_read_connection() as conn:
            conditions = []
            params: list = []
            if segment:
                conditions.append("segment = ?")
                params.append(segment)
            if instrument_type:
                conditions.append("instrument_type = ?")
                params.append(instrument_type)
            if conditions:
                result = conn.execute(
                    f"SELECT COUNT(*) FROM instrument_master WHERE {' AND '.join(conditions)}", params
                ).fetchone()
            else:
                result = conn.execute("SELECT COUNT(*) FROM instrument_master").fetchone()
            return result[0] if result else 0
