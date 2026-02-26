"""Watchlist repository."""

import logging

import duckdb

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

logger = logging.getLogger(__name__)


class WatchlistRepository(BaseRepository):
    def create_watchlist(self, name: str, segment: str | None = None) -> int | None:
        with self.get_connection() as conn:
            result = conn.execute(
                """
                INSERT INTO watchlists (name, segment) VALUES (?, ?) RETURNING id
            """,
                (name, segment),
            )
            row = result.fetchone()
            conn.commit()
            return row[0] if row else None

    def get_watchlists(self) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT w.id, w.name, w.segment, w.created_at,
                       COUNT(wi.instrument_key) as item_count
                FROM watchlists w
                LEFT JOIN watchlist_items wi ON w.id = wi.watchlist_id
                GROUP BY w.id, w.name, w.segment, w.created_at ORDER BY w.name
            """)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_watchlist_items(self, watchlist_id: int) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT wi.instrument_key, wi.added_at,
                       im.trading_symbol, im.name, im.segment, im.instrument_type
                FROM watchlist_items wi
                LEFT JOIN instrument_master im ON wi.instrument_key = im.instrument_key
                WHERE wi.watchlist_id = ? ORDER BY im.trading_symbol
            """,
                (watchlist_id,),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def add_to_watchlist(self, watchlist_id: int, instrument_keys: list[str]) -> int:
        with self.get_connection() as conn:
            count = 0
            for key in instrument_keys:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO watchlist_items (watchlist_id, instrument_key) VALUES (?, ?)",
                        (watchlist_id, key),
                    )
                    count += 1
                except duckdb.Error as e:
                    logger.debug(f"Skipping watchlist item insert: {e}")
            conn.commit()
            return count

    def remove_from_watchlist(self, watchlist_id: int, instrument_key: str) -> bool:
        with self.get_connection() as conn:
            conn.execute(
                "DELETE FROM watchlist_items WHERE watchlist_id = ? AND instrument_key = ?",
                (watchlist_id, instrument_key),
            )
            conn.commit()
            return True

    def delete_watchlist(self, watchlist_id: int) -> bool:
        with self.get_connection() as conn:
            conn.execute("DELETE FROM watchlist_items WHERE watchlist_id = ?", (watchlist_id,))
            conn.execute("DELETE FROM watchlists WHERE id = ?", (watchlist_id,))
            conn.commit()
            return True
