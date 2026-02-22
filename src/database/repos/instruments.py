"""Instrument repository — default instruments CRUD and F&O import."""

import logging

import duckdb

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

logger = logging.getLogger(__name__)


class InstrumentRepository(BaseRepository):
    def setup_default_instruments(self) -> bool:
        default_instruments = [
            {"key": "NSE_INDEX|Nifty 50", "symbol": "Nifty 50", "priority": 100, "category": "Index"},
            {"key": "NSE_INDEX|Nifty Bank", "symbol": "Bank Nifty", "priority": 90, "category": "Index"},
            {"key": "BSE_INDEX|SENSEX", "symbol": "Sensex", "priority": 80, "category": "Index"},
            {"key": "NSE_INDEX|Nifty Fin Service", "symbol": "FINNIFTY", "priority": 70, "category": "Index"},
            {"key": "NSE_INDEX|NIFTY MID SELECT", "symbol": "MIDCPNIFTY", "priority": 60, "category": "Index"},
            {"key": "BSE_INDEX|BANKEX", "symbol": "BANKEX", "priority": 50, "category": "Index"},
        ]
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            try:
                cursor.execute("SELECT category FROM default_instruments LIMIT 0")
            except duckdb.Error:
                cursor.execute("ALTER TABLE default_instruments ADD COLUMN category TEXT DEFAULT 'Index'")
            for inst in default_instruments:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO default_instruments (instrument_key, symbol, priority, category)
                    VALUES (?, ?, ?, ?)
                """,
                    (inst["key"], inst["symbol"], inst["priority"], inst["category"]),
                )
            logger.info("Default instruments configured")
            return True

    def get_default_instruments(self) -> list[str]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT instrument_key FROM default_instruments
                WHERE is_active = TRUE ORDER BY priority DESC
            """)
            return [row[0] for row in cursor.fetchall()]

    def get_active_instruments(self) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT id, instrument_key, symbol, is_active, priority,
                       COALESCE(category, 'Index') as category
                FROM default_instruments ORDER BY priority DESC
            """)
            return [dict_from_row(row) for row in cursor.fetchall()]

    def add_instrument(
        self, instrument_key: str, symbol: str, priority: int = 0, category: str = "Index"
    ) -> int | None:
        with self.get_connection() as conn:
            try:
                result = conn.execute(
                    """
                    INSERT INTO default_instruments (instrument_key, symbol, priority, category)
                    VALUES (?, ?, ?, ?) RETURNING id
                """,
                    (instrument_key, symbol, priority, category),
                )
                row = result.fetchone()
                conn.commit()
                return row[0] if row else None
            except duckdb.Error as e:
                if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                    return None
                raise

    def toggle_instrument(self, instrument_id: int, is_active: bool) -> bool:
        with self.get_connection() as conn:
            conn.execute("UPDATE default_instruments SET is_active = ? WHERE id = ?", (is_active, instrument_id))
            conn.commit()
            return True

    def remove_instrument(self, instrument_id: int) -> bool:
        with self.get_connection() as conn:
            conn.execute("DELETE FROM default_instruments WHERE id = ?", (instrument_id,))
            conn.commit()
            return True

    def insert_instrument(self, instrument_data: dict) -> bool:
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                INSERT OR REPLACE INTO instruments
                (instrument_key, symbol, name, exchange, segment, underlying_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    instrument_data["instrument_key"],
                    instrument_data["symbol"],
                    instrument_data.get("name"),
                    instrument_data.get("exchange"),
                    instrument_data.get("segment"),
                    instrument_data.get("underlying_type"),
                ),
            )
            return True

    # ── F&O Discovery ──

    def get_fo_underlying_instruments(self, category: str | None = None) -> list[dict]:
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            results = []

            if category is None or category == "stock":
                cursor.execute("""
                    SELECT DISTINCT m.name as underlying_name FROM instrument_master m
                    WHERE m.segment = 'NSE_FO' AND m.instrument_type = 'FUT'
                      AND m.name IS NOT NULL AND m.name != '' ORDER BY m.name
                """)
                fo_names = [row["underlying_name"] for row in cursor.fetchall()]
                index_names = {
                    "NIFTY",
                    "BANKNIFTY",
                    "FINNIFTY",
                    "MIDCPNIFTY",
                    "NIFTY 50",
                    "Nifty 50",
                    "Nifty Bank",
                    "Nifty Fin Service",
                    "NIFTY MID SELECT",
                    "SENSEX",
                    "BANKEX",
                    "NIFTYNXT50",
                }
                for name in fo_names:
                    if name.upper() in {n.upper() for n in index_names}:
                        continue
                    cursor.execute(
                        """
                        SELECT instrument_key, trading_symbol, name FROM instrument_master
                        WHERE segment = 'NSE_EQ' AND instrument_type = 'EQ' AND name = ? LIMIT 1
                    """,
                        (name,),
                    )
                    eq_row = cursor.fetchone()
                    if eq_row:
                        results.append(
                            {
                                "instrument_key": eq_row["instrument_key"],
                                "symbol": eq_row["trading_symbol"],
                                "category": "Stock F&O",
                            }
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT trading_symbol FROM instrument_master
                            WHERE segment = 'NSE_FO' AND instrument_type = 'FUT' AND name = ? LIMIT 1
                        """,
                            (name,),
                        )
                        fo_row = cursor.fetchone()
                        short_sym = fo_row["trading_symbol"].split()[0] if fo_row else name
                        results.append(
                            {"instrument_key": f"NSE_EQ|{name}", "symbol": short_sym, "category": "Stock F&O"}
                        )

            if category is None or category == "commodity":
                cursor.execute("""
                    SELECT DISTINCT m.name as underlying_name FROM instrument_master m
                    WHERE m.segment = 'MCX_FO' AND m.instrument_type = 'FUT'
                      AND m.name IS NOT NULL AND m.name != '' ORDER BY m.name
                """)
                for row in cursor.fetchall():
                    results.append(
                        {
                            "instrument_key": f"MCX_FO|{row['underlying_name']}",
                            "symbol": row["underlying_name"],
                            "category": "Commodity",
                        }
                    )

            if category is None or category == "bse_stock":
                cursor.execute("""
                    SELECT DISTINCT m.name as underlying_name FROM instrument_master m
                    WHERE m.segment = 'BSE_FO' AND m.instrument_type = 'FUT'
                      AND m.name IS NOT NULL AND m.name != '' ORDER BY m.name
                """)
                bse_names = [row["underlying_name"] for row in cursor.fetchall()]
                bse_index_names = {"SENSEX", "BANKEX"}
                for name in bse_names:
                    if name.upper() in bse_index_names:
                        continue
                    cursor.execute(
                        """
                        SELECT instrument_key, trading_symbol, name FROM instrument_master
                        WHERE segment = 'BSE_EQ' AND instrument_type = 'EQ' AND name = ? LIMIT 1
                    """,
                        (name,),
                    )
                    eq_row = cursor.fetchone()
                    if eq_row:
                        results.append(
                            {
                                "instrument_key": eq_row["instrument_key"],
                                "symbol": eq_row["trading_symbol"],
                                "category": "BSE F&O",
                            }
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT trading_symbol FROM instrument_master
                            WHERE segment = 'BSE_FO' AND instrument_type = 'FUT' AND name = ? LIMIT 1
                        """,
                            (name,),
                        )
                        fo_row = cursor.fetchone()
                        short_sym = fo_row["trading_symbol"].split()[0] if fo_row else name
                        results.append({"instrument_key": f"BSE_EQ|{name}", "symbol": short_sym, "category": "BSE F&O"})
            return results

    def get_fo_available_instruments(self, category: str | None = None) -> list[dict]:
        all_fo = self.get_fo_underlying_instruments(category)
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT instrument_key FROM default_instruments")
            existing_keys = {row[0] for row in cursor.fetchall()}
        return [inst for inst in all_fo if inst["instrument_key"] not in existing_keys]

    def bulk_import_fo_instruments(self, category: str | None = None) -> dict:
        available = self.get_fo_available_instruments(category)
        added = 0
        skipped = 0
        priority_map = {"Stock F&O": 50, "Commodity": 30, "BSE F&O": 40}
        with self.get_connection() as conn:
            for inst in available:
                try:
                    priority = priority_map.get(inst["category"], 50)
                    conn.execute(
                        """
                        INSERT INTO default_instruments (instrument_key, symbol, priority, category)
                        VALUES (?, ?, ?, ?)
                    """,
                        (inst["instrument_key"], inst["symbol"], priority, inst["category"]),
                    )
                    added += 1
                except duckdb.Error as e:
                    if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                        skipped += 1
                    else:
                        logger.warning(f"Failed to import {inst['symbol']}: {e}")
                        skipped += 1
            conn.commit()
        logger.info(f"Bulk F&O import: added={added}, skipped={skipped}")
        return {"added": added, "skipped": skipped, "total_available": len(available)}
