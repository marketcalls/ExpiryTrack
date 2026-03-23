"""Contract repository — insert, query, expiry management."""

import json
import logging
from datetime import datetime

import duckdb

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

logger = logging.getLogger(__name__)


class ContractRepository(BaseRepository):
    def insert_expiries(self, instrument_key: str, expiry_dates: list[str]) -> int:
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT expiry_date FROM expiries WHERE instrument_key = ?", (instrument_key,))
            existing = {str(row[0]) for row in cursor.fetchall()}
            count = 0
            for expiry_date in expiry_dates:
                if expiry_date in existing:
                    continue
                try:
                    date_obj = datetime.strptime(expiry_date, "%Y-%m-%d")
                    is_weekly = date_obj.weekday() == 3
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO expiries (instrument_key, expiry_date, is_weekly)
                        VALUES (?, ?, ?)
                    """,
                        (instrument_key, expiry_date, is_weekly),
                    )
                    count += 1
                except (ValueError, duckdb.Error) as e:
                    logger.error(f"Failed to insert expiry {expiry_date}: {e}")
            logger.info(f"Inserted {count} new expiries for {instrument_key}")
            return count

    def get_pending_expiries(self, instrument_key: str) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT * FROM expiries
                WHERE instrument_key = ? AND contracts_fetched = FALSE ORDER BY expiry_date
            """,
                (instrument_key,),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def mark_expiry_contracts_fetched(self, instrument_key: str, expiry_date: str) -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE expiries SET contracts_fetched = TRUE
                WHERE instrument_key = ? AND expiry_date = ?
            """,
                (instrument_key, expiry_date),
            )
            conn.commit()

    def insert_contracts(self, contracts: list[dict]) -> int:
        from ...utils.openalgo_symbol import to_openalgo_symbol

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            inserted = 0
            for contract in contracts:
                try:
                    expired_key = contract.get("instrument_key", "")
                    openalgo_symbol = to_openalgo_symbol(contract)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO contracts
                        (expired_instrument_key, instrument_key, expiry_date,
                         contract_type, strike_price, trading_symbol, openalgo_symbol,
                         lot_size, tick_size, exchange_token, freeze_quantity, minimum_lot, metadata)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            expired_key,
                            contract.get("underlying_key", ""),
                            contract.get("expiry", ""),
                            contract.get("instrument_type", ""),
                            contract.get("strike_price"),
                            contract.get("trading_symbol", ""),
                            openalgo_symbol,
                            contract.get("lot_size"),
                            contract.get("tick_size"),
                            contract.get("exchange_token", ""),
                            contract.get("freeze_quantity"),
                            contract.get("minimum_lot"),
                            json.dumps(contract),
                        ),
                    )
                    inserted += 1
                except duckdb.Error as e:
                    if "duplicate" not in str(e).lower() and "unique" not in str(e).lower():
                        logger.error(f"Failed to insert contract {contract.get('trading_symbol')}: {e}")
            logger.info(f"Inserted {inserted} contracts")
            return inserted

    def get_pending_contracts(self, limit: int = 100) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT * FROM contracts WHERE data_fetched = FALSE LIMIT ?", (limit,))
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_fetched_keys(self, expired_keys: list) -> set:
        if not expired_keys:
            return set()
        with self.get_read_connection() as conn:
            placeholders = ",".join(["?"] * len(expired_keys))
            rows = conn.execute(
                f"""
                SELECT expired_instrument_key FROM contracts
                WHERE data_fetched = TRUE AND expired_instrument_key IN ({placeholders})
            """,
                expired_keys,
            ).fetchall()
            return {row[0] for row in rows}

    def get_contract_by_openalgo_symbol(self, openalgo_symbol: str) -> dict | None:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT * FROM contracts WHERE openalgo_symbol = ?", (openalgo_symbol,))
            row = cursor.fetchone()
            return dict_from_row(row) if row else None

    def get_contracts_by_base_symbol(self, base_symbol: str) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT * FROM contracts WHERE openalgo_symbol LIKE ?
                ORDER BY expiry_date, strike_price
            """,
                (f"{base_symbol}%",),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_option_chain(self, base_symbol: str, expiry_date: str) -> dict[str, list[dict]]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            from ...utils.openalgo_symbol import OpenAlgoSymbolGenerator

            formatted_date = OpenAlgoSymbolGenerator.format_expiry_date(expiry_date)
            cursor.execute(
                """
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%CE' ORDER BY strike_price
            """,
                (f"{base_symbol}{formatted_date}%",),
            )
            calls = [dict_from_row(row) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%PE' ORDER BY strike_price
            """,
                (f"{base_symbol}{formatted_date}%",),
            )
            puts = [dict_from_row(row) for row in cursor.fetchall()]
            return {"calls": calls, "puts": puts}

    def get_futures_by_symbol(self, base_symbol: str) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT * FROM contracts
                WHERE openalgo_symbol LIKE ? AND openalgo_symbol LIKE '%FUT' ORDER BY expiry_date
            """,
                (f"{base_symbol}%",),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def search_openalgo_symbols(self, pattern: str) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT openalgo_symbol, trading_symbol, expiry_date, contract_type, strike_price
                FROM contracts WHERE openalgo_symbol LIKE ? ORDER BY openalgo_symbol LIMIT 100
            """,
                (f"%{pattern}%",),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def get_expiries_for_instrument(self, instrument: str) -> list[str]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT DISTINCT expiry_date FROM contracts
                WHERE instrument_key = ? ORDER BY expiry_date DESC
            """,
                (instrument,),
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def get_contracts_for_expiry(self, instrument: str, expiry_date: str) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT * FROM contracts
                WHERE instrument_key = ? AND expiry_date = ?
                ORDER BY strike_price, contract_type
            """,
                (instrument, expiry_date),
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    # ── Retry/Reset ──

    def increment_fetch_attempt(self, expired_key: str) -> None:
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE contracts SET fetch_attempts = fetch_attempts + 1,
                    last_attempted_at = CURRENT_TIMESTAMP
                WHERE expired_instrument_key = ?
            """,
                (expired_key,),
            )
            conn.commit()

    def get_failed_contracts(self, instrument_key: str | None = None) -> list[dict]:
        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            where = "WHERE c.data_fetched = FALSE AND c.fetch_attempts > 0 AND c.no_data = FALSE"
            params = []
            if instrument_key:
                where += " AND c.instrument_key = ?"
                params.append(instrument_key)
            cursor.execute(
                f"""
                SELECT c.expired_instrument_key, c.instrument_key, c.expiry_date,
                       c.trading_symbol, c.fetch_attempts, c.last_attempted_at
                FROM contracts c {where} ORDER BY c.last_attempted_at
            """,
                params,
            )
            return [dict_from_row(row) for row in cursor.fetchall()]

    def reset_fetch_attempts(self, instrument_key: str | None = None) -> int:
        with self.get_connection() as conn:
            where = "WHERE data_fetched = FALSE AND fetch_attempts > 0 AND no_data = FALSE"
            params = []
            if instrument_key:
                where += " AND instrument_key = ?"
                params.append(instrument_key)
            result = conn.execute(
                f"""
                UPDATE contracts SET fetch_attempts = 0, last_attempted_at = NULL
                {where} RETURNING expired_instrument_key
            """,
                params,
            )
            count = len(result.fetchall())
            conn.commit()
            return count

    def reset_contracts_for_refetch(self, instrument_key: str, expiry_date: str) -> int:
        with self.get_connection() as conn:
            result = conn.execute(
                """
                UPDATE contracts SET data_fetched = FALSE, no_data = FALSE
                WHERE instrument_key = ? AND expiry_date = ?
                RETURNING expired_instrument_key
            """,
                (instrument_key, expiry_date),
            )
            count = len(result.fetchall())
            conn.commit()
            logger.info(f"Reset {count} contracts for {instrument_key} expiry {expiry_date}")
            return count
