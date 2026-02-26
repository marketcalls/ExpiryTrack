"""Backtest repository — CRUD for strategies and backtest results."""

from __future__ import annotations

import json
import logging
from typing import Any

from .base import BaseRepository

logger = logging.getLogger(__name__)


class BacktestRepository(BaseRepository):
    """CRUD operations for strategies and backtest_results tables."""

    # ── Strategies ──

    def save_strategy(
        self,
        name: str,
        code: str,
        description: str = "",
        is_preset: bool = False,
        strategy_id: int | None = None,
    ) -> int:
        """Insert or update a strategy. Returns the id."""
        with self.get_connection() as conn:
            if strategy_id:
                conn.execute(
                    """UPDATE strategies
                       SET name = ?, description = ?, code = ?, is_preset = ?,
                           updated_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    [name, description, code, is_preset, strategy_id],
                )
                return strategy_id
            row = conn.execute(
                """INSERT INTO strategies (name, description, code, is_preset)
                   VALUES (?, ?, ?, ?)
                   RETURNING id""",
                [name, description, code, is_preset],
            ).fetchone()
            return int(row[0])

    def get_strategy(self, strategy_id: int) -> dict[str, Any] | None:
        """Fetch a single strategy by id."""
        with self.get_read_connection() as conn:
            row = conn.execute(
                "SELECT * FROM strategies WHERE id = ?", [strategy_id]
            ).fetchone()
            if not row:
                return None
            columns = [d[0] for d in conn.description]
            return self._row_to_dict(row, columns)

    def list_strategies(self) -> list[dict[str, Any]]:
        """List all strategies ordered by preset-first, then name."""
        with self.get_read_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM strategies ORDER BY is_preset DESC, name"
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            return [self._row_to_dict(r, columns) for r in rows]

    def delete_strategy(self, strategy_id: int) -> None:
        """Delete a strategy and its results."""
        with self.get_connection() as conn:
            conn.execute(
                "DELETE FROM backtest_results WHERE strategy_id = ?", [strategy_id]
            )
            conn.execute("DELETE FROM strategies WHERE id = ?", [strategy_id])

    # ── Results ──

    def save_result(
        self,
        strategy_id: int | None,
        task_id: str,
        instrument_key: str,
        data_source: str,
        interval: str,
        from_date: str | None,
        to_date: str | None,
        initial_capital: float,
        status: str = "running",
        metrics: dict | None = None,
        trades: list | None = None,
        equity_curve: list | None = None,
        error_message: str | None = None,
        bars_processed: int = 0,
    ) -> int:
        """Insert a backtest result. Returns the id."""
        with self.get_connection() as conn:
            row = conn.execute(
                """INSERT INTO backtest_results
                   (strategy_id, task_id, instrument_key, data_source, interval,
                    from_date, to_date, initial_capital, status, metrics, trades,
                    equity_curve, error_message, bars_processed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   RETURNING id""",
                [
                    strategy_id,
                    task_id,
                    instrument_key,
                    data_source,
                    interval,
                    from_date,
                    to_date,
                    initial_capital,
                    status,
                    json.dumps(metrics) if metrics else None,
                    json.dumps(trades) if trades else None,
                    json.dumps(equity_curve) if equity_curve else None,
                    error_message,
                    bars_processed,
                ],
            ).fetchone()
            return int(row[0])

    def update_result(self, result_id: int, **kwargs: Any) -> None:
        """Update specific fields on a backtest result."""
        # Serialize JSON fields
        for key in ("metrics", "trades", "equity_curve"):
            if key in kwargs and kwargs[key] is not None:
                kwargs[key] = json.dumps(kwargs[key])

        sets = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [result_id]
        with self.get_connection() as conn:
            conn.execute(
                f"UPDATE backtest_results SET {sets} WHERE id = ?", values
            )

    def get_result(self, result_id: int) -> dict[str, Any] | None:
        """Fetch a single result by id."""
        with self.get_read_connection() as conn:
            row = conn.execute(
                "SELECT * FROM backtest_results WHERE id = ?", [result_id]
            ).fetchone()
            if not row:
                return None
            columns = [d[0] for d in conn.description]
            result = self._row_to_dict(row, columns)
            self._parse_json_fields(result)
            return result

    def get_result_by_task(self, task_id: str) -> dict[str, Any] | None:
        """Fetch a result by task_id."""
        with self.get_read_connection() as conn:
            row = conn.execute(
                "SELECT * FROM backtest_results WHERE task_id = ?", [task_id]
            ).fetchone()
            if not row:
                return None
            columns = [d[0] for d in conn.description]
            result = self._row_to_dict(row, columns)
            self._parse_json_fields(result)
            return result

    def list_results(self, limit: int = 50) -> list[dict[str, Any]]:
        """List recent backtest results."""
        with self.get_read_connection() as conn:
            rows = conn.execute(
                """SELECT br.*, s.name as strategy_name
                   FROM backtest_results br
                   LEFT JOIN strategies s ON br.strategy_id = s.id
                   ORDER BY br.created_at DESC
                   LIMIT ?""",
                [limit],
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            results = [self._row_to_dict(r, columns) for r in rows]
            for r in results:
                self._parse_json_fields(r)
            return results

    def delete_result(self, result_id: int) -> None:
        """Delete a backtest result by id."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM backtest_results WHERE id = ?", [result_id])

    # ── Helpers ──

    @staticmethod
    def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
        d: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=False):
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            d[col] = val
        return d

    @staticmethod
    def _parse_json_fields(result: dict[str, Any]) -> None:
        """Parse JSON string fields into Python objects."""
        for key in ("metrics", "trades", "equity_curve"):
            val = result.get(key)
            if isinstance(val, str):
                try:
                    result[key] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
