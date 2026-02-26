"""Export history repository."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from .base import BaseRepository

logger = logging.getLogger(__name__)


class ExportsRepo(BaseRepository):
    """CRUD operations for the export_history table."""

    def save_export(
        self,
        export_format: str,
        instruments: str,
        expiries: str,
        file_path: str,
        file_size: int = 0,
        row_count: int = 0,
        contract_types: str | None = None,
    ) -> int:
        """Insert a new export history record. Returns the auto-generated id."""
        with self.get_connection() as conn:
            row = conn.execute(
                """
                INSERT INTO export_history
                    (format, instruments, expiries, file_path, file_size, row_count, contract_types)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                [export_format, instruments, expiries, file_path, file_size, row_count, contract_types],
            ).fetchone()
            return int(row[0])

    def get_recent_exports(self, limit: int = 20) -> list[dict]:
        """Fetch the most recent export history records."""
        with self.get_read_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM export_history ORDER BY created_at DESC LIMIT ?",
                [limit],
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            return [self._row_to_dict(r, columns) for r in rows]

    def delete_export(self, export_id: int) -> None:
        """Delete an export history record by id."""
        with self.get_connection() as conn:
            conn.execute("DELETE FROM export_history WHERE id = ?", [export_id])

    def get_expired_exports(self, days: int = 7) -> list[dict]:
        """Fetch export records older than the given number of days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with self.get_read_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM export_history WHERE created_at < ? ORDER BY created_at",
                [cutoff],
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            return [self._row_to_dict(r, columns) for r in rows]

    @staticmethod
    def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
        """Convert a DuckDB row tuple to a dict."""
        d = {}
        for col, val in zip(columns, row, strict=False):
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            d[col] = val
        return d
