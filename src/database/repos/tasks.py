"""Task repository — persistent task storage in DuckDB."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from .base import BaseRepository

logger = logging.getLogger(__name__)


class TaskRepository(BaseRepository):
    """CRUD operations for the tasks table."""

    def create_task(
        self,
        task_id: str,
        task_type: str,
        params: dict | None = None,
        status_message: str = "",
    ) -> int:
        """Insert a new task row. Returns the auto-generated id."""
        with self.get_connection() as conn:
            row = conn.execute(
                """
                INSERT INTO tasks (task_id, task_type, status, params, status_message, created_at)
                VALUES (?, ?, 'pending', ?, ?, CURRENT_TIMESTAMP)
                RETURNING id
                """,
                [task_id, task_type, json.dumps(params or {}), status_message],
            ).fetchone()
            return int(row[0])

    def update_task(self, task_id: str, **fields: Any) -> None:
        """Update arbitrary fields on a task row."""
        if not fields:
            return
        # Serialize JSON fields
        for key in ("params", "result"):
            if key in fields and not isinstance(fields[key], str):
                fields[key] = json.dumps(fields[key])

        set_clauses = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [task_id]

        with self.get_connection() as conn:
            conn.execute(
                f"UPDATE tasks SET {set_clauses} WHERE task_id = ?",  # noqa: S608
                values,
            )

    def get_task(self, task_id: str) -> dict | None:
        """Fetch a single task by task_id."""
        with self.get_read_connection() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", [task_id]
            ).fetchone()
            if not row:
                return None
            columns = [d[0] for d in conn.description]
            return self._row_to_dict(row, columns)

    def list_tasks(
        self,
        task_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """List tasks with optional filters."""
        conditions: list[str] = []
        params: list[Any] = []
        if task_type:
            conditions.append("task_type = ?")
            params.append(task_type)
        if status:
            conditions.append("status = ?")
            params.append(status)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)

        with self.get_read_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks {where} ORDER BY created_at DESC LIMIT ?",  # noqa: S608
                params,
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            return [self._row_to_dict(r, columns) for r in rows]

    def mark_stale_tasks_failed(self) -> int:
        """Mark any 'pending' or 'processing' tasks as 'failed' (crash recovery)."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'failed',
                    error_message = 'Marked failed on startup (crash recovery)',
                    completed_at = CURRENT_TIMESTAMP
                WHERE status IN ('pending', 'processing')
                """
            )
            # DuckDB rowcount is unreliable; count manually
            count = int(conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE error_message = 'Marked failed on startup (crash recovery)'"
            ).fetchone()[0])
            if count:
                logger.info(f"Crash recovery: marked {count} stale task(s) as failed")
            return count

    def cleanup_old_tasks(self, max_age_hours: int = 72) -> int:
        """Delete completed/failed tasks older than max_age_hours."""
        cutoff = (datetime.now() - timedelta(hours=max_age_hours)).isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                DELETE FROM tasks
                WHERE status IN ('completed', 'failed')
                  AND completed_at < ?
                """,
                [cutoff],
            )
            return 0  # DuckDB rowcount unreliable

    def get_task_history(
        self, task_type: str | None = None, limit: int = 20
    ) -> list[dict]:
        """Get recent completed/failed tasks for history view."""
        conditions: list[str] = ["status IN ('completed', 'failed')"]
        params: list[Any] = []
        if task_type:
            conditions.append("task_type = ?")
            params.append(task_type)

        where = "WHERE " + " AND ".join(conditions)
        params.append(limit)

        with self.get_read_connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM tasks {where} ORDER BY completed_at DESC LIMIT ?",  # noqa: S608
                params,
            ).fetchall()
            columns = [d[0] for d in conn.description] if conn.description else []
            return [self._row_to_dict(r, columns) for r in rows]

    @staticmethod
    def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
        """Convert a DuckDB row tuple to a dict, parsing JSON fields."""
        d = {}
        for col, val in zip(columns, row, strict=False):
            if col in ("params", "result") and isinstance(val, str):
                try:
                    val = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            d[col] = val
        return d
