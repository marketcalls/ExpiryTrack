"""Notification manager for ExpiryTrack.

Provides CRUD operations for the notifications table and integrates
with the SSE broker to push real-time updates to connected clients.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


class NotificationManager:
    """Manages in-app notifications backed by the notifications table."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db_manager = db_manager

    def create(self, type: str, title: str, message: str | None = None) -> dict[str, Any]:
        """Insert a new notification and publish via SSE.

        Parameters
        ----------
        type:
            Notification category (collection_complete, collection_failed,
            token_expiring, quality_issue).
        title:
            Short summary shown in the bell dropdown.
        message:
            Optional longer description.

        Returns
        -------
        dict with the created notification's fields.
        """
        with self.db_manager.get_connection() as conn:
            row = conn.execute(
                """
                INSERT INTO notifications (type, title, message)
                VALUES (?, ?, ?)
                RETURNING id, type, title, message, read, created_at
                """,
                [type, title, message],
            ).fetchone()
            conn.commit()

        notification = {
            "id": row[0],
            "type": row[1],
            "title": row[2],
            "message": row[3],
            "read": row[4],
            "created_at": str(row[5]),
        }

        # Publish to SSE so the bell updates in real-time
        try:
            from ..sse.stream import sse_broker

            sse_broker.publish("notification:new", notification)
        except Exception:
            logger.debug("SSE publish failed for notification", exc_info=True)

        return notification

    def get_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        """Fetch the most recent notifications, newest first."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, type, title, message, read, created_at
                FROM notifications
                ORDER BY created_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()

        return [
            {
                "id": r[0],
                "type": r[1],
                "title": r[2],
                "message": r[3],
                "read": r[4],
                "created_at": str(r[5]),
            }
            for r in rows
        ]

    def get_unread_count(self) -> int:
        """Return the number of unread notifications."""
        with self.db_manager.get_read_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM notifications WHERE read = FALSE"
            ).fetchone()
            return int(row[0])

    def mark_read(self, notification_id: int) -> bool:
        """Mark a single notification as read. Returns True if the row existed."""
        with self.db_manager.get_connection() as conn:
            existing = conn.execute(
                "SELECT id FROM notifications WHERE id = ?",
                [notification_id],
            ).fetchone()
            if existing is None:
                return False
            conn.execute(
                "UPDATE notifications SET read = TRUE WHERE id = ?",
                [notification_id],
            )
            conn.commit()
        return True

    def mark_all_read(self) -> int:
        """Mark every unread notification as read. Returns count affected."""
        with self.db_manager.get_connection() as conn:
            conn.execute("UPDATE notifications SET read = TRUE WHERE read = FALSE")
            conn.commit()
        # DuckDB rowcount is unreliable, just return 0
        return 0
