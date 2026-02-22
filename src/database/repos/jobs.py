"""Job status repository."""

import json
import logging
from typing import Any

from ..manager import DictCursor
from .base import BaseRepository

logger = logging.getLogger(__name__)


class JobRepository(BaseRepository):
    def create_job(self, job_type: str, **kwargs: Any) -> int:
        with self.get_connection() as conn:
            result = conn.execute(
                """
                INSERT INTO job_status
                (job_type, instrument_key, expiry_date, contract_key, status, started_at)
                VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP) RETURNING id
            """,
                (
                    job_type,
                    kwargs.get("instrument_key"),
                    kwargs.get("expiry_date"),
                    kwargs.get("contract_key"),
                ),
            ).fetchone()
            return int(result[0])

    def update_job_status(self, job_id: int, status: str, error: str | None = None) -> None:
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            if status == "completed":
                cursor.execute(
                    """
                    UPDATE job_status SET status = ?, completed_at = CURRENT_TIMESTAMP WHERE id = ?
                """,
                    (status, job_id),
                )
            elif status == "failed":
                cursor.execute(
                    """
                    UPDATE job_status SET status = ?, error_message = ?, retry_count = retry_count + 1 WHERE id = ?
                """,
                    (status, error, job_id),
                )
            else:
                cursor.execute("UPDATE job_status SET status = ? WHERE id = ?", (status, job_id))

    def save_checkpoint(self, job_id: int, checkpoint_data: dict) -> None:
        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("UPDATE job_status SET checkpoint = ? WHERE id = ?", (json.dumps(checkpoint_data), job_id))
