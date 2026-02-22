"""Persistent task tracker — in-memory + DuckDB backing store.

Wraps TaskTracker with database persistence so tasks survive restarts.
Batch-writes to DB every few seconds to avoid DuckDB single-writer lock contention.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database.manager import DatabaseManager

from .tracker import TaskTracker

logger = logging.getLogger(__name__)


class PersistentTaskTracker:
    """Thread-safe task tracker with DuckDB persistence.

    Hot path (create/update/get) uses in-memory TaskTracker.
    A background timer thread flushes dirty tasks to DB periodically.
    """

    def __init__(
        self,
        name: str,
        db_manager: DatabaseManager,
        flush_interval: float = 3.0,
        max_age_hours: int = 1,
    ) -> None:
        self.name = name
        self.db_manager = db_manager
        self.flush_interval = flush_interval
        self._tracker = TaskTracker(name, max_age_hours=max_age_hours)
        self._dirty: set[str] = set()
        self._dirty_lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._started = False

    # ── Public API (mirrors TaskTracker) ──

    def create(self, task_id: str, initial_state: dict) -> None:
        """Register a new task in-memory and persist to DB."""
        self._tracker.create(task_id, initial_state)

        # Persist immediately on create
        try:
            self.db_manager.tasks_repo.create_task(
                task_id=task_id,
                task_type=self.name,
                params=initial_state,
                status_message=initial_state.get("status_message", ""),
            )
        except Exception:
            logger.debug(f"Failed to persist task {task_id} to DB", exc_info=True)

        # Publish SSE event
        self._publish("task:created", {"task_id": task_id, "task_type": self.name, **initial_state})

        self._ensure_flush_timer()

    def update(self, task_id: str, **kwargs: object) -> None:
        """Update task in-memory and mark dirty for next DB flush."""
        self._tracker.update(task_id, **kwargs)
        with self._dirty_lock:
            self._dirty.add(task_id)

        # Publish SSE event
        status = kwargs.get("status")
        if status in ("completed", "failed"):
            event = f"task:{status}"
        else:
            event = "task:update"
        task_data = self._tracker.get(task_id) or {}
        self._publish(event, {"task_id": task_id, "task_type": self.name, **task_data})

    def get(self, task_id: str) -> dict | None:
        """Read from in-memory (fast). Falls back to DB if not found."""
        result = self._tracker.get(task_id)
        if result is not None:
            return result

        # Fallback: check DB
        try:
            return self.db_manager.tasks_repo.get_task(task_id)
        except Exception:
            return None

    def list_active(self) -> list[dict]:
        """Return in-memory active (processing) tasks."""
        return self._tracker.list_active()

    def list_all(self) -> list[dict]:
        """Return all in-memory tasks."""
        return self._tracker.list_all()

    def cleanup(self) -> None:
        """Remove old completed/failed tasks from memory."""
        self._tracker.cleanup()

    # ── SSE publish ──

    @staticmethod
    def _publish(event: str, data: dict) -> None:
        """Publish event to SSE broker (best-effort, never raises)."""
        try:
            from src.sse.stream import sse_broker

            sse_broker.publish(event, data)
        except Exception:
            pass

    # ── DB flush ──

    def flush(self) -> None:
        """Write all dirty tasks to DB."""
        with self._dirty_lock:
            dirty_ids = list(self._dirty)
            self._dirty.clear()

        for task_id in dirty_ids:
            task = self._tracker.get(task_id)
            if not task:
                continue
            try:
                fields: dict = {}
                if "status" in task:
                    fields["status"] = task["status"]
                if "progress" in task:
                    fields["progress"] = task["progress"]
                if "status_message" in task:
                    fields["status_message"] = task["status_message"]
                if "error" in task:
                    fields["error_message"] = task["error"]
                if task.get("status") in ("completed", "failed"):
                    fields["completed_at"] = datetime.now().isoformat()
                if fields:
                    self.db_manager.tasks_repo.update_task(task_id, **fields)
            except Exception:
                logger.debug(f"Failed to flush task {task_id}", exc_info=True)
                # Re-mark as dirty for next flush
                with self._dirty_lock:
                    self._dirty.add(task_id)

    def _ensure_flush_timer(self) -> None:
        """Start periodic flush timer if not already running."""
        if self._started:
            return
        self._started = True
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        """Schedule next flush."""
        self._timer = threading.Timer(self.flush_interval, self._flush_and_reschedule)
        self._timer.daemon = True
        self._timer.start()

    def _flush_and_reschedule(self) -> None:
        """Flush dirty tasks and schedule next flush."""
        try:
            self.flush()
        except Exception:
            logger.debug("Flush failed", exc_info=True)
        # Check if there's still activity
        with self._dirty_lock:
            has_dirty = bool(self._dirty)
        has_active = bool(self._tracker.list_active())
        if has_dirty or has_active:
            self._schedule_flush()
        else:
            self._started = False

    # ── Crash recovery ──

    def recover_stale_tasks(self) -> int:
        """Mark stale tasks from previous runs as failed. Call on app startup."""
        try:
            return self.db_manager.tasks_repo.mark_stale_tasks_failed()
        except Exception:
            logger.debug("Crash recovery failed", exc_info=True)
            return 0
