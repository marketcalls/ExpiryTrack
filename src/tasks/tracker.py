"""Unified in-memory task state tracker with automatic cleanup."""

import threading
from datetime import datetime, timedelta


class TaskTracker:
    """Thread-safe in-memory task state tracker.

    Replaces the duplicated dict+lock+cleanup pattern used by
    export_tasks and candle_tasks.
    """

    def __init__(self, name: str, max_age_hours: int = 1):
        self.name = name
        self._tasks: dict[str, dict] = {}
        self._lock = threading.Lock()
        self.max_age_hours = max_age_hours

    def create(self, task_id: str, initial_state: dict) -> None:
        """Register a new task with initial state."""
        with self._lock:
            self._tasks[task_id] = initial_state

    def get(self, task_id: str) -> dict | None:
        """Return a shallow copy of the task, or None."""
        with self._lock:
            task = self._tasks.get(task_id)
            return dict(task) if task else None

    def update(self, task_id: str, **kwargs: object) -> None:
        """Merge kwargs into the task dict."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].update(kwargs)

    def list_active(self) -> list[dict]:
        """Return tasks with status 'processing'."""
        with self._lock:
            return [dict(t) for t in self._tasks.values() if t.get("status") == "processing"]

    def list_all(self) -> list[dict]:
        """Return all tasks."""
        with self._lock:
            return [dict(t) for t in self._tasks.values()]

    def cleanup(self) -> None:
        """Remove completed/failed tasks older than max_age_hours."""
        cutoff = datetime.now() - timedelta(hours=self.max_age_hours)
        with self._lock:
            to_remove = [
                tid
                for tid, t in self._tasks.items()
                if t.get("status") in ("completed", "failed")
                and t.get("created_at")
                and datetime.fromisoformat(t["created_at"]) < cutoff
            ]
            for tid in to_remove:
                self._tasks.pop(tid, None)
