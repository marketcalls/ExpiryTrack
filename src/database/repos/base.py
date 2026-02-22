"""Base repository with shared database access."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..manager import DatabaseManager

logger = logging.getLogger(__name__)


class BaseRepository:
    """Base class for all repository classes."""

    def __init__(self, db_manager: DatabaseManager) -> None:
        self.db = db_manager

    def get_connection(self) -> Any:
        """Proxy to DatabaseManager.get_connection() (read-write)."""
        return self.db.get_connection()

    def get_read_connection(self) -> Any:
        """Proxy to DatabaseManager.get_read_connection() (read-only, no write lock)."""
        return self.db.get_read_connection()
