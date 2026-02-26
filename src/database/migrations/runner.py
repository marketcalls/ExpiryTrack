"""
Lightweight migration runner for ExpiryTrack.

Tracks applied migrations in a `schema_version` table.
Each migration module must expose an `up(conn)` function.
"""

import importlib
import logging
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# Ordered list of migration module names (without the package prefix)
MIGRATIONS = [
    "m001_add_openalgo_symbol",
    "m002_add_no_data_column",
    "m003_add_fetch_attempts",
    "m004_fix_expiries_contracts_fetched",
    "m005_add_oi_column",
    "m006_add_tasks_table",
    "m007_analytics_indexes",
    "m008_analytics_summary",
    "m009_drop_historical_fk",
    "m010_quality_reports",
    "m011_export_history",
    "m012_notifications",
    "m013_backtesting",
]


class MigrationRunner:
    """Tracks applied migrations in a `schema_version` table."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn
        self._ensure_version_table()

    def _ensure_version_table(self) -> None:
        """Create schema_version table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def get_current_version(self) -> int:
        """Return the latest applied migration number, or 0 if none."""
        result = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version"
        ).fetchone()
        return int(result[0])

    def get_applied_versions(self) -> set[int]:
        """Return set of all applied migration version numbers."""
        rows = self.conn.execute("SELECT version FROM schema_version").fetchall()
        return {row[0] for row in rows}

    def run_pending(self) -> int:
        """Apply all unapplied migrations in order. Returns count applied."""
        applied = self.get_applied_versions()
        count = 0

        for idx, module_name in enumerate(MIGRATIONS, start=1):
            if idx in applied:
                continue

            try:
                mod = importlib.import_module(
                    f".{module_name}", package="src.database.migrations"
                )
                mod.up(self.conn)
                self.conn.execute(
                    "INSERT INTO schema_version (version, name) VALUES (?, ?)",
                    (idx, module_name),
                )
                self.conn.commit()
                count += 1
                logger.info(f"Migration {idx} ({module_name}) applied")
            except Exception as e:
                logger.error(f"Migration {idx} ({module_name}) failed: {e}")
                try:
                    self.conn.rollback()
                except duckdb.Error:
                    pass
                raise

        if count:
            logger.info(f"Applied {count} migration(s), now at version {self.get_current_version()}")
        return count
