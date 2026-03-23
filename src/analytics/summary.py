"""Analytics daily summary — pre-aggregated data for fast dashboard queries."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database.manager import DatabaseManager

logger = logging.getLogger(__name__)


def refresh_daily_summary(db_manager: DatabaseManager, since_date: str | None = None) -> int:
    """Refresh the analytics_daily_summary table.

    Aggregates historical_data candle counts and volumes by date and instrument_key.
    Uses DELETE+INSERT instead of UPSERT for DuckDB compatibility.

    Args:
        db_manager: DatabaseManager instance.
        since_date: Only refresh from this date forward (YYYY-MM-DD).
                    If None, refreshes from 30 days ago.

    Returns:
        Number of summary rows inserted.
    """
    if since_date is None:
        since_date = (date.today() - timedelta(days=30)).isoformat()

    with db_manager.get_connection() as conn:
        # Delete existing rows for the refresh window
        conn.execute(
            "DELETE FROM analytics_daily_summary WHERE summary_date >= ?",
            [since_date],
        )

        # Insert fresh aggregation
        conn.execute(
            """
            INSERT INTO analytics_daily_summary
                (summary_date, instrument_key, candle_count, total_volume, contract_count, updated_at)
            SELECT
                CAST(h.timestamp AS DATE) AS summary_date,
                c.instrument_key,
                COUNT(*) AS candle_count,
                COALESCE(SUM(h.volume), 0) AS total_volume,
                COUNT(DISTINCT h.expired_instrument_key) AS contract_count,
                CURRENT_TIMESTAMP
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE CAST(h.timestamp AS DATE) >= ?
            GROUP BY CAST(h.timestamp AS DATE), c.instrument_key
            """,
            [since_date],
        )

        count = int(conn.execute(
            "SELECT COUNT(*) FROM analytics_daily_summary WHERE summary_date >= ?",
            [since_date],
        ).fetchone()[0])

        logger.info(f"Refreshed {count} daily summary rows since {since_date}")
        return count
