"""
Analytics Engine for ExpiryTrack
Provides pre-built analytical queries for the dashboard.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any

import duckdb

from ..config import config

if TYPE_CHECKING:
    from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


# ── Analytics Cache (#14) ─────────────────────────────────
class AnalyticsCache:
    """Thread-safe in-memory cache with per-method TTLs."""

    _cache: dict = {}
    _lock = threading.Lock()

    # TTLs in seconds per method
    TTLS = {
        "get_dashboard_summary": 60,  # 1 min (cheap with summary table)
        "get_candles_per_day": 300,  # 5 min (reads summary table)
        "get_contracts_by_type": 1800,  # 30 min
        "get_contracts_by_instrument": 1800,  # 30 min
        "get_data_coverage_by_expiry": 1800,  # 30 min
        "get_volume_by_expiry": 1800,  # 30 min
        "get_storage_breakdown": 600,  # 10 min
        "get_download_status": 120,  # 2 min
        "get_missing_contracts": 60,  # 1 min
    }

    @classmethod
    def get(cls, key: str) -> Any:
        with cls._lock:
            entry = cls._cache.get(key)
            if entry and time.time() < entry["expires"]:
                return entry["value"]
            return None

    @classmethod
    def set(cls, key: str, value: Any, ttl: int) -> None:
        with cls._lock:
            cls._cache[key] = {"value": value, "expires": time.time() + ttl}

    @classmethod
    def invalidate_all(cls) -> None:
        with cls._lock:
            cls._cache.clear()
        logger.debug("Analytics cache invalidated")


def cached_query(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that caches AnalyticsEngine query results."""

    @wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        method = func.__name__
        ttl = AnalyticsCache.TTLS.get(method, 300)
        # Build cache key from method + args
        key_parts = [method] + [str(a) for a in args] + [f"{k}={v}" for k, v in sorted(kwargs.items())]
        cache_key = hashlib.md5(":".join(key_parts).encode()).hexdigest()

        cached = AnalyticsCache.get(cache_key)
        if cached is not None:
            return cached

        result = func(self, *args, **kwargs)
        AnalyticsCache.set(cache_key, result, ttl)
        return result

    return wrapper


class AnalyticsEngine:
    """Runs analytical queries against the DuckDB database."""

    def __init__(self, db_manager: DatabaseManager | None = None) -> None:
        if db_manager is None:
            from ..database.manager import DatabaseManager

            db_manager = DatabaseManager()
        self.db_manager = db_manager
        self.timeout = config.ANALYTICS_QUERY_TIMEOUT
        self.max_points = config.ANALYTICS_MAX_CHART_POINTS

    # ------------------------------------------------------------------
    # Dashboard summary
    # ------------------------------------------------------------------

    @cached_query
    def get_dashboard_summary(self) -> dict:
        """Return top-level stats for the analytics dashboard.

        Uses analytics_daily_summary for candle/trading-day counts (fast),
        falls back to historical_data if summary table is empty.
        """
        with self.db_manager.get_read_connection() as conn:
            # Check if summary table has data
            has_summary = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM analytics_daily_summary LIMIT 1)"
            ).fetchone()[0]

            if has_summary:
                row = conn.execute("""
                    SELECT
                        (SELECT COUNT(DISTINCT instrument_key) FROM instruments),
                        (SELECT COUNT(*) FROM contracts),
                        (SELECT COUNT(*) FROM contracts WHERE data_fetched = TRUE),
                        (SELECT COALESCE(SUM(candle_count), 0) FROM analytics_daily_summary),
                        (SELECT COALESCE(SUM(contract_count), 0) FROM analytics_daily_summary),
                        (SELECT COUNT(DISTINCT summary_date) FROM analytics_daily_summary),
                        (SELECT COUNT(*) FROM contracts WHERE no_data = TRUE),
                        (SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE)
                """).fetchone()
            else:
                row = conn.execute("""
                    SELECT
                        (SELECT COUNT(DISTINCT instrument_key) FROM instruments),
                        (SELECT COUNT(*) FROM contracts),
                        (SELECT COUNT(*) FROM contracts WHERE data_fetched = TRUE),
                        (SELECT COUNT(*) FROM historical_data),
                        (SELECT COUNT(DISTINCT expired_instrument_key) FROM historical_data),
                        (SELECT COUNT(DISTINCT CAST(timestamp AS DATE)) FROM historical_data),
                        (SELECT COUNT(*) FROM contracts WHERE no_data = TRUE),
                        (SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE)
                """).fetchone()

            return {
                "instruments": row[0],
                "contracts": row[1],
                "fetched_contracts": row[2],
                "total_candles": row[3],
                "contracts_with_data": row[4],
                "trading_days": row[5],
                "coverage_pct": round(row[2] / row[1] * 100, 1) if row[1] > 0 else 0,
                "no_data_contracts": row[6],
                "pending_contracts": row[7],
            }

    # ------------------------------------------------------------------
    # Charts data
    # ------------------------------------------------------------------

    @cached_query
    def get_candles_per_day(self, instrument_key: str | None = None, limit: int = 60) -> dict:
        """Candles collected per trading day — bar chart data.

        Uses analytics_daily_summary when available (pre-aggregated).
        """
        with self.db_manager.get_read_connection() as conn:
            has_summary = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM analytics_daily_summary LIMIT 1)"
            ).fetchone()[0]

            if has_summary:
                where = ""
                params = []
                if instrument_key:
                    where = "WHERE s.instrument_key = ?"
                    params = [instrument_key]
                rows = conn.execute(
                    f"""
                    SELECT s.summary_date AS day, SUM(s.candle_count) AS cnt
                    FROM analytics_daily_summary s
                    {where}
                    GROUP BY s.summary_date
                    ORDER BY s.summary_date DESC
                    LIMIT ?
                """,
                    params + [limit],
                ).fetchall()
            else:
                where = ""
                params = []
                if instrument_key:
                    where = "WHERE h.expired_instrument_key LIKE ? || '%'"
                    params = [instrument_key]
                rows = conn.execute(
                    f"""
                    SELECT CAST(h.timestamp AS DATE) AS day, COUNT(*) AS cnt
                    FROM historical_data h
                    {where}
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT ?
                """,
                    params + [limit],
                ).fetchall()

            rows.reverse()
            return {
                "labels": [str(r[0]) for r in rows],
                "data": [r[1] for r in rows],
            }

    @cached_query
    def get_contracts_by_type(self) -> dict:
        """Contract count by type (CE / PE / FUT) — pie chart data."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT contract_type, COUNT(*) AS cnt
                FROM contracts
                GROUP BY contract_type
                ORDER BY cnt DESC
            """).fetchall()

            return {
                "labels": [r[0] for r in rows],
                "data": [r[1] for r in rows],
            }

    @cached_query
    def get_contracts_by_instrument(self) -> dict:
        """Contract count by underlying instrument — bar chart data."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT i.symbol, COUNT(*) AS cnt
                FROM contracts c
                JOIN instruments i ON c.instrument_key = i.instrument_key
                GROUP BY i.symbol
                ORDER BY cnt DESC
            """).fetchall()

            return {
                "labels": [r[0] for r in rows],
                "data": [r[1] for r in rows],
            }

    @cached_query
    def get_data_coverage_by_expiry(self, instrument_key: str | None = None) -> dict:
        """For each expiry: total contracts vs contracts with data — grouped bar chart."""
        with self.db_manager.get_read_connection() as conn:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(
                f"""
                SELECT
                    c.expiry_date,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE c.data_fetched = TRUE) AS fetched
                FROM contracts c
                {where}
                GROUP BY c.expiry_date
                ORDER BY c.expiry_date DESC
                LIMIT ?
            """,
                params + [self.max_points],
            ).fetchall()

            rows.reverse()
            return {
                "labels": [str(r[0]) for r in rows],
                "total": [r[1] for r in rows],
                "fetched": [r[2] for r in rows],
            }

    @cached_query
    def get_volume_by_expiry(self, instrument_key: str | None = None, limit: int = 20) -> dict:
        """Total volume per expiry — bar chart."""
        with self.db_manager.get_read_connection() as conn:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(
                f"""
                SELECT c.expiry_date, SUM(h.volume) AS total_vol
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                {where}
                GROUP BY c.expiry_date
                ORDER BY c.expiry_date DESC
                LIMIT ?
            """,
                params + [limit],
            ).fetchall()

            rows.reverse()
            return {
                "labels": [str(r[0]) for r in rows],
                "data": [int(r[1]) for r in rows],
            }

    @cached_query
    def get_storage_breakdown(self) -> dict:
        """Approximate row counts for each table — for storage overview."""
        with self.db_manager.get_read_connection() as conn:
            tables = ["instruments", "expiries", "contracts", "historical_data", "job_status"]
            result = {}
            for t in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    result[t] = count
                except duckdb.Error:
                    result[t] = 0
            return result

    # ------------------------------------------------------------------
    # Download status
    # ------------------------------------------------------------------

    @cached_query
    def get_download_status(self, instrument_key: str | None = None) -> list[dict]:
        """Per-expiry download status: total vs fetched contracts."""
        with self.db_manager.get_read_connection() as conn:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(
                f"""
                SELECT
                    c.instrument_key,
                    i.symbol AS instrument_name,
                    c.expiry_date,
                    COUNT(*) AS total_contracts,
                    COUNT(*) FILTER (WHERE c.data_fetched = TRUE) AS fetched_contracts,
                    SUM(CASE WHEN c.data_fetched = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct,
                    CASE
                        WHEN COUNT(*) FILTER (WHERE c.data_fetched = TRUE) = COUNT(*) THEN 'complete'
                        WHEN COUNT(*) FILTER (WHERE c.data_fetched = TRUE) = 0 THEN 'not_started'
                        ELSE 'partial'
                    END AS status,
                    COUNT(*) FILTER (WHERE c.no_data = TRUE) AS no_data_contracts
                FROM contracts c
                JOIN instruments i ON c.instrument_key = i.instrument_key
                {where}
                GROUP BY c.instrument_key, i.symbol, c.expiry_date
                ORDER BY c.expiry_date DESC
            """,
                params,
            ).fetchall()

            return [
                {
                    "instrument_key": r[0],
                    "instrument_name": r[1],
                    "expiry_date": str(r[2]),
                    "total_contracts": r[3],
                    "fetched_contracts": r[4],
                    "missing_contracts": r[3] - r[4],
                    "pct": round(float(r[5]), 1),
                    "status": r[6],
                    "no_data_contracts": r[7],
                }
                for r in rows
            ]

    @cached_query
    def get_missing_contracts(self, instrument_key: str, expiry_date: str) -> list[dict]:
        """Get unfetched contracts for a specific instrument+expiry."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute(
                """
                SELECT c.expired_instrument_key, c.trading_symbol, c.contract_type, c.strike_price
                FROM contracts c
                WHERE c.instrument_key = ? AND c.expiry_date = ? AND c.data_fetched = FALSE
                ORDER BY c.strike_price, c.contract_type
            """,
                [instrument_key, expiry_date],
            ).fetchall()

            return [
                {
                    "expired_instrument_key": r[0],
                    "trading_symbol": r[1],
                    "contract_type": r[2],
                    "strike_price": float(r[3]) if r[3] is not None else None,
                }
                for r in rows
            ]

    def get_recent_collections(self, limit: int = 10) -> list[dict]:
        """Recent job_status entries for display."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, job_type, instrument_key, status, started_at, completed_at, error_message
                FROM job_status
                ORDER BY id DESC
                LIMIT ?
            """,
                [limit],
            ).fetchall()

            return [
                {
                    "id": r[0],
                    "job_type": r[1],
                    "instrument_key": r[2],
                    "status": r[3],
                    "started_at": str(r[4]) if r[4] else None,
                    "completed_at": str(r[5]) if r[5] else None,
                    "error_message": r[6],
                }
                for r in rows
            ]
