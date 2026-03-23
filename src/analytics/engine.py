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
    import pandas as pd

    from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)


# Full 1-minute candles in an NSE trading day (09:15–15:30 = 375 minutes)
CANDLES_PER_TRADING_DAY = 375

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
        "get_oi_by_strike": 120,  # 2 min
        "get_pcr_trend": 300,  # 5 min
        "calculate_max_pain": 120,  # 2 min
        "get_oi_heatmap": 300,  # 5 min
        "get_volume_profile": 300,  # 5 min
        "get_expiry_comparison": 300,  # 5 min
        "get_coverage_calendar": 600,  # 10 min
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

            # Data freshness: last collection time and newest data date
            freshness = conn.execute("""
                SELECT
                    (SELECT MAX(completed_at) FROM job_status WHERE status = 'completed'),
                    (SELECT MAX(CAST(timestamp AS DATE)) FROM historical_data)
            """).fetchone()

            last_collection_at = str(freshness[0]) if freshness[0] else None
            newest_data_date = str(freshness[1]) if freshness[1] else None

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
                "last_collection_at": last_collection_at,
                "newest_data_date": newest_data_date,
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
    def get_download_status(self, instrument_key: str | None = None, page: int = 0, per_page: int = 0) -> list[dict] | dict:
        """Per-expiry download status: total vs fetched contracts.

        If page > 0 and per_page > 0, returns paginated dict with metadata.
        Otherwise returns flat list (backward compatible).
        """
        with self.db_manager.get_read_connection() as conn:
            where = ""
            params: list = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            base_query = f"""
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
            """

            if page > 0 and per_page > 0:
                # Count total groups first
                count_query = f"""
                    SELECT COUNT(*) FROM (
                        SELECT c.instrument_key, c.expiry_date
                        FROM contracts c
                        JOIN instruments i ON c.instrument_key = i.instrument_key
                        {where}
                        GROUP BY c.instrument_key, i.symbol, c.expiry_date
                    )
                """
                total_count = conn.execute(count_query, params).fetchone()[0]
                total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1

                offset = (page - 1) * per_page
                paginated_query = base_query + f" LIMIT {per_page} OFFSET {offset}"
                rows = conn.execute(paginated_query, params).fetchall()

                items = self._format_download_status_rows(rows)
                return {
                    "items": items,
                    "page": page,
                    "per_page": per_page,
                    "total": total_count,
                    "total_pages": total_pages,
                }
            else:
                rows = conn.execute(base_query, params).fetchall()
                return self._format_download_status_rows(rows)

    @staticmethod
    def _format_download_status_rows(rows) -> list[dict]:
        """Format download status query rows into dicts."""
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

    @cached_query
    def get_option_chain_data(self, instrument_key: str, expiry_date: str) -> list[dict]:
        """Get option chain data: CE/PE grouped by strike price."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    c.strike_price,
                    c.contract_type,
                    c.trading_symbol,
                    c.expired_instrument_key,
                    c.openalgo_symbol,
                    c.data_fetched,
                    (SELECT COUNT(*) FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key) as candle_count,
                    (SELECT h.close FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_close,
                    (SELECT h.volume FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_volume,
                    (SELECT h.oi FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_oi,
                    (SELECT h.open FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_open,
                    (SELECT h.high FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_high,
                    (SELECT h.low FROM historical_data h
                     WHERE h.expired_instrument_key = c.expired_instrument_key
                     ORDER BY h.timestamp DESC LIMIT 1) as last_low
                FROM contracts c
                WHERE c.instrument_key = ? AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                ORDER BY c.strike_price, c.contract_type
            """, [instrument_key, expiry_date]).fetchall()

            result = []
            for r in rows:
                result.append({
                    "strike_price": float(r[0]) if r[0] else 0,
                    "contract_type": r[1],
                    "trading_symbol": r[2],
                    "expired_instrument_key": r[3],
                    "openalgo_symbol": r[4],
                    "data_fetched": r[5],
                    "candle_count": r[6] or 0,
                    "last_close": float(r[7]) if r[7] else None,
                    "last_volume": int(r[8]) if r[8] else 0,
                    "last_oi": int(r[9]) if r[9] else 0,
                    "last_open": float(r[10]) if r[10] else None,
                    "last_high": float(r[11]) if r[11] else None,
                    "last_low": float(r[12]) if r[12] else None,
                })
            return result

    @cached_query
    def get_available_expiry_dates(self, instrument_key: str) -> list[str]:
        """Get all expiry dates for an instrument that have contracts."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT DISTINCT expiry_date
                FROM contracts
                WHERE instrument_key = ?
                ORDER BY expiry_date DESC
            """, [instrument_key]).fetchall()
            return [str(r[0]) for r in rows]

    @cached_query
    def get_available_instruments_with_contracts(self) -> list[dict]:
        """Get instruments that have contracts in the database."""
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT DISTINCT c.instrument_key, i.symbol
                FROM contracts c
                JOIN instruments i ON c.instrument_key = i.instrument_key
                ORDER BY i.symbol
            """).fetchall()
            return [{"instrument_key": r[0], "symbol": r[1]} for r in rows]

    # ------------------------------------------------------------------
    # Candlestick chart data (D5)
    # ------------------------------------------------------------------

    INTERVAL_BUCKETS = {
        "1minute": "1 minute",
        "5minute": "5 minutes",
        "15minute": "15 minutes",
        "1hour": "1 hour",
        "1day": "1 day",
    }

    def get_candle_data(
        self,
        instrument_key: str,
        contract_key: str | None = None,
        interval: str = "1minute",
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[dict]:
        """Return OHLCV candle data for charting.

        For ``candle_data`` rows the stored interval is always the original
        collection interval (typically ``1minute`` or ``1day``).  If the
        caller requests a coarser interval we aggregate on-the-fly using
        DuckDB's ``time_bucket``.

        Parameters
        ----------
        instrument_key:
            The instrument_key to chart (from candle_data table).
        contract_key:
            Optional expired_instrument_key — when provided we read from
            historical_data instead of candle_data so users can chart
            individual option/futures contracts.
        interval:
            One of 1minute, 5minute, 15minute, 1hour, 1day.
        from_date / to_date:
            ISO-format date strings for range filtering.
        """
        bucket = self.INTERVAL_BUCKETS.get(interval)
        if bucket is None:
            bucket = self.INTERVAL_BUCKETS["1minute"]

        with self.db_manager.get_read_connection() as conn:
            # Decide source table based on whether a specific contract was requested
            if contract_key:
                # Chart a specific contract from historical_data
                base_table = "historical_data"
                key_col = "expired_instrument_key"
                key_val = contract_key
            else:
                # Chart an instrument from candle_data
                base_table = "candle_data"
                key_col = "instrument_key"
                key_val = instrument_key

            conditions = [f"{key_col} = ?"]
            params: list = [key_val]

            # Filter by interval column when querying candle_data to avoid
            # mixing daily summary candles (interval='1day') with minute data.
            if base_table == "candle_data":
                if interval == "1day":
                    conditions.append("interval = '1day'")
                else:
                    # For 1min/5min/15min/1hour — use minute-level source rows
                    conditions.append("interval = '1minute'")

            if from_date:
                conditions.append("timestamp >= ?")
                params.append(from_date)
            if to_date:
                conditions.append("timestamp <= ?")
                params.append(to_date + " 23:59:59")

            where = " AND ".join(conditions)

            if interval == "1minute" or (base_table == "candle_data" and interval == "1day"):
                # No aggregation needed — return raw rows
                rows = conn.execute(
                    f"""
                    SELECT timestamp, open, high, low, close, volume
                    FROM {base_table}
                    WHERE {where}
                    ORDER BY timestamp
                    LIMIT ?
                    """,
                    params + [self.max_points],
                ).fetchall()
            else:
                # Aggregate using time_bucket
                rows = conn.execute(
                    f"""
                    SELECT
                        time_bucket(INTERVAL '{bucket}', timestamp) AS bucket_ts,
                        FIRST(open ORDER BY timestamp)  AS open,
                        MAX(high)                        AS high,
                        MIN(low)                         AS low,
                        LAST(close ORDER BY timestamp)   AS close,
                        SUM(volume)                      AS volume
                    FROM {base_table}
                    WHERE {where}
                    GROUP BY bucket_ts
                    ORDER BY bucket_ts
                    LIMIT ?
                    """,
                    params + [self.max_points],
                ).fetchall()

            return [
                {
                    "timestamp": str(r[0]),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "volume": int(r[5]) if r[5] else 0,
                }
                for r in rows
            ]

    def get_instruments_for_chart(self) -> list[dict]:
        """Return instruments that have candle data available.

        Joins ``candle_data`` with ``instrument_master`` so we can show
        a friendly trading symbol alongside the key.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT DISTINCT cd.instrument_key,
                       COALESCE(im.trading_symbol, cd.instrument_key) AS symbol,
                       COALESCE(im.name, '') AS name,
                       COALESCE(im.segment, '') AS segment
                FROM candle_data cd
                LEFT JOIN instrument_master im ON cd.instrument_key = im.instrument_key
                ORDER BY symbol
            """).fetchall()
            return [
                {
                    "instrument_key": r[0],
                    "symbol": r[1],
                    "name": r[2],
                    "segment": r[3],
                }
                for r in rows
            ]

    def get_contracts_for_chart(
        self, instrument_key: str, expiry_date: str | None = None
    ) -> list[dict]:
        """Return contracts available for charting (from historical_data).

        Allows the user to chart individual FUT/CE/PE contracts that have
        historical candle data collected via the collect workflow.
        """
        with self.db_manager.get_read_connection() as conn:
            conditions = ["c.instrument_key = ?", "c.data_fetched = TRUE"]
            params: list = [instrument_key]
            if expiry_date:
                conditions.append("c.expiry_date = ?")
                params.append(expiry_date)

            where = " AND ".join(conditions)
            rows = conn.execute(
                f"""
                SELECT c.expired_instrument_key,
                       c.trading_symbol,
                       c.contract_type,
                       c.strike_price,
                       c.expiry_date,
                       (SELECT COUNT(*) FROM historical_data h
                        WHERE h.expired_instrument_key = c.expired_instrument_key) AS candle_count
                FROM contracts c
                WHERE {where}
                ORDER BY c.expiry_date, c.contract_type, c.strike_price
            """,
                params,
            ).fetchall()

            return [
                {
                    "expired_instrument_key": r[0],
                    "trading_symbol": r[1],
                    "contract_type": r[2],
                    "strike_price": float(r[3]) if r[3] is not None else None,
                    "expiry_date": str(r[4]),
                    "candle_count": r[5] or 0,
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

    # ------------------------------------------------------------------
    # OI Analysis & Volume Profile (D6)
    # ------------------------------------------------------------------

    @cached_query
    def get_oi_by_strike(self, instrument_key: str, expiry_date: str) -> dict:
        """OI data grouped by strike price, split by CE/PE.

        For each strike, returns the latest OI reading for both CE and PE
        contracts from the historical_data table.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    c.strike_price,
                    c.contract_type,
                    h.oi
                FROM contracts c
                INNER JOIN (
                    SELECT expired_instrument_key, oi
                    FROM historical_data h1
                    WHERE (expired_instrument_key, timestamp) IN (
                        SELECT expired_instrument_key, MAX(timestamp)
                        FROM historical_data
                        GROUP BY expired_instrument_key
                    )
                ) h ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                  AND c.strike_price IS NOT NULL
                ORDER BY c.strike_price
            """, [instrument_key, expiry_date]).fetchall()

            # Group by strike
            strikes: dict[float, dict[str, int]] = {}
            for r in rows:
                sp = float(r[0])
                ct = r[1]
                oi_val = int(r[2]) if r[2] else 0
                if sp not in strikes:
                    strikes[sp] = {"CE": 0, "PE": 0}
                strikes[sp][ct] = oi_val

            sorted_strikes = sorted(strikes.keys())
            return {
                "strikes": [str(s) for s in sorted_strikes],
                "ce_oi": [strikes[s]["CE"] for s in sorted_strikes],
                "pe_oi": [strikes[s]["PE"] for s in sorted_strikes],
            }

    @cached_query
    def get_pcr_trend(self, instrument_key: str, expiry_date: str) -> dict:
        """Put-Call Ratio over time (daily).

        PCR = total put OI / total call OI per trading day.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    CAST(h.timestamp AS DATE) AS trading_day,
                    SUM(CASE WHEN c.contract_type = 'CE' THEN h.oi ELSE 0 END) AS total_ce_oi,
                    SUM(CASE WHEN c.contract_type = 'PE' THEN h.oi ELSE 0 END) AS total_pe_oi
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                GROUP BY trading_day
                ORDER BY trading_day
            """, [instrument_key, expiry_date]).fetchall()

            dates = []
            pcr_values = []
            ce_oi_values = []
            pe_oi_values = []
            for r in rows:
                ce_oi = int(r[1]) if r[1] else 0
                pe_oi = int(r[2]) if r[2] else 0
                pcr = round(pe_oi / ce_oi, 4) if ce_oi > 0 else 0
                dates.append(str(r[0]))
                pcr_values.append(pcr)
                ce_oi_values.append(ce_oi)
                pe_oi_values.append(pe_oi)

            return {
                "dates": dates,
                "pcr": pcr_values,
                "ce_oi": ce_oi_values,
                "pe_oi": pe_oi_values,
            }

    @cached_query
    def calculate_max_pain(self, instrument_key: str, expiry_date: str) -> dict:
        """Find the strike where total loss for option writers is minimum.

        For each candidate strike S, total pain =
          sum over all CE strikes K where K < S: (S - K) * CE_OI_at_K
          + sum over all PE strikes K where K > S: (K - S) * PE_OI_at_K

        The strike S with minimum total pain is the max pain point.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    c.strike_price,
                    c.contract_type,
                    h.oi
                FROM contracts c
                INNER JOIN (
                    SELECT expired_instrument_key, oi
                    FROM historical_data h1
                    WHERE (expired_instrument_key, timestamp) IN (
                        SELECT expired_instrument_key, MAX(timestamp)
                        FROM historical_data
                        GROUP BY expired_instrument_key
                    )
                ) h ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                  AND c.strike_price IS NOT NULL
            """, [instrument_key, expiry_date]).fetchall()

            # Build OI maps
            ce_oi: dict[float, int] = {}
            pe_oi: dict[float, int] = {}
            for r in rows:
                sp = float(r[0])
                ct = r[1]
                oi_val = int(r[2]) if r[2] else 0
                if ct == "CE":
                    ce_oi[sp] = ce_oi.get(sp, 0) + oi_val
                else:
                    pe_oi[sp] = pe_oi.get(sp, 0) + oi_val

            all_strikes = sorted(set(list(ce_oi.keys()) + list(pe_oi.keys())))
            if not all_strikes:
                return {"strikes": [], "pain": [], "max_pain_strike": None, "max_pain_value": None}

            # Calculate pain at each strike
            pain_at_strike: list[float] = []
            for s in all_strikes:
                total_pain = 0.0
                # CE ITM: strikes below s — call buyers profit, writers lose
                for k, oi in ce_oi.items():
                    if k < s:
                        total_pain += (s - k) * oi
                # PE ITM: strikes above s — put buyers profit, writers lose
                for k, oi in pe_oi.items():
                    if k > s:
                        total_pain += (k - s) * oi
                pain_at_strike.append(total_pain)

            min_pain_idx = pain_at_strike.index(min(pain_at_strike))
            max_pain_strike = all_strikes[min_pain_idx]

            return {
                "strikes": [str(s) for s in all_strikes],
                "pain": [round(p, 2) for p in pain_at_strike],
                "max_pain_strike": str(max_pain_strike),
                "max_pain_value": round(pain_at_strike[min_pain_idx], 2),
            }

    @cached_query
    def get_oi_heatmap(self, instrument_key: str, expiry_date: str) -> dict:
        """OI change data as a grid: strikes (rows) x dates (columns).

        For each strike/date combination, calculates the change in OI
        from the previous trading day.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    c.strike_price,
                    c.contract_type,
                    CAST(h.timestamp AS DATE) AS trading_day,
                    SUM(h.oi) AS total_oi
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                  AND c.strike_price IS NOT NULL
                GROUP BY c.strike_price, c.contract_type, trading_day
                ORDER BY c.strike_price, trading_day
            """, [instrument_key, expiry_date]).fetchall()

            # Build nested dict: (strike, type) -> {date: oi}
            data: dict[str, dict[str, dict[str, int]]] = {}
            all_dates: set[str] = set()
            all_strikes: set[float] = set()
            for r in rows:
                sp = float(r[0])
                ct = r[1]
                day = str(r[2])
                oi_val = int(r[3]) if r[3] else 0
                key = f"{sp}_{ct}"
                if key not in data:
                    data[key] = {}
                data[key][day] = oi_val
                all_dates.add(day)
                all_strikes.add(sp)

            sorted_dates = sorted(all_dates)
            sorted_strikes = sorted(all_strikes)

            # Calculate OI changes between consecutive dates
            heatmap_rows: list[dict] = []
            for sp in sorted_strikes:
                for ct in ["CE", "PE"]:
                    key = f"{sp}_{ct}"
                    oi_series = data.get(key, {})
                    changes: list[int | None] = []
                    for i, d in enumerate(sorted_dates):
                        current = oi_series.get(d, 0)
                        if i == 0:
                            changes.append(current)
                        else:
                            prev = oi_series.get(sorted_dates[i - 1], 0)
                            changes.append(current - prev)
                    heatmap_rows.append({
                        "strike": str(sp),
                        "type": ct,
                        "changes": changes,
                    })

            return {
                "dates": sorted_dates,
                "rows": heatmap_rows,
            }

    @cached_query
    def get_volume_profile(self, instrument_key: str, expiry_date: str) -> dict:
        """Volume-at-Price: aggregate volume across all price levels.

        Groups volume into price buckets based on the close price of each
        candle, returning data suitable for a horizontal bar chart.
        """
        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute("""
                SELECT
                    c.strike_price,
                    c.contract_type,
                    SUM(h.volume) AS total_volume
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                  AND c.contract_type IN ('CE', 'PE')
                  AND c.strike_price IS NOT NULL
                GROUP BY c.strike_price, c.contract_type
                ORDER BY c.strike_price
            """, [instrument_key, expiry_date]).fetchall()

            strikes: dict[float, dict[str, int]] = {}
            for r in rows:
                sp = float(r[0])
                ct = r[1]
                vol = int(r[2]) if r[2] else 0
                if sp not in strikes:
                    strikes[sp] = {"CE": 0, "PE": 0}
                strikes[sp][ct] = vol

            sorted_strikes = sorted(strikes.keys())
            return {
                "strikes": [str(s) for s in sorted_strikes],
                "ce_volume": [strikes[s]["CE"] for s in sorted_strikes],
                "pe_volume": [strikes[s]["PE"] for s in sorted_strikes],
            }

    # ------------------------------------------------------------------
    # Coverage Calendar (D14)
    # ------------------------------------------------------------------

    @cached_query
    def get_coverage_calendar(self, instrument_key: str, year: int) -> dict[str, float]:
        """Return a date->coverage% mapping for a GitHub-style heatmap.

        For each day in the given year that has any historical data for the
        instrument, we calculate coverage as:
            min(candle_count / CANDLES_PER_TRADING_DAY, 1.0) * 100

        CANDLES_PER_TRADING_DAY represents a full trading day of 1-minute data (09:15–15:30).
        Days with no data at all are omitted from the result (the frontend
        treats missing dates as non-trading / no-data days).

        Parameters
        ----------
        instrument_key:
            The instrument_key to check coverage for.
        year:
            Calendar year (e.g. 2026).

        Returns
        -------
        dict mapping ISO date strings to coverage percentages (0-100).
        """
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute(
                """
                SELECT trading_day, SUM(candle_count) AS candle_count
                FROM (
                    SELECT
                        CAST(h.timestamp AS DATE) AS trading_day,
                        COUNT(*) AS candle_count
                    FROM historical_data h
                    JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                    WHERE c.instrument_key = ?
                      AND CAST(h.timestamp AS DATE) >= CAST(? AS DATE)
                      AND CAST(h.timestamp AS DATE) <= CAST(? AS DATE)
                    GROUP BY trading_day
                    UNION ALL
                    SELECT
                        CAST(cd.timestamp AS DATE) AS trading_day,
                        COUNT(*) AS candle_count
                    FROM candle_data cd
                    WHERE cd.instrument_key = ?
                      AND CAST(cd.timestamp AS DATE) >= CAST(? AS DATE)
                      AND CAST(cd.timestamp AS DATE) <= CAST(? AS DATE)
                    GROUP BY trading_day
                ) combined
                GROUP BY trading_day
                ORDER BY trading_day
                """,
                [instrument_key, start_date, end_date,
                 instrument_key, start_date, end_date],
            ).fetchall()

        calendar: dict[str, float] = {}
        for r in rows:
            day_str = str(r[0])
            count = int(r[1])
            coverage = min(count / CANDLES_PER_TRADING_DAY, 1.0) * 100
            calendar[day_str] = round(coverage, 1)

        return calendar

    # ------------------------------------------------------------------
    # Comparative Expiry Analysis (D11)
    # ------------------------------------------------------------------

    @cached_query
    def get_expiry_comparison(self, instrument_key: str, expiry_dates: list[str]) -> dict:
        """Compare key metrics across multiple expiry dates for one instrument.

        For each expiry date, calculates:
        - total_volume: sum of all contract volumes
        - avg_spread: average high-low spread across all candles
        - active_strikes: count of distinct strike prices with trades
        - oi_concentration: top 5 strikes' OI as % of total OI
        - trading_days: count of distinct trading days with data
        - candle_count: total number of candle records

        Parameters
        ----------
        instrument_key:
            The instrument to analyse.
        expiry_dates:
            List of expiry date strings (ISO format) to compare.

        Returns
        -------
        dict keyed by expiry date string, each value is a metrics dict.
        """
        if not expiry_dates:
            return {}

        result: dict[str, dict] = {}

        with self.db_manager.get_read_connection() as conn:
            for expiry in expiry_dates:
                # Core metrics: volume, spread, candle count, trading days
                row = conn.execute("""
                    SELECT
                        COALESCE(SUM(h.volume), 0) AS total_volume,
                        COALESCE(AVG(h.high - h.low), 0) AS avg_spread,
                        COUNT(*) AS candle_count,
                        COUNT(DISTINCT CAST(h.timestamp AS DATE)) AS trading_days
                    FROM historical_data h
                    JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                    WHERE c.instrument_key = ?
                      AND c.expiry_date = ?
                """, [instrument_key, expiry]).fetchone()

                total_volume = int(row[0]) if row[0] else 0
                avg_spread = round(float(row[1]), 2) if row[1] else 0.0
                candle_count = int(row[2]) if row[2] else 0
                trading_days = int(row[3]) if row[3] else 0

                # Active strikes: distinct strike prices that have volume > 0
                active_row = conn.execute("""
                    SELECT COUNT(DISTINCT c.strike_price)
                    FROM historical_data h
                    JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                    WHERE c.instrument_key = ?
                      AND c.expiry_date = ?
                      AND c.strike_price IS NOT NULL
                      AND h.volume > 0
                """, [instrument_key, expiry]).fetchone()

                active_strikes = int(active_row[0]) if active_row and active_row[0] else 0

                # OI concentration: top 5 strikes' OI as % of total OI
                # Use latest OI reading per contract
                oi_rows = conn.execute("""
                    SELECT c.strike_price, h.oi
                    FROM contracts c
                    INNER JOIN (
                        SELECT expired_instrument_key, oi
                        FROM historical_data h1
                        WHERE (expired_instrument_key, timestamp) IN (
                            SELECT expired_instrument_key, MAX(timestamp)
                            FROM historical_data
                            GROUP BY expired_instrument_key
                        )
                    ) h ON h.expired_instrument_key = c.expired_instrument_key
                    WHERE c.instrument_key = ?
                      AND c.expiry_date = ?
                      AND c.strike_price IS NOT NULL
                """, [instrument_key, expiry]).fetchall()

                total_oi = 0
                strike_oi: dict[float, int] = {}
                for oi_r in oi_rows:
                    sp = float(oi_r[0])
                    oi_val = int(oi_r[1]) if oi_r[1] else 0
                    strike_oi[sp] = strike_oi.get(sp, 0) + oi_val
                    total_oi += oi_val

                # Sum of top 5 strike OIs
                top5_oi = sum(sorted(strike_oi.values(), reverse=True)[:5])
                oi_concentration = round(top5_oi / total_oi * 100, 1) if total_oi > 0 else 0.0

                result[expiry] = {
                    "total_volume": total_volume,
                    "avg_spread": avg_spread,
                    "active_strikes": active_strikes,
                    "oi_concentration": oi_concentration,
                    "trading_days": trading_days,
                    "candle_count": candle_count,
                }

        return result

    # ------------------------------------------------------------------
    # External Data Comparison (D15)
    # ------------------------------------------------------------------

    def compare_datasets(
        self,
        uploaded_df: pd.DataFrame,
        instrument_key: str,
        expiry_date: str,
    ) -> dict:
        """Compare an uploaded DataFrame against stored historical data.

        JOINs uploaded data with stored data on timestamp and reports
        mismatches for OHLCV fields.

        Parameters
        ----------
        uploaded_df:
            DataFrame with columns: timestamp, open, high, low, close, volume,
            and optionally oi. Timestamps should be parse-able by pandas.
        instrument_key:
            The instrument_key to filter contracts.
        expiry_date:
            The expiry_date to filter contracts.

        Returns
        -------
        dict with keys:
            - summary: uploaded_rows, stored_rows, matched_rows,
              identical_rows, mismatch_count, only_in_uploaded, only_in_stored
            - mismatches: list of dicts with timestamp, field,
              uploaded_value, stored_value, difference
        """
        import pandas as pd

        # Normalize the uploaded DataFrame
        uploaded_df = uploaded_df.copy()
        uploaded_df["timestamp"] = pd.to_datetime(uploaded_df["timestamp"])

        for col in ["open", "high", "low", "close"]:
            if col in uploaded_df.columns:
                uploaded_df[col] = pd.to_numeric(uploaded_df[col], errors="coerce")
        if "volume" in uploaded_df.columns:
            uploaded_df["volume"] = (
                pd.to_numeric(uploaded_df["volume"], errors="coerce").fillna(0).astype(int)
            )
        if "oi" in uploaded_df.columns:
            uploaded_df["oi"] = (
                pd.to_numeric(uploaded_df["oi"], errors="coerce").fillna(0).astype(int)
            )

        uploaded_rows = len(uploaded_df)

        # Fetch stored data for the instrument + expiry
        with self.db_manager.get_read_connection() as conn:
            stored_df = conn.execute(
                """
                SELECT h.timestamp, h.open, h.high, h.low, h.close, h.volume, h.oi
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                WHERE c.instrument_key = ?
                  AND c.expiry_date = ?
                ORDER BY h.timestamp
            """,
                [instrument_key, expiry_date],
            ).fetchdf()

        stored_rows = len(stored_df)

        if stored_df.empty or uploaded_df.empty:
            return {
                "summary": {
                    "uploaded_rows": uploaded_rows,
                    "stored_rows": stored_rows,
                    "matched_rows": 0,
                    "identical_rows": 0,
                    "mismatch_count": 0,
                    "only_in_uploaded": uploaded_rows,
                    "only_in_stored": stored_rows,
                },
                "mismatches": [],
            }

        stored_df["timestamp"] = pd.to_datetime(stored_df["timestamp"])

        # Merge on timestamp
        merged = pd.merge(
            uploaded_df,
            stored_df,
            on="timestamp",
            how="outer",
            suffixes=("_uploaded", "_stored"),
            indicator=True,
        )

        matched = merged[merged["_merge"] == "both"]
        only_uploaded = int((merged["_merge"] == "left_only").sum())
        only_stored = int((merged["_merge"] == "right_only").sum())
        matched_count = len(matched)

        # Compare fields
        compare_fields = ["open", "high", "low", "close", "volume"]
        if "oi" in uploaded_df.columns:
            compare_fields.append("oi")

        mismatches: list[dict] = []

        # Vectorised comparison — avoid iterrows() on potentially large DataFrames
        for field in compare_fields:
            up_col = f"{field}_uploaded"
            st_col = f"{field}_stored"
            if up_col not in matched.columns or st_col not in matched.columns:
                continue

            up_s = matched[up_col]
            st_s = matched[st_col]
            up_nan = up_s.isna()
            st_nan = st_s.isna()

            # One-sided NaN → mismatch
            one_nan_mask = (up_nan | st_nan) & ~(up_nan & st_nan)
            for rec in matched.loc[one_nan_mask, ["timestamp", up_col, st_col]].itertuples(index=False):
                uv = getattr(rec, up_col)
                sv = getattr(rec, st_col)
                mismatches.append({
                    "timestamp": str(rec.timestamp),
                    "field": field,
                    "uploaded_value": None if pd.isna(uv) else float(uv),
                    "stored_value": None if pd.isna(sv) else float(sv),
                    "difference": None,
                })

            # Numeric comparison with 0.001 tolerance
            numeric_mask = ~up_nan & ~st_nan
            if numeric_mask.any():
                up_f = up_s[numeric_mask].astype(float)
                st_f = st_s[numeric_mask].astype(float)
                diff_s = up_f - st_f
                mismatch_idx = diff_s[diff_s.abs() > 0.001].index
                for idx in mismatch_idx:
                    row = matched.loc[idx]
                    mismatches.append({
                        "timestamp": str(row["timestamp"]),
                        "field": field,
                        "uploaded_value": float(row[up_col]),
                        "stored_value": float(row[st_col]),
                        "difference": round(float(diff_s[idx]), 4),
                    })

        mismatch_timestamps = set(m["timestamp"] for m in mismatches)
        identical_count = matched_count - len(mismatch_timestamps)

        return {
            "summary": {
                "uploaded_rows": uploaded_rows,
                "stored_rows": stored_rows,
                "matched_rows": matched_count,
                "identical_rows": identical_count,
                "mismatch_count": len(mismatch_timestamps),
                "only_in_uploaded": only_uploaded,
                "only_in_stored": only_stored,
            },
            "mismatches": mismatches,
        }
