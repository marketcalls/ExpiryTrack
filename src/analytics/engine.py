"""
Analytics Engine for ExpiryTrack
Provides pre-built analytical queries for the dashboard.
"""
import hashlib
import logging
import threading
import time
from datetime import datetime
from functools import wraps
from typing import Dict, List, Optional

import duckdb

from ..config import config

logger = logging.getLogger(__name__)


# ── Analytics Cache (#14) ─────────────────────────────────
class AnalyticsCache:
    """Thread-safe in-memory cache with per-method TTLs."""

    _cache: Dict = {}
    _lock = threading.Lock()

    # TTLs in seconds per method
    TTLS = {
        'get_dashboard_summary': 300,       # 5 min
        'get_candles_per_day': 3600,        # 1 hr
        'get_contracts_by_type': 1800,      # 30 min
        'get_contracts_by_instrument': 1800,# 30 min
        'get_data_coverage_by_expiry': 1800,# 30 min
        'get_volume_by_expiry': 1800,       # 30 min
        'get_storage_breakdown': 600,       # 10 min
        'get_download_status': 120,         # 2 min
        'get_missing_contracts': 60,        # 1 min
    }

    @classmethod
    def get(cls, key: str):
        with cls._lock:
            entry = cls._cache.get(key)
            if entry and time.time() < entry['expires']:
                return entry['value']
            return None

    @classmethod
    def set(cls, key: str, value, ttl: int):
        with cls._lock:
            cls._cache[key] = {'value': value, 'expires': time.time() + ttl}

    @classmethod
    def invalidate_all(cls):
        with cls._lock:
            cls._cache.clear()
        logger.debug("Analytics cache invalidated")


def cached_query(func):
    """Decorator that caches AnalyticsEngine query results."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        method = func.__name__
        ttl = AnalyticsCache.TTLS.get(method, 300)
        # Build cache key from method + args
        key_parts = [method] + [str(a) for a in args] + [f"{k}={v}" for k, v in sorted(kwargs.items())]
        cache_key = hashlib.md5(':'.join(key_parts).encode()).hexdigest()

        cached = AnalyticsCache.get(cache_key)
        if cached is not None:
            return cached

        result = func(self, *args, **kwargs)
        AnalyticsCache.set(cache_key, result, ttl)
        return result
    return wrapper


class AnalyticsEngine:
    """Runs analytical queries against the DuckDB database."""

    def __init__(self, db_path=None):
        self.db_path = str(db_path or config.DB_PATH)
        self.timeout = config.ANALYTICS_QUERY_TIMEOUT
        self.max_points = config.ANALYTICS_MAX_CHART_POINTS

    def _connect(self):
        conn = duckdb.connect(self.db_path)
        return conn

    # ------------------------------------------------------------------
    # Dashboard summary
    # ------------------------------------------------------------------

    @cached_query
    def get_dashboard_summary(self) -> Dict:
        """Return top-level stats for the analytics dashboard."""
        conn = self._connect()
        try:
            row = conn.execute("""
                SELECT
                    (SELECT COUNT(DISTINCT instrument_key) FROM instruments) AS instruments,
                    (SELECT COUNT(*) FROM contracts) AS contracts,
                    (SELECT COUNT(*) FROM contracts WHERE data_fetched = TRUE) AS fetched_contracts,
                    (SELECT COUNT(*) FROM historical_data) AS total_candles,
                    (SELECT COUNT(DISTINCT expired_instrument_key) FROM historical_data) AS contracts_with_data,
                    (SELECT COUNT(DISTINCT CAST(timestamp AS DATE)) FROM historical_data) AS trading_days,
                    (SELECT COUNT(*) FROM contracts WHERE no_data = TRUE) AS no_data_contracts,
                    (SELECT COUNT(*) FROM contracts WHERE data_fetched = FALSE) AS pending_contracts
            """).fetchone()

            return {
                'instruments': row[0],
                'contracts': row[1],
                'fetched_contracts': row[2],
                'total_candles': row[3],
                'contracts_with_data': row[4],
                'trading_days': row[5],
                'coverage_pct': round(row[2] / row[1] * 100, 1) if row[1] > 0 else 0,
                'no_data_contracts': row[6],
                'pending_contracts': row[7],
            }
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Charts data
    # ------------------------------------------------------------------

    @cached_query
    def get_candles_per_day(self, instrument_key: Optional[str] = None, limit: int = 60) -> Dict:
        """Candles collected per trading day — bar chart data."""
        conn = self._connect()
        try:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE h.expired_instrument_key LIKE ? || '%'"
                params = [instrument_key]

            rows = conn.execute(f"""
                SELECT CAST(h.timestamp AS DATE) AS day, COUNT(*) AS cnt
                FROM historical_data h
                {where}
                GROUP BY day
                ORDER BY day DESC
                LIMIT ?
            """, params + [limit]).fetchall()

            rows.reverse()
            return {
                'labels': [str(r[0]) for r in rows],
                'data': [r[1] for r in rows],
            }
        finally:
            conn.close()

    @cached_query
    def get_contracts_by_type(self) -> Dict:
        """Contract count by type (CE / PE / FUT) — pie chart data."""
        conn = self._connect()
        try:
            rows = conn.execute("""
                SELECT contract_type, COUNT(*) AS cnt
                FROM contracts
                GROUP BY contract_type
                ORDER BY cnt DESC
            """).fetchall()

            return {
                'labels': [r[0] for r in rows],
                'data': [r[1] for r in rows],
            }
        finally:
            conn.close()

    @cached_query
    def get_contracts_by_instrument(self) -> Dict:
        """Contract count by underlying instrument — bar chart data."""
        conn = self._connect()
        try:
            rows = conn.execute("""
                SELECT i.symbol, COUNT(*) AS cnt
                FROM contracts c
                JOIN instruments i ON c.instrument_key = i.instrument_key
                GROUP BY i.symbol
                ORDER BY cnt DESC
            """).fetchall()

            return {
                'labels': [r[0] for r in rows],
                'data': [r[1] for r in rows],
            }
        finally:
            conn.close()

    @cached_query
    def get_data_coverage_by_expiry(self, instrument_key: Optional[str] = None) -> Dict:
        """For each expiry: total contracts vs contracts with data — grouped bar chart."""
        conn = self._connect()
        try:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(f"""
                SELECT
                    c.expiry_date,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE c.data_fetched = TRUE) AS fetched
                FROM contracts c
                {where}
                GROUP BY c.expiry_date
                ORDER BY c.expiry_date DESC
                LIMIT ?
            """, params + [self.max_points]).fetchall()

            rows.reverse()
            return {
                'labels': [str(r[0]) for r in rows],
                'total': [r[1] for r in rows],
                'fetched': [r[2] for r in rows],
            }
        finally:
            conn.close()

    @cached_query
    def get_volume_by_expiry(self, instrument_key: Optional[str] = None, limit: int = 20) -> Dict:
        """Total volume per expiry — bar chart."""
        conn = self._connect()
        try:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(f"""
                SELECT c.expiry_date, SUM(h.volume) AS total_vol
                FROM historical_data h
                JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
                {where}
                GROUP BY c.expiry_date
                ORDER BY c.expiry_date DESC
                LIMIT ?
            """, params + [limit]).fetchall()

            rows.reverse()
            return {
                'labels': [str(r[0]) for r in rows],
                'data': [int(r[1]) for r in rows],
            }
        finally:
            conn.close()

    @cached_query
    def get_storage_breakdown(self) -> Dict:
        """Approximate row counts for each table — for storage overview."""
        conn = self._connect()
        try:
            tables = ['instruments', 'expiries', 'contracts', 'historical_data', 'job_status']
            result = {}
            for t in tables:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    result[t] = count
                except Exception:
                    result[t] = 0
            return result
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Download status
    # ------------------------------------------------------------------

    @cached_query
    def get_download_status(self, instrument_key: Optional[str] = None) -> List[Dict]:
        """Per-expiry download status: total vs fetched contracts."""
        conn = self._connect()
        try:
            where = ""
            params = []
            if instrument_key:
                where = "WHERE c.instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(f"""
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
            """, params).fetchall()

            return [{
                'instrument_key': r[0],
                'instrument_name': r[1],
                'expiry_date': str(r[2]),
                'total_contracts': r[3],
                'fetched_contracts': r[4],
                'missing_contracts': r[3] - r[4],
                'pct': round(float(r[5]), 1),
                'status': r[6],
                'no_data_contracts': r[7],
            } for r in rows]
        finally:
            conn.close()

    @cached_query
    def get_missing_contracts(self, instrument_key: str, expiry_date: str) -> List[Dict]:
        """Get unfetched contracts for a specific instrument+expiry."""
        conn = self._connect()
        try:
            rows = conn.execute("""
                SELECT c.expired_instrument_key, c.trading_symbol, c.contract_type, c.strike_price
                FROM contracts c
                WHERE c.instrument_key = ? AND c.expiry_date = ? AND c.data_fetched = FALSE
                ORDER BY c.strike_price, c.contract_type
            """, [instrument_key, expiry_date]).fetchall()

            return [{
                'expired_instrument_key': r[0],
                'trading_symbol': r[1],
                'contract_type': r[2],
                'strike_price': float(r[3]) if r[3] is not None else None,
            } for r in rows]
        finally:
            conn.close()

    def get_recent_collections(self, limit: int = 10) -> List[Dict]:
        """Recent job_status entries for display."""
        conn = self._connect()
        try:
            rows = conn.execute("""
                SELECT id, job_type, instrument_key, status, started_at, completed_at, error_message
                FROM job_status
                ORDER BY id DESC
                LIMIT ?
            """, [limit]).fetchall()

            return [{
                'id': r[0],
                'job_type': r[1],
                'instrument_key': r[2],
                'status': r[3],
                'started_at': str(r[4]) if r[4] else None,
                'completed_at': str(r[5]) if r[5] else None,
                'error_message': r[6],
            } for r in rows]
        finally:
            conn.close()
