"""
Data Quality Checker for ExpiryTrack
Validates OHLCV data integrity after collection.
"""

import logging
from datetime import datetime
from datetime import time as dt_time

from ..config import config

logger = logging.getLogger(__name__)

# Market hours: 09:15 to 15:30 IST
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)

# Expected 1-minute candles per full trading day (375 minutes)
EXPECTED_CANDLES_PER_DAY = 375


class QualityViolation:
    """Single quality violation."""

    def __init__(self, check: str, severity: str, message: str, instrument_key: str = "", details: dict | None = None):
        self.check = check
        self.severity = severity  # 'error', 'warning', 'info'
        self.message = message
        self.instrument_key = instrument_key
        self.details = details or {}
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            "check": self.check,
            "severity": self.severity,
            "message": self.message,
            "instrument_key": self.instrument_key,
            "details": self.details,
            "timestamp": self.timestamp,
        }


class QualityReport:
    """Aggregated report from quality checks."""

    def __init__(self):
        self.violations: list[QualityViolation] = []
        self.checks_run: int = 0
        self.checks_passed: int = 0
        self.started_at = datetime.now()
        self.completed_at: datetime | None = None

    def add(self, violation: QualityViolation):
        self.violations.append(violation)

    @property
    def error_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "warning")

    @property
    def passed(self) -> bool:
        threshold = config.QUALITY_VIOLATION_THRESHOLD
        if self.checks_run == 0:
            return True
        error_rate = self.error_count / self.checks_run
        return error_rate <= threshold

    def to_dict(self) -> dict:
        return {
            "checks_run": self.checks_run,
            "checks_passed": self.checks_passed,
            "errors": self.error_count,
            "warnings": self.warning_count,
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class DataQualityChecker:
    """Runs quality checks against collected OHLCV data."""

    def __init__(self, db_manager=None):
        if db_manager is None:
            from ..database.manager import DatabaseManager

            db_manager = DatabaseManager()
        self.db_manager = db_manager

    def run_all_checks(self, instrument_key: str | None = None) -> QualityReport:
        """Run all quality checks. Optionally filter to a single instrument."""
        report = QualityReport()

        with self.db_manager.get_read_connection() as conn:
            self._check_ohlc_integrity(conn, report, instrument_key)
            self._check_negative_values(conn, report, instrument_key)
            self._check_zero_volume(conn, report, instrument_key)
            self._check_price_spikes(conn, report, instrument_key)
            self._check_gap_days(conn, report, instrument_key)
            self._check_duplicate_timestamps(conn, report, instrument_key)
            self._check_orphan_contracts(conn, report, instrument_key)

        report.completed_at = datetime.now()
        logger.info(
            f"Quality check complete: {report.checks_run} checks, "
            f"{report.error_count} errors, {report.warning_count} warnings"
        )
        return report

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_ohlc_integrity(self, conn, report: QualityReport, instrument_key: str | None):
        """Check H >= max(O, C) and L <= min(O, C)."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        rows = conn.execute(
            f"""
            SELECT expired_instrument_key, timestamp, open, high, low, close
            FROM historical_data
            WHERE (high < open OR high < close OR low > open OR low > close)
            {where}
            LIMIT 100
        """,
            params,
        ).fetchall()

        if not rows:
            report.checks_passed += 1
        else:
            for row in rows:
                report.add(
                    QualityViolation(
                        check="ohlc_integrity",
                        severity="error",
                        message=f"OHLC integrity violation: O={row[2]} H={row[3]} L={row[4]} C={row[5]}",
                        instrument_key=row[0],
                        details={"timestamp": str(row[1])},
                    )
                )

    def _check_negative_values(self, conn, report: QualityReport, instrument_key: str | None):
        """Check for negative prices or volume."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM historical_data
            WHERE (open < 0 OR high < 0 OR low < 0 OR close < 0 OR volume < 0)
            {where}
        """,
            params,
        ).fetchone()[0]

        if count == 0:
            report.checks_passed += 1
        else:
            report.add(
                QualityViolation(
                    check="negative_values",
                    severity="error",
                    message=f"{count:,} rows have negative price or volume",
                    instrument_key=instrument_key or "",
                )
            )

    def _check_zero_volume(self, conn, report: QualityReport, instrument_key: str | None):
        """Warn about excessive zero-volume candles (>20% of data)."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        result = conn.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE volume = 0) AS zero_vol,
                COUNT(*) AS total
            FROM historical_data
            WHERE 1=1 {where}
        """,
            params,
        ).fetchone()

        zero_vol, total = result[0], result[1]
        if total == 0:
            report.checks_passed += 1
            return

        ratio = zero_vol / total
        if ratio > 0.20:
            report.add(
                QualityViolation(
                    check="zero_volume",
                    severity="warning",
                    message=f"{ratio:.1%} of candles have zero volume ({zero_vol:,}/{total:,})",
                    instrument_key=instrument_key or "",
                )
            )
        else:
            report.checks_passed += 1

    def _check_price_spikes(self, conn, report: QualityReport, instrument_key: str | None):
        """Detect candles where close differs from previous close by >50%."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        rows = conn.execute(
            f"""
            WITH lagged AS (
                SELECT
                    expired_instrument_key,
                    timestamp,
                    close,
                    LAG(close) OVER (PARTITION BY expired_instrument_key ORDER BY timestamp) AS prev_close
                FROM historical_data
                WHERE 1=1 {where}
            )
            SELECT expired_instrument_key, timestamp, close, prev_close
            FROM lagged
            WHERE prev_close > 0
              AND ABS(close - prev_close) / prev_close > 0.50
            LIMIT 50
        """,
            params,
        ).fetchall()

        if not rows:
            report.checks_passed += 1
        else:
            for row in rows:
                pct = abs(row[2] - row[3]) / row[3] * 100
                report.add(
                    QualityViolation(
                        check="price_spike",
                        severity="warning",
                        message=f"Price spike {pct:.0f}%: {row[3]} -> {row[2]}",
                        instrument_key=row[0],
                        details={"timestamp": str(row[1])},
                    )
                )

    def _check_gap_days(self, conn, report: QualityReport, instrument_key: str | None):
        """Detect contracts with fewer than expected trading days."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        rows = conn.execute(
            f"""
            SELECT
                h.expired_instrument_key,
                c.expiry_date,
                COUNT(DISTINCT CAST(h.timestamp AS DATE)) AS trading_days,
                COUNT(*) AS total_candles
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE 1=1 {where}
            GROUP BY h.expired_instrument_key, c.expiry_date
            HAVING trading_days < 5
        """,
            params,
        ).fetchall()

        if not rows:
            report.checks_passed += 1
        else:
            for row in rows:
                report.add(
                    QualityViolation(
                        check="gap_days",
                        severity="warning",
                        message=f"Only {row[2]} trading days ({row[3]:,} candles) for contract expiring {row[1]}",
                        instrument_key=row[0],
                    )
                )

    def _check_duplicate_timestamps(self, conn, report: QualityReport, instrument_key: str | None):
        """Detect duplicate timestamps within a contract."""
        report.checks_run += 1
        where, params = self._instrument_filter(instrument_key)

        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                SELECT expired_instrument_key, timestamp, COUNT(*) AS cnt
                FROM historical_data
                WHERE 1=1 {where}
                GROUP BY expired_instrument_key, timestamp
                HAVING cnt > 1
            )
        """,
            params,
        ).fetchone()[0]

        if count == 0:
            report.checks_passed += 1
        else:
            report.add(
                QualityViolation(
                    check="duplicate_timestamps",
                    severity="error",
                    message=f"{count:,} duplicate timestamp entries found",
                    instrument_key=instrument_key or "",
                )
            )

    def _check_orphan_contracts(self, conn, report: QualityReport, instrument_key: str | None):
        """Detect contracts marked as data_fetched but with no historical data."""
        report.checks_run += 1
        where = ""
        params = []
        if instrument_key:
            where = "AND c.instrument_key = ?"
            params = [instrument_key]

        count = conn.execute(
            f"""
            SELECT COUNT(*) FROM contracts c
            LEFT JOIN historical_data h ON c.expired_instrument_key = h.expired_instrument_key
            WHERE c.data_fetched = TRUE AND h.expired_instrument_key IS NULL
            {where}
        """,
            params,
        ).fetchone()[0]

        if count == 0:
            report.checks_passed += 1
        else:
            report.add(
                QualityViolation(
                    check="orphan_contracts",
                    severity="warning",
                    message=f"{count:,} contracts marked as fetched but have no historical data",
                    instrument_key=instrument_key or "",
                )
            )

    # ------------------------------------------------------------------
    # Auto-fix
    # ------------------------------------------------------------------

    def fix_violations(self, instrument_key: str | None = None) -> dict:
        """Auto-fix common quality violations. Returns counts of fixes applied."""
        fixes = {"orphans_reset": 0, "duplicates_removed": 0}

        with self.db_manager.get_connection() as conn:
            try:
                # Fix 1: Re-mark orphan contracts (marked fetched but no data)
                where = ""
                params: list = []
                if instrument_key:
                    where = "AND c.instrument_key = ?"
                    params = [instrument_key]

                orphan_count = conn.execute(
                    f"""
                    UPDATE contracts c
                    SET data_fetched = FALSE
                    WHERE c.data_fetched = TRUE
                      AND c.expired_instrument_key NOT IN (
                          SELECT DISTINCT expired_instrument_key FROM historical_data
                      )
                      AND c.expired_instrument_key NOT IN (
                          SELECT DISTINCT instrument_key FROM candle_data
                      )
                    {where}
                """,
                    params,
                ).fetchone()
                fixes["orphans_reset"] = orphan_count[0] if orphan_count else 0

                # Fix 2: Remove duplicate timestamps (keep latest inserted)
                dup_where = ""
                dup_params: list = []
                if instrument_key:
                    dup_where = "AND expired_instrument_key LIKE ? || '%'"
                    dup_params = [instrument_key]

                dup_result = conn.execute(
                    f"""
                    DELETE FROM historical_data
                    WHERE rowid NOT IN (
                        SELECT MAX(rowid)
                        FROM historical_data
                        WHERE 1=1 {dup_where}
                        GROUP BY expired_instrument_key, timestamp
                    )
                    AND 1=1 {dup_where}
                """,
                    dup_params * 2,
                ).fetchone()
                fixes["duplicates_removed"] = dup_result[0] if dup_result else 0

                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

        logger.info(f"Quality fix applied: {fixes}")
        return fixes

    # ------------------------------------------------------------------
    # Market hours check
    # ------------------------------------------------------------------

    def check_market_hours(self, instrument_key: str | None = None) -> dict:
        """Check for candles outside market hours and days with low candle counts."""
        results = {"outside_hours": 0, "low_candle_days": []}

        with self.db_manager.get_read_connection() as conn:
            where, params = self._instrument_filter(instrument_key)

            # Count candles outside 9:15-15:30 IST
            outside = conn.execute(
                f"""
                SELECT COUNT(*) FROM historical_data
                WHERE (EXTRACT(HOUR FROM timestamp) < 9
                    OR (EXTRACT(HOUR FROM timestamp) = 9 AND EXTRACT(MINUTE FROM timestamp) < 15)
                    OR EXTRACT(HOUR FROM timestamp) > 15
                    OR (EXTRACT(HOUR FROM timestamp) = 15 AND EXTRACT(MINUTE FROM timestamp) > 30))
                {where}
            """,
                params,
            ).fetchone()
            results["outside_hours"] = outside[0] if outside else 0

            # Days with fewer than expected candles (< 50% of 375)
            low_days = conn.execute(
                f"""
                SELECT
                    expired_instrument_key,
                    CAST(timestamp AS DATE) AS trading_date,
                    COUNT(*) AS candle_count
                FROM historical_data
                WHERE 1=1 {where}
                GROUP BY expired_instrument_key, CAST(timestamp AS DATE)
                HAVING candle_count < {EXPECTED_CANDLES_PER_DAY // 2}
                ORDER BY candle_count ASC
                LIMIT 50
            """,
                params,
            ).fetchall()

            results["low_candle_days"] = [
                {
                    "instrument": row[0],
                    "date": str(row[1]),
                    "candle_count": row[2],
                    "expected": EXPECTED_CANDLES_PER_DAY,
                }
                for row in low_days
            ]

        return results

    # ------------------------------------------------------------------
    # Completeness scoring
    # ------------------------------------------------------------------

    def get_completeness_score(self, instrument_key: str, expiry_date: str) -> dict:
        """Calculate completeness score for a specific instrument+expiry."""
        with self.db_manager.get_read_connection() as conn:
            # Get contracts for this expiry
            contracts = conn.execute(
                """
                SELECT expired_instrument_key, data_fetched, no_data
                FROM contracts
                WHERE instrument_key = ? AND expiry_date = ?
            """,
                [instrument_key, expiry_date],
            ).fetchall()

            if not contracts:
                return {"total_contracts": 0, "completeness_pct": 0, "details": {}}

            total = len(contracts)
            fetched = sum(1 for c in contracts if c[1])
            no_data = sum(1 for c in contracts if c[2])
            pending = total - fetched - no_data

            # For fetched contracts, check actual trading days
            fetched_keys = [c[0] for c in contracts if c[1]]
            trading_day_scores = []

            if fetched_keys:
                placeholders = ",".join(["?"] * len(fetched_keys))
                days_data = conn.execute(
                    f"""
                    SELECT
                        expired_instrument_key,
                        COUNT(DISTINCT CAST(timestamp AS DATE)) AS actual_days
                    FROM historical_data
                    WHERE expired_instrument_key IN ({placeholders})
                    GROUP BY expired_instrument_key
                """,
                    fetched_keys,
                ).fetchall()

                for row in days_data:
                    trading_day_scores.append(row[1])

            avg_days = sum(trading_day_scores) / len(trading_day_scores) if trading_day_scores else 0

            completeness = round(((fetched + no_data) / total) * 100, 1) if total > 0 else 0

            return {
                "total_contracts": total,
                "fetched": fetched,
                "no_data": no_data,
                "pending": pending,
                "completeness_pct": completeness,
                "avg_trading_days": round(avg_days, 1),
            }

    def get_completeness_bulk(self, instrument_key: str | None = None) -> list[dict]:
        """Get completeness scores for all instrument+expiry combinations."""
        with self.db_manager.get_read_connection() as conn:
            where = ""
            params: list = []
            if instrument_key:
                where = "WHERE instrument_key = ?"
                params = [instrument_key]

            rows = conn.execute(
                f"""
                SELECT
                    instrument_key,
                    expiry_date,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE data_fetched = TRUE) AS fetched,
                    COUNT(*) FILTER (WHERE no_data = TRUE) AS no_data_count
                FROM contracts
                {where}
                GROUP BY instrument_key, expiry_date
                ORDER BY instrument_key, expiry_date
            """,
                params,
            ).fetchall()

            results = []
            for row in rows:
                total = row[2]
                fetched = row[3]
                no_data_count = row[4]
                pct = round(((fetched + no_data_count) / total) * 100, 1) if total > 0 else 0
                results.append({
                    "instrument_key": row[0],
                    "expiry": str(row[1]),
                    "total": total,
                    "fetched": fetched,
                    "no_data": no_data_count,
                    "completeness_pct": pct,
                })
            return results

    # ------------------------------------------------------------------
    # Persist report
    # ------------------------------------------------------------------

    def save_report(self, report: QualityReport, instrument_key: str | None = None) -> None:
        """Save a quality report to the database."""
        import json

        with self.db_manager.get_connection() as conn:
            try:
                violations_json = json.dumps([v.to_dict() for v in report.violations])
                conn.execute(
                    """
                    INSERT INTO quality_reports
                        (instrument_key, checks_run, checks_passed, errors, warnings, passed, violations)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    [
                        instrument_key,
                        report.checks_run,
                        report.checks_passed,
                        report.error_count,
                        report.warning_count,
                        report.passed,
                        violations_json,
                    ],
                )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

    def get_recent_reports(self, limit: int = 10) -> list[dict]:
        """Fetch recent quality reports."""
        import json

        with self.db_manager.get_read_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, run_date, instrument_key, checks_run, checks_passed,
                       errors, warnings, passed, violations, created_at
                FROM quality_reports
                ORDER BY created_at DESC
                LIMIT ?
            """,
                [limit],
            ).fetchall()

            return [
                {
                    "id": row[0],
                    "run_date": str(row[1]),
                    "instrument_key": row[2],
                    "checks_run": row[3],
                    "checks_passed": row[4],
                    "errors": row[5],
                    "warnings": row[6],
                    "passed": row[7],
                    "violations": json.loads(row[8]) if row[8] else [],
                    "created_at": str(row[9]),
                }
                for row in rows
            ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _instrument_filter(instrument_key: str | None) -> tuple[str, list]:
        """Return (where_fragment, params) for parameterized instrument filtering."""
        if not instrument_key:
            return "", []
        return "AND expired_instrument_key LIKE ? || '%'", [instrument_key]
