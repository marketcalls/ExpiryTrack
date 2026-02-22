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
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _instrument_filter(instrument_key: str | None) -> tuple[str, list]:
        """Return (where_fragment, params) for parameterized instrument filtering."""
        if not instrument_key:
            return "", []
        return "AND expired_instrument_key LIKE ? || '%'", [instrument_key]
