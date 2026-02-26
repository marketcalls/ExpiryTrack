"""
Advanced Scheduling System for ExpiryTrack
Uses APScheduler to manage automated data collection jobs.
"""

import logging
import threading
from collections import deque
from datetime import datetime

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import config

logger = logging.getLogger(__name__)


class SchedulerManager:
    """Manages scheduled collection and maintenance jobs."""

    _instance = None
    _singleton_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        self.scheduler = BackgroundScheduler(
            job_defaults={
                "coalesce": True,  # merge missed runs into one
                "max_instances": 1,
                "misfire_grace_time": config.SCHEDULER_MISFIRE_GRACE_TIME,
            }
        )
        self._job_history: deque = deque(maxlen=100)
        self.scheduler.add_listener(self._on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Start the scheduler if enabled in config."""
        if not config.SCHEDULER_ENABLED:
            logger.info("Scheduler is disabled (SCHEDULER_ENABLED=False)")
            return

        if self.scheduler.running:
            logger.warning("Scheduler is already running")
            return

        self._register_default_jobs()
        self.scheduler.start()
        logger.info("Scheduler started")

    def stop(self):
        """Gracefully shut down the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Scheduler stopped")

    @property
    def running(self) -> bool:
        return self.scheduler.running

    # ------------------------------------------------------------------
    # Job registration
    # ------------------------------------------------------------------

    def _register_default_jobs(self):
        """Add built-in jobs if they don't already exist."""

        # Daily collection job — runs at 18:30 IST (after market close)
        if not self.scheduler.get_job("daily_collection"):
            self.scheduler.add_job(
                self._run_daily_collection,
                CronTrigger(hour=18, minute=30, day_of_week="mon-fri"),
                id="daily_collection",
                name="Daily post-market data collection",
                replace_existing=True,
            )

        # Weekly full sync — Sunday at 10:00
        if not self.scheduler.get_job("weekly_sync"):
            self.scheduler.add_job(
                self._run_weekly_sync,
                CronTrigger(hour=10, minute=0, day_of_week="sun"),
                id="weekly_sync",
                name="Weekly full sync (catch-up)",
                replace_existing=True,
            )

        # Analytics summary refresh — daily at midnight
        if not self.scheduler.get_job("analytics_refresh"):
            self.scheduler.add_job(
                self._run_analytics_refresh,
                CronTrigger(hour=0, minute=5),
                id="analytics_refresh",
                name="Analytics daily summary refresh",
                replace_existing=True,
            )

        # Weekly quality check — Saturday at 11:00
        if not self.scheduler.get_job("weekly_quality_check"):
            self.scheduler.add_job(
                self._run_quality_check,
                CronTrigger(hour=11, minute=0, day_of_week="sat"),
                id="weekly_quality_check",
                name="Weekly data quality check",
                replace_existing=True,
            )

        # Daily export file cleanup — runs at 02:00
        if not self.scheduler.get_job("export_cleanup"):
            self.scheduler.add_job(
                self._run_export_cleanup,
                CronTrigger(hour=2, minute=0),
                id="export_cleanup",
                name="Daily export file cleanup (7-day retention)",
                replace_existing=True,
            )

        # Database checkpoint — every 6 hours
        if not self.scheduler.get_job("db_checkpoint"):
            self.scheduler.add_job(
                self._run_checkpoint,
                IntervalTrigger(hours=6),
                id="db_checkpoint",
                name="Database checkpoint",
                replace_existing=True,
            )

    def add_custom_job(self, job_id: str, func, trigger, **kwargs):
        """Add a user-defined scheduled job."""
        self.scheduler.add_job(func, trigger, id=job_id, replace_existing=True, **kwargs)
        logger.info(f"Custom job '{job_id}' added")

    def remove_job(self, job_id: str) -> bool:
        """Remove a scheduled job by id."""
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Job '{job_id}' removed")
            return True
        except JobLookupError:
            return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job."""
        try:
            self.scheduler.pause_job(job_id)
            return True
        except JobLookupError:
            return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        try:
            self.scheduler.resume_job(job_id)
            return True
        except JobLookupError:
            return False

    # ------------------------------------------------------------------
    # Job implementations
    # ------------------------------------------------------------------

    def _run_daily_collection(self):
        """Trigger daily post-market collection for all default instruments."""
        logger.info("Scheduler: starting daily collection")
        try:
            from ..collectors.task_manager import task_manager
            from ..database.manager import DatabaseManager

            db = DatabaseManager()
            instruments = db.get_default_instruments()

            if not instruments:
                logger.warning("No default instruments configured, skipping daily collection")
                return

            task_manager.create_task(
                {
                    "instruments": instruments,
                    "contract_type": "both",
                    "expiries": {},  # auto-detect recent expiries
                    "interval": config.DATA_INTERVAL,
                    "workers": 5,
                    "source": "scheduler",
                }
            )
            logger.info(f"Daily collection task created for {len(instruments)} instruments")

        except Exception as e:
            logger.error(f"Daily collection failed: {e}")
            raise

    def _run_weekly_sync(self):
        """Resume any incomplete contracts — catch-up sync."""
        logger.info("Scheduler: starting weekly sync")
        try:
            from ..collectors.task_manager import task_manager
            from ..database.manager import DatabaseManager

            db = DatabaseManager()
            pending = db.get_pending_contracts(limit=500)

            if not pending:
                logger.info("No pending contracts, weekly sync skipped")
                return

            logger.info(f"Weekly sync: {len(pending)} pending contracts found")
            # Create a resume task
            task_manager.create_task(
                {
                    "instruments": [],
                    "contract_type": "both",
                    "expiries": {},
                    "interval": config.DATA_INTERVAL,
                    "workers": 5,
                    "source": "scheduler_weekly_sync",
                    "resume": True,
                }
            )

        except Exception as e:
            logger.error(f"Weekly sync failed: {e}")
            raise

    def _run_analytics_refresh(self):
        """Refresh analytics daily summary table."""
        logger.info("Scheduler: refreshing analytics summary")
        try:
            from ..analytics.summary import refresh_daily_summary
            from ..database.manager import DatabaseManager

            db = DatabaseManager()
            refresh_daily_summary(db)
        except Exception as e:
            logger.error(f"Analytics refresh failed: {e}")
            raise

    def _run_quality_check(self):
        """Run weekly data quality check and save report."""
        logger.info("Scheduler: running weekly quality check")
        try:
            from ..database.manager import DatabaseManager
            from ..quality.checker import DataQualityChecker

            db = DatabaseManager()
            checker = DataQualityChecker(db)
            report = checker.run_all_checks()
            checker.save_report(report)
            logger.info(
                f"Weekly quality check complete: {report.checks_run} checks, "
                f"{report.error_count} errors, {report.warning_count} warnings"
            )
        except Exception as e:
            logger.error(f"Quality check failed: {e}")
            raise

    def _run_export_cleanup(self):
        """Delete export files older than 7 days and update history records."""
        logger.info("Scheduler: running export file cleanup")
        try:
            import os

            from ..database.manager import DatabaseManager

            db = DatabaseManager()
            expired = db.exports_repo.get_expired_exports(days=7)

            deleted_count = 0
            for record in expired:
                file_path = record.get("file_path", "")
                # Delete the file from disk if it exists
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        deleted_count += 1
                    except OSError as e:
                        logger.warning(f"Failed to delete export file {file_path}: {e}")

                # Remove the history record
                db.exports_repo.delete_export(record["id"])

            if deleted_count or expired:
                logger.info(
                    f"Export cleanup: deleted {deleted_count} file(s), "
                    f"removed {len(expired)} history record(s)"
                )
            else:
                logger.info("Export cleanup: no expired exports found")

        except Exception as e:
            logger.error(f"Export cleanup failed: {e}")
            raise

    def _run_checkpoint(self):
        """Run DuckDB CHECKPOINT to flush WAL."""
        logger.info("Scheduler: running database checkpoint")
        try:
            from ..database.manager import DatabaseManager

            db = DatabaseManager()
            db.vacuum()
        except Exception as e:
            logger.error(f"Checkpoint failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Event handling & status
    # ------------------------------------------------------------------

    def _on_job_event(self, event):
        """Record job execution history."""
        entry = {
            "job_id": event.job_id,
            "timestamp": datetime.now().isoformat(),
        }

        if hasattr(event, "exception") and event.exception:
            entry["status"] = "error"
            entry["error"] = str(event.exception)
            logger.error(f"Job '{event.job_id}' failed: {event.exception}")
        elif hasattr(event, "code") and event.code == EVENT_JOB_MISSED:
            entry["status"] = "missed"
            logger.warning(f"Job '{event.job_id}' missed its run window")
        else:
            entry["status"] = "success"

        self._job_history.append(entry)

    def get_jobs(self) -> list[dict]:
        """Return list of all scheduled jobs with their next run times."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                    "paused": job.next_run_time is None,
                    "trigger": str(job.trigger),
                }
            )
        return jobs

    def get_history(self, limit: int = 20) -> list[dict]:
        """Return recent job execution history."""
        items = list(self._job_history)
        return list(reversed(items[-limit:]))

    def get_status(self) -> dict:
        """Return scheduler status summary."""
        return {
            "running": self.scheduler.running,
            "enabled": config.SCHEDULER_ENABLED,
            "jobs": self.get_jobs(),
            "recent_history": self.get_history(10),
        }


# Singleton instance (lazy — started explicitly via start())
scheduler_manager = SchedulerManager()
