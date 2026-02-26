"""Tests for SchedulerManager — lifecycle, jobs, history, status."""

from unittest.mock import MagicMock, patch

from src.scheduler.scheduler import SchedulerManager


def _make_scheduler():
    """Create a fresh SchedulerManager with mock APScheduler (bypass singleton)."""
    mgr = object.__new__(SchedulerManager)
    mgr._initialized = True
    mgr.scheduler = MagicMock()
    mgr.scheduler.running = False
    mgr.scheduler.get_jobs.return_value = []
    mgr.scheduler.get_job.return_value = None
    from collections import deque

    mgr._job_history = deque(maxlen=100)
    return mgr


def test_start_when_enabled():
    mgr = _make_scheduler()
    with patch("src.scheduler.scheduler.config") as mock_config:
        mock_config.SCHEDULER_ENABLED = True
        mock_config.SCHEDULER_MISFIRE_GRACE_TIME = 60
        mgr.start()
        mgr.scheduler.start.assert_called_once()


def test_start_when_disabled():
    mgr = _make_scheduler()
    with patch("src.scheduler.scheduler.config") as mock_config:
        mock_config.SCHEDULER_ENABLED = False
        mgr.start()
        mgr.scheduler.start.assert_not_called()


def test_stop():
    mgr = _make_scheduler()
    mgr.scheduler.running = True
    mgr.stop()
    mgr.scheduler.shutdown.assert_called_once_with(wait=False)


def test_stop_when_not_running():
    mgr = _make_scheduler()
    mgr.scheduler.running = False
    mgr.stop()
    mgr.scheduler.shutdown.assert_not_called()


def test_get_status():
    mgr = _make_scheduler()
    mgr.scheduler.running = True
    with patch("src.scheduler.scheduler.config") as mock_config:
        mock_config.SCHEDULER_ENABLED = True
        status = mgr.get_status()

    assert status["running"] is True
    assert status["enabled"] is True
    assert "jobs" in status
    assert "recent_history" in status


def test_get_jobs():
    mgr = _make_scheduler()
    mock_job = MagicMock()
    mock_job.id = "test_job"
    mock_job.name = "Test Job"
    mock_job.next_run_time = None
    mock_job.trigger = "interval[0:05:00]"
    mgr.scheduler.get_jobs.return_value = [mock_job]

    jobs = mgr.get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["id"] == "test_job"
    assert jobs[0]["name"] == "Test Job"
    assert jobs[0]["paused"] is True  # next_run_time is None


def test_remove_job():
    mgr = _make_scheduler()
    assert mgr.remove_job("existing_job") is True
    mgr.scheduler.remove_job.assert_called_once_with("existing_job")


def test_remove_nonexistent_job():
    from apscheduler.jobstores.base import JobLookupError

    mgr = _make_scheduler()
    mgr.scheduler.remove_job.side_effect = JobLookupError("existing_job")
    assert mgr.remove_job("nonexistent") is False


def test_pause_resume_job():
    mgr = _make_scheduler()
    assert mgr.pause_job("test_job") is True
    mgr.scheduler.pause_job.assert_called_once_with("test_job")

    assert mgr.resume_job("test_job") is True
    mgr.scheduler.resume_job.assert_called_once_with("test_job")


def test_get_history():
    mgr = _make_scheduler()
    # Add some history entries
    mgr._job_history.append({"job_id": "job1", "status": "success", "timestamp": "t1"})
    mgr._job_history.append({"job_id": "job2", "status": "error", "timestamp": "t2"})

    history = mgr.get_history(limit=10)
    assert len(history) == 2
    # Most recent first
    assert history[0]["job_id"] == "job2"
    assert history[1]["job_id"] == "job1"
