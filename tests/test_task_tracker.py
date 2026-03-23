"""Tests for TaskTracker (src/tasks/tracker.py)."""

from datetime import datetime, timedelta

from src.tasks.tracker import TaskTracker


def test_create_and_get():
    """Create task, get it, verify fields."""
    tracker = TaskTracker("test")
    tracker.create(
        "t1",
        {
            "task_id": "t1",
            "status": "processing",
            "progress": 0,
            "created_at": datetime.now().isoformat(),
        },
    )

    task = tracker.get("t1")
    assert task is not None
    assert task["task_id"] == "t1"
    assert task["status"] == "processing"
    assert task["progress"] == 0


def test_get_nonexistent():
    """Get unknown task_id returns None."""
    tracker = TaskTracker("test")
    assert tracker.get("nonexistent_id") is None


def test_update():
    """Create, update fields, verify updated."""
    tracker = TaskTracker("test")
    tracker.create("t2", {"status": "processing", "progress": 0})

    tracker.update("t2", status="completed", progress=100)

    task = tracker.get("t2")
    assert task["status"] == "completed"
    assert task["progress"] == 100


def test_list_active():
    """Create 2 tasks (1 processing, 1 completed), list_active returns 1."""
    tracker = TaskTracker("test")
    tracker.create("active1", {"status": "processing"})
    tracker.create("done1", {"status": "completed"})

    active = tracker.list_active()
    assert len(active) == 1
    assert active[0]["status"] == "processing"


def test_list_all():
    """Create 2 tasks, list_all returns 2."""
    tracker = TaskTracker("test")
    tracker.create("a", {"status": "processing"})
    tracker.create("b", {"status": "completed"})

    all_tasks = tracker.list_all()
    assert len(all_tasks) == 2


def test_cleanup_removes_old():
    """Create old completed task with past timestamp, cleanup removes it."""
    tracker = TaskTracker("test", max_age_hours=0)

    # Create a task with a created_at in the past
    past_time = (datetime.now() - timedelta(seconds=5)).isoformat()
    tracker.create(
        "old_task",
        {"status": "completed", "created_at": past_time},
    )

    # Also create an active task that should NOT be removed
    tracker.create(
        "active_task",
        {"status": "processing", "created_at": datetime.now().isoformat()},
    )

    tracker.cleanup()

    assert tracker.get("old_task") is None
    assert tracker.get("active_task") is not None
