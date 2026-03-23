"""Tests for PersistentTaskTracker — CRUD, crash recovery, batch flush."""



from src.tasks.persistent_tracker import PersistentTaskTracker


class TestPersistentTaskTracker:
    def test_create_stores_in_memory_and_db(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        tracker.create("t-1", {"status": "processing", "progress": 0})

        # In-memory
        task = tracker.get("t-1")
        assert task is not None
        assert task["status"] == "processing"

        # In DB
        db_task = tmp_db.tasks_repo.get_task("t-1")
        assert db_task is not None
        assert db_task["task_type"] == "test"

    def test_update_modifies_in_memory(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        tracker.create("u-1", {"status": "processing", "progress": 0})
        tracker.update("u-1", progress=50, status_message="Halfway")

        task = tracker.get("u-1")
        assert task["progress"] == 50
        assert task["status_message"] == "Halfway"

    def test_get_returns_none_for_unknown(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        assert tracker.get("nonexistent") is None

    def test_get_falls_back_to_db(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        # Insert directly in DB
        tmp_db.tasks_repo.create_task("db-only", "test", params={"x": 1})
        # Not in memory — should fall back to DB
        task = tracker.get("db-only")
        assert task is not None
        assert task["task_type"] == "test"

    def test_list_active(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        tracker.create("a-1", {"status": "processing"})
        tracker.create("a-2", {"status": "completed"})
        tracker.create("a-3", {"status": "processing"})

        active = tracker.list_active()
        assert len(active) == 2

    def test_list_all(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        tracker.create("all-1", {"status": "processing"})
        tracker.create("all-2", {"status": "completed"})

        all_tasks = tracker.list_all()
        assert len(all_tasks) == 2

    def test_flush_persists_dirty_tasks(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        tracker.create("fl-1", {"status": "processing", "progress": 0})
        tracker.update("fl-1", status="completed", progress=100)

        # Manually flush
        tracker.flush()

        # Verify in DB
        db_task = tmp_db.tasks_repo.get_task("fl-1")
        assert db_task is not None
        assert db_task["status"] == "completed"

    def test_cleanup_removes_old_tasks(self, tmp_db):
        tracker = PersistentTaskTracker("test", tmp_db, max_age_hours=0)
        tracker.create("cl-1", {
            "status": "completed",
            "created_at": "2020-01-01T00:00:00",
        })
        tracker.cleanup()
        # After cleanup, old completed tasks are removed from memory
        # (cleanup only removes from memory, not DB)
        all_tasks = tracker.list_all()
        assert len(all_tasks) == 0

    def test_recover_stale_tasks(self, tmp_db):
        # Create tasks that look like they were running when app crashed
        tmp_db.tasks_repo.create_task("stale-1", "test")
        tmp_db.tasks_repo.update_task("stale-1", status="processing")

        tracker = PersistentTaskTracker("test", tmp_db, flush_interval=60)
        count = tracker.recover_stale_tasks()
        assert count >= 1

        # Verify the task is now failed
        task = tmp_db.tasks_repo.get_task("stale-1")
        assert task["status"] == "failed"
