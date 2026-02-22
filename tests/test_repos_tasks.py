"""Tests for TaskRepository — CRUD, stale detection, cleanup, history."""




class TestTaskRepository:
    def test_create_task(self, tmp_db):
        repo = tmp_db.tasks_repo
        task_id = repo.create_task("test-1", "collection", params={"key": "val"})
        assert isinstance(task_id, int)
        assert task_id > 0

    def test_get_task(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("get-1", "export", params={"fmt": "csv"}, status_message="Starting")
        task = repo.get_task("get-1")
        assert task is not None
        assert task["task_id"] == "get-1"
        assert task["task_type"] == "export"
        assert task["status"] == "pending"
        assert task["status_message"] == "Starting"
        # params should be parsed from JSON
        assert task["params"] == {"fmt": "csv"}

    def test_get_task_not_found(self, tmp_db):
        assert tmp_db.tasks_repo.get_task("nonexistent") is None

    def test_update_task(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("upd-1", "collection")
        repo.update_task("upd-1", status="processing", progress=50, status_message="Halfway")
        task = repo.get_task("upd-1")
        assert task["status"] == "processing"
        assert task["status_message"] == "Halfway"

    def test_update_task_no_fields_is_noop(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("noop-1", "collection")
        repo.update_task("noop-1")  # No fields — should not raise

    def test_update_task_json_serialization(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("json-1", "export")
        repo.update_task("json-1", result={"candles": 100, "errors": 0})
        task = repo.get_task("json-1")
        assert task["result"] == {"candles": 100, "errors": 0}

    def test_list_tasks_no_filter(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("list-1", "collection")
        repo.create_task("list-2", "export")
        tasks = repo.list_tasks()
        assert len(tasks) == 2

    def test_list_tasks_filter_by_type(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("f-1", "collection")
        repo.create_task("f-2", "export")
        repo.create_task("f-3", "collection")
        tasks = repo.list_tasks(task_type="collection")
        assert len(tasks) == 2
        assert all(t["task_type"] == "collection" for t in tasks)

    def test_list_tasks_filter_by_status(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("s-1", "collection")
        repo.create_task("s-2", "collection")
        repo.update_task("s-1", status="completed", completed_at="2025-01-01T00:00:00")
        tasks = repo.list_tasks(status="completed")
        assert len(tasks) == 1
        assert tasks[0]["task_id"] == "s-1"

    def test_list_tasks_limit(self, tmp_db):
        repo = tmp_db.tasks_repo
        for i in range(5):
            repo.create_task(f"lim-{i}", "collection")
        tasks = repo.list_tasks(limit=3)
        assert len(tasks) == 3

    def test_mark_stale_tasks_failed(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("stale-1", "collection")
        repo.update_task("stale-1", status="processing")
        repo.create_task("stale-2", "export")
        repo.update_task("stale-2", status="processing")
        repo.create_task("ok-1", "collection")
        repo.update_task("ok-1", status="completed", completed_at="2025-01-01T00:00:00")

        count = repo.mark_stale_tasks_failed()
        assert count == 2

        task1 = repo.get_task("stale-1")
        assert task1["status"] == "failed"
        assert "crash recovery" in task1["error_message"]

        task_ok = repo.get_task("ok-1")
        assert task_ok["status"] == "completed"

    def test_cleanup_old_tasks(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("old-1", "collection")
        repo.update_task("old-1", status="completed", completed_at="2020-01-01T00:00:00")
        # cleanup should not raise
        repo.cleanup_old_tasks(max_age_hours=1)

    def test_get_task_history(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("h-1", "collection")
        repo.update_task("h-1", status="completed", completed_at="2025-01-01T00:00:00")
        repo.create_task("h-2", "export")
        repo.update_task("h-2", status="failed", completed_at="2025-01-02T00:00:00", error_message="oops")
        repo.create_task("h-3", "collection")  # still pending — should not appear

        history = repo.get_task_history()
        assert len(history) == 2
        assert all(h["status"] in ("completed", "failed") for h in history)

    def test_get_task_history_filter_by_type(self, tmp_db):
        repo = tmp_db.tasks_repo
        repo.create_task("ht-1", "collection")
        repo.update_task("ht-1", status="completed", completed_at="2025-01-01T00:00:00")
        repo.create_task("ht-2", "export")
        repo.update_task("ht-2", status="completed", completed_at="2025-01-02T00:00:00")

        history = repo.get_task_history(task_type="export")
        assert len(history) == 1
        assert history[0]["task_type"] == "export"
