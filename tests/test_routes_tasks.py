"""Tests for tasks API blueprint."""



class TestTasksAPI:
    def test_list_tasks_requires_auth(self, client):
        resp = client.get("/api/tasks")
        assert resp.status_code == 401

    def test_list_tasks_empty(self, authed_client, tmp_db):
        resp = authed_client.get("/api/tasks")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["tasks"] == []
        assert data["count"] == 0

    def test_list_tasks_with_data(self, authed_client, tmp_db):
        tmp_db.tasks_repo.create_task("t-1", "collection", params={"key": "val"})
        tmp_db.tasks_repo.create_task("t-2", "export")

        resp = authed_client.get("/api/tasks")
        data = resp.get_json()
        assert data["count"] == 2

    def test_list_tasks_filter_by_type(self, authed_client, tmp_db):
        tmp_db.tasks_repo.create_task("ft-1", "collection")
        tmp_db.tasks_repo.create_task("ft-2", "export")

        resp = authed_client.get("/api/tasks?type=export")
        data = resp.get_json()
        assert data["count"] == 1
        assert data["tasks"][0]["task_type"] == "export"

    def test_get_task(self, authed_client, tmp_db):
        tmp_db.tasks_repo.create_task("gt-1", "collection", status_message="Running")

        resp = authed_client.get("/api/tasks/gt-1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["task_id"] == "gt-1"
        assert data["task_type"] == "collection"

    def test_get_task_not_found(self, authed_client, tmp_db):
        resp = authed_client.get("/api/tasks/nonexistent")
        assert resp.status_code == 404

    def test_cancel_task_not_found(self, authed_client, tmp_db):
        resp = authed_client.post("/api/tasks/nonexistent/cancel")
        # The endpoint first tries task_manager.cancel_task which returns False,
        # then falls back to DB lookup which returns None → 404
        assert resp.status_code == 404

    def test_cancel_completed_task_returns_400(self, authed_client, tmp_db):
        tmp_db.tasks_repo.create_task("ct-1", "collection")
        tmp_db.tasks_repo.update_task("ct-1", status="completed", completed_at="2025-01-01T00:00:00")

        resp = authed_client.post("/api/tasks/ct-1/cancel")
        assert resp.status_code == 400
        assert "not cancellable" in resp.get_json()["error"]
