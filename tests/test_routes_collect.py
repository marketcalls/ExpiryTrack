"""Tests for collect blueprint — wizard page, expiries API, start, status, tasks."""

from unittest.mock import patch


class TestCollectRoutes:
    def test_collect_page_no_credentials(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = False
        resp = client.get("/collect", follow_redirects=False)
        assert resp.status_code == 302

    def test_collect_page_no_token(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = True
        auth_manager_mock.is_token_valid.return_value = False
        resp = client.get("/collect", follow_redirects=False)
        assert resp.status_code == 302

    def test_collect_page_authenticated(self, authed_client):
        resp = authed_client.get("/collect")
        assert resp.status_code == 200

    def test_collect_status_not_found(self, authed_client):
        with patch("src.collectors.task_manager.task_manager") as mock_tm:
            mock_tm.get_task_status.return_value = None
            resp = authed_client.get("/api/collect/status/nonexistent")
            assert resp.status_code == 404

    def test_collect_status_found(self, authed_client):
        with patch("src.collectors.task_manager.task_manager") as mock_tm:
            mock_tm.get_task_status.return_value = {
                "task_id": "t-1",
                "status": "processing",
                "progress": 50,
            }
            resp = authed_client.get("/api/collect/status/t-1")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "processing"

    def test_collect_tasks(self, authed_client):
        with patch("src.collectors.task_manager.task_manager") as mock_tm:
            mock_tm.get_all_tasks.return_value = [
                {"task_id": "t-1", "status": "completed"},
            ]
            resp = authed_client.get("/api/collect/tasks")
            assert resp.status_code == 200
            data = resp.get_json()
            assert len(data["tasks"]) == 1
