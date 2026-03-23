"""Tests for export blueprint — wizard page, available-expiries, start, status, download."""

from unittest.mock import MagicMock, patch


class TestExportRoutes:
    def test_export_wizard_no_credentials(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = False
        resp = client.get("/export", follow_redirects=False)
        assert resp.status_code == 302

    def test_export_wizard_no_token(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = True
        auth_manager_mock.is_token_valid.return_value = False
        resp = client.get("/export", follow_redirects=False)
        assert resp.status_code == 302

    def test_export_wizard_authenticated(self, authed_client):
        resp = authed_client.get("/export")
        assert resp.status_code == 200

    def test_export_status_not_found(self, authed_client):
        with patch("src.routes.export._get_export_tracker") as mock_get:
            mock_tracker = MagicMock()
            mock_tracker.get.return_value = None
            mock_get.return_value = mock_tracker
            resp = authed_client.get("/api/export/status/nonexistent")
            assert resp.status_code == 404

    def test_export_status_found(self, authed_client):
        with patch("src.routes.export._get_export_tracker") as mock_get:
            mock_tracker = MagicMock()
            mock_tracker.get.return_value = {
                "task_id": "e-1",
                "status": "completed",
                "progress": 100,
            }
            mock_get.return_value = mock_tracker
            resp = authed_client.get("/api/export/status/e-1")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "completed"

    def test_export_download_not_found(self, authed_client):
        with patch("src.routes.export._get_export_tracker") as mock_get:
            mock_tracker = MagicMock()
            mock_tracker.get.return_value = None
            mock_get.return_value = mock_tracker
            resp = authed_client.get("/api/export/download/nonexistent")
            assert resp.status_code == 404

    def test_export_download_not_completed(self, authed_client):
        with patch("src.routes.export._get_export_tracker") as mock_get:
            mock_tracker = MagicMock()
            mock_tracker.get.return_value = {"status": "processing", "file_path": None}
            mock_get.return_value = mock_tracker
            resp = authed_client.get("/api/export/download/e-1")
            assert resp.status_code == 400

    def test_export_download_file_missing(self, authed_client):
        with patch("src.routes.export._get_export_tracker") as mock_get:
            mock_tracker = MagicMock()
            mock_tracker.get.return_value = {
                "status": "completed",
                "file_path": "/tmp/nonexistent_file.csv",
            }
            mock_get.return_value = mock_tracker
            resp = authed_client.get("/api/export/download/e-1")
            assert resp.status_code == 404
