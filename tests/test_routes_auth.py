"""Tests for auth blueprint — settings, save_credentials, login, callback, logout, token-status."""

from unittest.mock import MagicMock


class TestAuthRoutes:
    def test_settings_page_no_credentials(self, client):
        resp = client.get("/settings")
        assert resp.status_code == 200
        assert b"settings" in resp.data.lower() or b"Settings" in resp.data

    def test_settings_page_with_credentials(self, authed_client, auth_manager_mock):
        import app as app_module

        auth_manager_mock.has_credentials.return_value = True
        app_module.app.db_manager.get_credentials = MagicMock(
            return_value={
                "api_key": "test_key",
                "api_secret": "test_secret_1234",
                "redirect_uri": "http://localhost:5005/upstox/callback",
            }
        )
        resp = authed_client.get("/settings")
        assert resp.status_code == 200

    def test_save_credentials_success(self, client, auth_manager_mock):
        auth_manager_mock.save_credentials.return_value = True
        resp = client.post(
            "/save_credentials",
            data={
                "api_key": "new_key_12345",
                "api_secret": "new_secret_12345",
                "redirect_url": "http://localhost:5005/upstox/callback",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 302

    def test_save_credentials_validation_error(self, client, auth_manager_mock):
        # Empty api_key should fail validation
        resp = client.post(
            "/save_credentials",
            data={"api_key": "", "api_secret": "", "redirect_url": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 302

    def test_login_no_credentials(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = False
        resp = client.get("/login", follow_redirects=False)
        assert resp.status_code == 302

    def test_login_with_credentials(self, client, auth_manager_mock):
        auth_manager_mock.has_credentials.return_value = True
        auth_manager_mock.get_authorization_url.return_value = "https://api.upstox.com/v2/login/authorization/dialog"
        auth_manager_mock._oauth_state = "test_state"
        resp = client.get("/login", follow_redirects=False)
        assert resp.status_code == 302

    def test_callback_with_error(self, client, auth_manager_mock):
        resp = client.get("/upstox/callback?error=access_denied")
        assert resp.status_code == 400

    def test_callback_no_state_in_session(self, client, auth_manager_mock):
        # No oauth_state in session and no _oauth_state on auth → 403
        auth_manager_mock._oauth_state = None
        auth_manager_mock.clear_oauth_state = MagicMock()
        resp = client.get("/upstox/callback?code=abc&state=test_state")
        assert resp.status_code == 403

    def test_callback_invalid_state(self, client, auth_manager_mock):
        auth_manager_mock._oauth_state = "real_state"
        auth_manager_mock.clear_oauth_state = MagicMock()
        resp = client.get("/upstox/callback?code=abc&state=wrong_state")
        assert resp.status_code == 403

    def test_logout(self, client, auth_manager_mock):
        resp = client.get("/logout", follow_redirects=False)
        assert resp.status_code == 302
        auth_manager_mock.clear_tokens.assert_called_once()

    def test_token_status_not_valid(self, client, auth_manager_mock):
        auth_manager_mock.is_token_valid.return_value = False
        auth_manager_mock.token_expiry = None
        resp = client.get("/api/auth/token-status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["valid"] is False
        assert data["remaining_seconds"] == 0

    def test_token_status_valid(self, client, auth_manager_mock):
        import time

        auth_manager_mock.is_token_valid.return_value = True
        auth_manager_mock.token_expiry = time.time() + 3600
        resp = client.get("/api/auth/token-status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["valid"] is True
        assert data["remaining_seconds"] > 0
