"""Tests for AuthManager — credential loading, token validation, OAuth flow."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def auth_mgr(tmp_db):
    """AuthManager with isolated test DB (no real Upstox connection)."""
    with patch("src.auth.manager.DatabaseManager", return_value=tmp_db):
        from src.auth.manager import AuthManager

        mgr = AuthManager()
    return mgr


# ── has_credentials ──


def test_has_credentials_false_when_no_creds(auth_mgr):
    """No API key/secret stored => has_credentials() is False."""
    assert auth_mgr.has_credentials() is False


def test_has_credentials_true_after_save(auth_mgr):
    """After saving valid credentials, has_credentials() returns True."""
    auth_mgr.save_credentials("my_api_key", "my_api_secret")
    assert auth_mgr.has_credentials() is True


# ── is_token_valid ──


def test_is_token_valid_no_token(auth_mgr):
    """No access_token => is_token_valid() is False."""
    assert auth_mgr.is_token_valid() is False


def test_is_token_valid_expired(auth_mgr):
    """Token with past expiry => is_token_valid() is False."""
    auth_mgr.access_token = "some_token"
    auth_mgr.token_expiry = time.time() - 100  # expired 100s ago
    assert auth_mgr.is_token_valid() is False


def test_is_token_valid_active(auth_mgr):
    """Token with future expiry => is_token_valid() is True."""
    auth_mgr.access_token = "some_token"
    auth_mgr.token_expiry = time.time() + 3600  # expires in 1h
    assert auth_mgr.is_token_valid() is True


def test_is_token_valid_no_expiry(auth_mgr):
    """Token with no expiry set => is_token_valid() is True."""
    auth_mgr.access_token = "some_token"
    auth_mgr.token_expiry = None
    assert auth_mgr.is_token_valid() is True


# ── get_authorization_url ──


def test_get_authorization_url_no_creds_raises(auth_mgr):
    """get_authorization_url without credentials raises ValueError."""
    with pytest.raises(ValueError, match="credentials not configured"):
        auth_mgr.get_authorization_url()


def test_get_authorization_url_generates_url(auth_mgr):
    """With credentials, generates a valid Upstox authorization URL."""
    auth_mgr.api_key = "my_key"
    auth_mgr.api_secret = "my_secret"
    url = auth_mgr.get_authorization_url()
    assert "api.upstox.com" in url
    assert "client_id=my_key" in url
    assert "response_type=code" in url


# ── validate_oauth_state ──


def test_validate_oauth_state_matches(auth_mgr):
    """State validation succeeds when state matches."""
    auth_mgr.api_key = "k"
    auth_mgr.api_secret = "s"
    auth_mgr.get_authorization_url()  # sets _oauth_state
    state = auth_mgr._oauth_state
    assert auth_mgr.validate_oauth_state(state) is True


def test_validate_oauth_state_mismatch(auth_mgr):
    """State validation fails when state doesn't match."""
    auth_mgr._oauth_state = "expected_state"
    assert auth_mgr.validate_oauth_state("wrong_state") is False


def test_validate_oauth_state_no_stored(auth_mgr):
    """State validation fails when no state was stored."""
    assert auth_mgr.validate_oauth_state("any") is False


# ── get_headers ──


def test_get_headers_valid_token(auth_mgr):
    """With valid token, get_headers() returns Bearer auth header."""
    # Save creds + token to DB so _load_credentials works
    auth_mgr.save_credentials("key", "secret")
    auth_mgr.db_manager.save_token("tok123", time.time() + 3600)
    headers = auth_mgr.get_headers()
    assert headers["Authorization"] == "Bearer tok123"
    assert "application/json" in headers["Accept"]


def test_get_headers_invalid_token_raises(auth_mgr):
    """Without valid token, get_headers() raises ValueError."""
    with pytest.raises(ValueError, match="Invalid or expired token"):
        auth_mgr.get_headers()


# ── clear_tokens ──


def test_clear_tokens(auth_mgr):
    """clear_tokens() removes in-memory and DB tokens."""
    auth_mgr.access_token = "tok"
    auth_mgr.token_expiry = time.time() + 3600
    auth_mgr.clear_tokens()
    assert auth_mgr.access_token is None
    assert auth_mgr.token_expiry is None
    assert auth_mgr.is_token_valid() is False


# ── exchange_code_for_token ──


@pytest.mark.asyncio
async def test_exchange_code_success(auth_mgr):
    """Successful token exchange stores token and returns True."""
    auth_mgr.api_key = "key"
    auth_mgr.api_secret = "secret"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "access_token": "new_token_123",
        "expires_in": 86400,
    }

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("src.auth.manager.httpx.AsyncClient", return_value=mock_client):
        result = await auth_mgr.exchange_code_for_token("auth_code_123")

    assert result is True
    assert auth_mgr.access_token == "new_token_123"


@pytest.mark.asyncio
async def test_exchange_code_failure(auth_mgr):
    """Failed token exchange returns False."""
    auth_mgr.api_key = "key"
    auth_mgr.api_secret = "secret"

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"

    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("src.auth.manager.httpx.AsyncClient", return_value=mock_client):
        result = await auth_mgr.exchange_code_for_token("bad_code")

    assert result is False
