"""
Shared pytest fixtures for ExpiryTrack tests.
"""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Session-wide temp DB for any module-level singletons ──
# Some modules (task_manager, scheduler) create singletons at import time
# that instantiate DatabaseManager → connect to real DB → run migrations.
# This redirect ensures ALL DatabaseManager instances in tests use a temp DB.
_test_db_dir: Path | None = None


def _patched_db_init(original_init):
    """Wrap DatabaseManager.__init__ to redirect to a temp DB."""

    def wrapper(self, db_path=None):
        if db_path is not None:
            # Explicit path (e.g., tmp_db fixture) — use as-is
            original_init(self, db_path=db_path)
        else:
            # No path given (module-level singletons) — redirect to temp DB
            original_init(self, db_path=_test_db_dir / "fallback.duckdb")

    return wrapper


@pytest.fixture
def tmp_db(tmp_path):
    """Fresh DuckDB database for each test."""
    from src.database.manager import DatabaseManager

    db = DatabaseManager(db_path=tmp_path / "test.duckdb")
    yield db


@pytest.fixture
def mock_auth_manager():
    """Standalone mocked AuthManager (not tied to Flask app)."""
    mock = MagicMock()
    mock.is_token_valid.return_value = True
    mock.has_credentials.return_value = True
    mock.api_key = "test_key"
    mock.api_secret = "test_secret"
    mock.redirect_uri = "http://127.0.0.1:5005/upstox/callback"
    mock.access_token = "test_token"
    mock.token_expiry = None
    mock.base_url = "https://api.upstox.com/v2"
    mock.get_headers.return_value = {
        "Authorization": "Bearer test_token",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return mock


@pytest.fixture
def mock_api_client(mock_auth_manager):
    """Mocked UpstoxAPIClient with async methods."""
    mock = MagicMock()
    mock.auth_manager = mock_auth_manager
    mock.connect = AsyncMock()
    mock.close = AsyncMock()
    mock.get_expiries = AsyncMock(return_value=["2025-01-30", "2025-02-27"])
    mock.get_option_contracts = AsyncMock(return_value=[])
    mock.get_future_contracts = AsyncMock(return_value=[])
    mock.get_all_contracts_for_expiry = AsyncMock(
        return_value={"options": [], "futures": []}
    )
    mock.get_historical_data = AsyncMock(return_value=[])
    return mock


def _ensure_app_imported(tmp_path_factory):
    """Import app module, patching constructors to avoid hitting real DB."""
    global _test_db_dir

    if "app" in sys.modules:
        return sys.modules["app"]

    tmp = tmp_path_factory.mktemp("app_init")
    _test_db_dir = tmp

    # Monkeypatch DatabaseManager.__init__ for the ENTIRE test session so that
    # any module-level singleton (TaskManager, SchedulerManager, etc.) that
    # creates a DatabaseManager() gets a temp DB instead of the production one.
    from src.database.manager import DatabaseManager

    _orig_db_init = DatabaseManager.__init__
    DatabaseManager.__init__ = _patched_db_init(_orig_db_init)

    def fake_auth_init(self):
        self.base_url = "https://api.upstox.com/v2"
        self.db_manager = DatabaseManager(db_path=tmp / "init.duckdb")
        self.api_key = None
        self.api_secret = None
        self.redirect_uri = "http://127.0.0.1:5005/upstox/callback"
        self.access_token = None
        self.token_expiry = None

    with patch("src.auth.manager.AuthManager.__init__", fake_auth_init):
        import app as app_module

    return app_module


@pytest.fixture
def app(tmp_db, tmp_path_factory):
    """Flask app configured for testing with isolated DB."""
    app_module = _ensure_app_imported(tmp_path_factory)

    mock_auth = MagicMock()
    mock_auth.is_token_valid.return_value = False
    mock_auth.has_credentials.return_value = False
    mock_auth.token_expiry = None

    # Save originals
    orig_db = app_module.db_manager
    orig_auth = app_module.auth_manager

    # Replace managers on both module and app object (blueprints use current_app)
    app_module.db_manager = tmp_db
    app_module.auth_manager = mock_auth
    app_module.app.db_manager = tmp_db
    app_module.app.auth_manager = mock_auth

    app_module.app.config["TESTING"] = True
    app_module.app.config["SECRET_KEY"] = "test-secret-key"
    app_module.app.config["WTF_CSRF_ENABLED"] = False
    app_module.app.config["RATELIMIT_ENABLED"] = False

    yield app_module.app

    # Restore originals
    app_module.db_manager = orig_db
    app_module.auth_manager = orig_auth
    app_module.app.db_manager = orig_db
    app_module.app.auth_manager = orig_auth


@pytest.fixture
def client(app):
    """Flask test client with isolated DB."""
    with app.test_client() as c:
        yield c


@pytest.fixture
def auth_manager_mock(app):
    """Access the mocked auth_manager from the app module."""
    import app as app_module

    return app_module.auth_manager


@pytest.fixture
def authed_client(client, auth_manager_mock):
    """Test client with mocked valid auth."""
    auth_manager_mock.is_token_valid.return_value = True
    auth_manager_mock.has_credentials.return_value = True
    yield client
