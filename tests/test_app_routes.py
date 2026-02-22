"""
Tests for Flask app routes.
"""

import pandas as pd


def test_index_unauthenticated(client):
    """Home page returns 200 when not authenticated."""
    resp = client.get("/")
    assert resp.status_code == 200


def test_index_authenticated(authed_client, app):
    """Home page returns 200 with stats when authenticated."""
    resp = authed_client.get("/")
    assert resp.status_code == 200


def test_settings_page(client):
    """Settings page returns 200."""
    resp = client.get("/settings")
    assert resp.status_code == 200


def test_collect_page_unauthenticated(client):
    """Collect page redirects when not authenticated."""
    resp = client.get("/collect")
    # Should redirect to settings or login
    assert resp.status_code == 302


def test_collect_page_authenticated(authed_client):
    """Collect page returns 200 when authenticated."""
    resp = authed_client.get("/collect")
    assert resp.status_code == 200


def test_export_page(authed_client):
    """Export page returns 200 when authenticated."""
    resp = authed_client.get("/export")
    assert resp.status_code == 200


def test_api_auth_token_status(client, auth_manager_mock):
    """Token status returns JSON with valid flag."""
    auth_manager_mock.is_token_valid.return_value = False
    auth_manager_mock.token_expiry = None
    resp = client.get("/api/auth/token-status")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "valid" in data
    assert data["valid"] is False


def test_api_instruments_get(client):
    """Instruments list returns list."""
    resp = client.get("/api/instruments")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "instruments" in data
    assert isinstance(data["instruments"], list)


def test_api_instruments_master_segments(authed_client, tmp_db):
    """Segments endpoint returns segments list."""
    # Insert test data
    df = pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|TEST",
                "trading_symbol": "TEST",
                "name": "Test",
                "exchange": "NSE",
                "segment": "NSE_EQ",
                "instrument_type": "EQ",
                "isin": None,
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            }
        ]
    )
    tmp_db.bulk_insert_instrument_master(df)

    resp = authed_client.get("/api/instruments/master/segments")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "segments" in data


def test_api_instruments_master_types_requires_segment(authed_client):
    """Types endpoint requires segment parameter."""
    resp = authed_client.get("/api/instruments/master/types")
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_api_instruments_master_keys(authed_client, tmp_db):
    """Keys endpoint returns list for a segment."""
    df = pd.DataFrame(
        [
            {
                "instrument_key": "NSE_EQ|KEYTEST",
                "trading_symbol": "KEYTEST",
                "name": "Key Test",
                "exchange": "NSE",
                "segment": "NSE_EQ",
                "instrument_type": "EQ",
                "isin": None,
                "lot_size": 1,
                "tick_size": 0.05,
                "expiry": None,
                "strike_price": None,
                "option_type": None,
                "last_updated": "2025-01-01 00:00:00",
            }
        ]
    )
    tmp_db.bulk_insert_instrument_master(df)

    resp = authed_client.get("/api/instruments/master/keys?segment=NSE_EQ")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "keys" in data
    assert isinstance(data["keys"], list)


def test_api_backup_list(authed_client):
    """Backup list returns list."""
    resp = authed_client.get("/api/backup/list")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "backups" in data


def test_api_analytics_summary_requires_auth(client, auth_manager_mock):
    """Analytics summary returns 401 when not authenticated."""
    auth_manager_mock.is_token_valid.return_value = False
    resp = client.get("/api/analytics/summary")
    assert resp.status_code == 401


def test_api_candles_data_invalid_interval(authed_client):
    """Candles data returns 400 for invalid interval."""
    resp = authed_client.get("/api/candles/data/NSE_EQ|TEST?interval=invalid")
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_health_check(client):
    """Health check returns 200 with status."""
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] in ("ok", "degraded")
    assert "database" in data
    assert "auth" in data
    assert "timestamp" in data
