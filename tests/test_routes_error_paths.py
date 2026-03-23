"""Tests for error responses across blueprints."""

import json

# ── Unauthenticated (401) tests ──


def test_watchlists_list_401(client):
    """GET /api/watchlists returns 401 when unauthenticated."""
    resp = client.get("/api/watchlists")
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


def test_watchlists_create_401(client):
    """POST /api/watchlists returns 401 when unauthenticated."""
    resp = client.post(
        "/api/watchlists",
        data=json.dumps({"name": "test"}),
        content_type="application/json",
    )
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


def test_candles_collect_401(client):
    """POST /api/candles/collect returns 401 when unauthenticated."""
    resp = client.post(
        "/api/candles/collect",
        data=json.dumps({"instrument_keys": ["NSE_EQ|TEST"]}),
        content_type="application/json",
    )
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


def test_export_start_401(client):
    """POST /api/export/start returns 401 when unauthenticated."""
    resp = client.post(
        "/api/export/start",
        data=json.dumps({"format": "csv"}),
        content_type="application/json",
    )
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


def test_backup_create_401(client):
    """POST /api/backup/create returns 401 when unauthenticated."""
    resp = client.post("/api/backup/create")
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


def test_scheduler_status_401(client):
    """GET /api/scheduler/status returns 401 when unauthenticated."""
    resp = client.get("/api/scheduler/status")
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data


# ── Bad request (400) tests (authenticated) ──


def test_candles_collect_400_missing_body(authed_client):
    """POST /api/candles/collect with empty JSON object (no instrument_keys) returns 400."""
    resp = authed_client.post(
        "/api/candles/collect",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_export_start_400_invalid_format(authed_client):
    """POST /api/export/start with invalid format returns 400."""
    resp = authed_client.post(
        "/api/export/start",
        data=json.dumps({"format": "xml"}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_watchlists_create_400_missing_name(authed_client):
    """POST /api/watchlists with empty body returns 400."""
    resp = authed_client.post(
        "/api/watchlists",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data


def test_instruments_add_400_missing_fields(authed_client):
    """POST /api/instruments with empty body returns 400."""
    resp = authed_client.post(
        "/api/instruments",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data
