"""Tests for status blueprint — all 7 endpoints."""

import json
from unittest.mock import MagicMock, patch

# ── Status page ──


def test_status_page_authenticated(authed_client):
    """GET /status renders status page when authenticated."""
    with patch("src.collectors.task_manager.task_manager") as mock_tm:
        mock_tm.get_all_tasks.return_value = []
        resp = authed_client.get("/status")
    assert resp.status_code == 200


def test_status_page_unauthenticated_redirects(client):
    """GET /status redirects to login when unauthenticated."""
    resp = client.get("/status")
    assert resp.status_code == 302
    assert "/login" in resp.headers.get("Location", "")


# ── Download status page ──


def test_download_status_page_authenticated(authed_client):
    """GET /download-status renders page when authenticated."""
    resp = authed_client.get("/download-status")
    assert resp.status_code == 200


def test_download_status_page_unauthenticated_redirects(client):
    """GET /download-status redirects to login when unauthenticated."""
    resp = client.get("/download-status")
    assert resp.status_code == 302


# ── API: download status ──


def test_api_download_status(authed_client):
    """GET /api/download-status returns JSON list."""
    with patch("src.analytics.engine.AnalyticsEngine") as MockEngine:
        engine = MockEngine.return_value
        engine.get_download_status.return_value = [
            {"instrument_key": "NSE_INDEX|Nifty 50", "status": "complete"}
        ]
        resp = authed_client.get("/api/download-status")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)


def test_api_download_status_with_filter(authed_client):
    """GET /api/download-status?instrument=X filters by instrument."""
    with patch("src.analytics.engine.AnalyticsEngine") as MockEngine:
        engine = MockEngine.return_value
        engine.get_download_status.return_value = []
        resp = authed_client.get("/api/download-status?instrument=NSE_INDEX|Nifty 50")
    assert resp.status_code == 200
    engine.get_download_status.assert_called_once_with("NSE_INDEX|Nifty 50")


# ── API: missing contracts ──


def test_api_download_status_missing(authed_client):
    """GET /api/download-status/<key>/<date>/missing returns contracts."""
    with patch("src.analytics.engine.AnalyticsEngine") as MockEngine:
        engine = MockEngine.return_value
        engine.get_missing_contracts.return_value = []
        resp = authed_client.get(
            "/api/download-status/NSE_INDEX|Nifty 50/2025-01-30/missing"
        )
    assert resp.status_code == 200


def test_api_download_status_missing_bad_date(authed_client):
    """Invalid date format returns 400."""
    resp = authed_client.get(
        "/api/download-status/NSE_INDEX|Nifty 50/not-a-date/missing"
    )
    assert resp.status_code == 400


# ── API: resume ──


def test_api_resume_success(authed_client):
    """POST /api/download-status/resume creates task."""
    with patch("src.collectors.task_manager.task_manager") as mock_tm:
        mock_tm.create_task.return_value = "task-123"
        resp = authed_client.post(
            "/api/download-status/resume",
            data=json.dumps({
                "instrument": "NSE_INDEX|Nifty 50",
                "expiries": ["2025-01-30"],
            }),
            content_type="application/json",
        )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["task_id"] == "task-123"


def test_api_resume_missing_instrument(authed_client):
    """POST /api/download-status/resume without instrument returns 400."""
    resp = authed_client.post(
        "/api/download-status/resume",
        data=json.dumps({"expiries": ["2025-01-30"]}),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_api_resume_invalid_expiry_format(authed_client):
    """POST /api/download-status/resume with bad expiry format returns 400."""
    resp = authed_client.post(
        "/api/download-status/resume",
        data=json.dumps({
            "instrument": "NSE_INDEX|Nifty 50",
            "expiries": ["bad-date"],
        }),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_api_resume_too_many_expiries(authed_client):
    """POST /api/download-status/resume with >50 expiries returns 400."""
    resp = authed_client.post(
        "/api/download-status/resume",
        data=json.dumps({
            "instrument": "NSE_INDEX|Nifty 50",
            "expiries": [f"2025-01-{i:02d}" for i in range(1, 52)],
        }),
        content_type="application/json",
    )
    assert resp.status_code == 400


# ── API: force-refetch ──


def test_api_force_refetch_success(authed_client):
    """POST /api/download-status/force-refetch resets data_fetched."""
    resp = authed_client.post(
        "/api/download-status/force-refetch",
        data=json.dumps({
            "instrument_key": "NSE_INDEX|Nifty 50",
            "expiry_date": "2025-01-30",
        }),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True


def test_api_force_refetch_missing_fields(authed_client):
    """POST /api/download-status/force-refetch without required fields returns 400."""
    resp = authed_client.post(
        "/api/download-status/force-refetch",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert resp.status_code == 400


# ── API: retry-failed ──


def test_api_retry_failed(authed_client):
    """POST /api/download-status/retry-failed resets fetch attempts."""
    resp = authed_client.post(
        "/api/download-status/retry-failed",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True


# ── API: quality run ──


def test_api_quality_run(authed_client):
    """POST /api/quality/run returns quality report."""
    with patch("src.quality.checker.DataQualityChecker") as MockChecker:
        checker = MockChecker.return_value
        mock_report = MagicMock()
        mock_report.to_dict.return_value = {
            "passed": True,
            "checks_run": 5,
            "checks_passed": 5,
        }
        checker.run_all_checks.return_value = mock_report
        resp = authed_client.post(
            "/api/quality/run",
            data=json.dumps({}),
            content_type="application/json",
        )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["passed"] is True
