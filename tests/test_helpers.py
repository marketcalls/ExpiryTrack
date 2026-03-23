"""Tests for route helpers (src/routes/helpers.py)."""

import json


def test_api_error_shape(app):
    """Verify response JSON shape has error and status keys."""
    with app.test_request_context():
        from src.routes.helpers import api_error

        response, status_code = api_error("Something went wrong")
        data = json.loads(response.get_data(as_text=True))

        assert "error" in data
        assert "status" in data
        assert data["error"] == "Something went wrong"
        assert data["status"] == 400
        assert status_code == 400


def test_api_error_custom_status(app):
    """Verify 404 status code."""
    with app.test_request_context():
        from src.routes.helpers import api_error

        response, status_code = api_error("Not found", 404)
        data = json.loads(response.get_data(as_text=True))

        assert data["error"] == "Not found"
        assert data["status"] == 404
        assert status_code == 404


def test_api_success_shape(app):
    """Verify response JSON has success=True."""
    with app.test_request_context():
        from src.routes.helpers import api_success

        response, status_code = api_success({"count": 42})
        data = json.loads(response.get_data(as_text=True))

        assert data["success"] is True
        assert data["count"] == 42
        assert status_code == 200


def test_require_auth_returns_401(client):
    """Call a decorated endpoint without auth, get 401."""
    # GET /api/watchlists uses @require_auth
    resp = client.get("/api/watchlists")
    assert resp.status_code == 401
    data = resp.get_json()
    assert "error" in data
