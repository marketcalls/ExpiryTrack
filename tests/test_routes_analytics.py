"""Route tests for analytics blueprint — dashboard, charts, storage."""


def test_analytics_page_requires_auth(client):
    """Unauthenticated users get redirected."""
    resp = client.get("/analytics")
    assert resp.status_code == 302


def test_analytics_summary_requires_auth(client):
    resp = client.get("/api/analytics/summary")
    assert resp.status_code == 401


def test_analytics_summary(authed_client):
    resp = authed_client.get("/api/analytics/summary")
    assert resp.status_code == 200
    data = resp.json
    assert "instruments" in data
    assert "contracts" in data
    assert "total_candles" in data


def test_analytics_contracts_by_type(authed_client):
    resp = authed_client.get("/api/analytics/contracts-by-type")
    assert resp.status_code == 200
    data = resp.json
    assert "labels" in data
    assert "data" in data


def test_analytics_storage(authed_client):
    resp = authed_client.get("/api/analytics/storage")
    assert resp.status_code == 200
    data = resp.json
    assert "instruments" in data
    assert "contracts" in data


def test_analytics_candles_per_day(authed_client):
    resp = authed_client.get("/api/analytics/candles-per-day")
    assert resp.status_code == 200
    assert "labels" in resp.json
    assert "data" in resp.json


def test_analytics_data_coverage(authed_client):
    resp = authed_client.get("/api/analytics/data-coverage")
    assert resp.status_code == 200
    assert "labels" in resp.json
