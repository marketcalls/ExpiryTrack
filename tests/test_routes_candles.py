"""Route tests for candles blueprint — collect, status, tasks, analytics, data."""

def test_candles_collect_requires_auth(client):
    resp = client.post("/api/candles/collect", json={"instrument_keys": ["NSE_EQ|Nifty"]})
    assert resp.status_code == 401


def test_candles_collect_missing_body(authed_client):
    resp = authed_client.post("/api/candles/collect", json={})
    assert resp.status_code == 400


def test_candles_collect_missing_keys(authed_client):
    resp = authed_client.post("/api/candles/collect", json={"instrument_keys": []})
    assert resp.status_code == 400


def test_candles_collect_invalid_interval(authed_client):
    resp = authed_client.post(
        "/api/candles/collect",
        json={"instrument_keys": ["NSE_EQ|FOO"], "interval": "2minute"},
    )
    assert resp.status_code == 400
    assert "Invalid interval" in resp.json["error"]


def test_candles_collect_start(authed_client):
    """Start candle collection — returns task_id."""
    resp = authed_client.post(
        "/api/candles/collect",
        json={"instrument_keys": ["NSE_EQ|FOO"], "interval": "1day"},
    )
    assert resp.status_code == 200
    data = resp.json
    assert "task_id" in data
    assert data["status"] == "started"


def test_candles_status_not_found(authed_client):
    resp = authed_client.get("/api/candles/status/nonexistent-id")
    assert resp.status_code == 404


def test_candles_tasks_list(authed_client):
    resp = authed_client.get("/api/candles/tasks")
    assert resp.status_code == 200
    assert "tasks" in resp.json


def test_candles_analytics(authed_client, tmp_db):
    resp = authed_client.get("/api/candles/analytics")
    assert resp.status_code == 200
    data = resp.json
    assert "total_candles" in data


def test_candles_data_invalid_interval(authed_client):
    resp = authed_client.get("/api/candles/data/NSE_EQ|FOO?interval=2minute")
    assert resp.status_code == 400
