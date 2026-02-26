"""Route tests for watchlists blueprint — CRUD + items."""


def test_list_watchlists_requires_auth(client):
    resp = client.get("/api/watchlists")
    assert resp.status_code == 401


def test_create_watchlist(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": "My Watchlist"})
    assert resp.status_code == 200
    data = resp.json
    assert data["success"] is True
    assert "id" in data


def test_create_watchlist_empty_name(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": ""})
    assert resp.status_code == 400


def test_list_watchlists(authed_client):
    authed_client.post("/api/watchlists", json={"name": "Test WL"})
    resp = authed_client.get("/api/watchlists")
    assert resp.status_code == 200
    assert len(resp.json["watchlists"]) >= 1


def test_delete_watchlist(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": "To Delete"})
    wl_id = resp.json["id"]

    resp = authed_client.delete(f"/api/watchlists/{wl_id}")
    assert resp.status_code == 200
    assert resp.json["success"] is True


def test_get_watchlist_items(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": "Items WL"})
    wl_id = resp.json["id"]

    resp = authed_client.get(f"/api/watchlists/{wl_id}")
    assert resp.status_code == 200
    assert "items" in resp.json


def test_add_items_to_watchlist(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": "Items WL"})
    wl_id = resp.json["id"]

    resp = authed_client.post(
        f"/api/watchlists/{wl_id}/items",
        json={"instrument_keys": ["NSE_EQ|FOO", "NSE_EQ|BAR"]},
    )
    assert resp.status_code == 200
    assert resp.json["added"] == 2


def test_add_items_empty_keys(authed_client):
    resp = authed_client.post("/api/watchlists", json={"name": "Empty WL"})
    wl_id = resp.json["id"]

    resp = authed_client.post(f"/api/watchlists/{wl_id}/items", json={"instrument_keys": []})
    assert resp.status_code == 400
