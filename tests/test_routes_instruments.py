"""Route tests for instruments blueprint — CRUD, FO import, master search."""

def test_instruments_list_public(client):
    """Instruments list is public (no auth required)."""
    resp = client.get("/api/instruments")
    assert resp.status_code == 200
    assert "instruments" in resp.json


def test_instruments_add(authed_client):
    resp = authed_client.post(
        "/api/instruments",
        json={"instrument_key": "NSE_INDEX|Test", "symbol": "TEST", "category": "Index"},
    )
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert "id" in resp.json


def test_instruments_add_missing_fields(authed_client):
    resp = authed_client.post("/api/instruments", json={"symbol": "TEST"})
    assert resp.status_code == 400


def test_instruments_add_duplicate(authed_client):
    authed_client.post(
        "/api/instruments",
        json={"instrument_key": "NSE_INDEX|Dup", "symbol": "DUP"},
    )
    resp = authed_client.post(
        "/api/instruments",
        json={"instrument_key": "NSE_INDEX|Dup", "symbol": "DUP"},
    )
    assert resp.status_code == 409


def test_instruments_toggle(authed_client):
    resp = authed_client.post(
        "/api/instruments",
        json={"instrument_key": "NSE_INDEX|Toggle", "symbol": "TOG"},
    )
    inst_id = resp.json["id"]

    resp = authed_client.patch(f"/api/instruments/{inst_id}", json={"is_active": False})
    assert resp.status_code == 200
    assert resp.json["success"] is True


def test_instruments_delete(authed_client):
    resp = authed_client.post(
        "/api/instruments",
        json={"instrument_key": "NSE_INDEX|Del", "symbol": "DEL"},
    )
    inst_id = resp.json["id"]

    resp = authed_client.delete(f"/api/instruments/{inst_id}")
    assert resp.status_code == 200
    assert resp.json["success"] is True


def test_instruments_fo_available_requires_auth(client):
    resp = client.get("/api/instruments/fo-available")
    assert resp.status_code == 401


def test_instruments_fo_available_invalid_category(authed_client):
    resp = authed_client.get("/api/instruments/fo-available?category=invalid")
    assert resp.status_code == 400
