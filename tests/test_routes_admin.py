"""Route tests for admin blueprint — API keys, backup, scheduler."""


def test_api_keys_list_requires_auth(client):
    resp = client.get("/api/api-keys")
    assert resp.status_code == 401


def test_api_keys_create_and_list(authed_client):
    resp = authed_client.post("/api/api-keys", json={"name": "Test Key"})
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert "key" in resp.json

    resp = authed_client.get("/api/api-keys")
    assert resp.status_code == 200
    assert len(resp.json["keys"]) >= 1


def test_api_keys_revoke(authed_client):
    resp = authed_client.post("/api/api-keys", json={"name": "Revoke Test"})
    key_id = resp.json["key"]["id"]

    resp = authed_client.delete(f"/api/api-keys/{key_id}")
    assert resp.status_code == 200
    assert resp.json["success"] is True


def test_backup_create(authed_client):
    resp = authed_client.post("/api/backup/create")
    assert resp.status_code == 200
    assert resp.json["success"] is True
    assert "filename" in resp.json


def test_backup_list(authed_client):
    resp = authed_client.get("/api/backup/list")
    assert resp.status_code == 200
    assert "backups" in resp.json


def test_backup_restore_no_file(authed_client):
    resp = authed_client.post("/api/backup/restore")
    assert resp.status_code == 400
    assert "No file uploaded" in resp.json["error"]


def test_scheduler_status(authed_client):
    resp = authed_client.get("/api/scheduler/status")
    assert resp.status_code == 200
    data = resp.json
    assert "running" in data
    assert "jobs" in data


def test_scheduler_toggle_invalid(authed_client):
    resp = authed_client.post("/api/scheduler/toggle", json={"action": "invalid"})
    assert resp.status_code == 400


def test_scheduler_history(authed_client):
    resp = authed_client.get("/api/scheduler/history")
    assert resp.status_code == 200
    assert "history" in resp.json
