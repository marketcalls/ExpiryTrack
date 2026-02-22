"""Tests for API v1 blueprint — data, instruments, expiries endpoints."""

from unittest.mock import MagicMock


class TestAPIv1:
    def _setup_api_key(self, tmp_db):
        """Mock verify_api_key to return a valid key."""
        tmp_db.verify_api_key = MagicMock(
            return_value={"name": "test", "is_active": True}
        )

    def test_data_requires_api_key(self, client):
        resp = client.get("/api/v1/data")
        assert resp.status_code == 401
        assert "Missing X-API-Key" in resp.get_json()["error"]

    def test_data_invalid_api_key(self, client, tmp_db):
        tmp_db.verify_api_key = MagicMock(return_value=None)
        resp = client.get("/api/v1/data", headers={"X-API-Key": "bad_key"})
        assert resp.status_code == 401
        assert "Invalid" in resp.get_json()["error"]

    def test_data_empty_json(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v1/data", headers={"X-API-Key": "valid_key"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 0
        assert data["data"] == []
        assert data["limit"] == 1000
        assert data["offset"] == 0

    def test_data_with_filters(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v1/data?instrument_key=NSE_FO|NIFTY&limit=10&offset=0",
            headers={"X-API-Key": "valid_key"},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["limit"] == 10

    def test_data_csv_format(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v1/data?format=csv", headers={"X-API-Key": "valid_key"}
        )
        assert resp.status_code == 200
        assert resp.content_type.startswith("text/csv")

    def test_instruments_list(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        # Insert test instrument
        with tmp_db.get_connection() as conn:
            conn.execute(
                "INSERT INTO instruments (instrument_key, symbol, name, exchange) VALUES (?, ?, ?, ?)",
                ("NSE_FO|NIFTY", "NIFTY", "Nifty 50", "NSE"),
            )
        resp = authed_client.get(
            "/api/v1/instruments", headers={"X-API-Key": "valid_key"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data["instruments"]) == 1
        assert data["instruments"][0]["symbol"] == "NIFTY"

    def test_expiries_list(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        # Insert test contract
        with tmp_db.get_connection() as conn:
            conn.execute(
                "INSERT INTO instruments (instrument_key, symbol) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "NIFTY"),
            )
            conn.execute(
                "INSERT INTO expiries (instrument_key, expiry_date) VALUES (?, ?)",
                ("NSE_FO|NIFTY", "2025-01-30"),
            )
            conn.execute(
                """INSERT INTO contracts
                   (expired_instrument_key, instrument_key, trading_symbol, expiry_date, contract_type)
                   VALUES (?, ?, ?, ?, ?)""",
                ("NSE_FO|NIFTY|25JAN", "NSE_FO|NIFTY", "NIFTY25JANFUT", "2025-01-30", "FUT"),
            )
        resp = authed_client.get(
            "/api/v1/expiries/NSE_FO|NIFTY", headers={"X-API-Key": "valid_key"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data["expiries"]) == 1

    def test_data_limit_capped_at_50000(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v1/data?limit=100000", headers={"X-API-Key": "valid_key"}
        )
        assert resp.status_code == 200
        assert resp.get_json()["limit"] == 50000
