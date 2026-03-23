"""Tests for API v2 blueprint — cursor pagination, field selection, instruments, expiries, tasks."""

from unittest.mock import MagicMock

from src.api.pagination import decode_cursor, encode_cursor


class TestCursorPagination:
    def test_encode_decode_roundtrip(self):
        data = {"timestamp": "2025-01-15 09:15:00", "key": "NSE_FO|NIFTY|25JAN"}
        cursor = encode_cursor(data)
        decoded = decode_cursor(cursor)
        assert decoded == data

    def test_decode_invalid_cursor(self):
        assert decode_cursor("not_valid_base64!!!") is None

    def test_decode_empty_string(self):
        assert decode_cursor("") is None

    def test_encode_with_special_chars(self):
        data = {"key": "NSE_FO|NIFTY 50|25JAN", "timestamp": "2025-01-15T09:15:00"}
        cursor = encode_cursor(data)
        decoded = decode_cursor(cursor)
        assert decoded["key"] == "NSE_FO|NIFTY 50|25JAN"


class TestAPIv2:
    def _setup_api_key(self, tmp_db):
        """Mock verify_api_key to return a valid key."""
        tmp_db.verify_api_key = MagicMock(
            return_value={"name": "test", "is_active": True}
        )

    def test_data_requires_api_key(self, client):
        resp = client.get("/api/v2/data")
        assert resp.status_code == 401

    def test_data_empty(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get("/api/v2/data", headers={"X-API-Key": "valid"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["data"] == []
        assert data["has_more"] is False
        assert data["count"] == 0

    def test_data_with_field_selection(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v2/data?fields=timestamp,close,volume",
            headers={"X-API-Key": "valid"},
        )
        assert resp.status_code == 200

    def test_data_invalid_fields(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v2/data?fields=nonexistent",
            headers={"X-API-Key": "valid"},
        )
        assert resp.status_code == 400
        assert "No valid fields" in resp.get_json()["error"]

    def test_data_invalid_cursor(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v2/data?cursor=invalid!!!",
            headers={"X-API-Key": "valid"},
        )
        assert resp.status_code == 400

    def test_data_with_filters(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        resp = authed_client.get(
            "/api/v2/data?instrument_key=NSE_FO|NIFTY&limit=10",
            headers={"X-API-Key": "valid"},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 0

    def test_data_cursor_pagination(self, authed_client, tmp_db):
        """Insert data and verify cursor pagination works."""
        self._setup_api_key(tmp_db)

        # Insert test data
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
            for i in range(5):
                conn.execute(
                    """INSERT INTO historical_data
                       (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        "NSE_FO|NIFTY|25JAN",
                        f"2025-01-15 09:{15 + i}:00",
                        100 + i, 105 + i, 99 + i, 103 + i, 1000 + i * 100, 500,
                    ),
                )

        # First page — limit 2
        resp = authed_client.get(
            "/api/v2/data?limit=2", headers={"X-API-Key": "valid"}
        )
        assert resp.status_code == 200
        page1 = resp.get_json()
        assert page1["count"] == 2
        assert page1["has_more"] is True
        assert "next_cursor" in page1

        # Second page using cursor
        cursor = page1["next_cursor"]
        resp2 = authed_client.get(
            f"/api/v2/data?limit=2&cursor={cursor}",
            headers={"X-API-Key": "valid"},
        )
        assert resp2.status_code == 200
        page2 = resp2.get_json()
        assert page2["count"] == 2
        assert page2["has_more"] is True

        # Verify no overlap between pages
        ts1 = {r["timestamp"] for r in page1["data"]}
        ts2 = {r["timestamp"] for r in page2["data"]}
        assert ts1.isdisjoint(ts2)

    def test_instruments_list(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        with tmp_db.get_connection() as conn:
            conn.execute(
                "INSERT INTO instruments (instrument_key, symbol, name, exchange) VALUES (?, ?, ?, ?)",
                ("NSE_FO|NIFTY", "NIFTY", "Nifty 50", "NSE"),
            )
        resp = authed_client.get(
            "/api/v2/instruments", headers={"X-API-Key": "valid"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 1
        assert data["data"][0]["symbol"] == "NIFTY"
        assert "contract_count" in data["data"][0]
        assert "latest_expiry" in data["data"][0]

    def test_expiries_list(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
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
            "/api/v2/expiries/NSE_FO|NIFTY", headers={"X-API-Key": "valid"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 1
        assert data["data"][0]["futures"] == 1

    def test_tasks_list(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        tmp_db.tasks_repo.create_task("api-t1", "collection")
        tmp_db.tasks_repo.create_task("api-t2", "export")

        resp = authed_client.get(
            "/api/v2/tasks", headers={"X-API-Key": "valid"}
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 2

    def test_tasks_filter_by_type(self, authed_client, tmp_db):
        self._setup_api_key(tmp_db)
        tmp_db.tasks_repo.create_task("ft-1", "collection")
        tmp_db.tasks_repo.create_task("ft-2", "export")

        resp = authed_client.get(
            "/api/v2/tasks?type=export", headers={"X-API-Key": "valid"}
        )
        data = resp.get_json()
        assert data["count"] == 1
