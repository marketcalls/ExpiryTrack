"""Credential and API key repository."""

import logging
from typing import Any

from ..manager import DictCursor, dict_from_row
from .base import BaseRepository

logger = logging.getLogger(__name__)


class CredentialRepository(BaseRepository):
    def save_credentials(self, api_key: str, api_secret: str, redirect_uri: str | None = None) -> bool:
        from ...config import config
        from ...utils.encryption import encryption

        default_uri = config.UPSTOX_REDIRECT_URI

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            encrypted_key = encryption.encrypt(api_key)
            encrypted_secret = encryption.encrypt(api_secret)
            cursor.execute("SELECT COUNT(*) FROM credentials")
            exists = cursor.fetchone()[0] > 0
            if exists:
                cursor.execute(
                    """
                    UPDATE credentials
                    SET api_key = ?, api_secret = ?, redirect_uri = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """,
                    (encrypted_key, encrypted_secret, redirect_uri or default_uri),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO credentials (id, api_key, api_secret, redirect_uri)
                    VALUES (1, ?, ?, ?)
                """,
                    (encrypted_key, encrypted_secret, redirect_uri or default_uri),
                )
            return True

    def get_credentials(self) -> dict | None:
        from ...utils.encryption import encryption

        with self.get_read_connection() as conn:
            cursor = DictCursor(conn.cursor())
            cursor.execute("SELECT * FROM credentials WHERE id = 1")
            row = cursor.fetchone()
            if row:
                return {
                    "api_key": encryption.decrypt(row["api_key"]),
                    "api_secret": encryption.decrypt(row["api_secret"]),
                    "redirect_uri": row["redirect_uri"],
                    "access_token": encryption.decrypt(row["access_token"]) if row["access_token"] else None,
                    "token_expiry": row["token_expiry"],
                }
            return None

    def save_token(self, access_token: str, expiry: float) -> bool:
        from ...utils.encryption import encryption

        with self.get_connection() as conn:
            cursor = DictCursor(conn.cursor())
            encrypted_token = encryption.encrypt(access_token)
            cursor.execute(
                """
                UPDATE credentials
                SET access_token = ?, token_expiry = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """,
                (encrypted_token, expiry),
            )
            return True

    # ── API Keys ──

    def _ensure_api_keys_table(self, conn: Any) -> None:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY DEFAULT nextval('default_instruments_id_seq'),
                key_name TEXT NOT NULL,
                api_key TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                rate_limit_per_hour INTEGER DEFAULT 1000
            )
        """)

    def create_api_key(self, key_name: str) -> dict | None:
        import secrets as _secrets

        api_key = "expt_" + _secrets.token_urlsafe(32)
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            result = conn.execute(
                """
                INSERT INTO api_keys (key_name, api_key)
                VALUES (?, ?)
                RETURNING id, key_name, api_key, created_at
            """,
                (key_name, api_key),
            )
            row = result.fetchone()
            conn.commit()
            if row:
                return {"id": row[0], "key_name": row[1], "api_key": row[2], "created_at": str(row[3])}
            return None

    def verify_api_key(self, api_key: str) -> Any:
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            cursor = DictCursor(conn.cursor())
            cursor.execute(
                """
                SELECT id, key_name, rate_limit_per_hour FROM api_keys
                WHERE api_key = ? AND is_active = TRUE
            """,
                (api_key,),
            )
            row = cursor.fetchone()
            if row:
                conn.execute("UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE api_key = ?", (api_key,))
                conn.commit()
                return dict_from_row(row)
            return None

    def list_api_keys(self) -> list[dict]:
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            cursor = DictCursor(conn.cursor())
            cursor.execute("""
                SELECT id, key_name, api_key, created_at, last_used_at, is_active, rate_limit_per_hour
                FROM api_keys ORDER BY created_at DESC
            """)
            keys = []
            for row in cursor.fetchall():
                d = dict_from_row(row)
                full_key = d["api_key"]
                d["api_key_masked"] = full_key[:9] + "..." + full_key[-4:]
                keys.append(d)
            return keys

    def revoke_api_key(self, key_id: int) -> bool:
        with self.get_connection() as conn:
            self._ensure_api_keys_table(conn)
            conn.execute("UPDATE api_keys SET is_active = FALSE WHERE id = ?", (key_id,))
            conn.commit()
            return True
