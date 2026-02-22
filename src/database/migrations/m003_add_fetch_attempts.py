"""Add fetch_attempts and last_attempted_at columns to contracts."""

from typing import Any


def up(conn: Any) -> None:
    columns = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'contracts'"
        ).fetchall()
    ]
    if "fetch_attempts" not in columns:
        conn.execute("ALTER TABLE contracts ADD COLUMN fetch_attempts INTEGER DEFAULT 0")
    if "last_attempted_at" not in columns:
        conn.execute("ALTER TABLE contracts ADD COLUMN last_attempted_at TIMESTAMP")
