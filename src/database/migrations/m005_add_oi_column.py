"""Add oi column to historical_data table."""

from typing import Any


def up(conn: Any) -> None:
    columns = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'historical_data'"
        ).fetchall()
    ]
    if "oi" not in columns and "open_interest" not in columns:
        conn.execute("ALTER TABLE historical_data ADD COLUMN oi BIGINT DEFAULT 0")
