"""Add openalgo_symbol column and index to contracts table."""

from typing import Any


def up(conn: Any) -> None:
    columns = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'contracts'"
        ).fetchall()
    ]
    if "openalgo_symbol" not in columns:
        conn.execute("ALTER TABLE contracts ADD COLUMN openalgo_symbol TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_openalgo_symbol ON contracts(openalgo_symbol)")
