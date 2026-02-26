"""Migration m011: Add export_history table for tracking export operations."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS export_history_id_seq START 1
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS export_history (
            id INTEGER PRIMARY KEY DEFAULT nextval('export_history_id_seq'),
            format VARCHAR NOT NULL,
            instruments VARCHAR,
            expiries VARCHAR,
            file_path VARCHAR,
            file_size BIGINT DEFAULT 0,
            row_count INTEGER DEFAULT 0,
            contract_types VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def down(conn: Any) -> None:
    conn.execute("DROP TABLE IF EXISTS export_history")
    conn.execute("DROP SEQUENCE IF EXISTS export_history_id_seq")
