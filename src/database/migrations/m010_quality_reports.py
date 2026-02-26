"""Migration m010: Add quality_reports table for scheduled quality check storage."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS quality_reports_id_seq START 1
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quality_reports (
            id INTEGER PRIMARY KEY DEFAULT nextval('quality_reports_id_seq'),
            run_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            instrument_key VARCHAR,
            checks_run INTEGER DEFAULT 0,
            checks_passed INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            warnings INTEGER DEFAULT 0,
            passed BOOLEAN DEFAULT TRUE,
            violations JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def down(conn: Any) -> None:
    conn.execute("DROP TABLE IF EXISTS quality_reports")
    conn.execute("DROP SEQUENCE IF EXISTS quality_reports_id_seq")
