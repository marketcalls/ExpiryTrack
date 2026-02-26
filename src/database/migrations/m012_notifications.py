"""Migration m012: Add notifications table for in-app notification system."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS notifications_id_seq START 1
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY DEFAULT nextval('notifications_id_seq'),
            type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            message TEXT,
            read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def down(conn: Any) -> None:
    conn.execute("DROP TABLE IF EXISTS notifications")
    conn.execute("DROP SEQUENCE IF EXISTS notifications_id_seq")
