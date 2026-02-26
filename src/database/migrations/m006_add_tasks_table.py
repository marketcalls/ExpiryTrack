"""Migration m006: Add tasks table for persistent task tracking."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("CREATE SEQUENCE IF NOT EXISTS tasks_id_seq")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY DEFAULT nextval('tasks_id_seq'),
            task_id VARCHAR UNIQUE NOT NULL,
            task_type VARCHAR NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'pending',
            progress INTEGER DEFAULT 0,
            params JSON,
            result JSON,
            error_message TEXT,
            status_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at)")
