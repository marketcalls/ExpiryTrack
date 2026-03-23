"""Drop FK constraint on historical_data.expired_instrument_key.

DuckDB implements UPDATE as internal DELETE+INSERT, which means you cannot
UPDATE a parent row (contracts) when child rows (historical_data) reference it
via FK — even if only non-key columns are modified.  This makes the common
pattern of inserting candles and then marking the contract as fetched impossible.

The referential integrity is enforced by application logic, so the FK is
removed to work around this DuckDB limitation.

DuckDB does not support ALTER TABLE DROP CONSTRAINT, so we recreate the table.
"""

from typing import Any


def up(conn: Any) -> None:
    # Check if the FK exists — fresh databases won't have it
    fks = conn.execute(
        "SELECT constraint_text FROM duckdb_constraints() "
        "WHERE table_name = 'historical_data' AND constraint_type = 'FOREIGN KEY'"
    ).fetchall()

    if not fks:
        return  # No FK to drop (fresh DB with updated schema)

    # Reduce memory pressure for large tables (38GB+ DBs on 8GB RAM)
    conn.execute("SET threads = 1")
    conn.execute("SET preserve_insertion_order = false")

    # Recreate table without the FK
    conn.execute("""
        CREATE TABLE historical_data_new (
            expired_instrument_key TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DECIMAL(10,2) NOT NULL,
            high DECIMAL(10,2) NOT NULL,
            low DECIMAL(10,2) NOT NULL,
            close DECIMAL(10,2) NOT NULL,
            volume BIGINT NOT NULL,
            oi BIGINT DEFAULT 0,
            PRIMARY KEY (expired_instrument_key, timestamp)
        )
    """)

    # Copy data — if this fails, abort WITHOUT dropping the original table
    row_count = conn.execute("SELECT COUNT(*) FROM historical_data").fetchone()[0]
    if row_count > 0:
        conn.execute("INSERT INTO historical_data_new SELECT * FROM historical_data")
        # Verify copy succeeded
        new_count = conn.execute(
            "SELECT COUNT(*) FROM historical_data_new"
        ).fetchone()[0]
        if new_count != row_count:
            conn.execute("DROP TABLE historical_data_new")
            raise RuntimeError(
                f"Data copy verification failed: expected {row_count} rows, "
                f"got {new_count}. Original table preserved."
            )

    conn.execute("DROP TABLE historical_data")
    conn.execute("ALTER TABLE historical_data_new RENAME TO historical_data")

    # Recreate indexes (from m007 and initial schema)
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_instrument "
            "ON historical_data(expired_instrument_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_timestamp "
            "ON historical_data(timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_key_ts "
            "ON historical_data(expired_instrument_key, timestamp)"
        )
    except Exception:
        pass  # Indexes are optional optimization

    # Reset settings so read-only connections can open with default config
    conn.execute("RESET threads")
    conn.execute("RESET preserve_insertion_order")
