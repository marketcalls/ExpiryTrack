"""Migration m007: Add composite indexes for analytics queries."""

from typing import Any


def up(conn: Any) -> None:
    # Reduce memory pressure for large tables (38GB+ DBs on 8GB RAM)
    conn.execute("SET threads = 1")
    conn.execute("SET preserve_insertion_order = false")
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_timestamp ON historical_data(timestamp)"
        )
    except Exception:
        pass  # Skip if OOM — index is optional optimization
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_historical_key_ts "
            "ON historical_data(expired_instrument_key, timestamp)"
        )
    except Exception:
        pass
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_contracts_fetched "
        "ON contracts(instrument_key, expiry_date, data_fetched)"
    )
    # Reset settings so read-only connections can open with default config
    conn.execute("RESET threads")
    conn.execute("RESET preserve_insertion_order")
