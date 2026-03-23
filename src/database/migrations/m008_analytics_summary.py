"""Migration m008: Add analytics_daily_summary pre-aggregation table."""

from typing import Any


def up(conn: Any) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_daily_summary (
            summary_date DATE NOT NULL,
            instrument_key VARCHAR NOT NULL,
            candle_count BIGINT DEFAULT 0,
            total_volume BIGINT DEFAULT 0,
            contract_count INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (summary_date, instrument_key)
        )
    """)
