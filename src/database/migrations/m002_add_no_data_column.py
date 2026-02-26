"""Add no_data column to contracts and fix stuck contracts."""

from typing import Any


def up(conn: Any) -> None:
    columns = [
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'contracts'"
        ).fetchall()
    ]
    if "no_data" not in columns:
        conn.execute("ALTER TABLE contracts ADD COLUMN no_data BOOLEAN DEFAULT FALSE")
        # Mark expired contracts with no historical data as no_data
        conn.execute("""
            UPDATE contracts
            SET data_fetched = TRUE, no_data = TRUE
            WHERE data_fetched = FALSE
              AND expiry_date < CURRENT_DATE
              AND NOT EXISTS (
                  SELECT 1 FROM historical_data h
                  WHERE h.expired_instrument_key = contracts.expired_instrument_key
              )
        """)
