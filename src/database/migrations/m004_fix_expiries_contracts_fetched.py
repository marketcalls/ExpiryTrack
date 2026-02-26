"""Fix expiries contracts_fetched flag for expiries that already have contracts."""

from typing import Any


def up(conn: Any) -> None:
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name IN ('expiries', 'contracts')"
        ).fetchall()
    ]
    if "expiries" in tables and "contracts" in tables:
        conn.execute("""
            UPDATE expiries
            SET contracts_fetched = TRUE
            WHERE contracts_fetched = FALSE
              AND EXISTS (
                  SELECT 1 FROM contracts c
                  WHERE c.instrument_key = expiries.instrument_key
                    AND c.expiry_date = expiries.expiry_date
              )
        """)
