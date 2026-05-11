"""Live end-to-end test of the new DuckDB-backed download path.

Pre-requisites:
  - User is already authenticated (a valid token sits in SQLite `credentials`).
  - The collector code can reach the Upstox API.

What it does:
  1. Reads one currently-pending contract from SQLite (data_fetched = FALSE).
  2. Fetches a small slice of historical bars via UpstoxAPIClient.
  3. Writes them through DatabaseManager.insert_historical_data().
  4. Verifies the candles landed in DuckDB and that data_fetched flipped.

Run with:
    python -m test.test_live_download
"""
from __future__ import annotations

import asyncio
import sys
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.api.client import UpstoxAPIClient  # noqa: E402
from src.auth.manager import AuthManager  # noqa: E402
from src.config import config  # noqa: E402
from src.database.manager import DatabaseManager  # noqa: E402
from src.database.market_data import MarketDataStore  # noqa: E402


async def run() -> int:
    auth = AuthManager()
    if not auth.is_token_valid():
        print("Auth token is missing/expired. Please log in via the web UI.")
        return 1

    mgr = DatabaseManager()
    store = mgr.market_data

    # Pick the first pending contract (or any contract if none pending).
    with sqlite3.connect(str(config.DB_PATH)) as c:
        c.row_factory = sqlite3.Row
        row = c.execute("""
            SELECT * FROM contracts
            WHERE expiry_date IS NOT NULL
            ORDER BY data_fetched ASC, expiry_date DESC
            LIMIT 1
        """).fetchone()
    if row is None:
        print("No contracts in SQLite; run a collection first.")
        return 1
    contract = dict(row)
    expired_key = contract["expired_instrument_key"]
    expiry = contract["expiry_date"]
    print(f"Probing contract: {contract.get('trading_symbol')!r}  ({expired_key})")
    print(f"  openalgo_symbol = {contract.get('openalgo_symbol')!r}")

    # 7-day window ending at the expiry (Upstox returns the last available
    # data <= today even for expired contracts).
    end_date = expiry
    start_date = (datetime.strptime(expiry, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"  fetching {start_date} -> {end_date} (1minute)")

    client = UpstoxAPIClient(auth)
    async with client:
        candles = await client.get_historical_data(
            expired_key, start_date, end_date, "1minute"
        )

    if not candles:
        print("Upstox returned 0 candles for this contract / window.")
        return 1
    print(f"  fetched {len(candles)} candles, sample: {candles[0]}")

    # Snapshot DuckDB counts before/after.
    before = store.sql(
        "SELECT count(*) AS c FROM market_data WHERE expired_instrument_key = ?",
        [expired_key],
    )["c"].iloc[0]

    inserted = mgr.insert_historical_data(expired_key, candles)

    after = store.sql(
        "SELECT count(*) AS c FROM market_data WHERE expired_instrument_key = ?",
        [expired_key],
    )["c"].iloc[0]

    # Confirm data_fetched flipped.
    with sqlite3.connect(str(config.DB_PATH)) as c:
        c.row_factory = sqlite3.Row
        row2 = c.execute(
            "SELECT data_fetched FROM contracts WHERE expired_instrument_key = ?",
            (expired_key,),
        ).fetchone()

    print(f"Inserted: {inserted}, before={before}, after={after}, data_fetched={row2['data_fetched']}")

    # Show a couple of rows back.
    df = store.sql(
        """SELECT openalgo_symbol, ts, open, high, low, close, volume, oi
           FROM market_data WHERE expired_instrument_key = ?
           ORDER BY ts DESC LIMIT 3""",
        [expired_key],
    )
    print(df.to_string(index=False))

    summary = store.get_summary()
    print("Store summary:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
