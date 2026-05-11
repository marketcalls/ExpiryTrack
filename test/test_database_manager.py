"""End-to-end test of the DatabaseManager + MarketDataStore split.

Verifies that:
  - SQLite no longer creates a historical_data table
  - Inserting candles via DatabaseManager routes to DuckDB
  - get_summary_stats() reports candle counts from DuckDB
  - get_historical_data() reads back from DuckDB
  - contract.data_fetched flips to TRUE in SQLite
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.manager import DatabaseManager  # noqa: E402
from src.database.market_data import MarketDataStore  # noqa: E402


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    sqlite_path = tmp / "expirytrack.db"
    duck_path = tmp / "market_data.duckdb"

    # Reset the MarketDataStore singleton to point at the temp DuckDB file.
    MarketDataStore._instance = None
    store = MarketDataStore(db_path=duck_path, sqlite_path=sqlite_path)
    MarketDataStore._instance = store

    mgr = DatabaseManager(db_path=sqlite_path)

    # SQLite must not have a historical_data table.
    with sqlite3.connect(str(sqlite_path)) as c:
        rows = c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        names = {r[0] for r in rows}
    assert "historical_data" not in names, names
    print("SQLite tables:", sorted(names))

    # Insert an underlying + expiry + contract via the manager API.
    mgr.insert_instrument({
        "instrument_key": "NSE_INDEX|Nifty 50",
        "symbol": "Nifty 50",
        "segment": "NSE_INDEX",
    })
    mgr.insert_expiries("NSE_INDEX|Nifty 50", ["2026-04-28"])
    mgr.insert_contracts([{
        "instrument_key": "NSE_FO|66691|28-04-2026",  # API shape: this IS the expired_key
        "underlying_key": "NSE_INDEX|Nifty 50",
        "expiry": "2026-04-28",
        "instrument_type": "CE",
        "strike_price": 24000,
        "trading_symbol": "NIFTY 28APR26 24000 CE",
    }])

    # Insert candles via the legacy entry point (looks up the contract row).
    n = mgr.insert_historical_data(
        "NSE_FO|66691|28-04-2026",
        [
            ["2026-04-28T09:15:00+05:30", 100, 101, 99, 100, 1000, 50000],
            ["2026-04-28T09:16:00+05:30", 100.5, 102, 100, 101, 1500, 50100],
        ],
    )
    assert n == 2, n

    # Stats: candle count comes from DuckDB.
    stats = mgr.get_summary_stats()
    print("Stats:", stats)
    assert stats["total_contracts"] == 1
    assert stats["total_candles"] == 2
    assert stats["pending_contracts"] == 0  # flipped after data_fetched=TRUE

    # data_fetched flag flipped in SQLite.
    with sqlite3.connect(str(sqlite_path)) as c:
        c.row_factory = sqlite3.Row
        row = c.execute(
            "SELECT data_fetched FROM contracts WHERE expired_instrument_key = ?",
            ("NSE_FO|66691|28-04-2026",),
        ).fetchone()
    assert row["data_fetched"] in (1, True), row["data_fetched"]

    # Read-back through the legacy API returns the same 2 candles.
    rows = mgr.get_historical_data("NSE_FO|66691|28-04-2026")
    assert len(rows) == 2, rows

    # Count helper.
    assert mgr.get_historical_data_count("NSE_FO|66691|28-04-2026") == 2

    # Direct DuckDB queries via the store.
    df = store.scan(base_symbol="NIFTY", expiry="2026-04-28", contract_type="CE")
    assert len(df) == 2, df

    store.close()
    print("DatabaseManager + MarketDataStore integration test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
