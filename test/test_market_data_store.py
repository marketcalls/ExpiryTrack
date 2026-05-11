"""Smoke test for the DuckDB-backed MarketDataStore.

Run with:  python -m test.test_market_data_store
(uses a temporary DB; does not touch data/market_data.duckdb).
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.market_data import MarketDataStore  # noqa: E402


def main() -> int:
    tmp = Path(tempfile.mkdtemp()) / "test_market_data.duckdb"
    store = MarketDataStore(db_path=tmp)

    # API-shape contract (instrument_key = expired key, underlying_key = underlying).
    nifty_ce = {
        "instrument_key": "NSE_FO|66691|28-04-2026",
        "underlying_key": "NSE_INDEX|Nifty 50",
        "expiry": "2026-04-28",
        "instrument_type": "CE",
        "strike_price": 24000,
        "trading_symbol": "NIFTY 28APR26 24000 CE",
    }
    n = store.insert_candles(
        nifty_ce,
        [
            ["2026-04-28T09:15:00+05:30", 100, 101, 99, 100, 1000, 50000],
            ["2026-04-28T09:20:00+05:30", 104, 106, 103, 105, 3500, 51200],
        ],
    )

    # SQLite-shape row (expired_instrument_key set, instrument_key = underlying).
    nifty_pe = {
        "expired_instrument_key": "NSE_FO|66692|28-04-2026",
        "instrument_key": "NSE_INDEX|Nifty 50",
        "expiry_date": "2026-04-28",
        "contract_type": "PE",
        "strike_price": 24000,
        "trading_symbol": "NIFTY 28APR26 24000 PE",
    }
    n += store.insert_candles(
        nifty_pe,
        [["2026-04-28T09:15:00+05:30", 50, 51, 49, 50, 500, 30000]],
    )

    nifty_fut = {
        "expired_instrument_key": "NSE_FO|66693|28-04-2026",
        "instrument_key": "NSE_INDEX|Nifty 50",
        "expiry_date": "2026-04-28",
        "contract_type": "FUT",
        "strike_price": None,
        "trading_symbol": "NIFTY 28APR26 FUT",
    }
    n += store.insert_candles(
        nifty_fut,
        [["2026-04-28T09:15:00+05:30", 24000, 24050, 23990, 24020, 10000, 500000]],
    )

    assert n == 4, f"expected 4 inserted rows, got {n}"

    # 1m bars
    bars = store.get_bars(openalgo_symbol="NIFTY28APR2624000CE")
    assert len(bars) == 2, bars

    # 5m resample (both 1-min bars fall into the same bucket; expect 1 row)
    bars5 = store.get_bars(openalgo_symbol="NIFTY28APR2624000CE", timeframe="5m")
    assert not bars5.empty, "5m resample empty"

    # scan filters
    scan = store.scan(base_symbol="NIFTY", expiry="2026-04-28", contract_type="CE")
    assert len(scan) == 2, scan

    fut_scan = store.scan(base_symbol="NIFTY", contract_type="FUT")
    assert len(fut_scan) == 1, fut_scan

    # option chain snapshot
    chain = store.get_chain("NIFTY", "2026-04-28")
    assert len(chain) == 3, chain
    assert set(chain["contract_type"]) == {"CE", "PE", "FUT"}

    # summary
    summary = store.get_summary()
    assert summary["total_candles"] == 4
    assert summary["contracts_with_data"] == 3

    # symbols listing
    syms = store.list_symbols()
    assert len(syms) == 3, syms

    # Parquet export
    pq = tmp.parent / "out.parquet"
    store.to_parquet(pq, "SELECT * FROM market_data")
    assert pq.exists() and pq.stat().st_size > 0

    store.close()
    print("MarketDataStore smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
