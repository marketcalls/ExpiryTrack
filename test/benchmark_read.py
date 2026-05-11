"""Benchmark read latency for typical MarketDataStore queries.

Loads ~200k bars across 50 synthetic contracts, then times:
  * point lookups (single contract)
  * scans (by base_symbol + expiry)
  * resampled aggregations (5m / 1d views)
  * cross-DB joins to SQLite (skipped if no meta DB attached)
"""
from __future__ import annotations

import statistics
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.market_data import MarketDataStore  # noqa: E402


def time_n(fn, n: int = 5) -> tuple[float, float]:
    """Return (median_ms, min_ms) over n runs."""
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000)
    return statistics.median(samples), min(samples)


def main() -> int:
    tmp = Path(tempfile.mkdtemp()) / "bench.duckdb"
    store = MarketDataStore(db_path=tmp, sqlite_path=tmp.parent / "noop.db")

    # Synthesize 50 contracts x ~4000 candles each.
    n_contracts = 50
    bars_per = 4000
    print(f"Loading {n_contracts * bars_per:,} bars across {n_contracts} contracts...")

    base_ts = "2026-04-28T09:15:00+05:30"
    import pandas as pd
    ts_index = pd.date_range("2026-01-28 09:15", periods=bars_per, freq="1min", tz="Asia/Kolkata")
    ts_iso = ts_index.strftime("%Y-%m-%dT%H:%M:%S+05:30").tolist()

    t0 = time.perf_counter()
    for i in range(n_contracts):
        ctype = "CE" if i % 3 == 0 else ("PE" if i % 3 == 1 else "FUT")
        strike = 24000 + (i * 50)
        contract = {
            "expired_instrument_key": f"BENCH|{i}|28-04-2026",
            "instrument_key": "NSE_INDEX|Nifty 50",
            "expiry_date": "2026-04-28",
            "contract_type": ctype,
            "strike_price": None if ctype == "FUT" else strike,
            "trading_symbol": f"NIFTY 28APR26 {strike} {ctype}",
        }
        rows = [
            [ts_iso[j], 100 + j * 0.01, 101 + j * 0.01, 99 + j * 0.01,
             100.5 + j * 0.01, 1000 + j, 50_000 + j]
            for j in range(bars_per)
        ]
        store.insert_candles(contract, rows)
    print(f"Insert: {(time.perf_counter() - t0):.2f}s")

    # Cluster the table to maximize zone-map efficiency.
    print("Compacting (re-clustering)...")
    t0 = time.perf_counter()
    store.compact()
    print(f"Compact: {(time.perf_counter() - t0):.2f}s")

    # ---- Read benchmarks ------------------------------------------------
    queries = {
        "Point lookup by openalgo_symbol":
            lambda: store.get_bars(openalgo_symbol="NIFTY28APR2624500CE"),
        "scan(base_symbol='NIFTY', expiry='2026-04-28', contract_type='CE')":
            lambda: store.scan(base_symbol="NIFTY", expiry="2026-04-28", contract_type="CE"),
        "scan(strike_lt=24500, contract_type='CE')":
            lambda: store.scan(base_symbol="NIFTY", contract_type="CE", strike_lt=24500),
        "chain('NIFTY', '2026-04-28')":
            lambda: store.get_chain("NIFTY", "2026-04-28"),
        "Daily roll-up (1d view)":
            lambda: store.get_bars(openalgo_symbol="NIFTY28APR2624500CE", timeframe="1d"),
        "Top-volume aggregate":
            lambda: store.sql(
                "SELECT openalgo_symbol, sum(volume) v FROM market_data "
                "GROUP BY 1 ORDER BY v DESC LIMIT 10"
            ),
        "Last close per contract":
            lambda: store.sql(
                "SELECT openalgo_symbol, last(close ORDER BY ts) AS close "
                "FROM market_data GROUP BY 1"
            ),
    }

    print(f"\n{'Query':60s}  {'median ms':>10s}  {'min ms':>10s}")
    print("-" * 86)
    for name, fn in queries.items():
        med, mn = time_n(fn, n=5)
        print(f"{name:60s}  {med:10.2f}  {mn:10.2f}")

    summary = store.get_summary()
    print(f"\nTotal bars in store: {summary['total_candles']:,}")
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
