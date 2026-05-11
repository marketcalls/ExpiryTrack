"""Benchmark insert throughput for MarketDataStore.

Generates synthetic 1-minute bars (mimicking Upstox shape) and measures
rows/sec for the new DataFrame-bulk path.
"""
from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.market_data import MarketDataStore  # noqa: E402


def synth(n: int) -> list[list]:
    out = []
    base_ms = 1_700_000_000_000
    for i in range(n):
        out.append([
            f"2026-04-28T{(9 + i // 60) % 24:02d}:{i % 60:02d}:00+05:30",
            100.0 + i * 0.01, 101.0 + i * 0.01,
            99.0 + i * 0.01, 100.5 + i * 0.01,
            1000 + i, 50_000 + i,
        ])
    return out


def main() -> int:
    tmp = Path(tempfile.mkdtemp()) / "bench.duckdb"
    store = MarketDataStore(db_path=tmp)
    contract = {
        "expired_instrument_key": "BENCH_KEY",
        "instrument_key": "NSE_INDEX|Nifty 50",
        "expiry_date": "2026-04-28",
        "contract_type": "CE",
        "strike_price": 24000,
        "trading_symbol": "NIFTY 28APR26 24000 CE",
    }

    for batch in (1_000, 5_000, 20_000, 50_000):
        candles = synth(batch)
        t0 = time.perf_counter()
        n = store.insert_candles(contract, candles)
        elapsed = time.perf_counter() - t0
        rate = n / elapsed if elapsed else float("inf")
        print(f"{batch:>6d} rows  inserted={n}  took={elapsed*1000:7.1f} ms"
              f"  -> {rate:>9,.0f} rows/sec")

    # Verify content
    total = store.sql("SELECT count(*) AS c FROM market_data")["c"].iloc[0]
    print(f"market_data row count after bench: {total}")
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
