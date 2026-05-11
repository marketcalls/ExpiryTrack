"""Quick check that the backtest helper API returns DataFrames from
whatever data is currently in market_data.duckdb.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.backtest import BacktestData, bars, chain, scan  # noqa: E402
from src.database.market_data import MarketDataStore  # noqa: E402


def main() -> int:
    store = MarketDataStore.instance()
    summary = store.get_summary()
    print("Store summary:", summary)
    if not summary["total_candles"]:
        print("No data; skipping.")
        return 0

    sample = store.list_symbols(limit=1)
    sym = sample["openalgo_symbol"].iloc[0]
    base = sample["base_symbol"].iloc[0]
    exp = str(sample["expiry_date"].iloc[0])
    print(f"Probing {sym}  base={base}  expiry={exp}")

    df = bars(sym, timeframe="5m")
    print(f"bars(5m) rows: {len(df)}")
    assert len(df) > 0

    ch = chain(base, exp)
    print(f"chain rows: {len(ch)}")
    assert len(ch) > 0

    sc = scan(base_symbol=base, expiry=exp)
    print(f"scan rows: {len(sc)}")
    assert len(sc) > 0

    bt = BacktestData(base, exp, timeframe="15m")
    it = bt.iter_bars()
    first = next(iter(it), None)
    if first is not None:
        ts, snap = first
        print(f"iter_bars first ts={ts}, rows={len(snap)}")

    print("backtest API smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
