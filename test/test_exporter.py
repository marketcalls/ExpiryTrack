"""Verify the DuckDB-backed DataExporter produces valid CSV/JSON/ZIP.

Uses whatever data the running instance has in market_data.duckdb.
"""
from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.manager import DatabaseManager  # noqa: E402
from src.export.exporter import DataExporter  # noqa: E402


def main() -> int:
    mgr = DatabaseManager()
    exporter = DataExporter(mgr)

    summary = mgr.market_data.get_summary()
    print("Store summary:", summary)
    if not summary["total_candles"]:
        print("No candles in DuckDB; run a collection first.")
        return 1

    # Pick whichever instrument we have data for.
    instruments = mgr.market_data.sql(
        "SELECT DISTINCT instrument_key FROM market_data"
    )["instrument_key"].tolist()
    print("Instruments with data:", instruments)
    expiries = exporter.get_available_expiries(instruments)
    print("Available expiries:", expiries)

    options = {"include_openalgo": True, "include_metadata": True, "time_range": "all"}

    csv_path = exporter.export_to_csv(instruments, expiries, options, "t")
    assert Path(csv_path).exists() and Path(csv_path).stat().st_size > 0
    print(f"CSV  -> {csv_path}  ({Path(csv_path).stat().st_size} bytes)")

    json_path = exporter.export_to_json(instruments, expiries, options, "t")
    assert Path(json_path).exists()
    with open(json_path) as f:
        payload = json.load(f)
    assert "metadata" in payload and "data" in payload
    print(f"JSON -> {json_path}  (rows={payload['metadata']['row_count']})")

    zip_path = exporter.export_to_zip(instruments, expiries, options, "t")
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    print(f"ZIP  -> {zip_path}  files={names}")

    pq_path = exporter.export_to_parquet(instruments, expiries, options, "t")
    assert Path(pq_path).exists() and Path(pq_path).stat().st_size > 0
    print(f"PARQ -> {pq_path}  ({Path(pq_path).stat().st_size} bytes)")

    print("Exporter smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
