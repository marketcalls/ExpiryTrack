"""
Data Exporter for ExpiryTrack.

Exports historical OHLCV+OI data and OpenAlgo metadata in CSV / JSON / ZIP
formats. Backed by DuckDB: the bulk extraction is a single SQL query against
the market_data table (joined with SQLite `contracts` for metadata fields
that we did not denormalize, when needed).
"""
from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ..config import config
from ..database.manager import DatabaseManager
from ..database.market_data import MarketDataStore

logger = logging.getLogger(__name__)


# Columns we surface in user-facing exports.
EXPORT_COLUMNS = [
    "openalgo_symbol",
    "date",
    "time",
    "instrument",
    "expiry",
    "strike",
    "option_type",
    "trading_symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
]


class DataExporter:
    """Export collected data using DuckDB-native bulk queries."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db_manager = db_manager or DatabaseManager()
        self.store: MarketDataStore = self.db_manager.market_data
        self.export_dir: Path = config.EXPORTS_DIR
        self.export_dir.mkdir(exist_ok=True, parents=True)

    # ------------------------------------------------------------------
    # Public API used by the Flask routes.
    # ------------------------------------------------------------------
    def get_available_expiries(self, instruments: List[str]) -> Dict[str, List[str]]:
        """For each instrument key, return the set of expiry dates we have
        contracts for."""
        result: Dict[str, List[str]] = {}
        for instr in instruments:
            result[instr] = sorted(
                self.db_manager.get_expiries_for_instrument(instr),
                reverse=True,
            )
        return result

    def export_to_csv(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
        task_id: str,
    ) -> str:
        df = self._materialize(instruments, expiries, options)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        instr_name = self._friendly_name(instruments)
        base = f"ExpiryTrack_{instr_name}_{timestamp}"
        if options.get("include_openalgo", True):
            base = "OpenAlgo_" + base
        path = self.export_dir / f"{base}.csv"
        self._write_csv(df, path, options)
        logger.info(f"Exported {len(df)} rows to {path}")
        return str(path)

    def export_to_json(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
        task_id: str,
    ) -> str:
        df = self._materialize(instruments, expiries, options)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"ExpiryTrack_{timestamp}"
        if options.get("include_openalgo", True):
            base = "OpenAlgo_" + base
        path = self.export_dir / f"{base}.json"

        payload: Dict[str, Any] = {
            "metadata": {
                "exported_at": datetime.now().isoformat(),
                "instruments": instruments,
                "format": "OpenAlgo" if options.get("include_openalgo") else "Standard",
                "version": "2.0",
                "row_count": int(len(df)),
            },
            "data": {},
        }

        if not df.empty:
            for instr in instruments:
                instr_name = instr.split("|")[-1].replace(" ", "_")
                sub_instr = df[df["instrument_key"] == instr]
                payload["data"][instr_name] = {}
                for exp, sub_exp in sub_instr.groupby("expiry"):
                    contracts: List[Dict[str, Any]] = []
                    for sym, sub_sym in sub_exp.groupby("openalgo_symbol"):
                        head = sub_sym.iloc[0]
                        contracts.append({
                            "openalgo_symbol": sym,
                            "trading_symbol": head.get("trading_symbol", ""),
                            "strike": head.get("strike"),
                            "option_type": head.get("option_type"),
                            "historical_data": [
                                {
                                    "date": r["date"],
                                    "time": r["time"],
                                    "timestamp": r["timestamp"],
                                    "open": r["open"],
                                    "high": r["high"],
                                    "low": r["low"],
                                    "close": r["close"],
                                    "volume": r["volume"],
                                    "oi": r["oi"],
                                }
                                for _, r in sub_sym.iterrows()
                            ],
                        })
                    payload["data"][instr_name][str(exp)] = contracts

        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        logger.info(f"Exported data to {path}")
        return str(path)

    def export_to_zip(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
        task_id: str,
    ) -> str:
        df = self._materialize(instruments, expiries, options)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"ExpiryTrack_{timestamp}"
        if options.get("include_openalgo", True):
            base = "OpenAlgo_" + base
        path = self.export_dir / f"{base}.zip"

        separate_files = bool(options.get("separate_files", False))
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            if df.empty:
                # at least produce a header-only file so the UI doesn't error
                zf.writestr("empty.csv", self._csv_string(df, options))
            elif separate_files:
                for (instr_key, exp, ct), sub in df.groupby(
                    ["instrument_key", "expiry", "option_type"]
                ):
                    instr_name = instr_key.split("|")[-1].replace(" ", "_")
                    name = f"{instr_name}_{exp}_{ct}.csv"
                    zf.writestr(name, self._csv_string(sub, options))
            else:
                for (instr_key, exp), sub in df.groupby(
                    ["instrument_key", "expiry"]
                ):
                    instr_name = instr_key.split("|")[-1].replace(" ", "_")
                    name = f"{instr_name}_{exp}.csv"
                    zf.writestr(name, self._csv_string(sub, options))

        logger.info(f"Created ZIP archive: {path}")
        return str(path)

    def export_to_parquet(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
        task_id: str,
    ) -> str:
        """Direct DuckDB COPY ... TO ... PARQUET — fastest path for analytics."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        instr_name = self._friendly_name(instruments)
        base = f"ExpiryTrack_{instr_name}_{timestamp}"
        if options.get("include_openalgo", True):
            base = "OpenAlgo_" + base
        path = self.export_dir / f"{base}.parquet"

        query, params = self._build_query(instruments, expiries, options)
        self.store.to_parquet(path, query, params)
        logger.info(f"Wrote Parquet export: {path}")
        return str(path)

    # ------------------------------------------------------------------
    # Internals.
    # ------------------------------------------------------------------
    def _materialize(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
    ) -> pd.DataFrame:
        """Run the bulk query and shape the result into the export columns."""
        query, params = self._build_query(instruments, expiries, options)
        df = self.store.sql(query, params)
        if df.empty:
            return pd.DataFrame(columns=EXPORT_COLUMNS)

        # Convert ts to date/time/timestamp columns expected by the UI.
        ts = pd.to_datetime(df["ts"], utc=False, errors="coerce")
        df["date"] = ts.dt.strftime("%Y-%m-%d")
        df["time"] = ts.dt.strftime("%H:%M:%S")
        df["timestamp"] = df["ts"].astype(str)
        df = df.rename(columns={
            "strike_price": "strike",
            "contract_type": "option_type",
            "expiry_date": "expiry",
        })
        df["instrument"] = df["instrument_key"].map(
            lambda k: k.split("|")[-1].replace(" ", "_") if k else ""
        )
        # Keep `expiry` as plain YYYY-MM-DD for use in filenames and group keys.
        if "expiry" in df.columns:
            df["expiry"] = pd.to_datetime(df["expiry"]).dt.strftime("%Y-%m-%d")

        # Final column selection / ordering.
        cols = list(EXPORT_COLUMNS)
        if not options.get("include_openalgo", True):
            cols.remove("openalgo_symbol")
        if not options.get("include_metadata", True):
            for c in ("instrument", "expiry", "strike", "option_type", "trading_symbol"):
                if c in cols:
                    cols.remove(c)
        # Always keep underlying key for grouping downstream.
        df["instrument_key"] = df["instrument_key"]
        keep = cols + ["instrument_key"]
        present = [c for c in keep if c in df.columns]
        return df[present]

    def _build_query(
        self,
        instruments: List[str],
        expiries: Dict[str, List[str]],
        options: Dict,
    ) -> tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []

        if instruments:
            placeholders = ", ".join(["?"] * len(instruments))
            clauses.append(f"instrument_key IN ({placeholders})")
            params.extend(instruments)

        # Per-instrument expiry filter (or, if `expiries` is empty for an
        # instrument, include all).
        expiry_clauses: List[str] = []
        for instr, exp_list in (expiries or {}).items():
            if not exp_list:
                continue
            ph = ", ".join(["?"] * len(exp_list))
            expiry_clauses.append(f"(instrument_key = ? AND expiry_date IN ({ph}))")
            params.append(instr)
            params.extend(exp_list)
        if expiry_clauses:
            clauses.append("(" + " OR ".join(expiry_clauses) + ")")

        time_range = options.get("time_range", "all")
        if time_range != "all":
            days = {"1d": 1, "7d": 7, "30d": 30, "90d": 90}.get(time_range)
            if days:
                clauses.append("ts >= (expiry_date::TIMESTAMP - INTERVAL ? DAY)")
                params.append(days)

        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        query = f"""
            SELECT openalgo_symbol, base_symbol, instrument_key, expiry_date,
                   contract_type, strike_price, trading_symbol,
                   ts, open, high, low, close, volume, oi
            FROM market_data
            {where}
            ORDER BY expiry_date, openalgo_symbol, ts
        """
        return query, params

    def _csv_string(self, df: pd.DataFrame, options: Dict) -> str:
        buf = io.StringIO()
        cols = [c for c in EXPORT_COLUMNS if c in df.columns]
        df.to_csv(buf, columns=cols, index=False, quoting=csv.QUOTE_MINIMAL)
        return buf.getvalue()

    def _write_csv(self, df: pd.DataFrame, path: Path, options: Dict) -> None:
        cols = [c for c in EXPORT_COLUMNS if c in df.columns]
        if df.empty:
            pd.DataFrame(columns=cols).to_csv(path, index=False)
            logger.warning(f"No data found for export; wrote header-only CSV: {path}")
            return
        df.to_csv(path, columns=cols, index=False, quoting=csv.QUOTE_MINIMAL)

    def _friendly_name(self, instruments: List[str]) -> str:
        if not instruments:
            return "ALL"
        if len(instruments) == 1:
            return instruments[0].split("|")[-1].replace(" ", "_")
        return f"{len(instruments)}_instruments"
