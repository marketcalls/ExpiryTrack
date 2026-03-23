"""
Data Importer for ExpiryTrack — External Data Import & Comparison (D15)

Supports importing CSV and Parquet files into the historical_data table,
with column mapping, preview, and validation.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

# Required columns that must be mapped for a valid import
REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}

# Optional columns that can be mapped
OPTIONAL_COLUMNS = {"oi"}

# All valid target columns in historical_data
ALL_TARGET_COLUMNS = {"expired_instrument_key", "timestamp", "open", "high", "low", "close", "volume", "oi"}

# Maximum file size: 100 MB
MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024

# Supported file extensions
SUPPORTED_EXTENSIONS = {".csv", ".parquet"}


class DataImporter:
    """Import external data files into the ExpiryTrack historical_data table."""

    def __init__(self, db_manager: DatabaseManager | None = None) -> None:
        if db_manager is None:
            from ..database.manager import DatabaseManager
            db_manager = DatabaseManager()
        self.db_manager = db_manager

    def preview_file(self, file_path: str, file_type: str = "csv") -> dict[str, Any]:
        """Read the first 10 rows of a file and auto-detect columns.

        Parameters
        ----------
        file_path : str
            Path to the uploaded file.
        file_type : str
            One of 'csv' or 'parquet'.

        Returns
        -------
        dict with keys:
            - columns: list of detected column names
            - rows: list of dicts (first 10 rows)
            - total_rows: total number of rows in the file
            - suggested_mapping: dict mapping detected columns to target schema columns
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = path.stat().st_size
        if file_size > MAX_FILE_SIZE_BYTES:
            raise ValueError(f"File size ({file_size / 1024 / 1024:.1f} MB) exceeds maximum allowed (100 MB)")

        df = self._read_file(file_path, file_type)

        if df.empty:
            return {
                "columns": [],
                "rows": [],
                "total_rows": 0,
                "suggested_mapping": {},
            }

        columns = list(df.columns)
        total_rows = len(df)
        preview_df = df.head(10)

        # Convert preview rows to list of dicts, handling NaN
        rows = []
        for _, row in preview_df.iterrows():
            row_dict = {}
            for col in columns:
                val = row[col]
                if pd.isna(val):
                    row_dict[col] = None
                else:
                    row_dict[col] = str(val)
            rows.append(row_dict)

        suggested_mapping = self._suggest_mapping(columns)

        return {
            "columns": columns,
            "rows": rows,
            "total_rows": total_rows,
            "suggested_mapping": suggested_mapping,
        }

    def validate_mapping(self, column_mapping: dict[str, str]) -> dict[str, Any]:
        """Check that the required columns (timestamp, open, high, low, close, volume) are mapped.

        Parameters
        ----------
        column_mapping : dict
            Maps source column names to target schema column names.
            Example: {"Date": "timestamp", "Open Price": "open", ...}

        Returns
        -------
        dict with keys:
            - valid: bool
            - missing: list of missing required column names
            - mapped: list of successfully mapped column names
        """
        mapped_targets = set(column_mapping.values())
        mapped_targets.discard("")  # ignore unmapped columns

        missing = REQUIRED_COLUMNS - mapped_targets
        mapped = list(mapped_targets & ALL_TARGET_COLUMNS)

        return {
            "valid": len(missing) == 0,
            "missing": sorted(missing),
            "mapped": sorted(mapped),
        }

    def import_file(
        self,
        file_path: str,
        column_mapping: dict[str, str],
        instrument_key: str,
        file_type: str = "csv",
    ) -> dict[str, Any]:
        """Bulk import data from a file into historical_data.

        Parameters
        ----------
        file_path : str
            Path to the uploaded file.
        column_mapping : dict
            Maps source column names to target schema column names.
        instrument_key : str
            The expired_instrument_key to assign to all imported rows.
        file_type : str
            One of 'csv' or 'parquet'.

        Returns
        -------
        dict with keys:
            - imported_rows: number of rows inserted
            - instrument_key: the instrument key used
            - duplicates_skipped: number of rows that were duplicates (replaced)
        """
        # Validate mapping first
        validation = self.validate_mapping(column_mapping)
        if not validation["valid"]:
            raise ValueError(f"Invalid column mapping. Missing required columns: {validation['missing']}")

        if not instrument_key or not instrument_key.strip():
            raise ValueError("instrument_key is required")

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        df = self._read_file(file_path, file_type)

        if df.empty:
            return {"imported_rows": 0, "instrument_key": instrument_key, "duplicates_skipped": 0}

        # Rename columns according to the mapping
        rename_map = {src: tgt for src, tgt in column_mapping.items() if tgt and tgt in ALL_TARGET_COLUMNS}
        df = df.rename(columns=rename_map)

        # Keep only the columns we need
        target_cols = list(ALL_TARGET_COLUMNS & set(df.columns))
        df = df[target_cols].copy()

        # Set the instrument key for all rows
        df["expired_instrument_key"] = instrument_key

        # Ensure correct data types
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        if "oi" in df.columns:
            df["oi"] = pd.to_numeric(df["oi"], errors="coerce").fillna(0).astype(int)
        else:
            df["oi"] = 0

        # Drop rows with NaN in required fields
        required_cols = ["expired_instrument_key", "timestamp", "open", "high", "low", "close", "volume"]
        df = df.dropna(subset=required_cols)

        if df.empty:
            return {"imported_rows": 0, "instrument_key": instrument_key, "duplicates_skipped": 0}

        # Reorder columns to match historical_data schema
        df = df[["expired_instrument_key", "timestamp", "open", "high", "low", "close", "volume", "oi"]]

        # Get existing row count for this instrument to estimate duplicates
        existing_count = self.db_manager.historical.get_historical_data_count(instrument_key)

        # Bulk insert using DuckDB DataFrame insertion
        total_rows = len(df)
        with self.db_manager.get_connection() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute("INSERT OR REPLACE INTO historical_data SELECT * FROM df")
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise ValueError(f"Failed to import data: {e}") from e

        # Check new count to estimate duplicates
        new_count = self.db_manager.historical.get_historical_data_count(instrument_key)
        net_new = new_count - existing_count
        duplicates = total_rows - net_new if net_new < total_rows else 0

        logger.info(
            f"Imported {total_rows} rows for {instrument_key} "
            f"({net_new} new, {duplicates} replaced/duplicates)"
        )

        return {
            "imported_rows": total_rows,
            "instrument_key": instrument_key,
            "duplicates_skipped": duplicates,
        }

    def _read_file(self, file_path: str, file_type: str) -> pd.DataFrame:
        """Read a file into a pandas DataFrame."""
        file_type = file_type.lower().strip()
        path = Path(file_path)
        ext = path.suffix.lower()

        if file_type == "csv" or ext == ".csv":
            return pd.read_csv(file_path)
        elif file_type == "parquet" or ext == ".parquet":
            return pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}. Supported: csv, parquet")

    def _suggest_mapping(self, columns: list[str]) -> dict[str, str]:
        """Auto-suggest column mapping based on column name similarity.

        Returns a dict mapping source column name -> target column name.
        """
        mapping: dict[str, str] = {}

        # Normalize column names for matching
        aliases: dict[str, list[str]] = {
            "timestamp": ["timestamp", "datetime", "date_time", "time", "date", "ts", "dt"],
            "open": ["open", "open_price", "o", "opening", "open_val"],
            "high": ["high", "high_price", "h", "highest", "high_val"],
            "low": ["low", "low_price", "l", "lowest", "low_val"],
            "close": ["close", "close_price", "c", "closing", "close_val", "ltp", "last"],
            "volume": ["volume", "vol", "v", "qty", "quantity", "traded_qty", "total_volume"],
            "oi": ["oi", "open_interest", "openinterest", "open_int", "openint"],
        }

        used_targets: set[str] = set()

        for col in columns:
            col_lower = col.lower().strip().replace(" ", "_")
            matched = False
            for target, names in aliases.items():
                if target in used_targets:
                    continue
                if col_lower in names:
                    mapping[col] = target
                    used_targets.add(target)
                    matched = True
                    break
            if not matched:
                mapping[col] = ""  # unmapped

        return mapping
