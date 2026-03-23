"""Tests for DataExporter — CSV, JSON, Parquet, ZIP export, empty data, date filtering."""

import json
from pathlib import Path

import pandas as pd

from src.export.exporter import DataExporter


def _seed_export_data(db):
    """Insert test data for export tests."""
    with db.get_connection() as conn:
        conn.execute("""
            INSERT INTO instruments (instrument_key, symbol, name, exchange)
            VALUES ('NSE_INDEX|Nifty 50', 'NIFTY', 'Nifty 50', 'NSE')
        """)
        conn.execute("""
            INSERT INTO contracts (expired_instrument_key, instrument_key, expiry_date,
                                   contract_type, strike_price, trading_symbol, openalgo_symbol, data_fetched)
            VALUES
                ('NSE_FO|NIFTY25100CE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'CE', 25100,
                 'NIFTY 25100 CE', 'NIFTY30JAN25C25100', TRUE),
                ('NSE_FO|NIFTY25100PE', 'NSE_INDEX|Nifty 50', '2025-01-30', 'PE', 25100,
                 'NIFTY 25100 PE', 'NIFTY30JAN25P25100', TRUE)
        """)
        conn.execute("""
            INSERT INTO historical_data (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
            VALUES
                ('NSE_FO|NIFTY25100CE', '2025-01-28 09:15:00', 100, 110, 95, 105, 1000, 500),
                ('NSE_FO|NIFTY25100CE', '2025-01-29 09:15:00', 105, 115, 100, 110, 1200, 600),
                ('NSE_FO|NIFTY25100PE', '2025-01-28 09:15:00', 50, 55, 45, 48, 800, 300),
                ('NSE_FO|NIFTY25100PE', '2025-01-29 09:15:00', 48, 52, 40, 42, 900, 350)
        """)
        # Insert expiries for get_available_expiries
        conn.execute("""
            INSERT INTO expiries (instrument_key, expiry_date, contracts_fetched)
            VALUES ('NSE_INDEX|Nifty 50', '2025-01-30', TRUE)
        """)


def test_csv_export(tmp_db, tmp_path):
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|Nifty 50"]
    expiries = {"NSE_INDEX|Nifty 50": ["2025-01-30"]}
    options = {"include_openalgo": True, "include_metadata": True, "time_range": "all"}

    filepath = exporter.export_to_csv(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()

    df = pd.read_csv(filepath)
    assert len(df) == 4
    assert "openalgo_symbol" in df.columns
    assert "open" in df.columns
    assert "close" in df.columns


def test_json_export(tmp_db, tmp_path):
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|Nifty 50"]
    expiries = {"NSE_INDEX|Nifty 50": ["2025-01-30"]}
    options = {"include_openalgo": True, "time_range": "all"}

    filepath = exporter.export_to_json(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()

    with open(filepath) as f:
        data = json.load(f)

    assert "metadata" in data
    assert "data" in data
    assert data["metadata"]["format"] == "OpenAlgo"
    # Nifty_50 key in data
    assert "Nifty_50" in data["data"]


def test_zip_export(tmp_db, tmp_path):
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|Nifty 50"]
    expiries = {"NSE_INDEX|Nifty 50": ["2025-01-30"]}
    options = {"include_openalgo": True, "include_metadata": True, "time_range": "all"}

    filepath = exporter.export_to_zip(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()
    assert filepath.endswith(".zip")

    import zipfile

    with zipfile.ZipFile(filepath, "r") as zf:
        names = zf.namelist()
        assert len(names) >= 1
        # Should contain CSV with instrument name
        assert any("Nifty_50" in n for n in names)


def test_parquet_export(tmp_db, tmp_path):
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|Nifty 50"]
    expiries = {"NSE_INDEX|Nifty 50": ["2025-01-30"]}
    options = {"include_openalgo": True}

    filepath = exporter.export_to_parquet(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()
    assert filepath.endswith(".parquet")

    df = pd.read_parquet(filepath)
    assert len(df) == 4


def test_empty_export_csv(tmp_db, tmp_path):
    """CSV export with no matching data should create file with headers only."""
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|NonExistent"]
    expiries = {"NSE_INDEX|NonExistent": ["2099-01-01"]}
    options = {"include_openalgo": True, "time_range": "all"}

    filepath = exporter.export_to_csv(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()

    df = pd.read_csv(filepath)
    assert len(df) == 0  # No data rows


def test_empty_export_parquet(tmp_db, tmp_path):
    """Parquet export with no matching data should create empty parquet file."""
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|NonExistent"]
    expiries = {}
    options = {"include_openalgo": True}

    filepath = exporter.export_to_parquet(instruments, expiries, options, "test-task")
    assert Path(filepath).exists()


def test_get_available_expiries(tmp_db):
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)

    result = exporter.get_available_expiries(["NSE_INDEX|Nifty 50"])
    assert "NSE_INDEX|Nifty 50" in result
    assert "2025-01-30" in result["NSE_INDEX|Nifty 50"]


def test_time_range_filter(tmp_db, tmp_path):
    """Time range filter should limit data based on expiry date."""
    _seed_export_data(tmp_db)
    exporter = DataExporter(tmp_db)
    exporter.export_dir = tmp_path

    instruments = ["NSE_INDEX|Nifty 50"]
    expiries = {"NSE_INDEX|Nifty 50": ["2025-01-30"]}
    # 1d filter: only data within 1 day of expiry (2025-01-29+)
    options = {"include_openalgo": True, "include_metadata": True, "time_range": "1d"}

    filepath = exporter.export_to_csv(instruments, expiries, options, "test-task")
    df = pd.read_csv(filepath)
    # Only 2025-01-29 data (2 candles) should remain
    assert len(df) == 2
