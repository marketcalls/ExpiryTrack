"""
DB Health Check for NIFTY Backtest
====================================
Run this FIRST before running the main backtest to verify your database
has all the required tables and data.

Usage:
    python check_db.py
    # or if your DB is at a different path:
    python check_db.py /path/to/your/expirytrack.duckdb
"""

import sys
from datetime import date
import duckdb

# ── Change this path if your DB is somewhere else ──────────────────────────
DB_PATH = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
if len(sys.argv) > 1:
    DB_PATH = sys.argv[1]
# ───────────────────────────────────────────────────────────────────────────

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def check(label, ok, detail=""):
    icon = PASS if ok else FAIL
    print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))
    return ok

def warn(label, detail=""):
    print(f"  {WARN}  {label}" + (f"  →  {detail}" if detail else ""))

all_ok = True

print(f"\n{'='*60}")
print("  NIFTY BACKTEST — DATABASE HEALTH CHECK")
print(f"{'='*60}")
print(f"  DB: {DB_PATH}")

# ── 1. Connect ──────────────────────────────────────────────────────────────
section("1. CONNECTION")
try:
    con = duckdb.connect(DB_PATH, read_only=True)
    check("Connected to DuckDB", True)
except Exception as e:
    check("Connected to DuckDB", False, str(e))
    print("\n  Cannot continue — fix the DB path first.")
    sys.exit(1)

# ── 2. Tables ───────────────────────────────────────────────────────────────
section("2. REQUIRED TABLES")
tables_result = con.execute("SHOW TABLES").fetchall()
existing_tables = {r[0].lower() for r in tables_result}
print(f"  Tables found: {sorted(existing_tables)}\n")

required = {
    "contracts":      "Option contract metadata (strike, expiry, type)",
    "historical_data":"1-min option OHLC data",
    "candle_data":    "NIFTY spot/index 1-min OHLC data",
}
for tbl, desc in required.items():
    ok = tbl in existing_tables
    all_ok = all_ok and ok
    check(f"{tbl:20s}  ({desc})", ok)

# ── 3. contracts table ──────────────────────────────────────────────────────
if "contracts" in existing_tables:
    section("3. CONTRACTS TABLE")
    try:
        cols = [r[0] for r in con.execute("DESCRIBE contracts").fetchall()]
        print(f"  Columns: {cols}\n")
        needed_cols = ["expired_instrument_key", "trading_symbol", "strike_price",
                       "contract_type", "expiry_date", "lot_size", "instrument_key"]
        for c in needed_cols:
            check(f"Column: {c}", c in cols)

        row = con.execute("""
            SELECT COUNT(*), MIN(expiry_date), MAX(expiry_date)
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE', 'PE')
        """).fetchone()
        check("NIFTY CE/PE contracts exist", row[0] > 0,
              f"{row[0]:,} contracts | expiry range: {row[1]} → {row[2]}")

        # Check specific date range needed by backtest
        row2 = con.execute("""
            SELECT COUNT(*)
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE', 'PE')
              AND expiry_date >= '2024-10-01'
        """).fetchone()
        ok = row2[0] > 0
        all_ok = all_ok and ok
        check("Contracts from 2024-10-01 onwards", ok, f"{row2[0]:,} contracts")

    except Exception as e:
        warn("Could not inspect contracts table", str(e))

# ── 4. candle_data (NIFTY spot) ─────────────────────────────────────────────
if "candle_data" in existing_tables:
    section("4. CANDLE_DATA TABLE (NIFTY SPOT)")
    try:
        cols = [r[0] for r in con.execute("DESCRIBE candle_data").fetchall()]
        print(f"  Columns: {cols}\n")
        needed_cols = ["timestamp", "open", "high", "low", "close", "volume",
                       "instrument_key", "interval"]
        for c in needed_cols:
            check(f"Column: {c}", c in cols)

        row = con.execute("""
            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM candle_data
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND interval = '1minute'
        """).fetchone()
        ok = row[0] > 0
        all_ok = all_ok and ok
        check("NIFTY 1-min spot candles exist", ok,
              f"{row[0]:,} bars | {row[1]} → {row[2]}")

        # Warmup range check
        row2 = con.execute("""
            SELECT COUNT(*)
            FROM candle_data
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND interval = '1minute'
              AND timestamp >= '2024-08-01'
        """).fetchone()
        ok2 = row2[0] > 0
        all_ok = all_ok and ok2
        check("Spot data from 2024-08-01 (warmup start)", ok2,
              f"{row2[0]:,} bars")

        # Sample a few rows
        sample = con.execute("""
            SELECT timestamp, open, high, low, close
            FROM candle_data
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND interval = '1minute'
            ORDER BY timestamp DESC LIMIT 3
        """).fetchall()
        print(f"\n  Latest 3 spot bars:")
        for r in sample:
            print(f"    {r[0]}  O:{r[1]}  H:{r[2]}  L:{r[3]}  C:{r[4]}")

    except Exception as e:
        warn("Could not inspect candle_data table", str(e))

# ── 5. historical_data (option OHLC) ────────────────────────────────────────
if "historical_data" in existing_tables:
    section("5. HISTORICAL_DATA TABLE (OPTION OHLC)")
    try:
        cols = [r[0] for r in con.execute("DESCRIBE historical_data").fetchall()]
        print(f"  Columns: {cols}\n")
        needed_cols = ["expired_instrument_key", "timestamp", "close", "volume"]
        for c in needed_cols:
            check(f"Column: {c}", c in cols)

        row = con.execute("""
            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM historical_data
        """).fetchone()
        ok = row[0] > 0
        all_ok = all_ok and ok
        check("Option OHLC rows exist", ok,
              f"{row[0]:,} rows | {row[1]} → {row[2]}")

        # Check data aligns with backtest trading range
        row2 = con.execute("""
            SELECT COUNT(DISTINCT expired_instrument_key)
            FROM historical_data
            WHERE timestamp >= '2024-10-01'
        """).fetchone()
        ok2 = row2[0] > 0
        all_ok = all_ok and ok2
        check("Option data from 2024-10-01 onwards", ok2,
              f"{row2[0]:,} distinct contracts")

        # Sample a few rows
        sample = con.execute("""
            SELECT expired_instrument_key, timestamp, close, volume
            FROM historical_data
            WHERE timestamp >= '2024-10-01'
            ORDER BY timestamp DESC LIMIT 3
        """).fetchall()
        print(f"\n  Latest 3 option rows:")
        for r in sample:
            print(f"    {r[1]}  key={r[0]}  close={r[2]}  vol={r[3]}")

    except Exception as e:
        warn("Could not inspect historical_data table", str(e))

# ── 6. Cross-check: contracts ↔ historical_data ─────────────────────────────
if "contracts" in existing_tables and "historical_data" in existing_tables:
    section("6. CROSS-CHECK: CONTRACTS ↔ OPTION DATA")
    try:
        row = con.execute("""
            SELECT COUNT(DISTINCT h.expired_instrument_key)
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND h.timestamp >= '2024-10-01'
        """).fetchone()
        ok = row[0] > 0
        all_ok = all_ok and ok
        check("NIFTY option keys match between tables", ok,
              f"{row[0]:,} contracts with price data")
    except Exception as e:
        warn("Cross-check failed", str(e))

# ── 7. Final verdict ─────────────────────────────────────────────────────────
section("VERDICT")
if all_ok:
    print(f"  {PASS}  All checks passed — you're ready to run the backtest!\n")
    print("  Run:")
    print(f"    uv run python NIFTY_optimized_overnight.py\n")
else:
    print(f"  {FAIL}  Some checks failed — fix the issues above before running.\n")
    print("  Common fixes:")
    print("    - Wrong DB path → pass it as argument: python check_db.py /your/path.duckdb")
    print("    - Missing table  → check your ExpiryTrack data ingestion pipeline")
    print("    - No data in range → your data may not cover 2024-10-01 onwards\n")

con.close()
