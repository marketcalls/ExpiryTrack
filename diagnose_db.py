"""
ExpiryTrack DuckDB Diagnostic Script
=====================================
Run this to check data availability and find why backtest stops after Mar 12.

Usage:
    uv run python diagnose_db.py
    OR
    python diagnose_db.py
"""

import duckdb
import pandas as pd
from datetime import date

EXPITRACK_DB = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

print("=" * 70)
print("EXPIRYTRACK DUCKDB DIAGNOSTIC REPORT")
print("=" * 70)

con = duckdb.connect(EXPITRACK_DB, read_only=True)

# ── 1. SPOT DATA ──────────────────────────────────────────────────────────
print("\n[1] SPOT DATA (candle_data)")
print("-" * 50)
row = con.execute("""
    SELECT
        MIN(timestamp) AS first_bar,
        MAX(timestamp) AS last_bar,
        COUNT(*)       AS total_bars
    FROM candle_data
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND interval = '1minute'
""").fetchone()
print(f"  First bar      : {row[0]}")
print(f"  Last bar       : {row[1]}")
print(f"  Total 1min bars: {row[2]:,}")

# Last 5 days of spot data
print("\n  Last 5 trading days available:")
rows = con.execute("""
    SELECT DATE(timestamp) AS dt, COUNT(*) AS bars, MAX(close) AS last_close
    FROM candle_data
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND interval = '1minute'
    GROUP BY dt
    ORDER BY dt DESC
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"    {r[0]}  |  {r[1]:4d} bars  |  Close: {r[2]:.2f}")

# ── 2. CONTRACTS ──────────────────────────────────────────────────────────
print("\n[2] CONTRACTS TABLE")
print("-" * 50)
row = con.execute("""
    SELECT
        MIN(expiry_date) AS first_expiry,
        MAX(expiry_date) AS last_expiry,
        COUNT(*)         AS total_contracts
    FROM contracts
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND contract_type IN ('CE', 'PE')
""").fetchone()
print(f"  First expiry   : {row[0]}")
print(f"  Last expiry    : {row[1]}")
print(f"  Total contracts: {row[2]:,}")

# Last 5 expiries available
print("\n  Last 5 expiry dates in contracts table:")
rows = con.execute("""
    SELECT DISTINCT expiry_date, COUNT(*) AS strikes
    FROM contracts
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND contract_type IN ('CE', 'PE')
    GROUP BY expiry_date
    ORDER BY expiry_date DESC
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"    {r[0]}  |  {r[1]:4d} strike/type combos")

# ── 3. OPTION OHLC DATA (historical_data) ────────────────────────────────
print("\n[3] OPTION OHLC DATA (historical_data)")
print("-" * 50)
row = con.execute("""
    SELECT
        MIN(timestamp) AS first_ts,
        MAX(timestamp) AS last_ts,
        COUNT(*)       AS total_rows
    FROM historical_data
""").fetchone()
print(f"  First timestamp: {row[0]}")
print(f"  Last timestamp : {row[1]}")
print(f"  Total rows     : {row[2]:,}")

# Last 5 days in option data
print("\n  Last 5 days of option OHLC data:")
rows = con.execute("""
    SELECT DATE(timestamp) AS dt, COUNT(*) AS rows
    FROM historical_data
    GROUP BY dt
    ORDER BY dt DESC
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"    {r[0]}  |  {r[1]:,} rows")

# ── 4. GAP CHECK — Mar 12 to Mar 17 ──────────────────────────────────────
print("\n[4] GAP CHECK — Mar 12 to Mar 17, 2026")
print("-" * 50)

# Spot data in this range
rows = con.execute("""
    SELECT DATE(timestamp) AS dt, COUNT(*) AS bars
    FROM candle_data
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND interval = '1minute'
      AND timestamp >= '2026-03-12'
    GROUP BY dt
    ORDER BY dt
""").fetchall()
print("  Spot bars per day (Mar 12+):")
if rows:
    for r in rows:
        print(f"    {r[0]}  |  {r[1]:4d} bars")
else:
    print("    *** NO SPOT DATA AFTER MAR 12 ***")

# Option data in this range
rows = con.execute("""
    SELECT DATE(timestamp) AS dt, COUNT(*) AS rows
    FROM historical_data
    WHERE timestamp >= '2026-03-12'
    GROUP BY dt
    ORDER BY dt
""").fetchall()
print("\n  Option OHLC rows per day (Mar 12+):")
if rows:
    for r in rows:
        print(f"    {r[0]}  |  {r[1]:,} rows")
else:
    print("    *** NO OPTION DATA AFTER MAR 12 ***")

# Contracts expiring after Mar 12
rows = con.execute("""
    SELECT DISTINCT expiry_date
    FROM contracts
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND contract_type IN ('CE', 'PE')
      AND expiry_date >= '2026-03-12'
    ORDER BY expiry_date
    LIMIT 10
""").fetchall()
print("\n  Expiry dates available after Mar 12:")
if rows:
    for r in rows:
        print(f"    {r[0]}")
else:
    print("    *** NO EXPIRY DATES AFTER MAR 12 — THIS IS THE BUG ***")

# ── 5. SAMPLE CONTRACT KEY CHECK ─────────────────────────────────────────
print("\n[5] SAMPLE CONTRACT AVAILABILITY FOR RECENT DATES")
print("-" * 50)

# Pick a recent NIFTY ATM-ish strike and check if price data exists
test_cases = [
    ("2026-03-10 10:00:00", 22500, "PE"),
    ("2026-03-12 10:00:00", 22500, "PE"),
    ("2026-03-14 10:00:00", 22500, "PE"),
    ("2026-03-17 10:00:00", 22500, "PE"),
]

for ts_str, strike, ctype in test_cases:
    # Find the nearest expiry
    ts_date = pd.Timestamp(ts_str).date()
    exp_row = con.execute(f"""
        SELECT MIN(expiry_date) FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND contract_type = '{ctype}'
          AND expiry_date >= '{ts_date}'
    """).fetchone()
    expiry = exp_row[0] if exp_row else None

    if expiry is None:
        print(f"  {ts_str[:10]}  |  No expiry found >= {ts_date}  ← MISSING CONTRACT")
        continue

    key_row = con.execute(f"""
        SELECT expired_instrument_key FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND contract_type = '{ctype}'
          AND strike_price = {strike}
          AND expiry_date = '{expiry}'
        LIMIT 1
    """).fetchone()

    if key_row is None:
        print(f"  {ts_str[:10]}  |  Expiry={expiry}  |  Contract key NOT FOUND for {strike}{ctype}")
        continue

    key = key_row[0]
    price_row = con.execute(f"""
        SELECT close FROM historical_data
        WHERE expired_instrument_key = '{key}'
          AND timestamp = '{ts_str}'
        LIMIT 1
    """).fetchone()

    if price_row:
        print(f"  {ts_str[:10]}  |  Expiry={expiry}  |  {strike}{ctype}  |  Price={price_row[0]:.2f}  ✓")
    else:
        print(f"  {ts_str[:10]}  |  Expiry={expiry}  |  {strike}{ctype}  |  *** NO PRICE DATA ***")

con.close()

print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE — share the output above to identify the issue")
print("=" * 70)
