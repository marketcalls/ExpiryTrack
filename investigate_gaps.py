"""
ExpiryTrack Gap Investigation
==============================
Investigates two critical gaps:
  1. 2024-12-19 to 2024-12-26  (~7 trading days)
  2. 2025-02-20 to 2025-03-17  (~25 trading days)

Checks:
  A. Spot data availability in candle_data
  B. Option OHLC data availability in historical_data
  C. Contract coverage (expiry dates + strikes)
  D. Symbol format / naming changes
  E. Sample price lookups at exact timestamps the backtest would use

Usage:
    uv run python investigate_gaps.py
    OR
    python investigate_gaps.py
"""

import duckdb
import pandas as pd
from datetime import date, datetime

EXPITRACK_DB = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

con = duckdb.connect(EXPITRACK_DB, read_only=True)

SEP = "=" * 70

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def subsection(title):
    print(f"\n  --- {title} ---")

# ============================================================================
# HELPER: daily option row counts around a date range
# ============================================================================
def option_rows_per_day(start, end):
    rows = con.execute(f"""
        SELECT DATE(timestamp) AS dt, COUNT(*) AS rows
        FROM historical_data
        WHERE timestamp >= '{start}'
          AND timestamp <= '{end} 23:59:59'
        GROUP BY dt
        ORDER BY dt
    """).fetchall()
    return rows

# ============================================================================
# HELPER: spot bars per day
# ============================================================================
def spot_bars_per_day(start, end):
    rows = con.execute(f"""
        SELECT DATE(timestamp) AS dt, COUNT(*) AS bars, MIN(close) AS lo, MAX(close) AS hi
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '{start}'
          AND timestamp <= '{end} 23:59:59'
        GROUP BY dt
        ORDER BY dt
    """).fetchall()
    return rows

# ============================================================================
# HELPER: expiries in a range
# ============================================================================
def expiries_in_range(start, end):
    rows = con.execute(f"""
        SELECT DISTINCT expiry_date, COUNT(*) AS contracts
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND contract_type IN ('CE', 'PE')
          AND expiry_date >= '{start}'
          AND expiry_date <= '{end}'
        GROUP BY expiry_date
        ORDER BY expiry_date
    """).fetchall()
    return rows

# ============================================================================
# HELPER: sample price lookup
# ============================================================================
def check_price(bar_date, bar_time, strike, opt_type, expiry):
    key_row = con.execute(f"""
        SELECT expired_instrument_key, trading_symbol
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND contract_type = '{opt_type}'
          AND strike_price = {strike}
          AND expiry_date = '{expiry}'
        LIMIT 1
    """).fetchone()

    if not key_row:
        return f"  {bar_date} {bar_time} | {strike}{opt_type} exp={expiry} | NO CONTRACT KEY"

    key    = key_row[0]
    symbol = key_row[1]
    ts_str = f"{bar_date} {bar_time}:00"

    # Try exact match then up to 5min lookback
    for offset in range(6):
        ts = pd.Timestamp(ts_str) - pd.Timedelta(minutes=offset)
        row = con.execute("""
            SELECT close FROM historical_data
            WHERE expired_instrument_key = ?
              AND timestamp = ?
        """, [key, ts.to_pydatetime()]).fetchone()
        if row and row[0] and float(row[0]) > 0:
            return (f"  {bar_date} {bar_time} | {strike}{opt_type} exp={expiry} "
                    f"sym={symbol} | Price={row[0]:.2f} (offset={offset}min) ✓")

    # Check if ANY data exists for this contract at all
    any_row = con.execute("""
        SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
        FROM historical_data
        WHERE expired_instrument_key = ?
    """, [key]).fetchone()

    if any_row and any_row[2] and any_row[2] > 0:
        return (f"  {bar_date} {bar_time} | {strike}{opt_type} exp={expiry} "
                f"sym={symbol} | NO PRICE at {bar_time} but has data "
                f"{any_row[0]} to {any_row[1]} ({any_row[2]} rows)")
    else:
        return (f"  {bar_date} {bar_time} | {strike}{opt_type} exp={expiry} "
                f"sym={symbol} | CONTRACT EXISTS but ZERO PRICE DATA in historical_data ← BUG")

# ============================================================================
# HELPER: check what contracts exist for a specific expiry
# ============================================================================
def contracts_for_expiry(expiry):
    rows = con.execute(f"""
        SELECT contract_type,
               MIN(strike_price) AS min_strike,
               MAX(strike_price) AS max_strike,
               COUNT(*) AS n_strikes
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date = '{expiry}'
        GROUP BY contract_type
        ORDER BY contract_type
    """).fetchall()
    return rows

# ============================================================================
# HELPER: check symbol format in contracts table
# ============================================================================
def sample_symbols(expiry, n=5):
    rows = con.execute(f"""
        SELECT expired_instrument_key, trading_symbol, strike_price, contract_type
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date = '{expiry}'
        ORDER BY strike_price
        LIMIT {n}
    """).fetchall()
    return rows

# ============================================================================
# SECTION 1: OVERVIEW — last bar before and after each gap
# ============================================================================
section("SECTION 1: SPOT DATA CONTINUITY AROUND GAPS")

for gap_name, start, end in [
    ("GAP 1 (Dec 2024)", "2024-12-15", "2024-12-30"),
    ("GAP 2 (Feb-Mar 2025)", "2025-02-15", "2025-03-25"),
]:
    subsection(gap_name)
    rows = spot_bars_per_day(start, end)
    if not rows:
        print(f"  *** NO SPOT DATA in {start} to {end} ***")
    else:
        for r in rows:
            marker = " ←← MISSING" if r[1] < 100 else ""
            print(f"  {r[0]}  |  {r[1]:4d} bars  |  "
                  f"Lo: {r[2]:.0f}  Hi: {r[3]:.0f}{marker}")

# ============================================================================
# SECTION 2: OPTION DATA CONTINUITY
# ============================================================================
section("SECTION 2: OPTION OHLC DATA (historical_data) AROUND GAPS")

for gap_name, start, end in [
    ("GAP 1 (Dec 2024)", "2024-12-15", "2024-12-30"),
    ("GAP 2 (Feb-Mar 2025)", "2025-02-15", "2025-03-25"),
]:
    subsection(gap_name)
    rows = option_rows_per_day(start, end)
    if not rows:
        print(f"  *** NO OPTION DATA in {start} to {end} ***")
    else:
        for r in rows:
            marker = " ←← MISSING/LOW" if r[1] < 10000 else ""
            print(f"  {r[0]}  |  {r[1]:>8,} rows{marker}")

# ============================================================================
# SECTION 3: CONTRACT COVERAGE (expiry dates)
# ============================================================================
section("SECTION 3: CONTRACT EXPIRY COVERAGE AROUND GAPS")

for gap_name, start, end in [
    ("GAP 1 (Dec 2024 → Jan 2025)", "2024-12-01", "2025-01-31"),
    ("GAP 2 (Feb → Mar 2025)",      "2025-01-01", "2025-04-30"),
]:
    subsection(gap_name)
    rows = expiries_in_range(start, end)
    if not rows:
        print(f"  *** NO EXPIRY DATES in {start} to {end} ***")
    else:
        for r in rows:
            print(f"  Expiry: {r[0]}  |  {r[1]} strike/type combos")

# ============================================================================
# SECTION 4: DEEP DIVE — GAP 1 (Dec 19 to Dec 26)
# ============================================================================
section("SECTION 4: DEEP DIVE — GAP 1 (2024-12-19 to 2024-12-26)")

subsection("What happened on 2024-12-19 (last trade day before gap)?")
# Trade 193 exited at 2024-12-19 09:40 — let's check what data exists
test_cases_g1 = [
    ("2024-12-19", "09:15", 24150, "CE", "2024-12-19"),
    ("2024-12-19", "09:40", 24150, "CE", "2024-12-19"),
    ("2024-12-19", "10:00", 24150, "CE", "2024-12-26"),  # next expiry
    ("2024-12-20", "09:15", 24200, "CE", "2024-12-26"),
    ("2024-12-23", "09:15", 24000, "PE", "2024-12-26"),
    ("2024-12-24", "09:15", 24000, "PE", "2024-12-26"),
    ("2024-12-26", "09:15", 24000, "PE", "2024-12-26"),
]
for args in test_cases_g1:
    print(check_price(*args))

subsection("Contracts available for Dec 19 and Dec 26 expiry")
for exp in ["2024-12-19", "2024-12-26"]:
    rows = contracts_for_expiry(exp)
    if rows:
        for r in rows:
            print(f"  Expiry {exp} | {r[0]}: strikes {r[1]:.0f} to {r[2]:.0f} ({r[3]} contracts)")
    else:
        print(f"  Expiry {exp}: *** NO CONTRACTS ***")

subsection("Sample trading_symbol format for Dec 2024 expiries")
for exp in ["2024-12-19", "2024-12-26"]:
    rows = sample_symbols(exp, n=3)
    if rows:
        for r in rows:
            print(f"  key={r[0]}  sym={r[1]}  strike={r[2]}  type={r[3]}")
    else:
        print(f"  Expiry {exp}: no symbols")

subsection("Check if historical_data has rows for Dec 26 expiry contracts")
row = con.execute("""
    SELECT COUNT(*) AS total_rows,
           MIN(DATE(h.timestamp)) AS first_date,
           MAX(DATE(h.timestamp)) AS last_date
    FROM historical_data h
    JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
    WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
      AND c.expiry_date = '2024-12-26'
""").fetchone()
print(f"  historical_data rows for Dec 26 expiry: {row[0]:,} | "
      f"date range: {row[1]} to {row[2]}")

# ============================================================================
# SECTION 5: DEEP DIVE — GAP 2 (Feb 20 to Mar 17)
# ============================================================================
section("SECTION 5: DEEP DIVE — GAP 2 (2025-02-20 to 2025-03-17)")

subsection("Last trade before gap: 2025-02-13 13:30 exit, "
           "trade 174 held until 2025-02-20 15:29 (Expiry exit)")

# The backtest shows trade 174 entered 2025-02-13 and exited 2025-02-20 at 15:29
# After that there should be new entries but there aren't until Mar 17
test_cases_g2 = [
    ("2025-02-20", "15:29", 23100, "CE", "2025-02-20"),  # expiry exit bar
    ("2025-02-21", "09:15", 23100, "CE", "2025-02-25"),  # next day entry attempt
    ("2025-02-24", "09:15", 23000, "CE", "2025-02-25"),
    ("2025-02-25", "09:15", 23000, "CE", "2025-02-25"),  # expiry day
    ("2025-02-26", "09:15", 23000, "CE", "2025-03-04"),
    ("2025-03-03", "09:15", 22500, "PE", "2025-03-04"),
    ("2025-03-04", "09:15", 22500, "PE", "2025-03-04"),  # expiry day
    ("2025-03-05", "09:15", 22500, "PE", "2025-03-11"),
    ("2025-03-10", "09:15", 22500, "PE", "2025-03-11"),
    ("2025-03-11", "09:15", 22500, "PE", "2025-03-11"),  # expiry day
    ("2025-03-12", "09:15", 22500, "PE", "2025-03-17"),
    ("2025-03-13", "09:15", 22500, "CE", "2025-03-17"),
    ("2025-03-17", "09:15", 22500, "CE", "2025-03-17"),  # first trade after gap
]
for args in test_cases_g2:
    print(check_price(*args))

subsection("Contracts available for Feb-Mar 2025 expiries")
for exp in ["2025-02-20", "2025-02-25", "2025-03-04", "2025-03-11", "2025-03-17"]:
    rows = contracts_for_expiry(exp)
    if rows:
        for r in rows:
            print(f"  Expiry {exp} | {r[0]}: strikes {r[1]:.0f} to {r[2]:.0f} "
                  f"({r[3]} contracts)")
    else:
        print(f"  Expiry {exp}: *** NO CONTRACTS ***")

subsection("historical_data row counts for each Feb-Mar 2025 expiry")
for exp in ["2025-02-20", "2025-02-25", "2025-03-04", "2025-03-11", "2025-03-17"]:
    row = con.execute("""
        SELECT COUNT(*) AS total_rows,
               MIN(DATE(h.timestamp)) AS first_date,
               MAX(DATE(h.timestamp)) AS last_date
        FROM historical_data h
        JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
        WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
          AND c.expiry_date = ?
    """, [exp]).fetchone()
    print(f"  Expiry {exp}: {row[0]:>8,} rows | "
          f"date range: {row[1]} to {row[2]}")

subsection("Sample trading_symbol format for Feb-Mar 2025 expiries")
for exp in ["2025-02-20", "2025-02-25", "2025-03-04", "2025-03-17"]:
    rows = sample_symbols(exp, n=2)
    if rows:
        for r in rows:
            print(f"  Expiry {exp} | key={r[0]}  sym={r[1]}  "
                  f"strike={r[2]}  type={r[3]}")
    else:
        print(f"  Expiry {exp}: *** NO SYMBOLS ***")

# ============================================================================
# SECTION 6: SYMBOL FORMAT CHANGE DETECTION
# ============================================================================
section("SECTION 6: SYMBOL FORMAT CHANGE DETECTION")

subsection("Sample expired_instrument_key format across all months")
months = [
    ("2024-10", "2024-10-01", "2024-10-31"),
    ("2024-11", "2024-11-01", "2024-11-30"),
    ("2024-12", "2024-12-01", "2024-12-31"),
    ("2025-01", "2025-01-01", "2025-01-31"),
    ("2025-02", "2025-02-01", "2025-02-28"),
    ("2025-03", "2025-03-01", "2025-03-31"),
    ("2025-04", "2025-04-01", "2025-04-30"),
]

for month, start, end in months:
    row = con.execute(f"""
        SELECT expired_instrument_key, trading_symbol, expiry_date
        FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date >= '{start}'
          AND expiry_date <= '{end}'
          AND contract_type = 'CE'
        ORDER BY expiry_date, strike_price
        LIMIT 1
    """).fetchone()
    if row:
        print(f"  {month} | key={row[0]}  sym={row[1]}  exp={row[2]}")
    else:
        print(f"  {month} | *** NO CONTRACTS ***")

# ============================================================================
# SECTION 7: DATA INGESTION TIMESTAMPS
# ============================================================================
section("SECTION 7: HISTORICAL_DATA — INGESTION PATTERN CHECK")

subsection("How many unique expired_instrument_keys have data each month?")
rows = con.execute("""
    SELECT
        YEAR(timestamp)  AS yr,
        MONTH(timestamp) AS mo,
        COUNT(DISTINCT expired_instrument_key) AS unique_contracts,
        COUNT(*) AS total_rows,
        MIN(DATE(timestamp)) AS first_day,
        MAX(DATE(timestamp)) AS last_day
    FROM historical_data
    GROUP BY yr, mo
    ORDER BY yr, mo
""").fetchall()
print(f"  {'Month':<10} {'Contracts':>12} {'Total Rows':>12} "
      f"{'First Day':<12} {'Last Day'}")
print(f"  {'-'*60}")
prev_contracts = None
for r in rows:
    month_str = f"{r[0]}-{r[1]:02d}"
    marker = ""
    if prev_contracts and r[2] < prev_contracts * 0.5:
        marker = " ←← DROP"
    elif prev_contracts and r[2] > prev_contracts * 2:
        marker = " ←← JUMP"
    print(f"  {month_str:<10} {r[2]:>12,} {r[3]:>12,} "
          f"{str(r[4]):<12} {r[5]}{marker}")
    prev_contracts = r[2]

# ============================================================================
# SECTION 8: SPECIFIC BACKTEST ENTRY POINT CHECK
# ============================================================================
section("SECTION 8: EXACT BACKTEST ENTRY CHECKS AROUND GAPS")

subsection("After GAP 1: Why does backtest resume on 2024-12-26 at 09:15?")
# Find what spot price was on Dec 26 and what contracts were available
spot_row = con.execute("""
    SELECT close FROM candle_data
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND interval = '1minute'
      AND timestamp = '2024-12-26 09:15:00'
""").fetchone()
if spot_row:
    spot = float(spot_row[0])
    atm  = round(spot / 50) * 50
    print(f"  Spot on 2024-12-26 09:15: {spot:.2f} | ATM: {atm}")

    # nearest expiry would be Dec 26 itself (expiry day) → use next
    print(f"  Dec 26 is likely expiry day → would use next expiry")
    # check what next expiry is
    next_exp = con.execute("""
        SELECT DISTINCT expiry_date FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date > '2024-12-26'
        ORDER BY expiry_date LIMIT 3
    """).fetchall()
    for r in next_exp:
        print(f"  Next expiry after Dec 26: {r[0]}")
        print(check_price("2024-12-26", "09:15", atm, "PE", str(r[0])))
        print(check_price("2024-12-26", "09:15", atm - 500, "PE", str(r[0])))
        break
else:
    print("  No spot data for 2024-12-26 09:15")

subsection("After GAP 2: Why does backtest resume on 2025-03-17?")
spot_row = con.execute("""
    SELECT close FROM candle_data
    WHERE instrument_key = 'NSE_INDEX|Nifty 50'
      AND interval = '1minute'
      AND timestamp = '2025-03-17 09:15:00'
""").fetchone()
if spot_row:
    spot = float(spot_row[0])
    atm  = round(spot / 50) * 50
    print(f"  Spot on 2025-03-17 09:15: {spot:.2f} | ATM: {atm}")
    nearest = con.execute("""
        SELECT DISTINCT expiry_date FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date >= '2025-03-17'
        ORDER BY expiry_date LIMIT 1
    """).fetchone()
    if nearest:
        exp = str(nearest[0])
        print(f"  Nearest expiry: {exp}")
        print(check_price("2025-03-17", "09:15", atm, "CE", exp))
        # Also check days just before to confirm they have no data
        for test_date in ["2025-03-05", "2025-03-10", "2025-03-12", "2025-03-14"]:
            nearest2 = con.execute(f"""
                SELECT DISTINCT expiry_date FROM contracts
                WHERE instrument_key = 'NSE_INDEX|Nifty 50'
                  AND expiry_date >= '{test_date}'
                ORDER BY expiry_date LIMIT 1
            """).fetchone()
            if nearest2:
                print(check_price(test_date, "09:15", atm, "CE", str(nearest2[0])))

# ============================================================================
# SECTION 9: SUMMARY DIAGNOSIS
# ============================================================================
section("SECTION 9: SUMMARY — ROOT CAUSE ANALYSIS")

print("""
  This section will be populated based on findings above.
  Look for these patterns:

  CASE A — Missing contracts:
    "NO CONTRACTS" for a specific expiry → contracts table not synced
    Fix: Re-sync contracts for those expiry weeks

  CASE B — Contracts exist but no OHLC data:
    "CONTRACT EXISTS but ZERO PRICE DATA" → historical_data not synced
    Fix: Re-sync historical_data for those dates

  CASE C — Symbol format change:
    If key/sym format changed between months → ExpiryTrack changed
    its naming convention, old lookups fail
    Fix: Rebuild contract_index or re-download with new format

  CASE D — Market holiday / exchange closure:
    Genuine holiday (Christmas Dec 25, no trading) → expected gap
    But Dec 20-24 should have data if market was open

  CASE E — Data ingestion bug:
    Sudden DROP in unique_contracts count → sync script stopped
    or missed certain expiry weeks
""")

print(f"\n{SEP}")
print("  DIAGNOSTIC COMPLETE — review findings above")
print(SEP)

con.close()
