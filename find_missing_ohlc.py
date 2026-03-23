"""
ExpiryTrack Missing OHLC Data Finder
=====================================
Finds all contracts that exist in the contracts table
but have ZERO rows in historical_data.

This is the confirmed root cause of:
  - GAP 1: 2024-12-19 to 2024-12-26
  - GAP 2: 2025-02-20 to 2025-03-17

Output shows exactly which expiry weeks are affected
so you know what to re-sync.

Usage:
    uv run python find_missing_ohlc.py
"""

import duckdb
import pandas as pd

EXPITRACK_DB = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

con = duckdb.connect(EXPITRACK_DB, read_only=True)

print("=" * 75)
print("  FINDING CONTRACTS WITH ZERO OHLC DATA IN historical_data")
print("=" * 75)

# ── Step 1: Count contracts per expiry that have NO historical_data rows ──
print("\n[1] Contracts with ZERO rows in historical_data — grouped by expiry:\n")

rows = con.execute("""
    SELECT
        c.expiry_date,
        COUNT(c.expired_instrument_key)                          AS total_contracts,
        SUM(CASE WHEN h.cnt IS NULL OR h.cnt = 0 THEN 1 ELSE 0 END) AS missing_ohlc,
        SUM(CASE WHEN h.cnt > 0 THEN 1 ELSE 0 END)              AS has_ohlc
    FROM contracts c
    LEFT JOIN (
        SELECT expired_instrument_key, COUNT(*) AS cnt
        FROM historical_data
        GROUP BY expired_instrument_key
    ) h ON c.expired_instrument_key = h.expired_instrument_key
    WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
      AND c.contract_type IN ('CE', 'PE')
    GROUP BY c.expiry_date
    ORDER BY c.expiry_date
""").fetchall()

print(f"  {'Expiry':<14} {'Total':>8} {'Has OHLC':>10} {'Missing':>10}  Status")
print(f"  {'-'*60}")
problem_expiries = []
for r in rows:
    expiry, total, missing, has_ohlc = r
    if missing > 0:
        status = "❌ MISSING DATA"
        problem_expiries.append((str(expiry), total, missing))
    else:
        status = "✓"
    print(f"  {str(expiry):<14} {total:>8} {has_ohlc:>10} {missing:>10}  {status}")

# ── Step 2: Summary ────────────────────────────────────────────────────────
print(f"\n[2] Problem expiries summary ({len(problem_expiries)} affected):\n")
if not problem_expiries:
    print("  ✓ No missing OHLC data found!")
else:
    for exp, total, missing in problem_expiries:
        pct = missing / total * 100
        print(f"  {exp}  →  {missing}/{total} contracts missing OHLC  ({pct:.0f}% of expiry)")

# ── Step 3: Show sample missing contract keys for each problem expiry ──────
print(f"\n[3] Sample missing contract keys (first 5 per expiry):\n")
for exp, total, missing in problem_expiries:
    print(f"  --- {exp} ---")
    sample = con.execute("""
        SELECT c.expired_instrument_key, c.trading_symbol,
               c.strike_price, c.contract_type
        FROM contracts c
        LEFT JOIN (
            SELECT expired_instrument_key, COUNT(*) AS cnt
            FROM historical_data
            GROUP BY expired_instrument_key
        ) h ON c.expired_instrument_key = h.expired_instrument_key
        WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
          AND c.expiry_date = ?
          AND (h.cnt IS NULL OR h.cnt = 0)
        ORDER BY c.strike_price
        LIMIT 5
    """, [exp]).fetchall()
    for s in sample:
        print(f"    key={s[0]}  sym={s[1]}  strike={s[2]}  type={s[3]}")

# ── Step 4: Date range of existing data for surrounding expiries ───────────
print(f"\n[4] Compare: expiry with data vs expiry without data\n")

# pick one good expiry and one bad expiry for comparison
all_expiries = con.execute("""
    SELECT DISTINCT c.expiry_date,
           COALESCE(SUM(h.cnt), 0) AS total_rows
    FROM contracts c
    LEFT JOIN (
        SELECT expired_instrument_key, COUNT(*) AS cnt
        FROM historical_data
        GROUP BY expired_instrument_key
    ) h ON c.expired_instrument_key = h.expired_instrument_key
    WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
    GROUP BY c.expiry_date
    ORDER BY c.expiry_date
""").fetchall()

print(f"  {'Expiry':<14} {'Total OHLC rows':>18}  Status")
print(f"  {'-'*45}")
for r in all_expiries:
    expiry, total_rows = r
    if total_rows == 0:
        status = "❌ NO DATA"
    elif total_rows < 10000:
        status = "⚠️  LOW DATA"
    else:
        status = "✓"
    print(f"  {str(expiry):<14} {total_rows:>18,}  {status}")

# ── Step 5: Exact action items ─────────────────────────────────────────────
print(f"\n{'='*75}")
print("  ACTION REQUIRED")
print(f"{'='*75}")
print("""
  The following expiry weeks have CONTRACTS in the contracts table
  but ZERO rows in historical_data.

  This means your ExpiryTrack sync script:
    ✓ Downloaded contract metadata (strikes, symbols, keys)
    ✗ Did NOT download the 1-min OHLC bars for those contracts

  TO FIX:
    Re-run your ExpiryTrack data sync/download script
    specifically for the missing expiry dates shown above.

  If your sync script supports a date range parameter, run it for:
""")
for exp, total, missing in problem_expiries:
    # suggest syncing from 4 weeks before expiry to expiry date
    exp_dt = pd.Timestamp(exp)
    sync_from = (exp_dt - pd.Timedelta(days=28)).strftime("%Y-%m-%d")
    print(f"    Expiry {exp}  →  sync from {sync_from} to {exp}")

print(f"""
  If you don't have a targeted re-sync option, the simplest fix is
  to re-download ALL historical_data for those expired_instrument_keys
  that currently have 0 rows.

  The missing contract keys are listed in section [3] above.
""")

con.close()
print("Done.")
