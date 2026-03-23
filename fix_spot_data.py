"""
fix_spot_data.py — Force-download missing NIFTY 50 spot bars
=============================================================
Standalone script that directly downloads the 15 missing spot days
for Feb 20 – Mar 13, 2025 from Zerodha and inserts into candle_data.

Fully verbose — shows exactly what Zerodha returns at every step.

Usage:
    python3 fix_spot_data.py
"""

import json
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
import duckdb
import pandas as pd
import requests

# ── CONFIG — edit these ────────────────────────────────────────────────────────
DB_PATH    = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
TOKEN_FILE = Path(__file__).parent / ".zerodha_token.json"
API_KEY    = "7i2ivtf0ytqrr5xl"      # ← replace

# NIFTY 50 index instrument token — this NEVER changes on Zerodha
NIFTY_INDEX_TOKEN = 256265

# Exact missing days from diagnostic
MISSING_DAYS = [
    date(2025, 2, 20), date(2025, 2, 21), date(2025, 2, 24),
    date(2025, 2, 25), date(2025, 2, 27), date(2025, 2, 28),
    date(2025, 3, 3),  date(2025, 3, 4),  date(2025, 3, 5),
    date(2025, 3, 6),  date(2025, 3, 7),  date(2025, 3, 10),
    date(2025, 3, 11), date(2025, 3, 12), date(2025, 3, 13),
]
# Fetch entire gap in one request (< 60 days — within Zerodha's limit)
FETCH_FROM = date(2025, 2, 20)
FETCH_TO   = date(2025, 3, 17)
# ──────────────────────────────────────────────────────────────────────────────


def load_token():
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"No token file at {TOKEN_FILE}. "
            f"Run: python3 zerodha_gap_filler_v2.py --login")
    data = json.loads(TOKEN_FILE.read_text())
    age_h = (datetime.now() - datetime.fromisoformat(data["saved_at"])
             ).total_seconds() / 3600
    print(f"Token age: {age_h:.1f}h", end="")
    if age_h > 20:
        print("  WARNING: token may be stale — re-login if download fails")
    else:
        print("  (fresh)")
    return data["access_token"]


def check_candle_data_schema(con):
    """Print actual candle_data schema so we know exact column names."""
    cols = con.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'candle_data'
        ORDER BY ordinal_position
    """).fetchall()
    print("\ncandle_data schema:")
    for c in cols:
        print(f"  {c[0]:30s}  {c[1]}")
    return [c[0] for c in cols]


def count_existing_spot(con, d: date) -> int:
    """Count how many 1-min bars exist for a given date."""
    ts_start = datetime.combine(d, dtime(9, 0))
    ts_end   = datetime.combine(d, dtime(16, 0))
    row = con.execute("""
        SELECT COUNT(*) FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= ? AND timestamp <= ?
    """, [ts_start, ts_end]).fetchone()
    return row[0] if row else 0


def download_spot(access_token: str, from_date: date, to_date: date):
    """
    Download 1-minute OHLC for NIFTY index from Zerodha.
    Tries both 'minute' and '1minute' intervals.
    """
    # Zerodha uses 'minute' (not '1minute') for the historical API
    url = f"https://api.kite.trade/instruments/historical/{NIFTY_INDEX_TOKEN}/minute"
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {API_KEY}:{access_token}",
    }
    params = {
        "from": from_date.strftime("%Y-%m-%d 09:00:00"),
        "to":   to_date.strftime("%Y-%m-%d 15:30:00"),
        "continuous": "0",
        "oi": "0",
    }

    print(f"\nRequesting: GET {url}")
    print(f"  from={params['from']}")
    print(f"  to  ={params['to']}")

    r = requests.get(url, params=params, headers=headers, timeout=60)
    print(f"  HTTP {r.status_code}")

    if r.status_code != 200:
        print(f"  ERROR body: {r.text[:300]}")
        return []

    data = r.json()
    candles = data.get("data", {}).get("candles", [])
    print(f"  Candles returned: {len(candles):,}")

    if candles:
        print(f"  First candle: {candles[0]}")
        print(f"  Last  candle: {candles[-1]}")

    return candles


def parse_candles(candles: list) -> pd.DataFrame:
    """
    Parse Zerodha candle list to DataFrame.
    Zerodha format: [timestamp, open, high, low, close, volume, oi]
    """
    rows = []
    for c in candles:
        ts = pd.Timestamp(c[0]).tz_localize(None).replace(second=0, microsecond=0)
        if dtime(9, 15) <= ts.time() <= dtime(15, 30):
            rows.append({
                "timestamp": ts.to_pydatetime(),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": int(c[5]),
                "oi":     int(c[6]) if len(c) > 6 else 0,
            })
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    return df


def insert_spot(con, df: pd.DataFrame, col_names: list) -> int:
    """
    Insert spot data into candle_data using exact column names from schema.
    Always includes oi column (0 for index data).
    """
    if df.empty:
        return 0

    df = df.copy()
    df["instrument_key"] = "NSE_INDEX|Nifty 50"
    df["interval"]       = "1minute"

    # Ensure oi is present (index data has no OI, default 0)
    if "oi" in col_names and "oi" not in df.columns:
        df["oi"] = 0

    # Build column list — only columns that exist in both df and schema
    insert_cols = [c for c in col_names if c in df.columns]
    print(f"\n  Columns to insert: {insert_cols}")
    print(f"  Total rows to insert: {len(df):,}")

    col_str = ", ".join(insert_cols)
    sel_str = ", ".join([f"s.{c}" for c in insert_cols])

    # First count what already exists
    before = con.execute("SELECT COUNT(*) FROM candle_data WHERE instrument_key='NSE_INDEX|Nifty 50' AND interval='1minute'").fetchone()[0]

    # Upsert — skip existing timestamps
    con.register("_fix_spot", df[insert_cols])
    con.execute(f"""
        INSERT INTO candle_data ({col_str})
        SELECT {sel_str}
        FROM _fix_spot s
        WHERE NOT EXISTS (
            SELECT 1 FROM candle_data c
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.interval       = '1minute'
              AND c.timestamp      = s.timestamp
        )
    """)
    con.unregister("_fix_spot")

    after = con.execute("SELECT COUNT(*) FROM candle_data WHERE instrument_key='NSE_INDEX|Nifty 50' AND interval='1minute'").fetchone()[0]
    actual_inserted = after - before
    print(f"  Rows before: {before:,}  |  After: {after:,}  |  Net new: {actual_inserted:,}")
    return actual_inserted


def main():
    print("=" * 65)
    print("NIFTY SPOT DATA FIX — Feb 20 to Mar 13, 2025")
    print("=" * 65)

    access_token = load_token()
    con = duckdb.connect(DB_PATH, read_only=False)

    # Check schema
    col_names = check_candle_data_schema(con)

    # Pre-check: how many bars exist per missing day
    print("\nPre-check — existing bars per missing day:")
    print(f"  {'Date':<14}  {'Bars in DB':>12}")
    print(f"  {'-'*30}")
    for d in MISSING_DAYS:
        n = count_existing_spot(con, d)
        status = "OK" if n >= 300 else "MISSING" if n == 0 else f"THIN ({n})"
        print(f"  {str(d):<14}  {n:>8,}   {status}")

    # Download from Zerodha
    print(f"\n{'='*65}")
    print(f"Downloading {FETCH_FROM} to {FETCH_TO} ...")
    candles = download_spot(access_token, FETCH_FROM, FETCH_TO)

    if not candles:
        print("\nNO DATA returned from Zerodha!")
        print("\nTroubleshooting checklist:")
        print("  1. Is your token still valid? Re-login if > 6AM IST.")
        print("     python3 zerodha_gap_filler_v2.py --login")
        print("  2. Does your Zerodha plan include historical data API?")
        print("     (Historical data requires a paid API subscription)")
        print("  3. Try the Kite Connect API playground to test manually:")
        print(f"     GET https://api.kite.trade/instruments/historical/"
              f"{NIFTY_INDEX_TOKEN}/minute"
              f"?from=2025-02-20+09:00:00&to=2025-02-20+15:30:00")
        con.close()
        return

    # Parse
    df = parse_candles(candles)
    if df.empty:
        print("Candles parsed to 0 rows (all outside market hours?)")
        con.close()
        return

    # Check what days we actually got
    df["date"] = df["timestamp"].apply(lambda x: x.date())
    got_days = sorted(df["date"].unique())
    print(f"\nDays received from Zerodha: {len(got_days)}")
    for d in got_days:
        n = len(df[df["date"] == d])
        print(f"  {d}  →  {n} bars")

    missing_still = [d for d in MISSING_DAYS if d not in got_days]
    if missing_still:
        print(f"\nWARNING: {len(missing_still)} days NOT in Zerodha response:")
        for d in missing_still:
            print(f"  {d}  (Zerodha returned no data — may be holiday or API limit)")

    # Insert
    print(f"\nInserting {len(df):,} rows into candle_data...")
    n_inserted = insert_spot(con, df, col_names)
    con.commit()
    print(f"Committed {n_inserted:,} rows")

    # Post-check
    print(f"\n{'='*65}")
    print("Post-check — bars per day after insert:")
    print(f"  {'Date':<14}  {'Bars':>8}  {'Status'}")
    print(f"  {'-'*35}")
    all_ok = True
    for d in MISSING_DAYS:
        n = count_existing_spot(con, d)
        if n >= 300:
            status = "OK"
        elif n > 0:
            status = f"THIN ({n} bars)"
            all_ok = False
        else:
            status = "STILL MISSING"
            all_ok = False
        print(f"  {str(d):<14}  {n:>8,}  {status}")

    con.close()

    print(f"\n{'='*65}")
    if all_ok:
        print("SUCCESS — all missing spot days filled!")
        print("Next step: python3 NIFTY_backtest_validated.py")
    else:
        print("PARTIAL — some days still missing. See notes above.")
        print("The remaining missing days may be genuine market holidays,")
        print("or your Zerodha plan may not include historical index data.")


if __name__ == "__main__":
    main()
