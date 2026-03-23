"""
Zerodha Gap Filler — ExpiryTrack Database
==========================================
Downloads missing NIFTY option 1-minute OHLC data from Zerodha Kite Connect
for the two identified gap periods and inserts into ExpiryTrack DuckDB.

GAP 1 : 2024-12-19 → 2024-12-26  (Christmas week, ~7 days)
GAP 2 : 2025-02-20 → 2025-03-17  (24.7 days, most critical)

SETUP (one-time):
    pip install kiteconnect requests

USAGE:
    # Step 1 — Login and get access token (browser flow):
    python zerodha_gap_filler.py --login

    # Step 2 — Run the download + insert:
    python zerodha_gap_filler.py --run

    # Dry-run (just shows what would be downloaded, no DB writes):
    python zerodha_gap_filler.py --run --dry-run

    # Download only gap 1:
    python zerodha_gap_filler.py --run --gap 1

    # Download only gap 2:
    python zerodha_gap_filler.py --run --gap 2

NOTES:
    - Zerodha historical data limit: 60 days of 1-min data per request
    - Rate limit: ~3 requests/second (script respects this)
    - Resume-safe: already-inserted rows are skipped (UPSERT logic)
    - The script logs every contract to zerodha_gap_fill.log
"""

import argparse
import json
import logging
import sys
import time
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import duckdb
import pandas as pd
import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_PATH     = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
TOKEN_FILE  = Path(__file__).parent / ".zerodha_token.json"
LOG_FILE    = Path(__file__).parent / "zerodha_gap_fill.log"

# Your Zerodha API credentials
API_KEY    = "7i2ivtf0ytqrr5xl"        # ← REPLACE
API_SECRET = "rvsuw8hdt626hyxri8huqw085tzdd9jt"     # ← REPLACE

# Gap periods to fill (inclusive)
GAPS = {
    1: {
        "name": "Christmas Week",
        "start": date(2024, 12, 19),
        "end":   date(2024, 12, 26),
    },
    2: {
        "name": "Feb-Mar 2025 Gap",
        "start": date(2025, 2, 20),
        "end":   date(2025, 3, 17),
    },
}

# Zerodha API endpoints
KITE_BASE         = "https://api.kite.trade"
KITE_LOGIN_URL    = f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
KITE_INSTRUMENTS  = f"{KITE_BASE}/instruments/NFO"
KITE_HISTORICAL   = f"{KITE_BASE}/instruments/historical/{{token}}/minute"

# Rate limiting
REQUESTS_PER_SECOND = 3
REQUEST_DELAY       = 1.0 / REQUESTS_PER_SECOND   # ~0.33s between requests
RETRY_DELAY         = 5.0
MAX_RETRIES         = 3

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("gap_filler")


# ══════════════════════════════════════════════════════════════════════════════
# AUTH HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def save_token(access_token: str):
    TOKEN_FILE.write_text(json.dumps({
        "access_token": access_token,
        "saved_at": datetime.now().isoformat(),
    }), encoding="utf-8")
    log.info(f"Token saved to {TOKEN_FILE}")


def load_token() -> Optional[str]:
    if not TOKEN_FILE.exists():
        return None
    data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    saved = datetime.fromisoformat(data["saved_at"])
    # Zerodha tokens expire at 6:00 AM next day; warn if stale
    if (datetime.now() - saved).total_seconds() > 86400:
        log.warning("Token is older than 24h — may have expired. Re-login with --login")
    return data["access_token"]


def get_headers(access_token: str) -> dict:
    return {
        "X-Kite-Version": "3",
        "Authorization": f"token {API_KEY}:{access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def login_flow():
    """
    Opens Zerodha login page → user pastes request_token →
    exchanges it for access_token → saves to file.
    """
    print("=" * 70)
    print("ZERODHA LOGIN FLOW")
    print("=" * 70)
    print(f"\n1. Opening Zerodha login in your browser...")
    print(f"   URL: {KITE_LOGIN_URL}")
    webbrowser.open(KITE_LOGIN_URL)

    print("\n2. After login, Zerodha redirects to your redirect URL like:")
    print("   https://your-redirect.com/?request_token=XXXXXXXX&action=login&status=success")
    print("\n3. Copy the 'request_token' value from that URL and paste it below.")

    request_token = input("\n   Paste request_token here: ").strip()
    if not request_token:
        print("ERROR: No request_token provided.")
        sys.exit(1)

    # Exchange request_token for access_token
    import hashlib
    checksum = hashlib.sha256(
        f"{API_KEY}{request_token}{API_SECRET}".encode()
    ).hexdigest()

    resp = requests.post(
        f"{KITE_BASE}/session/token",
        data={
            "api_key":       API_KEY,
            "request_token": request_token,
            "checksum":      checksum,
        },
        headers={"X-Kite-Version": "3"},
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"ERROR: Token exchange failed: {resp.status_code}  {resp.text}")
        sys.exit(1)

    access_token = resp.json()["data"]["access_token"]
    save_token(access_token)
    print(f"\nSUCCESS — access_token saved. Now run: python {Path(__file__).name} --run")


# ══════════════════════════════════════════════════════════════════════════════
# ZERODHA INSTRUMENTS  (NFO CSV download)
# ══════════════════════════════════════════════════════════════════════════════

def download_nfo_instruments(access_token: str) -> pd.DataFrame:
    """
    Downloads the NFO instruments CSV from Zerodha.
    Returns a DataFrame with columns:
        instrument_token, tradingsymbol, name, expiry, strike, instrument_type
    """
    log.info("Downloading NFO instruments list from Zerodha...")
    resp = requests.get(
        KITE_INSTRUMENTS,
        headers={"X-Kite-Version": "3",
                 "Authorization": f"token {API_KEY}:{access_token}"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Instruments download failed: {resp.status_code}  {resp.text[:200]}")

    from io import StringIO
    df = pd.read_csv(StringIO(resp.text))
    # Keep only NIFTY options
    df = df[
        (df["name"] == "NIFTY") &
        (df["instrument_type"].isin(["CE", "PE"]))
    ].copy()
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    df["strike"] = df["strike"].astype(float)
    log.info(f"  {len(df):,} NIFTY option instruments loaded")
    return df


def build_token_map(zdf: pd.DataFrame) -> dict:
    """
    Returns {(strike, instrument_type, expiry_date): instrument_token}
    """
    tmap = {}
    for _, row in zdf.iterrows():
        key = (float(row["strike"]), row["instrument_type"], row["expiry"])
        tmap[key] = int(row["instrument_token"])
    return tmap


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_contracts_for_gaps(con, gaps: List[Dict]) -> pd.DataFrame:
    """
    Returns all NIFTY contracts from the DB whose expiry falls within
    any gap period, or that have trading dates within any gap period.
    We fetch contracts for expiries that are relevant to those date ranges.
    """
    # For each gap, we need contracts that are ACTIVE during the gap —
    # i.e. their expiry is AFTER the gap start.
    # We also include contracts expiring slightly before (they have premium decay).
    # Practical approach: fetch all contracts with expiry >= gap_start - 7 days
    all_contracts = []
    for gap in gaps:
        start_minus = gap["start"] - timedelta(days=7)
        end_plus    = gap["end"]   + timedelta(days=7)
        rows = con.execute("""
            SELECT expired_instrument_key, strike_price, contract_type, expiry_date
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE', 'PE')
              AND expiry_date >= ?
              AND expiry_date <= ?
            ORDER BY expiry_date, strike_price, contract_type
        """, [start_minus, end_plus]).fetchdf()
        rows["gap_start"] = gap["start"]
        rows["gap_end"]   = gap["end"]
        rows["gap_name"]  = gap["name"]
        all_contracts.append(rows)

    df = pd.concat(all_contracts, ignore_index=True).drop_duplicates(
        subset=["expired_instrument_key", "gap_start"])
    df["expiry_date"]   = pd.to_datetime(df["expiry_date"]).dt.date
    df["strike_price"]  = df["strike_price"].astype(float)
    log.info(f"  {len(df):,} contract-gap combinations to check")
    return df


def get_existing_dates(con, expired_instrument_key: str,
                       start: date, end: date) -> set:
    """Returns set of dates (date objects) that already have data in the DB."""
    rows = con.execute("""
        SELECT DISTINCT DATE(timestamp) AS d
        FROM historical_data
        WHERE expired_instrument_key = ?
          AND timestamp >= ?
          AND timestamp <= ?
    """, [expired_instrument_key,
          datetime.combine(start, datetime.min.time()),
          datetime.combine(end,   datetime.max.time())]).fetchall()
    return {r[0] for r in rows}


def insert_ohlc(con, expired_instrument_key: str, rows: List[Dict]):
    """
    Upserts OHLC rows into historical_data.
    Each row: {timestamp, open, high, low, close, volume}
    Skips rows that already exist (by primary key: expired_instrument_key + timestamp).
    """
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    df["expired_instrument_key"] = expired_instrument_key
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Build temp table and merge
    con.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_ohlc AS SELECT * FROM historical_data LIMIT 0")
    con.execute("DELETE FROM _tmp_ohlc")
    con.register("_insert_df", df)
    con.execute("""
        INSERT INTO _tmp_ohlc
        SELECT expired_instrument_key, timestamp, open, high, low, close, volume
        FROM _insert_df
    """)
    # UPSERT: insert only rows that don't exist
    result = con.execute("""
        INSERT INTO historical_data
        SELECT t.* FROM _tmp_ohlc t
        WHERE NOT EXISTS (
            SELECT 1 FROM historical_data h
            WHERE h.expired_instrument_key = t.expired_instrument_key
              AND h.timestamp = t.timestamp
        )
    """)
    con.unregister("_insert_df")
    return len(df)


# ══════════════════════════════════════════════════════════════════════════════
# ZERODHA HISTORICAL DATA DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def fetch_historical(access_token: str, instrument_token: int,
                     from_date: date, to_date: date) -> List[Dict]:
    """
    Fetches 1-minute OHLC from Zerodha for a single instrument + date range.
    Returns list of dicts: {timestamp, open, high, low, close, volume}
    """
    url = KITE_HISTORICAL.format(token=instrument_token)
    params = {
        "from":             from_date.strftime("%Y-%m-%d 09:00:00"),
        "to":               to_date.strftime("%Y-%m-%d 15:30:00"),
        "continuous":       "0",
        "oi":               "0",
    }
    headers = get_headers(access_token)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                candles = data.get("data", {}).get("candles", [])
                rows = []
                for c in candles:
                    # Zerodha format: [timestamp, open, high, low, close, volume, oi]
                    rows.append({
                        "timestamp": c[0],   # ISO string like "2025-02-20T09:15:00+0530"
                        "open":      float(c[1]),
                        "high":      float(c[2]),
                        "low":       float(c[3]),
                        "close":     float(c[4]),
                        "volume":    int(c[5]),
                    })
                return rows

            elif resp.status_code == 429:
                log.warning(f"  Rate limited — sleeping {RETRY_DELAY*2}s (attempt {attempt})")
                time.sleep(RETRY_DELAY * 2)

            elif resp.status_code in (500, 502, 503):
                log.warning(f"  Server error {resp.status_code} — retry {attempt}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)

            else:
                # 400 = bad token / expired contract — skip silently
                if resp.status_code != 400:
                    log.warning(f"  HTTP {resp.status_code}: {resp.text[:120]}")
                return []

        except requests.RequestException as e:
            log.warning(f"  Network error (attempt {attempt}): {e}")
            time.sleep(RETRY_DELAY)

    return []


def normalize_timestamps(rows: List[Dict]) -> List[Dict]:
    """
    Converts Zerodha timestamp strings to naive UTC+5:30 datetime objects,
    strips seconds (floor to minute), removes OI field if present.
    """
    out = []
    for r in rows:
        ts = pd.Timestamp(r["timestamp"]).tz_localize(None)  # strip tz
        ts = ts.replace(second=0, microsecond=0)
        # Only keep market hours: 09:15 to 15:30
        if dtime(9, 15) <= ts.time() <= dtime(15, 30):
            out.append({
                "timestamp": ts.to_pydatetime(),
                "open":   r["open"],
                "high":   r["high"],
                "low":    r["low"],
                "close":  r["close"],
                "volume": r["volume"],
            })
    return out


from datetime import time as dtime


# ══════════════════════════════════════════════════════════════════════════════
# MAIN DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════

def run_download(gaps_to_run: List[int], dry_run: bool = False):
    # Load token
    access_token = load_token()
    if not access_token:
        log.error("No access token found. Run with --login first.")
        sys.exit(1)

    # Open DB
    log.info(f"Opening database: {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=False)

    # Verify historical_data table exists and has expected schema
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "historical_data" not in tables:
        log.error("historical_data table not found in DB!")
        con.close()
        sys.exit(1)

    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='historical_data'"
    ).fetchall()]
    log.info(f"  historical_data columns: {cols}")

    # Download Zerodha instruments list
    zdf = download_nfo_instruments(access_token)
    token_map = build_token_map(zdf)
    log.info(f"  Token map built: {len(token_map):,} entries")

    # Collect gaps to process
    selected_gaps = [GAPS[g] for g in gaps_to_run]

    # Get contracts from DB that are relevant to gap periods
    log.info("\nQuerying contracts for gap periods...")
    contracts_df = get_contracts_for_gaps(con, selected_gaps)

    if len(contracts_df) == 0:
        log.warning("No contracts found for the gap periods. Check DB.")
        con.close()
        return

    # ── Summary before starting ────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("GAP FILL PLAN")
    print("=" * 70)
    for g in selected_gaps:
        sub = contracts_df[contracts_df["gap_name"] == g["name"]]
        print(f"\n  {g['name']}  ({g['start']} to {g['end']})")
        print(f"    Contracts to process: {len(sub)}")
        exp_counts = sub["expiry_date"].value_counts().sort_index()
        for exp, cnt in exp_counts.items():
            print(f"      expiry {exp}: {cnt} contracts (CE+PE)")
    print()

    if dry_run:
        log.info("DRY RUN — no data will be downloaded or inserted.")
        # Show which contracts have/don't have Zerodha tokens
        missing_tokens = 0
        for _, row in contracts_df.iterrows():
            key = (row["strike_price"], row["contract_type"],
                   row["expiry_date"])
            if key not in token_map:
                missing_tokens += 1
        log.info(f"  Contracts without Zerodha token: {missing_tokens} "
                 f"(of {len(contracts_df)})")
        log.info("  Run without --dry-run to execute.")
        con.close()
        return

    # ── Download loop ──────────────────────────────────────────────────────────
    stats = {
        "total": len(contracts_df),
        "downloaded": 0,
        "rows_inserted": 0,
        "skipped_no_token": 0,
        "skipped_no_data": 0,
        "skipped_already_exists": 0,
        "errors": 0,
    }

    log.info(f"Starting download for {len(contracts_df)} contract-gap pairs...")
    print()

    for idx, (_, row) in enumerate(contracts_df.iterrows(), 1):
        key        = row["expired_instrument_key"]
        strike     = row["strike_price"]
        ctype      = row["contract_type"]
        expiry     = row["expiry_date"]
        gap_start  = row["gap_start"]
        gap_end    = row["gap_end"]
        gap_name   = row["gap_name"]

        # Progress
        pct = idx / len(contracts_df) * 100
        print(f"\r  [{idx:4d}/{len(contracts_df)}  {pct:5.1f}%]  "
              f"{gap_name:20s}  {ctype} {int(strike):6d}  expiry:{expiry}   ",
              end="", flush=True)

        # Find Zerodha token
        zkey = (strike, ctype, expiry)
        if zkey not in token_map:
            log.debug(f"  No Zerodha token: {ctype} {strike} exp:{expiry}")
            stats["skipped_no_token"] += 1
            continue

        token = token_map[zkey]

        # Check what dates already exist in DB for this contract+gap
        existing_dates = get_existing_dates(con, key, gap_start, gap_end)

        # Build list of trading days in the gap that are missing
        # (Mon-Fri only, excluding known exchange holidays)
        KNOWN_HOLIDAYS_2024_25 = {
            date(2024, 12, 25),   # Christmas
            date(2025, 2, 26),    # Mahashivratri
        }

        trading_days_needed = set()
        d = gap_start
        while d <= gap_end:
            if d.weekday() < 5 and d not in KNOWN_HOLIDAYS_2024_25:
                trading_days_needed.add(d)
            d += timedelta(days=1)

        missing_days = trading_days_needed - existing_dates
        if not missing_days:
            stats["skipped_already_exists"] += 1
            log.debug(f"  Already complete: {ctype} {strike} exp:{expiry}")
            continue

        # Download: fetch full gap range in one call (< 60 days, Zerodha limit)
        rows = fetch_historical(access_token, token, gap_start, gap_end)
        time.sleep(REQUEST_DELAY)   # rate limit

        if not rows:
            stats["skipped_no_data"] += 1
            log.debug(f"  No data returned: {ctype} {strike} exp:{expiry}")
            continue

        # Normalize timestamps
        rows = normalize_timestamps(rows)
        if not rows:
            stats["skipped_no_data"] += 1
            continue

        # Insert into DB
        try:
            n = insert_ohlc(con, key, rows)
            con.commit()
            stats["downloaded"] += 1
            stats["rows_inserted"] += n
            log.debug(f"  Inserted {n} rows: {ctype} {strike} exp:{expiry}")
        except Exception as e:
            log.error(f"  DB insert error for {key}: {e}")
            stats["errors"] += 1

    print()  # newline after progress bar

    # ── Final report ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("DOWNLOAD COMPLETE")
    print("=" * 70)
    print(f"  Total contract-gap pairs  : {stats['total']}")
    print(f"  Successfully downloaded   : {stats['downloaded']}")
    print(f"  Rows inserted             : {stats['rows_inserted']:,}")
    print(f"  Skipped (no Zerodha token): {stats['skipped_no_token']}")
    print(f"  Skipped (no data returned): {stats['skipped_no_data']}")
    print(f"  Skipped (already in DB)   : {stats['skipped_already_exists']}")
    print(f"  Errors                    : {stats['errors']}")
    print(f"\n  Log file: {LOG_FILE}")

    # Verify insertion
    print("\n  VERIFICATION:")
    for g in selected_gaps:
        count = con.execute("""
            SELECT COUNT(*) FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.contract_type IN ('CE','PE')
              AND h.timestamp >= ?
              AND h.timestamp <= ?
        """, [
            datetime.combine(g["start"], dtime(9, 15)),
            datetime.combine(g["end"],   dtime(15, 30)),
        ]).fetchone()[0]
        print(f"  {g['name']:25s} ({g['start']} to {g['end']}): "
              f"{count:,} rows now in DB")

    con.close()
    log.info("Done.")


# ══════════════════════════════════════════════════════════════════════════════
# DIAGNOSTIC: check what's currently in DB for gap periods
# ══════════════════════════════════════════════════════════════════════════════

def diagnose():
    """Prints detailed coverage info for gap periods — no download."""
    log.info(f"Opening database: {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)

    print("\n" + "=" * 70)
    print("GAP PERIOD DIAGNOSTIC")
    print("=" * 70)

    for gid, gap in GAPS.items():
        print(f"\n  GAP {gid}: {gap['name']}  ({gap['start']} to {gap['end']})")

        # Days with data
        rows = con.execute("""
            SELECT DATE(h.timestamp) AS d,
                   c.expiry_date,
                   c.contract_type,
                   COUNT(DISTINCT c.expired_instrument_key) AS contracts,
                   COUNT(*) AS bars
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.contract_type IN ('CE','PE')
              AND h.timestamp >= ?
              AND h.timestamp <= ?
            GROUP BY d, c.expiry_date, c.contract_type
            ORDER BY d, c.expiry_date, c.contract_type
        """, [
            datetime.combine(gap["start"], dtime(9, 0)),
            datetime.combine(gap["end"],   dtime(16, 0)),
        ]).fetchdf()

        if rows.empty:
            print("    NO DATA in DB for this period.")
        else:
            print(f"    {'Date':<14} {'Expiry':<14} {'Type':<5} {'Contracts':>10} {'Bars':>8}")
            print(f"    {'-'*55}")
            for _, r in rows.iterrows():
                print(f"    {str(r['d']):<14} {str(r['expiry_date']):<14} "
                      f"{r['contract_type']:<5} {r['contracts']:>10} {r['bars']:>8}")

        # Count total contracts that SHOULD have data
        total_contracts = con.execute("""
            SELECT COUNT(*)
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE','PE')
              AND expiry_date >= ?
              AND expiry_date <= ?
        """, [gap["start"] - timedelta(days=7),
              gap["end"]   + timedelta(days=7)]).fetchone()[0]
        print(f"\n    Relevant contracts in DB: {total_contracts}")
        print(f"    Days with data:           {rows['d'].nunique() if not rows.empty else 0}")

    con.close()


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATE DOWNLOADED DATA
# ══════════════════════════════════════════════════════════════════════════════

def validate():
    """
    After download, checks completeness.
    A complete day should have ~375 bars per contract (09:15 to 15:29).
    """
    con = duckdb.connect(DB_PATH, read_only=True)

    print("\n" + "=" * 70)
    print("POST-DOWNLOAD VALIDATION")
    print("=" * 70)

    for gid, gap in GAPS.items():
        print(f"\n  GAP {gid}: {gap['name']}")
        rows = con.execute("""
            SELECT DATE(h.timestamp) AS d,
                   COUNT(DISTINCT c.expired_instrument_key) AS contracts,
                   COUNT(*) AS total_bars,
                   AVG(COUNT(*) / COUNT(DISTINCT c.expired_instrument_key))
                     OVER (PARTITION BY DATE(h.timestamp)) AS avg_bars_per_contract
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.contract_type IN ('CE','PE')
              AND h.timestamp >= ?
              AND h.timestamp <= ?
            GROUP BY d
            ORDER BY d
        """, [
            datetime.combine(gap["start"], dtime(9, 0)),
            datetime.combine(gap["end"],   dtime(16, 0)),
        ]).fetchdf()

        if rows.empty:
            print("    Still NO DATA. Download may have failed.")
            continue

        print(f"    {'Date':<14} {'Contracts':>10} {'Total Bars':>12} {'Avg Bars/Contract':>18} {'Status'}")
        print(f"    {'-'*65}")
        for _, r in rows.iterrows():
            avg_bars = r["total_bars"] / r["contracts"] if r["contracts"] > 0 else 0
            status = "OK" if avg_bars > 300 else "PARTIAL" if avg_bars > 50 else "SPARSE"
            print(f"    {str(r['d']):<14} {int(r['contracts']):>10} "
                  f"{int(r['total_bars']):>12} {avg_bars:>18.1f}  {status}")

    con.close()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Zerodha gap filler for ExpiryTrack database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--login",    action="store_true", help="Authenticate with Zerodha")
    parser.add_argument("--run",      action="store_true", help="Download and insert data")
    parser.add_argument("--diagnose", action="store_true", help="Show current DB coverage for gap periods")
    parser.add_argument("--validate", action="store_true", help="Validate downloaded data completeness")
    parser.add_argument("--dry-run",  action="store_true", help="Simulate without writing to DB")
    parser.add_argument("--gap",      type=int, choices=[1, 2],
                        help="Only process gap 1 or gap 2 (default: both)")

    args = parser.parse_args()

    if args.login:
        login_flow()

    elif args.diagnose:
        diagnose()

    elif args.validate:
        validate()

    elif args.run:
        gaps_to_run = [args.gap] if args.gap else [1, 2]
        run_download(gaps_to_run=gaps_to_run, dry_run=args.dry_run)

    else:
        parser.print_help()
        print("\nQuick start:")
        print(f"  1. Edit API_KEY and API_SECRET in {Path(__file__).name}")
        print(f"  2. python {Path(__file__).name} --login")
        print(f"  3. python {Path(__file__).name} --diagnose   # see current gaps")
        print(f"  4. python {Path(__file__).name} --run        # download + insert")
        print(f"  5. python {Path(__file__).name} --validate   # verify completeness")
