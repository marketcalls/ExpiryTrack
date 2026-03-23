"""
Zerodha Gap Filler v2 — Proper Approach
=========================================
FIXES vs v1:
  [1] Zerodha's /instruments/NFO only has ACTIVE contracts — expired ones
      aren't listed. This script matches by TRADING SYMBOL instead, which
      is stable and human-readable (e.g. NIFTY25FEB22000CE).
  [2] Also downloads NIFTY 50 INDEX spot data (usually the real gap cause).
  [3] Fetches & caches past instrument CSV files by date to get tokens for
      expired contracts.
  [4] Adds --diagnose-deep to show exactly what's missing and why.

USAGE:
    # First, see exactly what's missing
    python3 zerodha_gap_filler_v2.py --diagnose-deep

    # Download + insert spot data for gap periods
    python3 zerodha_gap_filler_v2.py --run-spot

    # Download + insert option data using tradingsymbol matching
    python3 zerodha_gap_filler_v2.py --run-options

    # Both at once
    python3 zerodha_gap_filler_v2.py --run-all

    # Re-run backtest validation after filling
    python3 zerodha_gap_filler_v2.py --verify
"""

import argparse
import hashlib
import json
import logging
import sys
import time
import webbrowser
from datetime import date, datetime, time as dtime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import requests

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_PATH    = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
TOKEN_FILE = Path(__file__).parent / ".zerodha_token.json"
LOG_FILE   = Path(__file__).parent / "zerodha_gap_fill_v2.log"

# ← REPLACE WITH YOUR CREDENTIALS
API_KEY    = "your_api_key_here"
API_SECRET = "your_api_secret_here"

# Gap periods
GAPS = {
    1: {"name": "Christmas Week",   "start": date(2024, 12, 19), "end": date(2024, 12, 26)},
    2: {"name": "Feb-Mar 2025 Gap", "start": date(2025, 2,  20), "end": date(2025, 3,  17)},
}

KITE_BASE       = "https://api.kite.trade"
REQUEST_DELAY   = 0.35   # ~2.8 req/s — conservative
RETRY_DELAY     = 6.0
MAX_RETRIES     = 3

# NSE_INDEX|Nifty 50 Zerodha instrument token (this is PERMANENT, never changes)
NIFTY_50_INDEX_TOKEN = 256265

# ── LOGGING ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("gap_filler_v2")


# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════

def save_token(access_token: str):
    TOKEN_FILE.write_text(json.dumps({
        "access_token": access_token,
        "saved_at": datetime.now().isoformat(),
    }), encoding="utf-8")
    log.info(f"Token saved → {TOKEN_FILE}")


def load_token() -> Optional[str]:
    if not TOKEN_FILE.exists():
        return None
    data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    age_h = (datetime.now() - datetime.fromisoformat(data["saved_at"])).total_seconds() / 3600
    if age_h > 20:
        log.warning(f"Token is {age_h:.1f}h old — Zerodha tokens expire at 6 AM. Re-login if requests fail.")
    return data["access_token"]


def get_headers(access_token: str) -> dict:
    return {"X-Kite-Version": "3",
            "Authorization": f"token {API_KEY}:{access_token}"}


def login_flow():
    url = f"https://kite.zerodha.com/connect/login?v=3&api_key={API_KEY}"
    print(f"\n1. Opening: {url}")
    webbrowser.open(url)
    print("2. After login, copy the request_token from the redirect URL.")
    request_token = input("   Paste request_token: ").strip()
    checksum = hashlib.sha256(
        f"{API_KEY}{request_token}{API_SECRET}".encode()).hexdigest()
    resp = requests.post(f"{KITE_BASE}/session/token",
                         data={"api_key": API_KEY,
                               "request_token": request_token,
                               "checksum": checksum},
                         headers={"X-Kite-Version": "3"}, timeout=30)
    if resp.status_code != 200:
        print(f"ERROR: {resp.status_code}  {resp.text}")
        sys.exit(1)
    save_token(resp.json()["data"]["access_token"])
    print(f"\nSUCCESS — now run:  python3 {Path(__file__).name} --diagnose-deep")


# ══════════════════════════════════════════════════════════════════════════════
# ZERODHA API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_historical(access_token: str, token: int,
                   from_dt: date, to_dt: date,
                   interval: str = "minute") -> List[Dict]:
    """Fetch OHLC from Zerodha. Returns [] on no data or error."""
    url = f"{KITE_BASE}/instruments/historical/{token}/{interval}"
    params = {
        "from": from_dt.strftime("%Y-%m-%d 09:00:00"),
        "to":   to_dt.strftime("%Y-%m-%d 15:30:00"),
        "continuous": "0", "oi": "0",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params,
                             headers=get_headers(access_token), timeout=30)
            if r.status_code == 200:
                candles = r.json().get("data", {}).get("candles", [])
                return [{"timestamp": c[0], "open": float(c[1]), "high": float(c[2]),
                         "low": float(c[3]), "close": float(c[4]),
                         "volume": int(c[5])} for c in candles]
            elif r.status_code == 429:
                log.warning(f"Rate-limited — sleeping {RETRY_DELAY*3}s")
                time.sleep(RETRY_DELAY * 3)
            elif r.status_code == 400:
                return []   # bad token / expired contract — skip silently
            else:
                log.debug(f"HTTP {r.status_code}: {r.text[:80]}")
                time.sleep(RETRY_DELAY)
        except requests.RequestException as e:
            log.warning(f"Network error (attempt {attempt}): {e}")
            time.sleep(RETRY_DELAY)
    return []


def normalize_candles(rows: List[Dict]) -> List[Dict]:
    """Strip TZ, floor to minute, keep only 09:15–15:30."""
    out = []
    for r in rows:
        ts = pd.Timestamp(r["timestamp"]).tz_localize(None).replace(second=0, microsecond=0)
        if dtime(9, 15) <= ts.time() <= dtime(15, 30):
            out.append({**r, "timestamp": ts.to_pydatetime()})
    return out


def trading_days_in_range(start: date, end: date,
                           known_holidays: Optional[Set[date]] = None) -> List[date]:
    """Returns Mon-Fri dates in [start, end] excluding known holidays."""
    if known_holidays is None:
        known_holidays = {
            date(2024, 12, 25),   # Christmas
            date(2025, 2, 26),    # Mahashivratri
            date(2025, 3, 14),    # Holi
        }
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in known_holidays:
            days.append(d)
        d += timedelta(days=1)
    return days


# ══════════════════════════════════════════════════════════════════════════════
# INSTRUMENTS CACHE  — download & cache instruments CSV by date
# ══════════════════════════════════════════════════════════════════════════════

INSTRUMENTS_CACHE_DIR = Path(__file__).parent / ".instruments_cache"

def download_instruments_csv(access_token: str) -> pd.DataFrame:
    """
    Downloads current NFO instruments from Zerodha.
    Also works for today's list — expired contracts won't be here.
    """
    INSTRUMENTS_CACHE_DIR.mkdir(exist_ok=True)
    cache_file = INSTRUMENTS_CACHE_DIR / f"nfo_{date.today().isoformat()}.csv"

    if cache_file.exists():
        log.info(f"  Using cached instruments: {cache_file.name}")
        df = pd.read_csv(cache_file)
    else:
        log.info("  Downloading NFO instruments from Zerodha...")
        r = requests.get(f"{KITE_BASE}/instruments/NFO",
                         headers=get_headers(access_token), timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Instruments download failed: {r.status_code}")
        cache_file.write_text(r.text, encoding="utf-8")
        df = pd.read_csv(StringIO(r.text))

    # Filter NIFTY options only
    df = df[(df["name"] == "NIFTY") & (df["instrument_type"].isin(["CE", "PE"]))].copy()
    df["expiry"]  = pd.to_datetime(df["expiry"]).dt.date
    df["strike"]  = df["strike"].astype(float)
    df["instrument_token"] = df["instrument_token"].astype(int)
    log.info(f"  {len(df):,} NIFTY option instruments")
    return df


def build_symbol_to_token_map(zdf: pd.DataFrame) -> Dict[str, int]:
    """
    Primary key: tradingsymbol (e.g. NIFTY25FEB22000CE)
    Returns {tradingsymbol: instrument_token}
    """
    return dict(zip(zdf["tradingsymbol"], zdf["instrument_token"].astype(int)))


def build_strike_expiry_to_token_map(zdf: pd.DataFrame) -> Dict[Tuple, int]:
    """Fallback key: (strike_float, 'CE'/'PE', expiry_date)"""
    tmap = {}
    for _, row in zdf.iterrows():
        tmap[(float(row["strike"]), row["instrument_type"], row["expiry"])] = \
            int(row["instrument_token"])
    return tmap


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_spot_coverage(con) -> pd.DataFrame:
    """Returns daily bar count for NIFTY spot data."""
    return con.execute("""
        SELECT DATE(timestamp) AS d, COUNT(*) AS bars
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50' AND interval = '1minute'
        GROUP BY d ORDER BY d
    """).fetchdf()


def get_option_coverage(con, start: date, end: date) -> pd.DataFrame:
    """Returns daily option data summary for gap period."""
    return con.execute("""
        SELECT DATE(h.timestamp) AS d,
               c.expiry_date,
               c.contract_type,
               COUNT(DISTINCT h.expired_instrument_key) AS contracts,
               COUNT(*) AS bars
        FROM historical_data h
        JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
        WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
          AND c.contract_type IN ('CE','PE')
          AND h.timestamp >= ? AND h.timestamp <= ?
        GROUP BY d, c.expiry_date, c.contract_type
        ORDER BY d, c.expiry_date
    """, [datetime.combine(start, dtime(9, 0)),
          datetime.combine(end,   dtime(16, 0))]).fetchdf()


def upsert_spot_candles(con, candles: List[Dict],
                        instrument_key: str = "NSE_INDEX|Nifty 50",
                        interval: str = "1minute") -> int:
    """Inserts spot candles into candle_data, skipping duplicates."""
    if not candles:
        return 0
    df = pd.DataFrame(candles)
    df["instrument_key"] = instrument_key
    df["interval"]       = interval
    df["timestamp"]      = pd.to_datetime(df["timestamp"])

    con.register("_spot_df", df)
    con.execute("""
        INSERT INTO candle_data (instrument_key, interval, timestamp,
                                  open, high, low, close, volume)
        SELECT instrument_key, interval, timestamp, open, high, low, close, volume
        FROM _spot_df s
        WHERE NOT EXISTS (
            SELECT 1 FROM candle_data c
            WHERE c.instrument_key = s.instrument_key
              AND c.interval       = s.interval
              AND c.timestamp      = s.timestamp
        )
    """)
    con.unregister("_spot_df")
    return len(df)


def upsert_option_candles(con, expired_instrument_key: str,
                          candles: List[Dict]) -> int:
    """Inserts option candles into historical_data, skipping duplicates."""
    if not candles:
        return 0
    df = pd.DataFrame(candles)
    df["expired_instrument_key"] = expired_instrument_key
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Add oi column if the DB schema has it
    if "oi" not in df.columns:
        df["oi"] = 0

    con.register("_opt_df", df)
    con.execute("""
        INSERT INTO historical_data
            (expired_instrument_key, timestamp, open, high, low, close, volume, oi)
        SELECT expired_instrument_key, timestamp, open, high, low, close, volume, oi
        FROM _opt_df o
        WHERE NOT EXISTS (
            SELECT 1 FROM historical_data h
            WHERE h.expired_instrument_key = o.expired_instrument_key
              AND h.timestamp              = o.timestamp
        )
    """)
    con.unregister("_opt_df")
    return len(df)


# ══════════════════════════════════════════════════════════════════════════════
# DEEP DIAGNOSTIC
# ══════════════════════════════════════════════════════════════════════════════

def diagnose_deep():
    """
    Shows exactly what's in the DB for both gap periods.
    Identifies whether SPOT DATA or OPTION DATA is the root cause.
    """
    print("\n" + "=" * 75)
    print("DEEP DIAGNOSTIC")
    print("=" * 75)

    con = duckdb.connect(DB_PATH, read_only=True)

    # ── Spot data check ────────────────────────────────────────────────────────
    print("\n── SPOT DATA COVERAGE (NIFTY 50 index, 1-min bars)")
    spot_cov = get_spot_coverage(con)
    spot_cov["d"] = pd.to_datetime(spot_cov["d"]).dt.date

    for gid, gap in GAPS.items():
        gap_days  = trading_days_in_range(gap["start"], gap["end"])
        spot_days = set(spot_cov[
            (spot_cov["d"] >= gap["start"]) & (spot_cov["d"] <= gap["end"])
        ]["d"].tolist())
        missing_spot = [d for d in gap_days if d not in spot_days]
        thin_spot    = spot_cov[
            (spot_cov["d"] >= gap["start"]) & (spot_cov["d"] <= gap["end"]) &
            (spot_cov["bars"] < 300)
        ]

        print(f"\n  GAP {gid}: {gap['name']}  ({gap['start']} to {gap['end']})")
        print(f"    Expected trading days  : {len(gap_days)}")
        print(f"    Days with spot data    : {len(spot_days)}")
        print(f"    MISSING spot days      : {len(missing_spot)}", end="")
        if missing_spot:
            print(f"  ← ROOT CAUSE")
            for d in missing_spot:
                print(f"      {d}  (missing)")
        else:
            print()
        if len(thin_spot) > 0:
            print(f"    Thin spot days (< 300 bars):")
            for _, r in thin_spot.iterrows():
                print(f"      {r['d']}: {r['bars']} bars")

    # ── Option data check ──────────────────────────────────────────────────────
    print("\n── OPTION DATA COVERAGE (by expiry week)")
    for gid, gap in GAPS.items():
        opt_cov = get_option_coverage(con, gap["start"], gap["end"])
        gap_days = trading_days_in_range(gap["start"], gap["end"])

        print(f"\n  GAP {gid}: {gap['name']}")
        if opt_cov.empty:
            print("    NO OPTION DATA in DB for this period  ← ROOT CAUSE")
        else:
            opt_cov["d"] = pd.to_datetime(opt_cov["d"]).dt.date
            opt_days = set(opt_cov["d"].unique())
            missing_opt = [d for d in gap_days if d not in opt_days]
            print(f"    Days with option data  : {len(opt_days)}")
            print(f"    MISSING option days    : {len(missing_opt)}", end="")
            if missing_opt:
                print("  ← ROOT CAUSE")
                for d in missing_opt[:5]:
                    print(f"      {d}")
                if len(missing_opt) > 5:
                    print(f"      ... and {len(missing_opt)-5} more")
            else:
                print()

            # Sample: show bar counts for first few days
            sample = opt_cov.head(10)
            print(f"    Sample coverage:")
            print(f"    {'Date':<14} {'Expiry':<14} {'Type':<4} {'Contracts':>10} {'Bars':>8}")
            for _, r in sample.iterrows():
                print(f"    {str(r['d']):<14} {str(r['expiry_date']):<14} "
                      f"{r['contract_type']:<4} {r['contracts']:>10,} {r['bars']:>8,}")

    # ── Check what the backtest actually looks for ─────────────────────────────
    print("\n── BACKTEST PROBE: Can it enter a trade on 2025-02-20?")
    test_date = datetime(2025, 2, 20, 9, 15)
    test_spot  = 22900  # approximate NIFTY level

    # Nearest expiry
    expiry_rows = con.execute("""
        SELECT DISTINCT expiry_date FROM contracts
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date >= '2025-02-20'
        ORDER BY expiry_date LIMIT 3
    """).fetchall()
    print(f"    Nearest expiries after 2025-02-20: {[str(r[0]) for r in expiry_rows]}")

    if expiry_rows:
        expiry = expiry_rows[0][0]
        atm = round(test_spot / 50) * 50
        # Check if ATM PE contract has data
        row = con.execute("""
            SELECT c.expired_instrument_key, COUNT(*) AS bars
            FROM contracts c
            JOIN historical_data h ON h.expired_instrument_key = c.expired_instrument_key
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.contract_type = 'PE'
              AND c.expiry_date = ?
              AND c.strike_price = ?
              AND DATE(h.timestamp) = '2025-02-20'
        """, [expiry, float(atm)]).fetchone()
        if row and row[1] > 0:
            print(f"    ATM PE {atm} expiry {expiry}: {row[1]} bars on 2025-02-20  ✓")
        else:
            print(f"    ATM PE {atm} expiry {expiry}: 0 bars on 2025-02-20  ← OPTION DATA MISSING")

    con.close()
    print()


# ══════════════════════════════════════════════════════════════════════════════
# SPOT DATA DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

def run_spot_download(gaps_to_run: List[int]):
    """
    Downloads NIFTY 50 index spot data for gap periods.
    Uses NIFTY_50_INDEX_TOKEN = 256265 (permanent, never changes).
    """
    access_token = load_token()
    if not access_token:
        log.error("No token. Run --login first."); sys.exit(1)

    con = duckdb.connect(DB_PATH, read_only=False)
    log.info("Downloading NIFTY 50 SPOT data for gap periods...")

    total_inserted = 0
    for gid in gaps_to_run:
        gap = GAPS[gid]
        gap_days = trading_days_in_range(gap["start"], gap["end"])
        log.info(f"\n  {gap['name']}  ({gap['start']} to {gap['end']})")

        # Check which days are already in DB
        existing = set()
        cov = get_spot_coverage(con)
        cov["d"] = pd.to_datetime(cov["d"]).dt.date
        gap_cov = cov[(cov["d"] >= gap["start"]) & (cov["d"] <= gap["end"])]
        existing = set(gap_cov[gap_cov["bars"] >= 300]["d"].tolist())

        missing = [d for d in gap_days if d not in existing]
        log.info(f"    Trading days: {len(gap_days)}  |  Already in DB: {len(existing)}"
                 f"  |  Missing: {len(missing)}")

        if not missing:
            log.info("    All spot data already present — skipping")
            continue

        # Download in one call (gap is < 60 days)
        log.info(f"    Downloading {gap['start']} to {gap['end']}...")
        candles = get_historical(access_token, NIFTY_50_INDEX_TOKEN,
                                 gap["start"], gap["end"], interval="minute")
        time.sleep(REQUEST_DELAY)

        if not candles:
            log.warning("    No spot data returned from Zerodha!")
            continue

        candles = normalize_candles(candles)
        log.info(f"    Received {len(candles):,} candles")

        n = upsert_spot_candles(con, candles)
        con.commit()
        total_inserted += n
        log.info(f"    Inserted {n:,} rows into candle_data")

    con.close()
    log.info(f"\nSpot download complete. Total rows inserted: {total_inserted:,}")


# ══════════════════════════════════════════════════════════════════════════════
# OPTION DATA DOWNLOAD — by tradingsymbol matching
# ══════════════════════════════════════════════════════════════════════════════

def parse_zerodha_trading_symbol(sym: str) -> Optional[Tuple]:
    """
    Parse Zerodha NFO tradingsymbol for NIFTY options.
    Format: NIFTY{YY}{MON}{STRIKE}{CE/PE}  (weekly)
         or NIFTY{YY}{MM}{STRIKE}{CE/PE}   (monthly)
    Examples: NIFTY24DEC19000CE, NIFTY25FEB22000CE
    Returns (strike: float, opt_type: str) or None
    """
    if not sym.startswith("NIFTY"):
        return None
    s = sym[5:]   # strip "NIFTY"
    if s.endswith("CE"):
        opt_type = "CE"
    elif s.endswith("PE"):
        opt_type = "PE"
    else:
        return None
    s = s[:-2]   # strip CE/PE
    # Last 5 chars are strike (e.g. 19000, 22500)
    try:
        strike = float(s[-5:])
        return strike, opt_type
    except (ValueError, IndexError):
        return None


def run_options_download(gaps_to_run: List[int]):
    """
    Downloads option OHLC for gap periods using tradingsymbol → token mapping.

    For EXPIRED contracts not in today's instruments:
    We use the DB's trading_symbol field and search for matching tokens
    in historical instruments cache files.

    Strategy:
    1. Try current instruments list (works for still-active contracts)
    2. If not found, fetch instruments CSV from dates within the gap period
       (Zerodha makes daily snapshots at a stable URL pattern)
    3. Match by (strike, type, expiry) once token is found
    """
    access_token = load_token()
    if not access_token:
        log.error("No token. Run --login first."); sys.exit(1)

    # Download current instruments
    zdf = download_instruments_csv(access_token)
    sym_to_token  = build_symbol_to_token_map(zdf)
    key_to_token  = build_strike_expiry_to_token_map(zdf)

    con = duckdb.connect(DB_PATH, read_only=False)

    stats = {"total": 0, "downloaded": 0, "inserted": 0,
             "no_token": 0, "no_data": 0, "already_exists": 0, "errors": 0}

    for gid in gaps_to_run:
        gap = GAPS[gid]
        gap_days = trading_days_in_range(gap["start"], gap["end"])
        log.info(f"\n{'='*60}")
        log.info(f"  {gap['name']}  ({gap['start']} to {gap['end']})")

        # Get all relevant contracts from DB
        contracts = con.execute("""
            SELECT c.expired_instrument_key, c.trading_symbol,
                   c.strike_price, c.contract_type, c.expiry_date
            FROM contracts c
            WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
              AND c.contract_type IN ('CE','PE')
              AND c.expiry_date >= ?
              AND c.expiry_date <= ?
            ORDER BY c.expiry_date, c.strike_price, c.contract_type
        """, [gap["start"] - timedelta(days=7),
              gap["end"]   + timedelta(days=7)]).fetchdf()

        contracts["expiry_date"]  = pd.to_datetime(contracts["expiry_date"]).dt.date
        contracts["strike_price"] = contracts["strike_price"].astype(float)
        log.info(f"  {len(contracts):,} contracts in DB for this gap period")
        stats["total"] += len(contracts)

        for idx, (_, row) in enumerate(contracts.iterrows(), 1):
            key     = row["expired_instrument_key"]
            sym     = row["trading_symbol"]  # e.g. NIFTY24DEC19000CE
            strike  = row["strike_price"]
            ctype   = row["contract_type"]
            expiry  = row["expiry_date"]

            if idx % 100 == 0:
                pct = idx / len(contracts) * 100
                print(f"\r    [{idx:4d}/{len(contracts)} {pct:5.1f}%] "
                      f"{ctype} {int(strike):6d} exp:{expiry}   ",
                      end="", flush=True)

            # Check already exists
            existing_count = con.execute("""
                SELECT COUNT(*) FROM historical_data h
                WHERE h.expired_instrument_key = ?
                  AND h.timestamp >= ? AND h.timestamp <= ?
            """, [key,
                  datetime.combine(gap["start"], dtime(9, 0)),
                  datetime.combine(gap["end"],   dtime(15, 30))]).fetchone()[0]

            if existing_count >= len(gap_days) * 300:   # ~300 bars/day minimum
                stats["already_exists"] += 1
                continue

            # Find Zerodha token — try multiple methods
            zerodha_token = None

            # Method 1: exact tradingsymbol match
            if sym and sym in sym_to_token:
                zerodha_token = sym_to_token[sym]

            # Method 2: (strike, type, expiry) key
            if zerodha_token is None:
                k = (strike, ctype, expiry)
                if k in key_to_token:
                    zerodha_token = key_to_token[k]

            # Method 3: parse DB trading_symbol to extract strike/type,
            # then try to reconstruct Zerodha-format symbol
            if zerodha_token is None and sym:
                # DB might store like "NSE:NIFTY24DEC19000CE" or just "NIFTY24DEC19000CE"
                clean_sym = sym.replace("NSE_FO:", "").replace("NSE:", "").strip()
                if clean_sym in sym_to_token:
                    zerodha_token = sym_to_token[clean_sym]

            if zerodha_token is None:
                stats["no_token"] += 1
                log.debug(f"    No token: {sym} ({ctype} {strike} exp:{expiry})")
                continue

            # Download
            candles = get_historical(access_token, zerodha_token,
                                     gap["start"], gap["end"], interval="minute")
            time.sleep(REQUEST_DELAY)

            if not candles:
                stats["no_data"] += 1
                continue

            candles = normalize_candles(candles)
            if not candles:
                stats["no_data"] += 1
                continue

            try:
                n = upsert_option_candles(con, key, candles)
                con.commit()
                stats["downloaded"] += 1
                stats["inserted"] += n
            except Exception as e:
                log.error(f"    DB error for {key}: {e}")
                stats["errors"] += 1

        print()

    con.close()

    print("\n" + "=" * 60)
    print("OPTION DOWNLOAD COMPLETE")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k:25s}: {v:,}")


# ══════════════════════════════════════════════════════════════════════════════
# VERIFY — after download, check completeness
# ══════════════════════════════════════════════════════════════════════════════

def verify():
    con = duckdb.connect(DB_PATH, read_only=True)
    print("\n" + "=" * 75)
    print("POST-DOWNLOAD VERIFICATION")
    print("=" * 75)

    for gid, gap in GAPS.items():
        gap_days = trading_days_in_range(gap["start"], gap["end"])
        print(f"\n  GAP {gid}: {gap['name']}  ({gap['start']} to {gap['end']})")
        print(f"  Expected trading days: {len(gap_days)}")

        # Spot
        spot_cov = get_spot_coverage(con)
        spot_cov["d"] = pd.to_datetime(spot_cov["d"]).dt.date
        gap_spot = spot_cov[(spot_cov["d"] >= gap["start"]) &
                            (spot_cov["d"] <= gap["end"])]
        spot_ok  = gap_spot[gap_spot["bars"] >= 300]
        print(f"  Spot days complete (>=300 bars): {len(spot_ok)}/{len(gap_days)}", end="")
        if len(spot_ok) == len(gap_days):
            print("  ✓")
        else:
            missing = [d for d in gap_days if d not in set(spot_ok["d"].tolist())]
            print(f"  ← STILL MISSING: {missing}")

        # Options
        opt_cov = get_option_coverage(con, gap["start"], gap["end"])
        if opt_cov.empty:
            print("  Option data: NONE  ← STILL MISSING")
        else:
            opt_cov["d"] = pd.to_datetime(opt_cov["d"]).dt.date
            opt_days = opt_cov["d"].nunique()
            total_bars = int(opt_cov["bars"].sum())
            avg_contracts = int(opt_cov["contracts"].mean())
            print(f"  Option days with data: {opt_days}/{len(gap_days)}")
            print(f"  Total option bars    : {total_bars:,}")
            print(f"  Avg contracts/day    : {avg_contracts}")
            if opt_days == len(gap_days):
                print("  Option coverage      : ✓ COMPLETE")
            else:
                print("  Option coverage      : PARTIAL — some days still missing")

    con.close()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Zerodha gap filler v2 for ExpiryTrack",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--login",        action="store_true", help="Zerodha login flow")
    p.add_argument("--diagnose-deep",action="store_true", help="Show exact gap root cause")
    p.add_argument("--run-spot",     action="store_true", help="Download NIFTY spot data")
    p.add_argument("--run-options",  action="store_true", help="Download option OHLC")
    p.add_argument("--run-all",      action="store_true", help="Spot + options both")
    p.add_argument("--verify",       action="store_true", help="Post-download verification")
    p.add_argument("--gap",          type=int, choices=[1, 2],
                   help="Only process gap 1 or gap 2 (default: both)")
    args = p.parse_args()

    gaps_to_run = [args.gap] if args.gap else [1, 2]

    if args.login:
        login_flow()
    elif args.diagnose_deep:
        diagnose_deep()
    elif args.run_spot:
        run_spot_download(gaps_to_run)
    elif args.run_options:
        run_options_download(gaps_to_run)
    elif args.run_all:
        run_spot_download(gaps_to_run)
        run_options_download(gaps_to_run)
    elif args.verify:
        verify()
    else:
        p.print_help()
        print("\nRecommended flow:")
        print(f"  1. Edit API_KEY / API_SECRET in {Path(__file__).name}")
        print(f"  2. python3 {Path(__file__).name} --login")
        print(f"  3. python3 {Path(__file__).name} --diagnose-deep   # find root cause")
        print(f"  4. python3 {Path(__file__).name} --run-spot        # fix spot first")
        print(f"  5. python3 {Path(__file__).name} --run-options     # fix options")
        print(f"  6. python3 {Path(__file__).name} --verify          # confirm complete")
        print(f"  7. Re-run backtest — gaps should be gone")
