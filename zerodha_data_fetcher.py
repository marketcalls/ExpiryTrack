"""
Zerodha Data Fetcher for Scalper Analytics
============================================
Fetches supplementary data via KiteConnect API and stores it
in the ExpiryTrack DuckDB so the analytics script can use it.

What this fetches:
  1. India VIX          — 1-min + daily historical (fixes Q23, Q68)
  2. USDINR Spot/Futures — daily historical (enables Q66)
  3. SGX Nifty proxy    — via Nifty Futures as Gift Nifty approximation (Q61)
  4. Nifty IT Index     — 1-min historical (enables Q98)
  5. Nifty Auto/FMCG    — for sectoral sync questions
  6. Option OI snapshot — current + historical for Max Pain (Q65, Q69, Q70, Q71)
  7. FII/DII activity   — from NSE website (not Zerodha, scraped daily) (Q89)

Usage:
  # Step 1: Get access token (do this once per day, token expires daily)
  python3 zerodha_data_fetcher.py --get-token

  # Step 2: Fetch all historical data (run once)
  python3 zerodha_data_fetcher.py --fetch-all --from 2023-01-01

  # Step 3: Daily top-up (run each morning before market opens)
  python3 zerodha_data_fetcher.py --daily-update

  # Step 4: Fetch option OI for Max Pain (run during/after market)
  python3 zerodha_data_fetcher.py --option-oi

  # Discover all instrument tokens
  python3 zerodha_data_fetcher.py --discover

Setup:
  pip install kiteconnect duckdb pandas requests
"""

import argparse
import json
import os
import sys
import time
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import requests

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("Install kiteconnect: pip install kiteconnect")
    sys.exit(1)

# ============================================================================
# CONFIGURATION
# ============================================================================
API_KEY      = "7i2ivtf0ytqrr5xl"       # ← paste from console.zerodha.com
API_SECRET   = "rvsuw8hdt626hyxri8huqw085tzdd9jt"    # ← paste from console.zerodha.com
DB_PATH      = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"
TOKEN_FILE   = Path.home() / ".zerodha_access_token.json"

# Instrument tokens (run --discover to verify these match your account)
INSTRUMENTS = {
    # Indices — interval: 1minute, day

    "GIFT NIFTY":   {"exchange": "NSEIX",  "tradingsymbol": "GIFT NIFTY",  "token": 291849},
     # update monthly
}

# Nifty option chain: ATM ± 20 strikes at 50-pt interval
NIFTY_STRIKE_INTERVAL = 50
NIFTY_OI_STRIKES      = 40       # total strikes to fetch (20 above + 20 below ATM)
NIFTY_OPTION_EXCHANGE = "NFO"


# ============================================================================
# KITE CONNECTION
# ============================================================================
def get_kite() -> KiteConnect:
    """Return authenticated KiteConnect instance."""
    if API_KEY == "YOUR_ZERODHA_API_KEY":
        print("\nERROR: Set API_KEY and API_SECRET in the CONFIG section.")
        print("Get them from: https://console.zerodha.com/account/credentials\n")
        sys.exit(1)

    kite = KiteConnect(api_key=API_KEY)

    # Load saved access token
    if TOKEN_FILE.exists():
        try:
            saved = json.loads(TOKEN_FILE.read_text())
            token_date = saved.get("date", "")
            if token_date == date.today().isoformat():
                kite.set_access_token(saved["access_token"])
                return kite
            else:
                print("  Saved token is from a previous day — need fresh login.")
        except Exception:
            pass

    print("\nAccess token not found or expired.")
    print("Run:  python3 zerodha_data_fetcher.py --get-token")
    sys.exit(1)


def get_new_token():
    """Open Zerodha login URL and exchange request token for access token."""
    kite = KiteConnect(api_key=API_KEY)
    login_url = kite.login_url()
    print(f"\nOpening Zerodha login in browser...")
    print(f"URL: {login_url}\n")
    webbrowser.open(login_url)

    print("After login, you will be redirected to a URL like:")
    print("  http://127.0.0.1/?request_token=XXXXXXXX&action=login&status=success")
    print()
    request_token = input("Paste the request_token from that URL: ").strip()

    try:
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        TOKEN_FILE.write_text(json.dumps({
            "access_token": access_token,
            "date": date.today().isoformat(),
        }))
        print(f"\nAccess token saved to {TOKEN_FILE}")
        print("Valid for today only — run --get-token again tomorrow.")
        return access_token
    except Exception as e:
        print(f"\nERROR: Failed to generate session: {e}")
        sys.exit(1)


# ============================================================================
# DB HELPERS
# ============================================================================
def connect_db():
    try:
        return duckdb.connect(DB_PATH, read_only=False)
    except Exception as e:
        print(f"\nERROR: Cannot open DB at {DB_PATH}")
        print(f"  {e}")
        print("  Close any other Python sessions using this DB first.\n")
        sys.exit(1)


def ensure_tables(con):
    """Create supplementary tables if they don't exist."""

    # Supplementary 1-min data (same schema as candle_data but writable)
    con.execute("""
        CREATE TABLE IF NOT EXISTS zerodha_candles (
            instrument_key  VARCHAR,
            interval        VARCHAR,
            timestamp       TIMESTAMP,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          BIGINT,
            oi              BIGINT DEFAULT 0,
            PRIMARY KEY (instrument_key, interval, timestamp)
        )
    """)

    # Option OI snapshots for Max Pain
    con.execute("""
        CREATE TABLE IF NOT EXISTS option_oi_snapshots (
            snapshot_time   TIMESTAMP,
            expiry          DATE,
            strike          INTEGER,
            option_type     VARCHAR,   -- CE or PE
            oi              BIGINT,
            oi_change       BIGINT,
            ltp             DOUBLE,
            PRIMARY KEY (snapshot_time, expiry, strike, option_type)
        )
    """)

    # Max Pain computed per expiry per day
    con.execute("""
        CREATE TABLE IF NOT EXISTS max_pain (
            date            DATE,
            expiry          DATE,
            max_pain_strike INTEGER,
            nifty_close     DOUBLE,
            diff_pts        DOUBLE,
            PRIMARY KEY (date, expiry)
        )
    """)

    # FII/DII daily activity
    con.execute("""
        CREATE TABLE IF NOT EXISTS fii_dii (
            date            DATE PRIMARY KEY,
            fii_net         DOUBLE,   -- Rs Crore, positive = buying
            dii_net         DOUBLE,
            fii_buy         DOUBLE,
            fii_sell        DOUBLE,
            dii_buy         DOUBLE,
            dii_sell        DOUBLE
        )
    """)

    con.commit()
    print("  Tables ensured.")


def upsert_candles(con, key: str, interval: str, df: pd.DataFrame):
    """Insert or replace candle rows."""
    if df.empty:
        return 0

    df = df.copy()
    df["instrument_key"] = key
    df["interval"]       = interval
    if "oi" not in df.columns:
        df["oi"] = 0

    df = df.rename(columns={"date": "timestamp"})
    if "timestamp" not in df.columns and df.index.name in ("date", "timestamp"):
        df = df.reset_index().rename(columns={df.index.name: "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df[["instrument_key","interval","timestamp","open","high","low","close","volume","oi"]]
    df = df.dropna(subset=["open","close"])

    inserted = 0
    for _, row in df.iterrows():
        try:
            con.execute("""
                INSERT OR REPLACE INTO zerodha_candles
                VALUES (?,?,?,?,?,?,?,?,?)
            """, [
                row["instrument_key"], row["interval"],
                row["timestamp"], float(row["open"]), float(row["high"]),
                float(row["low"]), float(row["close"]),
                int(row.get("volume", 0) or 0),
                int(row.get("oi", 0) or 0),
            ])
            inserted += 1
        except Exception:
            pass

    con.commit()
    return inserted


# ============================================================================
# HISTORICAL DATA FETCHING
# ============================================================================
def fetch_historical(kite: KiteConnect, token: int, interval: str,
                     from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch historical OHLCV from Zerodha.
    Handles Zerodha's max date range per request:
      1-min  → 60 days per call
      day    → unlimited
    """
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt   = datetime.strptime(to_date,   "%Y-%m-%d")

    # Zerodha rate limits: 3 req/sec, 60-day chunks for minute data
    chunk_days = 60 if "minute" in interval else 365
    all_data   = []
    current    = from_dt

    while current <= to_dt:
        chunk_end = min(current + timedelta(days=chunk_days - 1), to_dt)
        try:
            data = kite.historical_data(
                instrument_token=token,
                from_date=current.strftime("%Y-%m-%d"),
                to_date=chunk_end.strftime("%Y-%m-%d"),
                interval=interval,
                continuous=False,
                oi=True,
            )
            if data:
                all_data.extend(data)
            time.sleep(0.35)  # stay under rate limit
        except Exception as e:
            print(f"    Warn: chunk {current.date()} failed: {e}")
            time.sleep(1)

        current = chunk_end + timedelta(days=1)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def discover_instruments(kite: KiteConnect):
    """Print instrument tokens for key symbols."""
    print("\n=== INSTRUMENT DISCOVERY ===")

    for exchange in ["NSE", "NFO", "CDS"]:
        try:
            insts = kite.instruments(exchange)
            df = pd.DataFrame(insts)
            print(f"\n--- {exchange} (showing key symbols) ---")

            # VIX and major indices
            if exchange == "NSE":
                keywords = ["INDIA VIX", "NIFTY 50", "NIFTY BANK", "NIFTY IT",
                           "NIFTY MID", "NIFTY FIN", "NIFTY AUTO", "NIFTY FMCG"]
                mask = df["tradingsymbol"].str.upper().isin([k.upper() for k in keywords])
                for _, r in df[mask].iterrows():
                    print(f"  {r['tradingsymbol']:<30} token={r['instrument_token']}  "
                          f"exchange={r['exchange']}  type={r['instrument_type']}")

            # Currency futures
            elif exchange == "CDS":
                usdinr = df[df["name"] == "USDINR"].head(5)
                for _, r in usdinr.iterrows():
                    print(f"  {r['tradingsymbol']:<30} token={r['instrument_token']}  "
                          f"expiry={r.get('expiry','')}")

            # Nifty futures (for Gift Nifty proxy)
            elif exchange == "NFO":
                fut = df[(df["name"] == "NIFTY") &
                         (df["instrument_type"] == "FUT")].head(3)
                for _, r in fut.iterrows():
                    print(f"  {r['tradingsymbol']:<30} token={r['instrument_token']}  "
                          f"expiry={r.get('expiry','')}")

        except Exception as e:
            print(f"  {exchange}: {e}")

    print("\nUpdate the INSTRUMENTS dict with the correct tokens above.")


# ============================================================================
# FETCH ALL SUPPLEMENTARY DATA
# ============================================================================
def fetch_index_data(kite: KiteConnect, con, from_date: str, to_date: str):
    """Fetch 1-min + daily data for all indices."""

    fetch_plan = [
        # (name, db_key, interval)
        ("India VIX",      "INDIA_VIX",  "minute", "NSE_INDEX|India VIX"),
        ("India VIX daily","INDIA_VIX",  "day",    "NSE_INDEX|India VIX"),
        ("Nifty IT",       "NIFTYIT",    "minute", "NSE_INDEX|Nifty IT"),
        ("Nifty IT daily", "NIFTYIT",    "day",    "NSE_INDEX|Nifty IT"),
        ("Nifty Auto",     "NIFTYAUTO",  "minute", "NSE_INDEX|Nifty Auto"),
        ("Nifty FMCG",     "NIFTYFMCG",  "day",    "NSE_INDEX|Nifty FMCG"),
        ("Nifty Infra",    "NIFTYINFRA", "day",    "NSE_INDEX|Nifty Infra"),
    ]

    for name, key, interval, db_key in fetch_plan:
        cfg = INSTRUMENTS.get(key.split("_")[0] if "_" in key else key)
        if cfg is None or cfg.get("token") is None:
            print(f"  SKIP {name} — token not set in INSTRUMENTS dict")
            continue

        print(f"  Fetching {name} ({interval})...", end=" ", flush=True)
        df = fetch_historical(
            kite, cfg["token"],
            "minute" if interval == "minute" else "day",
            from_date, to_date
        )
        if df.empty:
            print("no data")
            continue

        # Rename for DB
        df = df.rename(columns={"date": "timestamp"})
        n = upsert_candles(con, db_key, interval, df)
        print(f"{len(df):,} bars → {n} inserted")


def fetch_usdinr(kite: KiteConnect, con, from_date: str, to_date: str):
    """Fetch USDINR spot/futures daily data."""
    print("  Fetching USDINR...", end=" ", flush=True)

    cfg = INSTRUMENTS.get("USDINR_FUT")
    if cfg is None or cfg.get("token") is None:
        # Try to discover USDINR token dynamically
        try:
            insts = kite.instruments("CDS")
            df = pd.DataFrame(insts)
            usdinr = df[(df["name"] == "USDINR") &
                       (df["instrument_type"] == "FUT")].sort_values("expiry")
            if usdinr.empty:
                print("not found in CDS segment")
                return
            token = int(usdinr.iloc[0]["instrument_token"])
            sym   = usdinr.iloc[0]["tradingsymbol"]
            print(f"  Auto-discovered USDINR token={token} ({sym})", end=" ")
        except Exception as e:
            print(f"error: {e}")
            return
    else:
        token = cfg["token"]

    df = fetch_historical(kite, token, "day", from_date, to_date)
    if df.empty:
        print("no data")
        return

    df = df.rename(columns={"date": "timestamp"})
    n = upsert_candles(con, "USDINR", "day", df)
    print(f"{len(df):,} days → {n} inserted")


def fetch_nifty_futures(kite: KiteConnect, con, from_date: str, to_date: str):
    """
    Fetch Nifty continuous futures as Gift Nifty proxy.
    Zerodha doesn't have SGX/GIFT Nifty directly.
    Nifty futures price correlates very tightly with Gift Nifty.
    """
    print("  Fetching Nifty Futures (Gift Nifty proxy)...", end=" ", flush=True)
    try:
        insts = kite.instruments("NFO")
        df_i  = pd.DataFrame(insts)
        fut   = df_i[(df_i["name"] == "NIFTY") &
                     (df_i["instrument_type"] == "FUT")].sort_values("expiry")
        if fut.empty:
            print("not found")
            return

        # Fetch front month + next month for continuity
        for _, row in fut.head(3).iterrows():
            token  = int(row["instrument_token"])
            expiry = str(row["expiry"])[:10]
            sym    = row["tradingsymbol"]
            df = fetch_historical(kite, token, "day", from_date, to_date)
            if not df.empty:
                df = df.rename(columns={"date": "timestamp"})
                n = upsert_candles(con, f"NIFTY_FUT_{expiry}", "day", df)
                print(f"\n    {sym}: {len(df)} days → {n} inserted", end="")

        print()
    except Exception as e:
        print(f"error: {e}")


# ============================================================================
# OPTION OI & MAX PAIN
# ============================================================================
def get_nifty_spot(kite: KiteConnect) -> float:
    """Get current Nifty spot price."""
    try:
        q = kite.quote("NSE:NIFTY 50")
        return float(q["NSE:NIFTY 50"]["last_price"])
    except Exception:
        return 24000.0


def get_expiry_dates(kite: KiteConnect) -> list:
    """Get next 3 Nifty weekly expiry dates."""
    try:
        insts = kite.instruments("NFO")
        df    = pd.DataFrame(insts)
        nifty_opts = df[(df["name"] == "NIFTY") &
                        (df["instrument_type"].isin(["CE", "PE"]))].copy()
        nifty_opts["expiry"] = pd.to_datetime(nifty_opts["expiry"])
        today = pd.Timestamp.today().normalize()
        future = nifty_opts[nifty_opts["expiry"] >= today]["expiry"].unique()
        return sorted(future)[:3]
    except Exception as e:
        print(f"  expiry fetch error: {e}")
        return []


def fetch_option_oi(kite: KiteConnect, con):
    """
    Fetch current option OI for Nifty ATM ± 20 strikes.
    Computes and stores Max Pain for each expiry.
    """
    print("\n  Fetching Nifty option OI...")
    spot     = get_nifty_spot(kite)
    atm      = round(spot / NIFTY_STRIKE_INTERVAL) * NIFTY_STRIKE_INTERVAL
    expiries = get_expiry_dates(kite)

    if not expiries:
        print("  No expiry dates found")
        return

    try:
        all_insts = kite.instruments("NFO")
        df_i      = pd.DataFrame(all_insts)
    except Exception as e:
        print(f"  instruments fetch failed: {e}")
        return

    snapshot_time = datetime.now().replace(second=0, microsecond=0)
    today         = date.today()

    for expiry in expiries[:2]:  # nearest 2 expiries
        exp_str = expiry.strftime("%Y-%m-%d")
        print(f"    Expiry {exp_str} | ATM={atm:.0f}...", end=" ", flush=True)

        strikes = [atm + i * NIFTY_STRIKE_INTERVAL
                   for i in range(-NIFTY_OI_STRIKES//2, NIFTY_OI_STRIKES//2 + 1)]

        # Build symbol list for batch quote
        symbols = []
        sym_map = {}
        nifty_opts = df_i[
            (df_i["name"] == "NIFTY") &
            (df_i["instrument_type"].isin(["CE", "PE"])) &
            (pd.to_datetime(df_i["expiry"]) == expiry)
        ]

        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                row = nifty_opts[
                    (nifty_opts["strike"] == strike) &
                    (nifty_opts["instrument_type"] == opt_type)
                ]
                if len(row):
                    sym = f"NFO:{row.iloc[0]['tradingsymbol']}"
                    symbols.append(sym)
                    sym_map[sym] = (strike, opt_type)

        if not symbols:
            print("no symbols found")
            continue

        # Fetch in chunks of 500 (Zerodha limit)
        oi_data = {}
        for i in range(0, len(symbols), 500):
            chunk = symbols[i:i+500]
            try:
                quotes = kite.quote(chunk)
                oi_data.update(quotes)
                time.sleep(0.2)
            except Exception as e:
                print(f"\n      chunk error: {e}")

        # Store OI + compute Max Pain
        pain_data = {}  # strike → total pain
        inserted  = 0

        for sym, (strike, opt_type) in sym_map.items():
            if sym not in oi_data:
                continue
            q  = oi_data[sym]
            oi = int(q.get("oi", 0) or 0)
            oi_change = int(q.get("oi_day_high", 0) or 0) - int(q.get("oi_day_low", 0) or 0)
            ltp = float(q.get("last_price", 0) or 0)

            try:
                con.execute("""
                    INSERT OR REPLACE INTO option_oi_snapshots
                    VALUES (?,?,?,?,?,?,?)
                """, [snapshot_time, expiry.date(), strike, opt_type,
                      oi, oi_change, ltp])
                inserted += 1
            except Exception:
                pass

            pain_data.setdefault(strike, {"CE": 0, "PE": 0})
            pain_data[strike][opt_type] = oi

        # Max Pain = strike where total ITM value is minimised
        if pain_data:
            all_strikes = sorted(pain_data.keys())
            pain_values = {}
            for test_strike in all_strikes:
                total = 0
                for s, ois in pain_data.items():
                    # CE ITM pain: strikes below test_strike
                    if s < test_strike:
                        total += ois["CE"] * (test_strike - s)
                    # PE ITM pain: strikes above test_strike
                    if s > test_strike:
                        total += ois["PE"] * (s - test_strike)
                pain_values[test_strike] = total

            mp_strike = min(pain_values, key=pain_values.get)
            diff = spot - mp_strike

            try:
                con.execute("""
                    INSERT OR REPLACE INTO max_pain VALUES (?,?,?,?,?)
                """, [today, expiry.date(), mp_strike, spot, diff])
            except Exception:
                pass

            print(f"{inserted} OI rows | Max Pain={mp_strike:.0f} "
                  f"(spot {'+' if diff>0 else ''}{diff:.0f} pts away)")
        else:
            print(f"{inserted} OI rows (no pain data)")

    con.commit()


# ============================================================================
# FII/DII DATA (NSE website scrape — no API key needed)
# ============================================================================
def fetch_fii_dii(con, from_date: str, to_date: str):
    """
    Fetch FII/DII cash market activity from NSE website.
    This is public data, no API key required.
    """
    print("\n  Fetching FII/DII data from NSE...", end=" ", flush=True)

    url = "https://www.nseindia.com/api/fiidiiTradeReact"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/",
    }

    try:
        session = requests.Session()
        # First hit the main page to get cookies
        session.get("https://www.nseindia.com/", headers=headers, timeout=10)
        time.sleep(1)

        resp = session.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code}")
            return

        data = resp.json()
        rows = []
        for item in data:
            try:
                dt = datetime.strptime(item.get("date", ""), "%d-%b-%Y").date()
                rows.append({
                    "date":     dt,
                    "fii_buy":  float(str(item.get("fiiBuy",  "0")).replace(",", "") or 0),
                    "fii_sell": float(str(item.get("fiiSell", "0")).replace(",", "") or 0),
                    "dii_buy":  float(str(item.get("diiBuy",  "0")).replace(",", "") or 0),
                    "dii_sell": float(str(item.get("diiSell", "0")).replace(",", "") or 0),
                })
                rows[-1]["fii_net"] = rows[-1]["fii_buy"] - rows[-1]["fii_sell"]
                rows[-1]["dii_net"] = rows[-1]["dii_buy"] - rows[-1]["dii_sell"]
            except Exception:
                continue

        inserted = 0
        for r in rows:
            try:
                con.execute("""
                    INSERT OR REPLACE INTO fii_dii VALUES (?,?,?,?,?,?,?)
                """, [r["date"], r["fii_net"], r["dii_net"],
                      r["fii_buy"], r["fii_sell"], r["dii_buy"], r["dii_sell"]])
                inserted += 1
            except Exception:
                pass
        con.commit()
        print(f"{inserted} days inserted")

    except Exception as e:
        print(f"error: {e}")
        print("  FII/DII fetch requires NSE website access — may fail outside India")


# ============================================================================
# DAILY UPDATE (run each morning)
# ============================================================================
def daily_update(kite: KiteConnect, con):
    """Quick daily top-up: fetch yesterday's data for all instruments."""
    yesterday = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    today     = date.today().strftime("%Y-%m-%d")

    print(f"\nDaily update: {yesterday} → {today}")

    fetch_index_data(kite, con, yesterday, today)
    fetch_usdinr(kite, con, yesterday, today)
    fetch_fii_dii(con, yesterday, today)
    fetch_option_oi(kite, con)

    print("\nDaily update complete.")


# ============================================================================
# STATUS CHECK
# ============================================================================
def show_status(con):
    """Show what supplementary data is available."""
    print("\n=== SUPPLEMENTARY DATA STATUS ===\n")

    print("zerodha_candles:")
    try:
        rows = con.execute("""
            SELECT instrument_key, interval,
                   COUNT(*) as bars,
                   MIN(timestamp)::DATE as first,
                   MAX(timestamp)::DATE as last
            FROM zerodha_candles
            GROUP BY instrument_key, interval
            ORDER BY instrument_key, interval
        """).fetchall()
        if rows:
            for r in rows:
                print(f"  {str(r[0]):<25} {str(r[1]):<8} "
                      f"{r[2]:>7,} bars  {r[3]} → {r[4]}")
        else:
            print("  (empty — run --fetch-all first)")
    except Exception as e:
        print(f"  error: {e}")

    print("\noption_oi_snapshots:")
    try:
        r = con.execute("SELECT COUNT(*), MIN(snapshot_time)::DATE, MAX(snapshot_time)::DATE "
                        "FROM option_oi_snapshots").fetchone()
        print(f"  {r[0]:,} rows | {r[1]} → {r[2]}")
    except Exception as e:
        print(f"  error: {e}")

    print("\nmax_pain:")
    try:
        rows = con.execute("""
            SELECT date, expiry, max_pain_strike, nifty_close, diff_pts
            FROM max_pain ORDER BY date DESC LIMIT 10
        """).fetchall()
        if rows:
            print(f"  {'Date':<12} {'Expiry':<12} {'MaxPain':>8} {'Spot':>8} {'Diff':>8}")
            for r in rows:
                print(f"  {str(r[0]):<12} {str(r[1]):<12} "
                      f"{r[2]:>8.0f} {r[3]:>8.0f} {r[4]:>+8.0f}")
        else:
            print("  (empty — run --option-oi during market hours)")
    except Exception as e:
        print(f"  error: {e}")

    print("\nfii_dii:")
    try:
        rows = con.execute("""
            SELECT date, fii_net, dii_net
            FROM fii_dii ORDER BY date DESC LIMIT 5
        """).fetchall()
        if rows:
            for r in rows:
                print(f"  {str(r[0]):<12} FII={r[1]:>+10,.0f}Cr  DII={r[2]:>+10,.0f}Cr")
        else:
            print("  (empty — run --fetch-all)")
    except Exception as e:
        print(f"  error: {e}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Zerodha Data Fetcher")
    parser.add_argument("--get-token",    action="store_true",
                        help="Get fresh access token via browser login")
    parser.add_argument("--discover",     action="store_true",
                        help="List instrument tokens for key symbols")
    parser.add_argument("--fetch-all",    action="store_true",
                        help="Fetch full historical data for all instruments")
    parser.add_argument("--daily-update", action="store_true",
                        help="Fetch yesterday's data (run each morning)")
    parser.add_argument("--option-oi",    action="store_true",
                        help="Fetch current option OI + compute Max Pain")
    parser.add_argument("--fii",          action="store_true",
                        help="Fetch FII/DII data from NSE website")
    parser.add_argument("--status",       action="store_true",
                        help="Show what supplementary data is available")
    parser.add_argument("--from",  dest="from_date", default="2023-01-01")
    parser.add_argument("--to",    dest="to_date",
                        default=date.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    print("=" * 60)
    print("ZERODHA DATA FETCHER")
    print("=" * 60)

    if args.get_token:
        get_new_token()
        return

    if args.status:
        con = connect_db()
        ensure_tables(con)
        show_status(con)
        con.close()
        return

    # All other commands need auth
    kite = get_kite()
    print(f"  Connected to Zerodha API")

    if args.discover:
        discover_instruments(kite)
        return

    con = connect_db()
    ensure_tables(con)

    if args.fetch_all:
        print(f"\nFetching all data: {args.from_date} → {args.to_date}")
        fetch_index_data(kite, con, args.from_date, args.to_date)
        fetch_usdinr(kite, con, args.from_date, args.to_date)
        fetch_nifty_futures(kite, con, args.from_date, args.to_date)
        fetch_fii_dii(con, args.from_date, args.to_date)
        print("\nFull fetch complete.")

    elif args.daily_update:
        daily_update(kite, con)

    elif args.option_oi:
        fetch_option_oi(kite, con)

    elif args.fii:
        fetch_fii_dii(con, args.from_date, args.to_date)

    else:
        parser.print_help()

    show_status(con)
    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
