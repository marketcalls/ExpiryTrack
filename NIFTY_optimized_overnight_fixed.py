"""
SuperTrend Credit Spread Backtest - OPTIMIZED OVERNIGHT
=========================================================
Primary config: ST(100, 2.0) — matches live trading script
Also runs ST(80, 3.6) and ST(35, 5.0) for comparison.
Uses actual 1-min option OHLC data from ExpiryTrack DuckDB.

BEHAVIOR:
  - IMMEDIATE ENTRY on any SuperTrend flip (no entry window restriction)
  - IMMEDIATE EXIT + RE-ENTRY on reversal flip at ANY time
  - On EXPIRY DAY, always use NEXT week expiry (avoid gamma explosion)
  - Entry allowed from 09:15 (market open) — no artificial delay
  - Target re-entry: after 95% premium decay, re-enter if trend continues

  EXIT RULES:
    - SuperTrend reversal signal (any time)
    - 95% premium target hit (with re-entry)
    - Max loss (spread fully ITM)
    - Expiry day at 15:15 (must close before expiry)
    - NO forced EOD exit (overnight hold)

Data source: ExpiryTrack DuckDB (real 1-min option OHLC)

Usage:
    uv run python backtesting/supertrend_credit_spread_optimized/NIFTY_optimized_backtest.py
"""

import os
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPITRACK_DB = "/Users/bond7/Desktop/Project/ExpiryTrack/data/expirytrack.duckdb"

# ============================================================================
# STRATEGY PARAMETERS
# ============================================================================
ST_PERIOD =  80
ST_MULTIPLIER = 3.6
TIMEFRAME = "5min"

SPREAD_WIDTH = 500
STRIKE_INTERVAL = 50

LOT_SIZE = 65
NUM_LOTS = 3
QTY = LOT_SIZE * NUM_LOTS

TARGET_PCT = 0.95

# Zerodha F&O Options Charges (per executed order / on premium)
BROKERAGE_PER_ORDER = 20          # Flat Rs 20 per executed order
STT_SELL_PCT = 0.001              # 0.1% on sell side premium
TXN_CHARGE_PCT = 0.0003553        # 0.03553% NSE transaction charge on premium
SEBI_PER_CRORE = 10               # Rs 10 per crore turnover
GST_PCT = 0.18                    # 18% on (brokerage + txn + SEBI)
STAMP_BUY_PCT = 0.00003           # 0.003% on buy side

TRADE_START = dtime(9, 15)
EXPIRY_EXIT = dtime(15, 15)       # Only force exit on expiry day

CAPITAL = 300_000

# ============================================================================
# SEBI MARGIN SIMULATION PARAMETERS
# ============================================================================
# Based on Zerodha broker data for 5-lot NIFTY 500pt credit spread (NRML):
#   Normal day spread margin  : Rs 59,000 - 68,000  (use 68,000 worst case)
#   Expiry day spread margin  : Rs 89,000 - 98,000  (use 98,000 worst case)
#   Difference ~Rs 30K = effective ELM on hedged spread at Nifty ~24,000
#
# ELM Rule (SEBI, effective 20-Nov-2024, Zerodha confirmed):
#   2% x spot x lot_size x num_lots on expiry day short legs, even if hedged.
#   Scales proportionally with Nifty spot.
#
# SEBI Peak Margin Rule:
#   4 random snapshots/day. 10% buffer covers intraday MTM swings.
#   Simulated at: 10:00, 11:30, 13:00, 14:30 IST.
#
# Set SIMULATE_MARGIN = False to skip all margin checks (old behaviour).

SIMULATE_MARGIN        = True

MARGIN_BASE_SPOT       = 24_000   # Nifty level broker margins were calibrated at
MARGIN_NORMAL_BASE     = 68_000   # Normal day spread margin at base spot
MARGIN_ELM_EFFECTIVE   = 30_000   # ELM increment on hedged spread at base spot
                                   # (observed: 98K - 68K = 30K at Nifty 24K)
ELM_RAW_PCT            = 0.02     # 2% x spot x lot_size x num_lots (naked ELM formula)
                                   # Used for logging only — not added on top of broker calc

MARGIN_PEAK_BUFFER     = 1.10     # 10% buffer for SEBI 4-snapshot peak margin rule

PEAK_SNAPSHOT_TIMES    = [        # Simulated SEBI intraday peak margin check times
    dtime(10,  0),
    dtime(11, 30),
    dtime(13,  0),
    dtime(14, 30),
]


# ============================================================================
# SUPERTREND (Wilder's RMA smoothing, alpha=1/period)
# ============================================================================
def compute_supertrend(highs, lows, closes, period, multiplier):
    n = len(closes)
    tr = np.zeros(n)
    tr[0] = highs[0] - lows[0]
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))

    alpha = 1.0 / period
    atr = np.zeros(n)
    if n >= period:
        atr[period - 1] = np.mean(tr[:period])
        for i in range(period, n):
            atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    else:
        # Not enough bars for a proper ATR — use mean of available true ranges as fallback
        print(f"  WARNING: compute_supertrend received only {n} bars but period={period}. "
              f"ATR fallback applied; results may be unreliable.")
        atr[:] = np.mean(tr[:n]) if n > 0 else 0.0

    hl2 = (highs + lows) / 2.0
    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr
    direction = np.ones(n, dtype=int)
    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    for i in range(1, n):
        if upper_band[i] < final_upper[i - 1] or closes[i - 1] > final_upper[i - 1]:
            final_upper[i] = upper_band[i]
        else:
            final_upper[i] = final_upper[i - 1]

        if lower_band[i] > final_lower[i - 1] or closes[i - 1] < final_lower[i - 1]:
            final_lower[i] = lower_band[i]
        else:
            final_lower[i] = final_lower[i - 1]

        if direction[i - 1] == 1:
            direction[i] = 1 if closes[i] >= final_lower[i] else -1
        else:
            direction[i] = -1 if closes[i] <= final_upper[i] else 1

    supertrend = np.where(direction == 1, final_lower, final_upper)
    return direction, supertrend


# ============================================================================
# OPTION DATA HELPERS
# ============================================================================
class OptionDataProvider:
    """Provides real option premium data from ExpiryTrack DuckDB."""

    def __init__(self, db_path):
        self.con = duckdb.connect(db_path, read_only=True)
        self._load_contracts()
        self._load_expiries()
        self._build_contract_index()

    def _load_contracts(self):
        print("  Loading NIFTY option contracts...")
        rows = self.con.execute("""
            SELECT expired_instrument_key, trading_symbol, strike_price,
                   contract_type, expiry_date, lot_size
            FROM contracts
            WHERE instrument_key = 'NSE_INDEX|Nifty 50'
              AND contract_type IN ('CE', 'PE')
            ORDER BY expiry_date, strike_price
        """).fetchall()
        self.contracts_raw = rows
        print(f"    Loaded {len(rows):,} contracts")

    def _load_expiries(self):
        expiry_set = set()
        for row in self.contracts_raw:
            exp = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            expiry_set.add(exp)
        self.expiry_dates = sorted(expiry_set)
        print(f"    Available expiries: {len(self.expiry_dates)} "
              f"({self.expiry_dates[0]} to {self.expiry_dates[-1]})")

    def _build_contract_index(self):
        self.contract_index = {}
        for row in self.contracts_raw:
            key = row[0]
            strike = float(row[2])
            ctype = row[3]
            exp = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            self.contract_index[(strike, ctype, exp)] = key
        print(f"    Contract index built: {len(self.contract_index):,} entries")

    def get_nearest_expiry(self, trade_date):
        """Get nearest expiry >= trade_date."""
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        for exp in self.expiry_dates:
            if exp >= td:
                return exp
        return None

    def get_next_expiry(self, trade_date):
        """Get NEXT expiry AFTER the nearest one (skip current week)."""
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        found_nearest = False
        for exp in self.expiry_dates:
            if exp >= td:
                if not found_nearest:
                    found_nearest = True
                    nearest = exp
                    continue
                return exp  # This is the next one after nearest
        return None

    def get_contract_key(self, strike, option_type, expiry):
        if isinstance(expiry, (datetime, pd.Timestamp)):
            expiry = expiry.date()
        return self.contract_index.get((float(strike), option_type, expiry))

    def get_option_price(self, contract_key, timestamp, lookback_minutes=5):
        if contract_key is None:
            return None, None

        ts = timestamp.replace(second=0, microsecond=0)
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime().replace(second=0, microsecond=0)

        for offset in range(lookback_minutes + 1):
            check_ts = ts - timedelta(minutes=offset)
            row = self.con.execute("""
                SELECT close, volume
                FROM historical_data
                WHERE expired_instrument_key = ?
                  AND timestamp = ?
            """, [contract_key, check_ts]).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                return float(row[0]), check_ts

        return None, None

    def close(self):
        self.con.close()


# ============================================================================
# DATA LOADING
# ============================================================================
TRADE_DATA_START = "2024-10-01"   # Trade from 1 Oct 2024
WARMUP_START = "2024-07-01"       # 3 months warmup for SuperTrend before Oct 2024


def load_spot_data():
    print("Loading NIFTY spot data from ExpiryTrack...")
    con = duckdb.connect(EXPITRACK_DB, read_only=True)

    # Load from WARMUP_START to ensure SuperTrend has enough bars
    df = con.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM candle_data
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND interval = '1minute'
          AND timestamp >= '{WARMUP_START}'
        ORDER BY timestamp
    """).fetchdf()
    con.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df = df[~df.index.duplicated(keep="first")]
    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)
    df["volume"] = df["volume"].astype(int)
    print(f"  Loaded {len(df):,} 1-min bars ({df.index[0]} to {df.index[-1]})")

    df_5m = df.resample(TIMEFRAME).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume": "sum"
    }).dropna()
    print(f"  Resampled to {len(df_5m):,} 5-min bars")
    return df_5m


# ============================================================================
# SIMULATION - OPTIMIZED OVERNIGHT
# ============================================================================
# ============================================================================
# SEBI MARGIN ENGINE
# ============================================================================
def compute_margin(spot, is_expiry_day):
    """
    Estimate SEBI spread margin for a 5-lot NIFTY 500pt credit spread.

    Scales linearly with Nifty spot from calibrated broker data:
      - Normal day : MARGIN_NORMAL_BASE  × (spot / MARGIN_BASE_SPOT)
      - Expiry day : normal + MARGIN_ELM_EFFECTIVE × (spot / MARGIN_BASE_SPOT)

    The ELM effective on a HEDGED spread is ~Rs 30K at Nifty 24K (broker
    observed). This is much less than the naked-short ELM because the buy
    leg offsets a portion of the exchange-calculated ELM requirement.

    Returns:
        margin       (float) raw broker margin
        margin_req   (float) margin × MARGIN_PEAK_BUFFER  (what you need in account)
        elm_component (float) ELM portion for expiry days (logging only)
        elm_raw      (float) 2% × spot × lot_size × num_lots (naked ELM for reference)
    """
    scale         = spot / MARGIN_BASE_SPOT
    normal        = MARGIN_NORMAL_BASE * scale
    elm_component = (MARGIN_ELM_EFFECTIVE * scale) if is_expiry_day else 0.0
    margin        = normal + elm_component
    margin_req    = margin * MARGIN_PEAK_BUFFER
    elm_raw       = ELM_RAW_PCT * spot * LOT_SIZE * NUM_LOTS  # naked ELM (reference)
    return margin, margin_req, elm_component, elm_raw


def run_simulation(df, provider, st_period, st_mult, spread_width):
    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    n = len(close)

    st_dir, st_val = compute_supertrend(high, low, close, st_period, st_mult)

    # Find the first bar index >= TRADE_DATA_START (skip warmup period)
    trade_start_date = pd.Timestamp(TRADE_DATA_START).date()
    start_idx = 1
    for idx in range(1, n):
        if df.index[idx].date() >= trade_start_date:
            start_idx = idx
            break
    print(f"  Warmup bars: {start_idx} | Trading from bar {start_idx} ({df.index[start_idx]})")

    trades = []
    total_pnl = 0
    in_trade = False
    trade_data = {}

    skipped_no_contract = 0
    skipped_no_price = 0
    skipped_negative_credit = 0
    skipped_used_next_expiry = 0  # entries that used next-week expiry on expiry day
    skipped_no_exit_price = 0
    target_reentries = 0
    pending_flip_entries = 0
    total_signals = 0
    pending_flip = None  # Stores flip direction for pre-window or post-window flips
    reentry_when_flat = 0  # Entries triggered by daily re-entry when flat logic

    # ---- SEBI Margin simulation state ----
    running_pnl          = 0.0    # cumulative P&L so far this simulation
    skipped_margin       = 0      # trades skipped due to insufficient capital
    peak_violations      = 0      # SEBI snapshot times where equity < margin_req
    peak_violation_dates = []     # (date, time, equity, margin_req) for each breach
    margin_log           = []     # per-trade margin record for reporting
    # Track which snapshot times have been checked today (reset each calendar day)
    _last_snapshot_date  = None
    _snapshots_checked   = set()

    # Debug: log all entry failures after this date
    DEBUG_AFTER = pd.Timestamp("2026-03-12").date()

    def _try_enter_spread(bar_time, bar_date, spot, direction, provider, spread_width):
        """Try to enter a new spread. Returns (trade_data, used_next_expiry) or (None, False)."""
        nonlocal skipped_no_contract, skipped_no_price, skipped_negative_credit, \
            skipped_used_next_expiry, skipped_margin

        atm_strike = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL

        # On expiry day, use NEXT expiry
        nearest_expiry = provider.get_nearest_expiry(bar_time)
        is_expiry_day = nearest_expiry is not None and bar_date == nearest_expiry
        if is_expiry_day:
            expiry = provider.get_next_expiry(bar_time)
            if expiry is not None:
                skipped_used_next_expiry += 1
            else:
                if bar_date > DEBUG_AFTER:
                    print(f"  [DEBUG] {bar_time} | FAIL: expiry_day=True, no next expiry after {nearest_expiry}")
                skipped_no_contract += 1
                return None, False
        else:
            expiry = nearest_expiry

        if expiry is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no nearest expiry found for {bar_date}")
            skipped_no_contract += 1
            return None, False

        if direction == "BULLISH":
            sell_strike = atm_strike
            buy_strike = atm_strike - spread_width
            opt_type = "PE"
            spread_type = "BULL_PUT"
        else:
            sell_strike = atm_strike
            buy_strike = atm_strike + spread_width
            opt_type = "CE"
            spread_type = "BEAR_CALL"

        sell_key = provider.get_contract_key(sell_strike, opt_type, expiry)
        buy_key = provider.get_contract_key(buy_strike, opt_type, expiry)

        if sell_key is None or buy_key is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no contract key | "
                      f"expiry={expiry} spot={spot:.0f} atm={atm_strike} {opt_type} "
                      f"sell={sell_strike}({'MISSING' if sell_key is None else 'OK'}) "
                      f"buy={buy_strike}({'MISSING' if buy_key is None else 'OK'})")
            skipped_no_contract += 1
            return None, False

        sell_premium, _ = provider.get_option_price(sell_key, bar_time)
        buy_premium, _ = provider.get_option_price(buy_key, bar_time)

        if sell_premium is None or buy_premium is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no price data | "
                      f"expiry={expiry} {opt_type} "
                      f"sell={sell_strike}({'MISSING' if sell_premium is None else f'{sell_premium:.2f}'}) "
                      f"buy={buy_strike}({'MISSING' if buy_premium is None else f'{buy_premium:.2f}'})")
            skipped_no_price += 1
            return None, False

        net_credit = sell_premium - buy_premium
        if net_credit <= 0:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: negative/zero credit | "
                      f"sell_prem={sell_premium:.2f} buy_prem={buy_premium:.2f} credit={net_credit:.2f}")
            skipped_negative_credit += 1
            return None, False

        # ---- SEBI Margin check before accepting this entry ----
        equity_now = CAPITAL + running_pnl
        margin_at_entry, margin_req_at_entry, elm_comp, elm_raw = compute_margin(spot, is_expiry_day)

        if SIMULATE_MARGIN:
            if equity_now < margin_req_at_entry:
                if bar_date > DEBUG_AFTER:
                    print(f"  [DEBUG] {bar_time} | FAIL: insufficient margin | "
                          f"equity={equity_now:.0f} required={margin_req_at_entry:.0f}")
                skipped_margin += 1
                return None, False

        td = {
            "spread_type": spread_type,
            "entry_time": bar_time,
            "entry_spot": spot,
            "atm_strike": atm_strike,
            "sell_strike": sell_strike,
            "buy_strike": buy_strike,
            "opt_type": opt_type,
            "sell_key": sell_key,
            "buy_key": buy_key,
            "sell_premium": sell_premium,
            "buy_premium": buy_premium,
            "net_credit": net_credit,
            "expiry": expiry,
            "is_next_expiry": is_expiry_day,
            # Margin info recorded at entry for reporting
            "equity_at_entry":  round(equity_now),
            "margin_at_entry":  round(margin_at_entry),
            "margin_req":       round(margin_req_at_entry),
            "elm_component":    round(elm_comp),
            "elm_raw":          round(elm_raw),
            "margin_util_pct":  round(margin_at_entry / equity_now * 100, 1) if equity_now > 0 else 0,
        }
        return td, is_expiry_day

    for i in range(start_idx, n):
        bar_time = df.index[i]
        t = bar_time.time()
        bar_date = bar_time.date() if isinstance(bar_time, (datetime, pd.Timestamp)) else bar_time

        # ============================================================
        # SEBI PEAK MARGIN SNAPSHOT CHECK (4 times per day)
        # For any open overnight position, check equity >= margin_req
        # at each of the 4 simulated SEBI snapshot times.
        # ============================================================
        if SIMULATE_MARGIN and in_trade:
            if bar_date != _last_snapshot_date:
                _last_snapshot_date = bar_date
                _snapshots_checked = set()

            if t in PEAK_SNAPSHOT_TIMES and t not in _snapshots_checked:
                _snapshots_checked.add(t)
                spot_now = close[i]
                nearest_exp = provider.get_nearest_expiry(bar_time)
                is_exp = nearest_exp is not None and bar_date == nearest_exp
                _, margin_req, elm_c, _ = compute_margin(spot_now, is_exp)
                equity_now = CAPITAL + running_pnl
                if equity_now < margin_req:
                    peak_violations += 1
                    peak_violation_dates.append({
                        "date": bar_date, "time": t,
                        "equity": round(equity_now),
                        "margin_req": round(margin_req),
                        "shortfall": round(margin_req - equity_now),
                        "is_expiry": is_exp,
                    })

        # ============================================================
        # POST-EXPIRY FORCE CLOSE
        # If bar_date is PAST the option expiry, the option has already
        # expired and will have no more price data. Retroactively close
        # at the last available price on expiry day (walk back from 15:29).
        # ============================================================
        if in_trade and bar_date > trade_data["expiry"]:
            expiry = trade_data["expiry"]
            exit_pnl = None
            # Try from 15:29 down to 14:45 on expiry day
            for hh, mm in [(15,29),(15,25),(15,20),(15,15),(15,10),(15,5),(15,0),(14,55),(14,45)]:
                candidate = pd.Timestamp(datetime.combine(expiry, dtime(hh, mm)))
                exit_pnl = _exit_trade(provider, trade_data, candidate, "Expiry")
                if exit_pnl is not None:
                    break
            if exit_pnl is not None:
                trades.append(exit_pnl)
                total_pnl += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]  # track for margin checks
            else:
                print(f"  WARNING: Could not price expired trade (expiry={expiry}) — trade dropped!")
                skipped_no_exit_price += 1
            in_trade = False
            # Do NOT continue — fall through so we can enter a new trade this bar

        # ============================================================
        # EXPIRY DAY EXIT (only forced exit - must close before expiry)
        # ============================================================
        if in_trade and t >= EXPIRY_EXIT:
            expiry_date = trade_data["expiry"]
            if bar_date == expiry_date:
                exit_pnl = _exit_trade(provider, trade_data, bar_time, "Expiry")
                if exit_pnl is not None:
                    trades.append(exit_pnl)
                    total_pnl += exit_pnl["total_pnl"]
                    running_pnl += exit_pnl["total_pnl"]  # track for margin checks
                    in_trade = False
                    continue
                else:
                    # No price available at this bar — keep in_trade=True, retry next bar
                    skipped_no_exit_price += 1
                    continue

        # ============================================================
        # DETECT FLIPS AT ANY TIME (for exits and pending entries)
        # ============================================================
        any_flip = None
        if i > 0:
            if st_dir[i] == 1 and st_dir[i - 1] == -1:
                any_flip = "BULLISH"
            elif st_dir[i] == -1 and st_dir[i - 1] == 1:
                any_flip = "BEARISH"

        # ============================================================
        # IN-TRADE MONITORING (runs across days - no EOD exit!)
        # ============================================================
        if in_trade:
            spot = close[i]

            # SuperTrend reversal exit (checked ANY time)
            if any_flip:
                should_exit = (
                    (trade_data["spread_type"] == "BULL_PUT" and any_flip == "BEARISH") or
                    (trade_data["spread_type"] == "BEAR_CALL" and any_flip == "BULLISH")
                )
                if should_exit:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Reversal")
                    if exit_pnl is not None:
                        trades.append(exit_pnl)
                        total_pnl += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]  # track for margin checks
                        in_trade = False

                        # IMMEDIATE RE-ENTRY in flip direction
                        new_td, entered = _try_enter_spread(
                            bar_time, bar_date, spot, any_flip, provider, spread_width)
                        if new_td is not None:
                            trade_data = new_td
                            in_trade = True
                            pending_flip = None
                        else:
                            pending_flip = any_flip
                        continue  # Skip further checks on this bar
                    else:
                        # No price at exit — keep position open, note the failure
                        skipped_no_exit_price += 1
                        continue

            # Max loss check (use actual trade strikes, not recomputed)
            if trade_data["spread_type"] == "BULL_PUT":
                if spot <= trade_data["buy_strike"]:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "MaxLoss")
                    if exit_pnl is not None:
                        trades.append(exit_pnl)
                        total_pnl += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]  # track for margin checks
                        in_trade = False
                        continue
                    else:
                        skipped_no_exit_price += 1
                        continue
            elif trade_data["spread_type"] == "BEAR_CALL":
                if spot >= trade_data["buy_strike"]:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "MaxLoss")
                    if exit_pnl is not None:
                        trades.append(exit_pnl)
                        total_pnl += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]  # track for margin checks
                        in_trade = False
                        continue
                    else:
                        skipped_no_exit_price += 1
                        continue

            # Target check every 3 bars
            if i % 3 == 0:
                sell_price, _ = provider.get_option_price(
                    trade_data["sell_key"], bar_time)
                buy_price, _ = provider.get_option_price(
                    trade_data["buy_key"], bar_time)
                if sell_price is not None and buy_price is not None:
                    current_spread_value = sell_price - buy_price
                    entry_credit = trade_data["net_credit"]
                    if entry_credit > 0 and (entry_credit - current_spread_value) >= entry_credit * TARGET_PCT:
                        exit_pnl = _exit_trade(provider, trade_data, bar_time, "Target")
                        if exit_pnl is not None:
                            trades.append(exit_pnl)
                            total_pnl += exit_pnl["total_pnl"]
                            running_pnl += exit_pnl["total_pnl"]  # track for margin checks
                            in_trade = False

                            # ---- RE-ENTRY AFTER TARGET if trend continues ----
                            curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
                            new_td, entered = _try_enter_spread(
                                bar_time, bar_date, spot, curr_direction, provider, spread_width)
                            if new_td is not None:
                                trade_data = new_td
                                in_trade = True
                                target_reentries += 1
                            continue
                        else:
                            # Can't get exit price — keep position open, retry next bar
                            skipped_no_exit_price += 1

        # ============================================================
        # PRE-WINDOW FLIP (not in trade): immediate entry
        # ============================================================
        if any_flip and not in_trade:
            total_signals += 1
            spot = close[i]
            new_td, entered = _try_enter_spread(
                bar_time, bar_date, spot, any_flip, provider, spread_width)
            if new_td is not None:
                trade_data = new_td
                in_trade = True
                pending_flip = None
            else:
                # Save as pending if entry failed (no contract/price at this time)
                pending_flip = any_flip

        # ============================================================
        # PENDING FLIP: Execute when data becomes available
        # ============================================================
        if pending_flip and not in_trade and t >= TRADE_START:
            spot = close[i]
            new_td, entered = _try_enter_spread(
                bar_time, bar_date, spot, pending_flip, provider, spread_width)
            if new_td is not None:
                trade_data = new_td
                in_trade = True
                pending_flip_entries += 1
                pending_flip = None
            elif t >= dtime(9, 45):
                # Give up after 09:45 if still can't enter
                pending_flip = None

        # ============================================================
        # RE-ENTRY WHEN FLAT: If not in trade, enter in the current
        # ST direction at 09:15 each day.
        # This handles the case where ST holds a trend for weeks
        # without flipping — we stay invested instead of sitting flat.
        # Only triggers once per day (at 09:15 bar) to avoid spamming.
        #
        # FIX: Also clears any stale pending_flip from a previous day.
        # Without this, a pending_flip set on (e.g.) expiry day that
        # could never be filled would block re-entry on subsequent days
        # because the old code required `not pending_flip` to be True.
        # Example: Mar 12 reversal → pending_flip set → Mar 13 expiry
        # day next-expiry = None → flip never executed → pending_flip
        # blocks the Mar 16 and Mar 17 09:15 re-entry → NO TRADES.
        # ============================================================
        if not in_trade and t == TRADE_START:
            # Clear any stale pending_flip from a previous day — it
            # will never succeed if it hasn't by the next morning.
            if pending_flip is not None:
                pending_flip = None
            spot = close[i]
            curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
            new_td, entered = _try_enter_spread(
                bar_time, bar_date, spot, curr_direction, provider, spread_width)
            if new_td is not None:
                trade_data = new_td
                in_trade = True
                reentry_when_flat += 1

    # Force close any open position at end of data
    if in_trade:
        # Try the last bar first, then walk back up to 5 bars to find a valid exit price
        exit_pnl = None
        for lookback in range(min(5, len(df))):
            candidate_bar = df.index[-(1 + lookback)]
            exit_pnl = _exit_trade(provider, trade_data, candidate_bar, "DataEnd")
            if exit_pnl is not None:
                break
        if exit_pnl is not None:
            trades.append(exit_pnl)
            total_pnl += exit_pnl["total_pnl"]
            running_pnl += exit_pnl["total_pnl"]  # track for margin checks
        else:
            print("  WARNING: Could not price open position at DataEnd — trade dropped!")
            skipped_no_exit_price += 1
        in_trade = False

    print(f"\n  Total signals: {total_signals}")
    print(f"  Pending flip entries (entered on next available bar, gave up after 09:45): {pending_flip_entries}")
    print(f"  Re-entries when flat (daily 09:15 re-entry, no flip): {reentry_when_flat}")
    print(f"  Target re-entries (trend continued): {target_reentries}")
    print(f"  Entered using next-week expiry (expiry day): {skipped_used_next_expiry}")
    print(f"  Skipped - no contract: {skipped_no_contract}")
    print(f"  Skipped - no price data: {skipped_no_price}")
    print(f"  Skipped - negative credit: {skipped_negative_credit}")
    print(f"  Dropped - no exit price: {skipped_no_exit_price}")
    if SIMULATE_MARGIN:
        print(f"  Skipped - insufficient margin: {skipped_margin}")
        print(f"  SEBI peak margin violations: {peak_violations}")
    print(f"  Executed trades: {len(trades)}")
    return trades, total_pnl, peak_violations, peak_violation_dates


def _exit_trade(provider, trade_data, exit_time, reason):
    sell_exit, _ = provider.get_option_price(trade_data["sell_key"], exit_time)
    buy_exit, _ = provider.get_option_price(trade_data["buy_key"], exit_time)

    if sell_exit is None or buy_exit is None:
        return None

    exit_spread_value = sell_exit - buy_exit
    pnl_per_unit = trade_data["net_credit"] - exit_spread_value
    gross_pnl = pnl_per_unit * QTY

    # --- Full Zerodha F&O Options Charges ---
    sell_entry_prem = trade_data["sell_premium"]
    buy_entry_prem = trade_data["buy_premium"]

    brokerage = BROKERAGE_PER_ORDER * 4
    stt = STT_SELL_PCT * (sell_entry_prem * QTY + buy_exit * QTY)
    total_turnover = (sell_entry_prem + buy_entry_prem + sell_exit + buy_exit) * QTY
    txn_charges = TXN_CHARGE_PCT * total_turnover
    sebi = SEBI_PER_CRORE * total_turnover / 1e7
    gst = GST_PCT * (brokerage + txn_charges + sebi)
    stamp = STAMP_BUY_PCT * (buy_entry_prem * QTY + sell_exit * QTY)

    total_charges = brokerage + stt + txn_charges + sebi + gst + stamp
    total_pnl = gross_pnl - total_charges

    # Calculate days held
    entry_date = trade_data["entry_time"]
    if isinstance(entry_date, pd.Timestamp):
        entry_date = entry_date.to_pydatetime()
    exit_dt = exit_time
    if isinstance(exit_dt, pd.Timestamp):
        exit_dt = exit_dt.to_pydatetime()
    days_held = (exit_dt.date() - entry_date.date()).days

    return {
        "entry_time": trade_data["entry_time"],
        "exit_time": exit_time,
        "type": trade_data["spread_type"],
        "entry_spot": trade_data["entry_spot"],
        "sell_strike": trade_data["sell_strike"],
        "buy_strike": trade_data["buy_strike"],
        "sell_entry": trade_data["sell_premium"],
        "buy_entry": trade_data["buy_premium"],
        "sell_exit": sell_exit,
        "buy_exit": buy_exit,
        "net_credit": trade_data["net_credit"],
        "exit_spread": exit_spread_value,
        "pnl_per_unit": pnl_per_unit,
        "gross_pnl": gross_pnl,
        "charges": total_charges,
        "total_pnl": total_pnl,
        "exit_reason": reason,
        "expiry": trade_data["expiry"],
        "days_held": days_held,
        "overnight": days_held > 0,
        "is_next_expiry": trade_data.get("is_next_expiry", False),
        # Margin fields recorded at entry (pass-through from trade_data)
        "equity_at_entry":  trade_data.get("equity_at_entry", 0),
        "margin_at_entry":  trade_data.get("margin_at_entry", 0),
        "margin_req":       trade_data.get("margin_req", 0),
        "elm_component":    trade_data.get("elm_component", 0),
        "elm_raw":          trade_data.get("elm_raw", 0),
        "margin_util_pct":  trade_data.get("margin_util_pct", 0),
    }


def format_expiry_tag(expiry_date):
    if isinstance(expiry_date, str):
        expiry_date = pd.to_datetime(expiry_date)
    if hasattr(expiry_date, 'strftime'):
        return expiry_date.strftime("%d %b %y").upper()
    return str(expiry_date)


def build_contract_name(strike, opt_type, expiry):
    exp_tag = format_expiry_tag(expiry)
    return f"NIFTY {int(strike)} {opt_type} {exp_tag}"


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("=" * 100)
    print("SUPERTREND CREDIT SPREAD BACKTEST - OPTIMIZED OVERNIGHT")
    print("=" * 100)
    print(f"  Data Source:  ExpiryTrack DuckDB (actual 1-min option OHLC)")
    print(f"  SuperTrend:   Period={ST_PERIOD}, Multiplier={ST_MULTIPLIER}")
    print(f"  Timeframe:    {TIMEFRAME}")
    print(f"  Spread:       {SPREAD_WIDTH}pt | Strike Interval: {STRIKE_INTERVAL}")
    print(f"  Position:     {NUM_LOTS} lots x {LOT_SIZE} = {QTY} qty")
    print(f"  Target:       {TARGET_PCT*100:.0f}% premium decay")
    print(f"  Capital:      Rs {CAPITAL:,}")
    print(f"  HOLD MODE:    OVERNIGHT (no EOD exit)")
    print(f"  ENTRY MODE:   Immediate on any flip (no window restriction)")
    print(f"  OPTIMIZATION: Expiry day -> use NEXT week expiry")
    print()

    df_spot = load_spot_data()

    print("\nInitializing option data provider...")
    provider = OptionDataProvider(EXPITRACK_DB)

    configs = [
        ("ST(100, 2.0) — Live script match", 100, 2.0, 500),
        ("ST(80, 3.6)  — Previous best",      80, 3.6, 500),
        ("ST(35, 5.0)  — Comparison",          35, 5.0, 500),
    ]

    all_results = []

    for name, period, mult, sw in configs:
        print(f"\n{'='*100}")
        print(f"RUNNING: {name} | Spread: {sw}pt | OPTIMIZED OVERNIGHT")
        print(f"{'='*100}")

        trades, total_pnl, peak_viol, peak_viol_dates = run_simulation(df_spot, provider, period, mult, sw)

        if len(trades) == 0:
            print("  No trades executed.")
            all_results.append({
                "Config": name, "Trades": 0, "WR%": 0, "PF": 0,
                "Total_PnL": 0, "ROI%": 0,
            })
            continue

        tdf = pd.DataFrame(trades)
        wins = tdf[tdf["total_pnl"] > 0]
        losses = tdf[tdf["total_pnl"] <= 0]
        n_trades = len(tdf)
        wr = len(wins) / n_trades * 100
        avg_win = wins["total_pnl"].mean() if len(wins) > 0 else 0
        avg_loss = losses["total_pnl"].mean() if len(losses) > 0 else 0
        gross_win = wins["total_pnl"].sum() if len(wins) > 0 else 0
        gross_loss = abs(losses["total_pnl"].sum()) if len(losses) > 0 else 1
        pf = gross_win / gross_loss if gross_loss > 0 else 99
        roi = total_pnl / CAPITAL * 100

        overnight_trades = tdf[tdf["overnight"]]
        intraday_trades = tdf[~tdf["overnight"]]
        next_exp_trades = tdf[tdf["is_next_expiry"]]

        all_results.append({
            "Config": name, "Trades": n_trades,
            "WR%": round(wr, 1), "PF": round(pf, 2),
            "Total_PnL": round(total_pnl), "ROI%": round(roi, 1),
            "Avg_Win": round(avg_win), "Avg_Loss": round(avg_loss),
            "Overnight": len(overnight_trades),
            "NextExp": len(next_exp_trades),
            "Avg_Days": round(tdf["days_held"].mean(), 1),
            "PeakViol": peak_viol if SIMULATE_MARGIN else "OFF",
        })

        print(f"\n  RESULTS: {name}")
        print(f"  {'='*70}")
        print(f"  Total Trades:       {n_trades}")
        print(f"  Winners:            {len(wins)} ({wr:.1f}%)")
        print(f"  Losers:             {len(losses)} ({100-wr:.1f}%)")
        print(f"  Avg Winner:         Rs {avg_win:+,.0f}")
        print(f"  Avg Loser:          Rs {avg_loss:+,.0f}")
        print(f"  Profit Factor:      {pf:.2f}")
        print(f"  Total P&L:          Rs {total_pnl:+,.0f}")
        print(f"  ROI on {CAPITAL/100000:.0f}L:        {roi:+.1f}%")
        print(f"  Max Drawdown:       Rs {(tdf['total_pnl'].cumsum().cummax() - tdf['total_pnl'].cumsum()).max():,.0f}")

        # Next expiry trades breakdown
        print(f"\n  EXPIRY DAY TRADES (used next expiry):")
        if len(next_exp_trades) > 0:
            ne_wr = (next_exp_trades["total_pnl"] > 0).mean() * 100
            print(f"    {len(next_exp_trades)} trades | WR: {ne_wr:.1f}% | "
                  f"Avg P&L: Rs {next_exp_trades['total_pnl'].mean():+,.0f} | "
                  f"Total: Rs {next_exp_trades['total_pnl'].sum():+,.0f}")
        else:
            print(f"    None")

        # Overnight vs intraday breakdown
        print(f"\n  OVERNIGHT vs INTRADAY:")
        print(f"  {'':>20s} {'Trades':>7s} {'WR%':>6s} {'Avg P&L':>10s} {'Total P&L':>12s} {'Avg Days':>9s}")
        print(f"  {'-'*70}")
        if len(overnight_trades) > 0:
            o_wr = (overnight_trades["total_pnl"] > 0).mean() * 100
            print(f"  {'Overnight (held)':>20s} {len(overnight_trades):>7d} {o_wr:>6.1f} "
                  f"{overnight_trades['total_pnl'].mean():>+10,.0f} "
                  f"{overnight_trades['total_pnl'].sum():>+12,.0f} "
                  f"{overnight_trades['days_held'].mean():>9.1f}")
        if len(intraday_trades) > 0:
            i_wr = (intraday_trades["total_pnl"] > 0).mean() * 100
            print(f"  {'Intraday (same day)':>20s} {len(intraday_trades):>7d} {i_wr:>6.1f} "
                  f"{intraday_trades['total_pnl'].mean():>+10,.0f} "
                  f"{intraday_trades['total_pnl'].sum():>+12,.0f} "
                  f"{intraday_trades['days_held'].mean():>9.1f}")

        # Days held distribution
        print(f"\n  DAYS HELD DISTRIBUTION:")
        for d in sorted(tdf["days_held"].unique()):
            sub = tdf[tdf["days_held"] == d]
            dwr = (sub["total_pnl"] > 0).mean() * 100
            label = "same day" if d == 0 else f"{d} day{'s' if d > 1 else ''}"
            print(f"    {label:>10s}: {len(sub):4d} trades | WR: {dwr:5.1f}% | "
                  f"Avg: Rs {sub['total_pnl'].mean():+,.0f} | "
                  f"Total: Rs {sub['total_pnl'].sum():+,.0f}")

        # Exit reasons
        print(f"\n  Exit Reasons:")
        for reason, grp in tdf.groupby("exit_reason"):
            grp_wr = (grp["total_pnl"] > 0).mean() * 100
            print(f"    {reason:10s}: {len(grp):3d} trades | "
                  f"WR: {grp_wr:5.1f}% | Avg Days: {grp['days_held'].mean():.1f} | "
                  f"P&L: Rs {grp['total_pnl'].sum():+,.0f}")

        for stype in ["BULL_PUT", "BEAR_CALL"]:
            sub = tdf[tdf["type"] == stype]
            if len(sub) > 0:
                swr = (sub["total_pnl"] > 0).mean() * 100
                print(f"\n  {stype}: {len(sub)} trades | WR: {swr:.1f}% | "
                      f"P&L: Rs {sub['total_pnl'].sum():+,.0f}")

        # Premium analysis
        print(f"\n  PREMIUM ANALYSIS:")
        print(f"    Avg Sell Premium:  Rs {tdf['sell_entry'].mean():.2f}")
        print(f"    Avg Buy Premium:   Rs {tdf['buy_entry'].mean():.2f}")
        print(f"    Avg Net Credit:    Rs {tdf['net_credit'].mean():.2f}")
        print(f"    Avg Exit Spread:   Rs {tdf['exit_spread'].mean():.2f}")
        print(f"    Avg P&L/unit:      Rs {tdf['pnl_per_unit'].mean():.2f}")

        # Trade log
        print(f"\n  TRADE LOG:")
        print(f"  {'#':>4s} {'Entry':>19s} {'Exit':>19s} {'Days':>4s} {'NxExp':>5s} {'Type':>10s} "
              f"{'Sell Contract':>28s} {'Buy Contract':>28s} "
              f"{'Credit':>7s} {'Reason':>8s} {'P&L':>10s}")
        print(f"  {'-'*155}")
        for idx_t, t in tdf.iterrows():
            entry_ts = pd.Timestamp(t['entry_time'])
            exit_ts = pd.Timestamp(t['exit_time'])
            opt_type = "PE" if t["type"] == "BULL_PUT" else "CE"
            sell_contract = build_contract_name(t["sell_strike"], opt_type, t["expiry"])
            buy_contract = build_contract_name(t["buy_strike"], opt_type, t["expiry"])
            ne_flag = "YES" if t.get("is_next_expiry", False) else ""
            print(f"  {idx_t+1:>4d} "
                  f"{entry_ts.strftime('%Y-%m-%d %H:%M'):>19s} "
                  f"{exit_ts.strftime('%Y-%m-%d %H:%M'):>19s} "
                  f"{t['days_held']:>4.0f} "
                  f"{ne_flag:>5s} "
                  f"{t['type']:>10s} "
                  f"{sell_contract:>28s} {buy_contract:>28s} "
                  f"{t['net_credit']:>7.2f} "
                  f"{t['exit_reason']:>8s} "
                  f"{t['total_pnl']:>+10,.0f}")

        # Monthly P&L
        print(f"\n  MONTHLY P&L:")
        tdf["entry_time"] = pd.to_datetime(tdf["entry_time"])
        tdf["month"] = tdf["entry_time"].dt.to_period("M")
        cumulative = 0
        for month, grp in tdf.groupby("month"):
            m_pnl = grp["total_pnl"].sum()
            cumulative += m_pnl
            m_wr = (grp["total_pnl"] > 0).mean() * 100
            m_overnight = (grp["days_held"] > 0).sum()
            m_next_exp = grp["is_next_expiry"].sum()
            print(f"    {month} | {len(grp):3d} trades ({m_overnight} overnight, {m_next_exp} next-exp) | "
                  f"WR: {m_wr:5.1f}% | "
                  f"P&L: Rs {m_pnl:+10,.0f} | Cumulative: Rs {cumulative:+10,.0f}")

        # ---- SEBI MARGIN SIMULATION REPORT ----
        if SIMULATE_MARGIN and "equity_at_entry" in tdf.columns:
            tdf["equity_at_entry"] = pd.to_numeric(tdf["equity_at_entry"], errors="coerce").fillna(0)
            tdf["margin_req"]      = pd.to_numeric(tdf["margin_req"],      errors="coerce").fillna(0)
            tdf["margin_util_pct"] = pd.to_numeric(tdf["margin_util_pct"], errors="coerce").fillna(0)
            tdf["elm_component"]   = pd.to_numeric(tdf["elm_component"],   errors="coerce").fillna(0)
            tdf["elm_raw"]         = pd.to_numeric(tdf["elm_raw"],         errors="coerce").fillna(0)

            expiry_day_trades = tdf[tdf["elm_component"] > 0]
            normal_day_trades = tdf[tdf["elm_component"] == 0]

            print(f"\n  SEBI MARGIN SIMULATION REPORT")
            print(f"  {'='*70}")
            print(f"  Capital simulated         : Rs {CAPITAL:>10,.0f}")
            print(f"  SEBI Peak margin buffer   : {MARGIN_PEAK_BUFFER:.0%}")
            print(f"  Peak margin violations    : {peak_viol}  (times equity < margin_req at snapshot)")

            print(f"\n  NORMAL DAY MARGIN ({len(normal_day_trades)} trades):")
            if len(normal_day_trades):
                print(f"    Avg margin required     : Rs {normal_day_trades['margin_req'].mean():>10,.0f}")
                print(f"    Max margin required     : Rs {normal_day_trades['margin_req'].max():>10,.0f}")
                print(f"    Avg margin utilisation  : {normal_day_trades['margin_util_pct'].mean():.1f}% of equity")
                print(f"    Min equity at entry     : Rs {normal_day_trades['equity_at_entry'].min():>10,.0f}")

            print(f"\n  EXPIRY DAY MARGIN ({len(expiry_day_trades)} trades that were open on expiry day):")
            if len(expiry_day_trades):
                print(f"    Avg margin required     : Rs {expiry_day_trades['margin_req'].mean():>10,.0f}")
                print(f"    Max margin required     : Rs {expiry_day_trades['margin_req'].max():>10,.0f}")
                print(f"    Avg ELM on hedged spread: Rs {expiry_day_trades['elm_component'].mean():>10,.0f}")
                print(f"    Max ELM on hedged spread: Rs {expiry_day_trades['elm_component'].max():>10,.0f}")
                print(f"    Avg ELM naked reference : Rs {expiry_day_trades['elm_raw'].mean():>10,.0f}  (2% x spot x lot x lots)")
                print(f"    Avg margin utilisation  : {expiry_day_trades['margin_util_pct'].mean():.1f}% of equity")
                print(f"    Min equity at entry     : Rs {expiry_day_trades['equity_at_entry'].min():>10,.0f}")

            # Worst single capital moment (min equity at any entry across all trades)
            worst_idx = tdf["equity_at_entry"].idxmin()
            worst = tdf.loc[worst_idx]
            print(f"\n  WORST CAPITAL MOMENT (lowest equity at any trade entry):")
            print(f"    Date         : {pd.Timestamp(worst['entry_time']).date()}")
            print(f"    Equity       : Rs {worst['equity_at_entry']:>10,.0f}")
            print(f"    Margin req   : Rs {worst['margin_req']:>10,.0f}")
            print(f"    Headroom     : Rs {worst['equity_at_entry'] - worst['margin_req']:>10,.0f}"
                  f"  {'SAFE' if worst['equity_at_entry'] >= worst['margin_req'] else 'BREACHED'}")
            print(f"    Utilisation  : {worst['margin_util_pct']:.1f}% of equity")

            # Monthly margin utilisation summary
            tdf["month"] = pd.to_datetime(tdf["entry_time"]).dt.to_period("M")
            print(f"\n  MONTHLY MARGIN UTILISATION:")
            print(f"  {'Month':<10} {'Trades':>6} {'Min Equity':>12} {'Max Margin%':>12} {'Violations':>11}")
            print(f"  {'-'*56}")
            for month, grp in tdf.groupby("month"):
                eq_min   = grp["equity_at_entry"].min()
                util_max = grp["margin_util_pct"].max()
                n_viol = sum(1 for v in peak_viol_dates
                             if hasattr(v.get("date"), "year") and
                             str(pd.Period(v["date"], "M")) == str(month))
                print(f"  {str(month):<10} {len(grp):>6} "
                      f"Rs {eq_min:>9,.0f} "
                      f"{util_max:>11.1f}% "
                      f"{n_viol:>11}")

            if peak_viol > 0:
                print(f"\n  SEBI PEAK MARGIN BREACH DETAIL:")
                print(f"  {'Date':<12} {'Time':<8} {'Equity':>12} {'Required':>12} {'Shortfall':>12} {'Expiry?':>8}")
                print(f"  {'-'*60}")
                for v in peak_viol_dates[:20]:
                    print(f"  {str(v['date']):<12} {str(v['time']):<8} "
                          f"Rs {v['equity']:>9,.0f} "
                          f"Rs {v['margin_req']:>9,.0f} "
                          f"Rs {v['shortfall']:>9,.0f} "
                          f"{'YES' if v['is_expiry'] else 'no':>8}")
                if len(peak_viol_dates) > 20:
                    print(f"  ... and {len(peak_viol_dates)-20} more")
            else:
                print(f"\n  No SEBI peak margin breaches detected.")
                print(f"  Capital of Rs {CAPITAL:,} is sufficient for all {n_trades} trades.")

        # Save
        csv_path = Path(BASE_DIR) / f"optimized_trades_ST{period}_{mult}_{sw}.csv"
        tdf.drop(columns=["month"], errors="ignore").to_csv(csv_path, index=False)
        print(f"\n  Trades saved: {csv_path}")

    # Summary comparison
    print(f"\n\n{'='*100}")
    print("SUMMARY - ALL CONFIGS (OPTIMIZED OVERNIGHT)")
    print(f"{'='*100}")
    summary_df = pd.DataFrame(all_results)
    print(summary_df.to_string(index=False))

    # Plot
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig = make_subplots(
            rows=len(configs), cols=1, shared_xaxes=True,
            subplot_titles=[f"{c[0]} (Optimized Overnight)" for c in configs],
        )

        for idx, (name, period, mult, sw) in enumerate(configs):
            csv_path = Path(BASE_DIR) / f"optimized_trades_ST{period}_{mult}_{sw}.csv"
            if csv_path.exists():
                tdf = pd.read_csv(csv_path, parse_dates=["entry_time", "exit_time"])
                if len(tdf) > 0:
                    cum_pnl = tdf["total_pnl"].cumsum()
                    fig.add_trace(go.Scatter(
                        x=tdf["entry_time"], y=cum_pnl,
                        mode="lines+markers", name=name,
                        fill="tozeroy",
                    ), row=idx + 1, col=1)

        fig.update_layout(
            title="Credit Spread OPTIMIZED OVERNIGHT - REAL Option Premiums<br>"
                  "<sub>No entries after 13:30 | Expiry day -> next week expiry</sub>",
            template="plotly_dark", height=400 * len(configs), width=1200,
        )

        plot_path = Path(BASE_DIR) / "optimized_pnl.html"
        fig.write_html(str(plot_path))
        print(f"\nChart: {plot_path}")
        fig.show()
    except Exception as e:
        print(f"Plot error: {e}")

    provider.close()
    print("\nDone.")
