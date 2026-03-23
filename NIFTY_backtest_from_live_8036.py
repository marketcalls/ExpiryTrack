"""
SuperTrend Credit Spread Backtest — Converted from Live Trading Script
=======================================================================
SOURCE: supertrend_credit_spread_openalgo-2.py (ST(100, 2.0) live script)

PARITY NOTES — what was kept identical to the live script:
  - compute_supertrend()       : exact same function, no changes
  - ATR period / multiplier    : 100 / 2.0 (same as live CONFIG)
  - Timeframe                  : 5-min bars resampled from 1-min DuckDB data
  - Strike selection           : ATM sell, ATM ± spread_width buy
  - Expiry logic               : expiry day → use NEXT expiry
  - Entry logic                : immediate on flip, pending if no price
  - Exit logic                 : reversal / target (95%) / expiry (15:15)
  - Max-loss stop              : exit when spread cost >= 85% of spread width
  - Re-entry after target      : immediate in ST direction
  - Re-entry after reversal    : immediate in flip direction
  - Daily re-entry when flat   : 09:15 if no pending flip (stay invested)
  - Overnight hold             : NO forced EOD exit

BUGS FIXED vs original backtest:
  [1] Target check: every bar (was i%3 — wrong absolute alignment)
  [2] Max-loss: uses spread cost >= 85% of width (matches live _check_target)
      Old backtest used spot <= buy_strike which is too late / different logic
  [3] STT: corrected to charge on all 4 legs (entry sell, exit buy-back,
      exit sell-hedge, entry buy). Old code missed the exit buy-back STT.
  [4] Target re-entry: spot explicitly captured before in_trade set False
  [5] pending_flip cleared at 09:15 each morning (stale flip fix)

Data source: ExpiryTrack DuckDB (real 1-min option OHLC)

Usage:
    uv run python NIFTY_backtest_from_live.py
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
# STRATEGY PARAMETERS — mirrors live CONFIG exactly
# ============================================================================
ST_PERIOD      = 80
ST_MULTIPLIER  = 3.6
TIMEFRAME      = "5min"

SPREAD_WIDTH   = 500
STRIKE_INTERVAL = 50

LOT_SIZE  = 65
NUM_LOTS  = 3
QTY       = LOT_SIZE * NUM_LOTS

TARGET_PCT    = 0.95          # 95% premium decay → take profit
MAX_LOSS_PCT  = 0.85          # exit when spread cost >= 85% of spread width

TRADE_START  = dtime(9, 15)
EXPIRY_EXIT  = dtime(15, 15)  # forced exit on expiry day only

CAPITAL = 300_000

# ============================================================================
# CHARGES — Zerodha F&O (mirrors what live script implicitly incurs)
# ============================================================================
BROKERAGE_PER_ORDER = 20        # flat Rs 20 per executed order
STT_SELL_PCT        = 0.001     # 0.1% STT on sell-side premium
TXN_CHARGE_PCT      = 0.0003553 # NSE transaction charge
SEBI_PER_CRORE      = 10        # Rs 10 per crore turnover
GST_PCT             = 0.18      # 18% on brokerage + txn + sebi
STAMP_BUY_PCT       = 0.00003   # 0.003% on buy-side premium

# ============================================================================
# SEBI MARGIN SIMULATION — scaled from Zerodha broker data
# ============================================================================
SIMULATE_MARGIN      = True
MARGIN_BASE_SPOT     = 24_000
MARGIN_NORMAL_BASE   = 68_000
MARGIN_ELM_EFFECTIVE = 30_000
ELM_RAW_PCT          = 0.02
MARGIN_PEAK_BUFFER   = 1.10

PEAK_SNAPSHOT_TIMES = [dtime(10, 0), dtime(11, 30), dtime(13, 0), dtime(14, 30)]

# ============================================================================
# DATA RANGE
# ============================================================================
TRADE_DATA_START = "2024-10-01"
WARMUP_START     = "2024-07-01"   # 3 months warmup for ST(100)


# ============================================================================
# SUPERTREND — exact copy from live script (no changes)
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
        # Not enough bars — use mean of available true ranges (matches live)
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
# OPTION DATA PROVIDER
# ============================================================================
class OptionDataProvider:
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
            key   = row[0]
            strike = float(row[2])
            ctype  = row[3]
            exp    = row[4]
            if isinstance(exp, datetime):
                exp = exp.date()
            self.contract_index[(strike, ctype, exp)] = key
        print(f"    Contract index built: {len(self.contract_index):,} entries")

    def get_nearest_expiry(self, trade_date):
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        for exp in self.expiry_dates:
            if exp >= td:
                return exp
        return None

    def get_next_expiry(self, trade_date):
        td = trade_date.date() if isinstance(trade_date, (datetime, pd.Timestamp)) else trade_date
        found_nearest = False
        for exp in self.expiry_dates:
            if exp >= td:
                if not found_nearest:
                    found_nearest = True
                    continue
                return exp
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
                SELECT close FROM historical_data
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
def load_spot_data():
    print("Loading NIFTY spot data from ExpiryTrack...")
    con = duckdb.connect(EXPITRACK_DB, read_only=True)
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
# MARGIN ENGINE
# ============================================================================
def compute_margin(spot, is_expiry_day):
    scale         = spot / MARGIN_BASE_SPOT
    normal        = MARGIN_NORMAL_BASE * scale
    elm_component = (MARGIN_ELM_EFFECTIVE * scale) if is_expiry_day else 0.0
    margin        = normal + elm_component
    margin_req    = margin * MARGIN_PEAK_BUFFER
    elm_raw       = ELM_RAW_PCT * spot * LOT_SIZE * NUM_LOTS
    return margin, margin_req, elm_component, elm_raw


# ============================================================================
# EXIT TRADE
# ============================================================================
def _exit_trade(provider, trade_data, exit_time, reason, qty_override=None):
    qty = qty_override if qty_override is not None else QTY

    sell_exit, _ = provider.get_option_price(trade_data["sell_key"], exit_time)
    buy_exit, _  = provider.get_option_price(trade_data["buy_key"],  exit_time)

    if sell_exit is None or buy_exit is None:
        return None

    exit_spread_value = sell_exit - buy_exit
    pnl_per_unit      = trade_data["net_credit"] - exit_spread_value
    gross_pnl         = pnl_per_unit * qty

    # ---- Zerodha F&O charges (corrected — all 4 legs) ----
    sell_entry = trade_data["sell_premium"]
    buy_entry  = trade_data["buy_premium"]

    brokerage   = BROKERAGE_PER_ORDER * 4   # 4 orders total
    # STT on sell-side legs: entry sell + exit sell-of-hedge + exit buy-back-of-sell
    stt         = STT_SELL_PCT * (
                    sell_entry * qty     # entry: sold call/put
                  + sell_exit  * qty     # exit:  bought back the sold leg (STT on seller side)
                  + buy_exit   * qty     # exit:  sold the hedge leg
                  )
    total_turnover = (sell_entry + buy_entry + sell_exit + buy_exit) * qty
    txn_charges    = TXN_CHARGE_PCT * total_turnover
    sebi           = SEBI_PER_CRORE * total_turnover / 1e7
    gst            = GST_PCT * (brokerage + txn_charges + sebi)
    stamp          = STAMP_BUY_PCT * (buy_entry * qty + sell_exit * qty)

    total_charges = brokerage + stt + txn_charges + sebi + gst + stamp
    total_pnl     = gross_pnl - total_charges

    entry_dt = trade_data["entry_time"]
    if isinstance(entry_dt, pd.Timestamp):
        entry_dt = entry_dt.to_pydatetime()
    exit_dt = exit_time
    if isinstance(exit_dt, pd.Timestamp):
        exit_dt = exit_dt.to_pydatetime()
    days_held = (exit_dt.date() - entry_dt.date()).days

    return {
        "entry_time":     trade_data["entry_time"],
        "exit_time":      exit_time,
        "type":           trade_data["spread_type"],
        "entry_spot":     trade_data["entry_spot"],
        "sell_strike":    trade_data["sell_strike"],
        "buy_strike":     trade_data["buy_strike"],
        "sell_entry":     sell_entry,
        "buy_entry":      buy_entry,
        "sell_exit":      sell_exit,
        "buy_exit":       buy_exit,
        "net_credit":     trade_data["net_credit"],
        "exit_spread":    exit_spread_value,
        "pnl_per_unit":   pnl_per_unit,
        "gross_pnl":      gross_pnl,
        "charges":        total_charges,
        "total_pnl":      total_pnl,
        "exit_reason":    reason,
        "expiry":         trade_data["expiry"],
        "days_held":      days_held,
        "overnight":      days_held > 0,
        "is_next_expiry": trade_data.get("is_next_expiry", False),
        "qty":            qty,
        # Margin fields
        "equity_at_entry":  trade_data.get("equity_at_entry", 0),
        "margin_at_entry":  trade_data.get("margin_at_entry", 0),
        "margin_req":       trade_data.get("margin_req", 0),
        "elm_component":    trade_data.get("elm_component", 0),
        "elm_raw":          trade_data.get("elm_raw", 0),
        "margin_util_pct":  trade_data.get("margin_util_pct", 0),
    }


# ============================================================================
# SIMULATION
# ============================================================================
def run_simulation(df, provider):
    close = df["close"].values.astype(float)
    high  = df["high"].values.astype(float)
    low   = df["low"].values.astype(float)
    n     = len(close)

    st_dir, _ = compute_supertrend(high, low, close, ST_PERIOD, ST_MULTIPLIER)

    # Skip warmup bars
    trade_start_date = pd.Timestamp(TRADE_DATA_START).date()
    start_idx = 1
    for idx in range(1, n):
        if df.index[idx].date() >= trade_start_date:
            start_idx = idx
            break
    print(f"  Warmup bars: {start_idx} | Trading from bar {start_idx} ({df.index[start_idx]})")

    trades      = []
    total_pnl   = 0.0
    running_pnl = 0.0
    in_trade    = False
    trade_data  = {}

    # Counters
    skipped_no_contract    = 0
    skipped_no_price       = 0
    skipped_negative_credit = 0
    skipped_used_next_expiry = 0
    skipped_no_exit_price  = 0
    skipped_margin         = 0
    target_reentries       = 0
    pending_flip_entries   = 0
    reentry_when_flat      = 0
    total_signals          = 0
    pending_flip           = None

    # SEBI margin tracking
    peak_violations      = 0
    peak_violation_dates = []
    _last_snapshot_date  = None
    _snapshots_checked   = set()

    # ---- DEBUG: log entry failures after Mar 12 2026 ----
    DEBUG_AFTER = pd.Timestamp("2026-03-12").date()

    # ------------------------------------------------------------------
    def _try_enter(bar_time, bar_date, spot, direction):
        """
        Try to enter a credit spread. Mirrors live _enter_spread() logic.
        Returns trade_data dict or None.
        """
        nonlocal skipped_no_contract, skipped_no_price, skipped_negative_credit
        nonlocal skipped_used_next_expiry, skipped_margin

        atm_strike = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL

        nearest_expiry = provider.get_nearest_expiry(bar_time)
        is_expiry_day  = nearest_expiry is not None and bar_date == nearest_expiry

        # Mirrors live: expiry day → use NEXT expiry
        if is_expiry_day:
            expiry = provider.get_next_expiry(bar_time)
            if expiry is not None:
                skipped_used_next_expiry += 1
            else:
                if bar_date > DEBUG_AFTER:
                    print(f"  [DEBUG] {bar_time} | FAIL: expiry_day=True, "
                          f"no next expiry after {nearest_expiry}")
                skipped_no_contract += 1
                return None
        else:
            expiry = nearest_expiry

        if expiry is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no nearest expiry for {bar_date}")
            skipped_no_contract += 1
            return None

        if direction == "BULLISH":
            sell_strike = atm_strike
            buy_strike  = atm_strike - SPREAD_WIDTH
            opt_type    = "PE"
            spread_type = "BULL_PUT"
        else:
            sell_strike = atm_strike
            buy_strike  = atm_strike + SPREAD_WIDTH
            opt_type    = "CE"
            spread_type = "BEAR_CALL"

        sell_key = provider.get_contract_key(sell_strike, opt_type, expiry)
        buy_key  = provider.get_contract_key(buy_strike,  opt_type, expiry)

        if sell_key is None or buy_key is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no contract key | "
                      f"expiry={expiry} spot={spot:.0f} atm={atm_strike} {opt_type} "
                      f"sell={sell_strike}({'MISSING' if sell_key is None else 'OK'}) "
                      f"buy={buy_strike}({'MISSING' if buy_key is None else 'OK'})")
            skipped_no_contract += 1
            return None

        sell_premium, _ = provider.get_option_price(sell_key, bar_time)
        buy_premium,  _ = provider.get_option_price(buy_key,  bar_time)

        if sell_premium is None or buy_premium is None:
            if bar_date > DEBUG_AFTER:
                print(f"  [DEBUG] {bar_time} | FAIL: no price | "
                      f"expiry={expiry} {opt_type} "
                      f"sell={sell_strike}({'MISSING' if sell_premium is None else f'{sell_premium:.2f}'}) "
                      f"buy={buy_strike}({'MISSING' if buy_premium is None else f'{buy_premium:.2f}'})")
            skipped_no_price += 1
            return None

        net_credit = sell_premium - buy_premium
        if net_credit <= 0:
            skipped_negative_credit += 1
            return None

        # Margin check (mirrors live _find_max_lots logic)
        equity_now = CAPITAL + running_pnl
        margin_at_entry, margin_req_at_entry, elm_comp, elm_raw = \
            compute_margin(spot, is_expiry_day)

        if SIMULATE_MARGIN and equity_now < margin_req_at_entry:
            skipped_margin += 1
            return None

        return {
            "spread_type":    spread_type,
            "entry_time":     bar_time,
            "entry_spot":     spot,
            "atm_strike":     atm_strike,
            "sell_strike":    sell_strike,
            "buy_strike":     buy_strike,
            "opt_type":       opt_type,
            "sell_key":       sell_key,
            "buy_key":        buy_key,
            "sell_premium":   sell_premium,
            "buy_premium":    buy_premium,
            "net_credit":     net_credit,
            "expiry":         expiry,
            "is_next_expiry": is_expiry_day,
            "equity_at_entry":  round(equity_now),
            "margin_at_entry":  round(margin_at_entry),
            "margin_req":       round(margin_req_at_entry),
            "elm_component":    round(elm_comp),
            "elm_raw":          round(elm_raw),
            "margin_util_pct":  round(margin_at_entry / equity_now * 100, 1)
                                if equity_now > 0 else 0,
        }

    # ------------------------------------------------------------------
    for i in range(start_idx, n):
        bar_time = df.index[i]
        t        = bar_time.time()
        bar_date = bar_time.date() if isinstance(bar_time, (datetime, pd.Timestamp)) \
                   else bar_time

        # ── SEBI PEAK MARGIN SNAPSHOTS ────────────────────────────────
        if SIMULATE_MARGIN and in_trade:
            if bar_date != _last_snapshot_date:
                _last_snapshot_date = bar_date
                _snapshots_checked  = set()
            if t in PEAK_SNAPSHOT_TIMES and t not in _snapshots_checked:
                _snapshots_checked.add(t)
                nearest_exp = provider.get_nearest_expiry(bar_time)
                is_exp      = nearest_exp is not None and bar_date == nearest_exp
                _, margin_req, _, _ = compute_margin(close[i], is_exp)
                equity_now = CAPITAL + running_pnl
                if equity_now < margin_req:
                    peak_violations += 1
                    peak_violation_dates.append({
                        "date":       bar_date, "time": t,
                        "equity":     round(equity_now),
                        "margin_req": round(margin_req),
                        "shortfall":  round(margin_req - equity_now),
                        "is_expiry":  is_exp,
                    })

        # ── POST-EXPIRY FORCE CLOSE ───────────────────────────────────
        # Option expired without hitting 15:15 exit — close retroactively
        if in_trade and bar_date > trade_data["expiry"]:
            expiry = trade_data["expiry"]
            exit_pnl = None
            for hh, mm in [(15,29),(15,25),(15,20),(15,15),(15,10),(15,5),
                           (15,0),(14,55),(14,45)]:
                candidate = pd.Timestamp(datetime.combine(expiry, dtime(hh, mm)))
                exit_pnl  = _exit_trade(provider, trade_data, candidate, "Expiry")
                if exit_pnl:
                    break
            if exit_pnl:
                trades.append(exit_pnl)
                total_pnl   += exit_pnl["total_pnl"]
                running_pnl += exit_pnl["total_pnl"]
            else:
                print(f"  WARNING: Could not price expired trade "
                      f"(expiry={expiry}) — dropped!")
                skipped_no_exit_price += 1
            in_trade = False
            # fall through — can enter new trade this bar

        # ── EXPIRY DAY FORCED EXIT AT 15:15 (mirrors live tick()) ─────
        if in_trade and t >= EXPIRY_EXIT:
            if bar_date == trade_data["expiry"]:
                exit_pnl = _exit_trade(provider, trade_data, bar_time, "Expiry")
                if exit_pnl:
                    trades.append(exit_pnl)
                    total_pnl   += exit_pnl["total_pnl"]
                    running_pnl += exit_pnl["total_pnl"]
                    in_trade = False
                    continue
                else:
                    skipped_no_exit_price += 1
                    continue

        # ── DETECT ST FLIP ────────────────────────────────────────────
        any_flip = None
        if i > 0:
            if   st_dir[i] == 1 and st_dir[i-1] == -1:
                any_flip = "BULLISH"
            elif st_dir[i] == -1 and st_dir[i-1] == 1:
                any_flip = "BEARISH"

        spot = close[i]   # captured once — used by all blocks below

        # ── IN-TRADE MONITORING (mirrors live tick() + on_bar()) ──────
        if in_trade:

            # -- Reversal exit (mirrors live on_bar flip handling) -----
            if any_flip:
                should_exit = (
                    (trade_data["spread_type"] == "BULL_PUT" and any_flip == "BEARISH") or
                    (trade_data["spread_type"] == "BEAR_CALL" and any_flip == "BULLISH")
                )
                if should_exit:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Reversal")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False

                        # Immediate re-entry in flip direction (mirrors live)
                        new_td = _try_enter(bar_time, bar_date, spot, any_flip)
                        if new_td:
                            trade_data   = new_td
                            in_trade     = True
                            pending_flip = None
                        else:
                            pending_flip = any_flip
                        continue
                    else:
                        skipped_no_exit_price += 1
                        continue

            # -- Max-loss stop (mirrors live _check_target logic) ------
            # Exit when spread cost-to-close >= MAX_LOSS_PCT * spread_width
            # This matches live: cost_to_close >= spread_width * max_loss_pct
            sell_price, _ = provider.get_option_price(trade_data["sell_key"], bar_time)
            buy_price,  _ = provider.get_option_price(trade_data["buy_key"],  bar_time)

            if sell_price is not None and buy_price is not None:
                current_spread    = sell_price - buy_price
                max_loss_threshold = SPREAD_WIDTH * MAX_LOSS_PCT

                # Max-loss stop
                if current_spread >= max_loss_threshold:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "MaxLoss")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False
                        continue
                    else:
                        skipped_no_exit_price += 1
                        continue

                # Target check (mirrors live _check_target)
                # Profit = net_credit - current_spread >= net_credit * target_pct
                entry_credit  = trade_data["net_credit"]
                profit        = entry_credit - current_spread
                target_profit = entry_credit * TARGET_PCT

                if entry_credit > 0 and profit >= target_profit:
                    exit_pnl = _exit_trade(provider, trade_data, bar_time, "Target")
                    if exit_pnl:
                        trades.append(exit_pnl)
                        total_pnl   += exit_pnl["total_pnl"]
                        running_pnl += exit_pnl["total_pnl"]
                        in_trade = False

                        # Re-entry after target if trend continues (mirrors live)
                        curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
                        new_td = _try_enter(bar_time, bar_date, spot, curr_direction)
                        if new_td:
                            trade_data = new_td
                            in_trade   = True
                            target_reentries += 1
                        continue
                    else:
                        skipped_no_exit_price += 1

        # ── FLIP SIGNAL WHEN FLAT (mirrors live on_bar) ───────────────
        if any_flip and not in_trade:
            total_signals += 1
            new_td = _try_enter(bar_time, bar_date, spot, any_flip)
            if new_td:
                trade_data   = new_td
                in_trade     = True
                pending_flip = None
            else:
                pending_flip = any_flip

        # ── PENDING FLIP: retry each bar until 09:45 ─────────────────
        # Mirrors live tick() pending_flip check
        if pending_flip and not in_trade and t >= TRADE_START:
            new_td = _try_enter(bar_time, bar_date, spot, pending_flip)
            if new_td:
                trade_data         = new_td
                in_trade           = True
                pending_flip_entries += 1
                pending_flip       = None
            elif t >= dtime(9, 45):
                pending_flip = None  # give up

        # ── DAILY RE-ENTRY WHEN FLAT AT 09:15 ────────────────────────
        # Mirrors live bootstrap: if IDLE and ST has direction → queue entry
        # Clears stale pending_flip from previous day first (bug fix #5)
        if not in_trade and t == TRADE_START:
            if pending_flip is not None:
                pending_flip = None   # clear stale flip from previous day
            curr_direction = "BULLISH" if st_dir[i] == 1 else "BEARISH"
            new_td = _try_enter(bar_time, bar_date, spot, curr_direction)
            if new_td:
                trade_data        = new_td
                in_trade          = True
                reentry_when_flat += 1

    # ── FORCE CLOSE OPEN POSITION AT DATA END ────────────────────────
    if in_trade:
        exit_pnl = None
        for lookback in range(min(5, len(df))):
            candidate = df.index[-(1 + lookback)]
            exit_pnl  = _exit_trade(provider, trade_data, candidate, "DataEnd")
            if exit_pnl:
                break
        if exit_pnl:
            trades.append(exit_pnl)
            total_pnl   += exit_pnl["total_pnl"]
            running_pnl += exit_pnl["total_pnl"]
        else:
            print("  WARNING: Could not price open position at DataEnd — dropped!")
            skipped_no_exit_price += 1

    print(f"\n  Total ST flip signals       : {total_signals}")
    print(f"  Pending flip entries        : {pending_flip_entries}")
    print(f"  Re-entries when flat (09:15): {reentry_when_flat}")
    print(f"  Target re-entries           : {target_reentries}")
    print(f"  Used next-week expiry       : {skipped_used_next_expiry}")
    print(f"  Skipped - no contract       : {skipped_no_contract}")
    print(f"  Skipped - no price data     : {skipped_no_price}")
    print(f"  Skipped - negative credit   : {skipped_negative_credit}")
    print(f"  Dropped - no exit price     : {skipped_no_exit_price}")
    if SIMULATE_MARGIN:
        print(f"  Skipped - insufficient margin: {skipped_margin}")
        print(f"  SEBI peak margin violations  : {peak_violations}")
    print(f"  Executed trades             : {len(trades)}")

    return trades, total_pnl, peak_violations, peak_violation_dates


# ============================================================================
# REPORTING HELPERS
# ============================================================================
def format_expiry_tag(expiry_date):
    if isinstance(expiry_date, str):
        expiry_date = pd.to_datetime(expiry_date)
    if hasattr(expiry_date, "strftime"):
        return expiry_date.strftime("%d %b %y").upper()
    return str(expiry_date)


def build_contract_name(strike, opt_type, expiry):
    return f"NIFTY {int(strike)} {opt_type} {format_expiry_tag(expiry)}"


# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    print("=" * 90)
    print("SUPERTREND CREDIT SPREAD BACKTEST — FROM LIVE SCRIPT")
    print("=" * 90)
    print(f"  Data Source  : ExpiryTrack DuckDB (actual 1-min option OHLC)")
    print(f"  SuperTrend   : Period={ST_PERIOD}, Multiplier={ST_MULTIPLIER}")
    print(f"  Timeframe    : {TIMEFRAME}")
    print(f"  Spread       : {SPREAD_WIDTH}pt | Strike Interval: {STRIKE_INTERVAL}")
    print(f"  Position     : {NUM_LOTS} lots x {LOT_SIZE} = {QTY} qty")
    print(f"  Target       : {TARGET_PCT*100:.0f}% premium decay")
    print(f"  Max Loss     : {MAX_LOSS_PCT*100:.0f}% of spread width "
          f"(= Rs {SPREAD_WIDTH * MAX_LOSS_PCT:.0f}/unit)")
    print(f"  Capital      : Rs {CAPITAL:,}")
    print(f"  Hold Mode    : OVERNIGHT (no forced EOD exit)")
    print(f"  Entry        : Immediate on flip, pending retry till 09:45")
    print(f"  Expiry Day   : Uses NEXT WEEK expiry")
    print()

    df_spot  = load_spot_data()

    print("\nInitializing option data provider...")
    provider = OptionDataProvider(EXPITRACK_DB)

    print(f"\n{'='*90}")
    print(f"RUNNING BACKTEST: ST({ST_PERIOD}, {ST_MULTIPLIER}) | "
          f"Spread: {SPREAD_WIDTH}pt | OVERNIGHT")
    print(f"{'='*90}")

    trades, total_pnl, peak_viol, peak_viol_dates = \
        run_simulation(df_spot, provider)

    if len(trades) == 0:
        print("  No trades executed.")
        provider.close()
        exit(0)

    tdf = pd.DataFrame(trades)
    wins   = tdf[tdf["total_pnl"] > 0]
    losses = tdf[tdf["total_pnl"] <= 0]
    n_trades = len(tdf)
    wr       = len(wins) / n_trades * 100
    avg_win  = wins["total_pnl"].mean()   if len(wins)   > 0 else 0
    avg_loss = losses["total_pnl"].mean() if len(losses) > 0 else 0
    gross_win  = wins["total_pnl"].sum()    if len(wins)   > 0 else 0
    gross_loss = abs(losses["total_pnl"].sum()) if len(losses) > 0 else 1
    pf   = gross_win / gross_loss if gross_loss > 0 else 99
    roi  = total_pnl / CAPITAL * 100
    mdd  = (tdf["total_pnl"].cumsum().cummax()
            - tdf["total_pnl"].cumsum()).max()

    overnight_trades  = tdf[tdf["overnight"]]
    intraday_trades   = tdf[~tdf["overnight"]]
    next_exp_trades   = tdf[tdf["is_next_expiry"]]

    print(f"\n  RESULTS")
    print(f"  {'='*65}")
    print(f"  Total Trades        : {n_trades}")
    print(f"  Winners             : {len(wins)} ({wr:.1f}%)")
    print(f"  Losers              : {len(losses)} ({100-wr:.1f}%)")
    print(f"  Avg Winner          : Rs {avg_win:+,.0f}")
    print(f"  Avg Loser           : Rs {avg_loss:+,.0f}")
    print(f"  Profit Factor       : {pf:.2f}")
    print(f"  Total P&L           : Rs {total_pnl:+,.0f}")
    print(f"  ROI on {CAPITAL/100000:.0f}L           : {roi:+.1f}%")
    print(f"  Max Drawdown        : Rs {mdd:,.0f}")

    print(f"\n  EXPIRY DAY TRADES (used next expiry):")
    if len(next_exp_trades) > 0:
        ne_wr = (next_exp_trades["total_pnl"] > 0).mean() * 100
        print(f"    {len(next_exp_trades)} trades | WR: {ne_wr:.1f}% | "
              f"Avg: Rs {next_exp_trades['total_pnl'].mean():+,.0f} | "
              f"Total: Rs {next_exp_trades['total_pnl'].sum():+,.0f}")

    print(f"\n  OVERNIGHT vs INTRADAY:")
    print(f"  {'':>22s} {'Trades':>7} {'WR%':>6} {'Avg P&L':>10} "
          f"{'Total P&L':>12} {'Avg Days':>9}")
    print(f"  {'-'*68}")
    if len(overnight_trades) > 0:
        o_wr = (overnight_trades["total_pnl"] > 0).mean() * 100
        print(f"  {'Overnight':>22s} {len(overnight_trades):>7} {o_wr:>6.1f} "
              f"{overnight_trades['total_pnl'].mean():>+10,.0f} "
              f"{overnight_trades['total_pnl'].sum():>+12,.0f} "
              f"{overnight_trades['days_held'].mean():>9.1f}")
    if len(intraday_trades) > 0:
        i_wr = (intraday_trades["total_pnl"] > 0).mean() * 100
        print(f"  {'Intraday':>22s} {len(intraday_trades):>7} {i_wr:>6.1f} "
              f"{intraday_trades['total_pnl'].mean():>+10,.0f} "
              f"{intraday_trades['total_pnl'].sum():>+12,.0f} "
              f"{intraday_trades['days_held'].mean():>9.1f}")

    print(f"\n  DAYS HELD DISTRIBUTION:")
    for d in sorted(tdf["days_held"].unique()):
        sub  = tdf[tdf["days_held"] == d]
        dwr  = (sub["total_pnl"] > 0).mean() * 100
        label = "same day" if d == 0 else f"{d} day{'s' if d>1 else ''}"
        print(f"    {label:>10s}: {len(sub):4d} trades | WR: {dwr:5.1f}% | "
              f"Avg: Rs {sub['total_pnl'].mean():+,.0f} | "
              f"Total: Rs {sub['total_pnl'].sum():+,.0f}")

    print(f"\n  EXIT REASONS:")
    for reason, grp in tdf.groupby("exit_reason"):
        grp_wr = (grp["total_pnl"] > 0).mean() * 100
        print(f"    {reason:10s}: {len(grp):4d} trades | WR: {grp_wr:5.1f}% | "
              f"Avg Days: {grp['days_held'].mean():.1f} | "
              f"P&L: Rs {grp['total_pnl'].sum():+,.0f}")

    for stype in ["BULL_PUT", "BEAR_CALL"]:
        sub = tdf[tdf["type"] == stype]
        if len(sub) > 0:
            swr = (sub["total_pnl"] > 0).mean() * 100
            print(f"\n  {stype}: {len(sub)} trades | WR: {swr:.1f}% | "
                  f"P&L: Rs {sub['total_pnl'].sum():+,.0f}")

    print(f"\n  PREMIUM ANALYSIS:")
    print(f"    Avg Sell Premium : Rs {tdf['sell_entry'].mean():.2f}")
    print(f"    Avg Buy Premium  : Rs {tdf['buy_entry'].mean():.2f}")
    print(f"    Avg Net Credit   : Rs {tdf['net_credit'].mean():.2f}")
    print(f"    Avg Exit Spread  : Rs {tdf['exit_spread'].mean():.2f}")
    print(f"    Avg P&L/unit     : Rs {tdf['pnl_per_unit'].mean():.2f}")

    # Trade log
    print(f"\n  TRADE LOG:")
    print(f"  {'#':>4} {'Entry':>19} {'Exit':>19} {'Days':>4} {'NxExp':>5} "
          f"{'Type':>10} {'Sell Contract':>28} {'Buy Contract':>28} "
          f"{'Credit':>7} {'Reason':>8} {'P&L':>10}")
    print(f"  {'-'*150}")
    for idx_t, row in tdf.iterrows():
        entry_ts = pd.Timestamp(row["entry_time"])
        exit_ts  = pd.Timestamp(row["exit_time"])
        opt_type = "PE" if row["type"] == "BULL_PUT" else "CE"
        sell_c   = build_contract_name(row["sell_strike"], opt_type, row["expiry"])
        buy_c    = build_contract_name(row["buy_strike"],  opt_type, row["expiry"])
        ne_flag  = "YES" if row.get("is_next_expiry", False) else ""
        print(f"  {idx_t+1:>4d} "
              f"{entry_ts.strftime('%Y-%m-%d %H:%M'):>19} "
              f"{exit_ts.strftime('%Y-%m-%d %H:%M'):>19} "
              f"{row['days_held']:>4.0f} "
              f"{ne_flag:>5} "
              f"{row['type']:>10} "
              f"{sell_c:>28} {buy_c:>28} "
              f"{row['net_credit']:>7.2f} "
              f"{row['exit_reason']:>8} "
              f"{row['total_pnl']:>+10,.0f}")

    # Monthly P&L
    print(f"\n  MONTHLY P&L:")
    tdf["entry_time"] = pd.to_datetime(tdf["entry_time"])
    tdf["month"]      = tdf["entry_time"].dt.to_period("M")
    cumulative = 0
    for month, grp in tdf.groupby("month"):
        m_pnl      = grp["total_pnl"].sum()
        cumulative += m_pnl
        m_wr        = (grp["total_pnl"] > 0).mean() * 100
        m_overnight = (grp["days_held"] > 0).sum()
        m_next_exp  = grp["is_next_expiry"].sum()
        print(f"    {month} | {len(grp):3d} trades "
              f"({m_overnight} overnight, {m_next_exp} next-exp) | "
              f"WR: {m_wr:5.1f}% | "
              f"P&L: Rs {m_pnl:+10,.0f} | Cumulative: Rs {cumulative:+10,.0f}")

    # SEBI Margin report
    if SIMULATE_MARGIN and "equity_at_entry" in tdf.columns:
        for col in ["equity_at_entry", "margin_req", "margin_util_pct",
                    "elm_component", "elm_raw"]:
            tdf[col] = pd.to_numeric(tdf[col], errors="coerce").fillna(0)

        expiry_day_trades = tdf[tdf["elm_component"] > 0]
        normal_day_trades = tdf[tdf["elm_component"] == 0]

        print(f"\n  SEBI MARGIN REPORT")
        print(f"  {'='*65}")
        print(f"  Capital simulated       : Rs {CAPITAL:>10,}")
        print(f"  SEBI Peak buffer        : {MARGIN_PEAK_BUFFER:.0%}")
        print(f"  Peak margin violations  : {peak_viol}")

        print(f"\n  NORMAL DAY ({len(normal_day_trades)} trades):")
        if len(normal_day_trades):
            print(f"    Avg margin required   : Rs {normal_day_trades['margin_req'].mean():>10,.0f}")
            print(f"    Max margin required   : Rs {normal_day_trades['margin_req'].max():>10,.0f}")
            print(f"    Avg utilisation       : {normal_day_trades['margin_util_pct'].mean():.1f}%")
            print(f"    Min equity at entry   : Rs {normal_day_trades['equity_at_entry'].min():>10,.0f}")

        print(f"\n  EXPIRY DAY ({len(expiry_day_trades)} trades):")
        if len(expiry_day_trades):
            print(f"    Avg margin required   : Rs {expiry_day_trades['margin_req'].mean():>10,.0f}")
            print(f"    Max margin required   : Rs {expiry_day_trades['margin_req'].max():>10,.0f}")
            print(f"    Avg ELM (hedged)      : Rs {expiry_day_trades['elm_component'].mean():>10,.0f}")
            print(f"    Avg utilisation       : {expiry_day_trades['margin_util_pct'].mean():.1f}%")

        worst_idx = tdf["equity_at_entry"].idxmin()
        worst = tdf.loc[worst_idx]
        headroom = worst["equity_at_entry"] - worst["margin_req"]
        print(f"\n  WORST CAPITAL MOMENT:")
        print(f"    Date       : {pd.Timestamp(worst['entry_time']).date()}")
        print(f"    Equity     : Rs {worst['equity_at_entry']:>10,.0f}")
        print(f"    Margin req : Rs {worst['margin_req']:>10,.0f}")
        print(f"    Headroom   : Rs {headroom:>10,.0f}  "
              f"{'SAFE' if headroom >= 0 else 'BREACHED'}")

        if peak_viol == 0:
            print(f"\n  No SEBI peak margin breaches detected.")
        else:
            print(f"\n  SEBI PEAK MARGIN BREACH DETAIL:")
            print(f"  {'Date':<12} {'Time':<8} {'Equity':>12} "
                  f"{'Required':>12} {'Shortfall':>12} {'Expiry?':>8}")
            for v in peak_viol_dates[:20]:
                print(f"  {str(v['date']):<12} {str(v['time']):<8} "
                      f"Rs {v['equity']:>9,} "
                      f"Rs {v['margin_req']:>9,} "
                      f"Rs {v['shortfall']:>9,} "
                      f"{'YES' if v['is_expiry'] else 'no':>8}")

    # Save CSV
    csv_path = Path(BASE_DIR) / f"backtest_from_live_ST{ST_PERIOD}_{ST_MULTIPLIER}_{SPREAD_WIDTH}.csv"
    tdf.drop(columns=["month"], errors="ignore").to_csv(csv_path, index=False)
    print(f"\n  Trades saved: {csv_path}")

    # Plotly chart
    try:
        import plotly.graph_objects as go
        cum_pnl = tdf["total_pnl"].cumsum()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=tdf["entry_time"], y=cum_pnl,
            mode="lines+markers",
            name=f"ST({ST_PERIOD},{ST_MULTIPLIER}) Overnight",
            fill="tozeroy",
        ))
        fig.update_layout(
            title=f"SuperTrend Credit Spread Backtest (From Live Script) — "
                  f"ST({ST_PERIOD},{ST_MULTIPLIER})<br>"
                  f"<sub>Total P&L: Rs {total_pnl:+,.0f} | "
                  f"WR: {wr:.1f}% | PF: {pf:.2f} | ROI: {roi:+.1f}%</sub>",
            xaxis_title="Date",
            yaxis_title="Cumulative P&L (Rs)",
            template="plotly_dark",
            height=600, width=1400,
        )
        plot_path = Path(BASE_DIR) / "backtest_from_live_pnl.html"
        fig.write_html(str(plot_path))
        print(f"  Chart saved: {plot_path}")
        fig.show()
    except Exception as e:
        print(f"  Plot error: {e}")

    provider.close()
    print("\nDone.")
