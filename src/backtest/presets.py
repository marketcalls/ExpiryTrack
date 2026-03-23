"""Built-in preset strategies for backtesting."""

PRESET_STRATEGIES = [
    {
        "name": "SMA Crossover",
        "description": "Buy when fast SMA crosses above slow SMA, sell when it crosses below.",
        "code": """class SmaCrossover(Strategy):
    def on_init(self):
        self.fast_period = 10
        self.slow_period = 30
        self.closes = []

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        self.closes.append(close)

        if len(self.closes) < self.slow_period:
            return

        fast_sma = sum(self.closes[-self.fast_period:]) / self.fast_period
        slow_sma = sum(self.closes[-self.slow_period:]) / self.slow_period

        if fast_sma > slow_sma and self.position <= 0:
            if self.position < 0:
                self.cover(abs(self.position))
            self.buy(1)
        elif fast_sma < slow_sma and self.position >= 0:
            if self.position > 0:
                self.sell(self.position)
            self.short(1)
""",
    },
    {
        "name": "RSI Mean Reversion",
        "description": "Buy when RSI drops below 30 (oversold), sell when RSI rises above 70 (overbought).",
        "code": """class RsiMeanReversion(Strategy):
    def on_init(self):
        self.period = 14
        self.oversold = 30
        self.overbought = 70
        self.closes = []

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        self.closes.append(close)

        if len(self.closes) < self.period + 1:
            return

        # Calculate RSI
        gains = []
        losses = []
        for i in range(-self.period, 0):
            change = self.closes[i] - self.closes[i - 1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))

        avg_gain = sum(gains) / self.period
        avg_loss = sum(losses) / self.period

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        if rsi < self.oversold and self.position <= 0:
            if self.position < 0:
                self.cover(abs(self.position))
            self.buy(1)
        elif rsi > self.overbought and self.position > 0:
            self.sell(self.position)
""",
    },
    {
        "name": "Bollinger Band Breakout",
        "description": "Buy on upper band breakout, sell on lower band breakdown. 20-period, 2 std dev.",
        "code": """class BollingerBreakout(Strategy):
    def on_init(self):
        self.period = 20
        self.num_std = 2
        self.closes = []

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        self.closes.append(close)

        if len(self.closes) < self.period:
            return

        window = self.closes[-self.period:]
        sma = sum(window) / self.period
        variance = sum((x - sma) ** 2 for x in window) / self.period
        std = math.sqrt(variance)

        upper = sma + self.num_std * std
        lower = sma - self.num_std * std

        if close > upper and self.position <= 0:
            if self.position < 0:
                self.cover(abs(self.position))
            self.buy(1)
        elif close < lower and self.position >= 0:
            if self.position > 0:
                self.sell(self.position)
            self.short(1)
""",
    },
    {
        "name": "Volume Breakout",
        "description": "Buy on 2x average volume spike, hold for 5 bars.",
        "code": """class VolumeBreakout(Strategy):
    def on_init(self):
        self.lookback = 20
        self.vol_multiplier = 2.0
        self.hold_bars = 5
        self.volumes = []
        self.entry_bar = -999

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        self.volumes.append(volume)

        # Exit after hold period
        if self.position > 0 and (self.bar_index - self.entry_bar) >= self.hold_bars:
            self.sell(self.position)

        if len(self.volumes) < self.lookback:
            return

        avg_vol = sum(self.volumes[-self.lookback:]) / self.lookback

        if avg_vol > 0 and volume > avg_vol * self.vol_multiplier and self.position == 0:
            if close > self.volumes[-2] if len(self.volumes) > 1 else True:
                self.buy(1)
                self.entry_bar = self.bar_index
""",
    },
    {
        "name": "Wave Scalper (F&O)",
        "description": "Simulates paired buy/sell limit orders at configurable gaps from last fill price. Gap widths scale with position imbalance. Designed for NIFTY FUT 1-min data.",
        "code": """class WaveScalper(Strategy):
    def on_init(self):
        # -- Configurable parameters (from wave.yml defaults) --
        self.buy_gap = 25          # Points below reference to place buy
        self.sell_gap = 25         # Points above reference to place sell
        self.lot_size = 75         # 1 NIFTY lot
        self.max_lots = 10         # Max position in lots (delta cap)

        # -- Internal state --
        self.ref_price = None      # Last fill price / reference
        self.pending_buy = None    # Current buy limit level
        self.pending_sell = None   # Current sell limit level
        self.lots_held = 0         # Net lots: +ve=long, -ve=short

        # Multiplier scale (from wave.py _generate_multiplier_scale)
        # Format: {position_imbalance: [buy_mult, sell_mult]}
        # When long N lots: buy gap widens (harder to buy more), sell gap stays 1x
        # When short N lots: sell gap widens, buy gap stays 1x
        self.mult = {
            0: [1.0, 1.0],
            1: [1.3, 1.0], 2: [1.7, 1.0], 3: [2.5, 1.0],
            4: [3.0, 1.0], 5: [10.0, 1.0],
            -1: [1.0, 1.3], -2: [1.0, 1.7], -3: [1.0, 2.5],
            -4: [1.0, 3.0], -5: [1.0, 10.0],
        }

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        # First bar: set reference price and pending levels
        if self.ref_price is None:
            self.ref_price = close
            self._set_levels()
            return

        bought = False
        sold = False

        # Check buy fill: did the low touch/cross our buy level?
        if self.pending_buy and low <= self.pending_buy and self.lots_held < self.max_lots:
            self.buy(self.lot_size)
            self.lots_held += 1
            self.ref_price = self.pending_buy
            bought = True

        # Check sell fill: did the high touch/cross our sell level?
        if self.pending_sell and high >= self.pending_sell and self.lots_held > -self.max_lots:
            if not bought:  # Only one fill per bar
                if self.position > 0:
                    self.sell(min(self.lot_size, self.position))
                else:
                    self.short(self.lot_size)
                self.lots_held -= 1
                self.ref_price = self.pending_sell
                sold = True

        # Recalculate levels after any fill
        if bought or sold:
            self._set_levels()

    def _set_levels(self):
        key = max(-5, min(5, self.lots_held))
        m = self.mult.get(key, [100.0, 100.0])
        self.pending_buy = round(self.ref_price - self.buy_gap * m[0], 2)
        self.pending_sell = round(self.ref_price + self.sell_gap * m[1], 2)
""",
    },
]
