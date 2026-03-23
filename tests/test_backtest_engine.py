"""Tests for the backtesting engine, sandbox, and routes."""

import pytest

from src.backtest.metrics import compute_metrics
from src.backtest.sandbox import SandboxError, compile_strategy, validate_code
from src.backtest.strategy import Strategy


# ── Strategy base class tests ──


class TestStrategy:
    def test_buy_sell_basic(self):
        s = Strategy()
        s._cash = 100000.0
        s._initial_capital = 100000.0
        s._current_price = 100.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.buy(10)
        assert s.position == 10
        assert s._avg_price == 100.0
        # Cash reduced by cost + commission
        assert s.cash < 100000.0

        s._current_price = 110.0
        s._bar_index = 1
        s.sell(10)
        assert s.position == 0
        assert len(s._trades) == 1
        assert s._trades[0].side == "long"
        assert s._trades[0].pnl > 0  # Profitable trade

    def test_short_cover_basic(self):
        s = Strategy()
        s._cash = 100000.0
        s._initial_capital = 100000.0
        s._current_price = 100.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.short(5)
        assert s.position == -5
        assert s._avg_price == 100.0

        s._current_price = 90.0
        s._bar_index = 1
        s.cover(5)
        assert s.position == 0
        assert len(s._trades) == 1
        assert s._trades[0].side == "short"
        assert s._trades[0].pnl > 0  # Price went down, short is profitable

    def test_commission_deducted(self):
        s = Strategy()
        s._cash = 100000.0
        s._initial_capital = 100000.0
        s._commission_rate = 0.001  # 0.1% for easy math
        s._current_price = 1000.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.buy(10)
        # Cost = 10 * 1000 = 10000, commission = 10000 * 0.001 = 10
        expected_cash = 100000.0 - 10000.0 - 10.0
        assert abs(s.cash - expected_cash) < 0.01

    def test_equity_includes_position_value(self):
        s = Strategy()
        s._cash = 100000.0
        s._initial_capital = 100000.0
        s._commission_rate = 0.0
        s._current_price = 100.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.buy(10)
        s._current_price = 110.0
        # Cash = 100000 - 10*100 = 99000, position value = 10*110 = 1100
        assert s.equity == 99000.0 + 1100.0

    def test_sell_capped_to_position(self):
        s = Strategy()
        s._cash = 100000.0
        s._initial_capital = 100000.0
        s._current_price = 100.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.buy(5)
        s.sell(100)  # Trying to sell more than position
        assert s.position == 0

    def test_zero_qty_no_op(self):
        s = Strategy()
        s._cash = 100000.0
        s._current_price = 100.0
        s._current_time = "2025-01-01"
        s._bar_index = 0

        s.buy(0)
        assert s.position == 0
        s.sell(0)
        s.short(0)
        s.cover(0)
        assert len(s._trades) == 0


# ── Sandbox tests ──


class TestSandbox:
    def test_valid_strategy_compiles(self):
        code = """
class TestStrat(Strategy):
    def on_init(self):
        self.x = 0
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        if close > 100:
            self.buy(1)
"""
        s = compile_strategy(code)
        assert isinstance(s, Strategy)

    def test_import_blocked(self):
        code = "import os\nclass S(Strategy): pass"
        violations = validate_code(code)
        assert len(violations) > 0

    def test_open_blocked(self):
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        f = open('/etc/passwd')
"""
        violations = validate_code(code)
        assert len(violations) > 0

    def test_eval_blocked(self):
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        eval('1+1')
"""
        violations = validate_code(code)
        assert len(violations) > 0

    def test_exec_blocked(self):
        code = """
class S(Strategy):
    def on_init(self):
        exec('print(1)')
"""
        violations = validate_code(code)
        assert len(violations) > 0

    def test_subprocess_blocked(self):
        code = "subprocess.run(['ls'])\nclass S(Strategy): pass"
        violations = validate_code(code)
        assert len(violations) > 0

    def test_dunder_globals_blocked(self):
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        x = self.__globals__
"""
        violations = validate_code(code)
        assert len(violations) > 0

    def test_no_strategy_subclass_raises(self):
        code = "x = 42"
        with pytest.raises(SandboxError, match="No Strategy subclass"):
            compile_strategy(code)

    def test_syntax_error_raises(self):
        code = "class S(Strategy):\n    def on_init(self"
        with pytest.raises(SandboxError, match="Syntax error"):
            compile_strategy(code)

    def test_safe_builtins_work(self):
        code = """
class S(Strategy):
    def on_init(self):
        self.data = list(range(10))
        self.m = max(self.data)
        self.total = sum(self.data)
        self.r = round(3.14159, 2)
        self.sq = math.sqrt(16)
"""
        s = compile_strategy(code)
        s.on_init()
        assert s.m == 9
        assert s.total == 45
        assert s.r == 3.14
        assert s.sq == 4.0


# ── Metrics tests ──


class TestMetrics:
    def _make_trade(self, pnl, side="long"):
        from src.backtest.strategy import Trade
        return Trade(
            entry_time="2025-01-01",
            exit_time="2025-01-02",
            side=side,
            qty=1,
            entry_price=100,
            exit_price=110 if pnl > 0 else 90,
            pnl=pnl,
            commission=0.3,
            bar_index_entry=0,
            bar_index_exit=1,
        )

    def test_basic_metrics(self):
        trades = [self._make_trade(100), self._make_trade(-50), self._make_trade(75)]
        curve = [
            {"bar": 0, "time": "2025-01-01", "equity": 100000},
            {"bar": 1, "time": "2025-01-02", "equity": 100100},
            {"bar": 2, "time": "2025-01-03", "equity": 100050},
            {"bar": 3, "time": "2025-01-04", "equity": 100125},
        ]
        m = compute_metrics(trades, curve, 100000, 0.9)
        assert m["total_trades"] == 3
        assert m["win_rate"] > 60
        assert m["total_commission"] == 0.9

    def test_no_trades(self):
        curve = [{"bar": 0, "time": "t", "equity": 100000}]
        m = compute_metrics([], curve, 100000, 0)
        assert m["total_trades"] == 0
        assert m["win_rate"] == 0

    def test_drawdown(self):
        curve = [
            {"bar": 0, "time": "t", "equity": 100000},
            {"bar": 1, "time": "t", "equity": 110000},
            {"bar": 2, "time": "t", "equity": 99000},
            {"bar": 3, "time": "t", "equity": 105000},
        ]
        m = compute_metrics([], curve, 100000, 0)
        assert m["max_drawdown"] == 11000
        assert m["max_drawdown_pct"] == 10.0


# ── Engine tests ──


class TestEngine:
    def test_engine_with_no_data(self, tmp_db):
        from src.backtest.engine import BacktestEngine

        engine = BacktestEngine(tmp_db)
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        pass
"""
        result = engine.run(code, "NONEXISTENT", "candle_data", "1day")
        assert result["bars_processed"] == 0
        assert result.get("error")

    def test_engine_with_sample_data(self, tmp_db):
        from src.backtest.engine import BacktestEngine

        # Insert sample candle data (50 unique days across 2 months)
        from datetime import date, timedelta

        base_date = date(2025, 1, 1)
        with tmp_db.get_connection() as conn:
            for i in range(50):
                d = base_date + timedelta(days=i)
                price = 100 + i * 0.5
                conn.execute(
                    """INSERT INTO candle_data
                       (instrument_key, timestamp, open, high, low, close, volume, oi, interval)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        "NSE_EQ|RELIANCE",
                        f"{d.isoformat()}T09:15:00",
                        price,
                        price + 2,
                        price - 1,
                        price + 1,
                        10000 + i * 100,
                        0,
                        "1day",
                    ],
                )

        engine = BacktestEngine(tmp_db)
        code = """
class S(Strategy):
    def on_init(self):
        self.closes = []

    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        self.closes.append(close)
        if len(self.closes) >= 10:
            sma = sum(self.closes[-10:]) / 10
            if close > sma and self.position == 0:
                self.buy(1)
            elif close < sma and self.position > 0:
                self.sell(self.position)
"""
        result = engine.run(code, "NSE_EQ|RELIANCE", "candle_data", "1day")
        assert result["bars_processed"] == 50
        assert "metrics" in result
        assert "trades" in result
        assert "equity_curve" in result

    def test_engine_force_close(self, tmp_db):
        """Engine should force-close open positions at end."""
        from src.backtest.engine import BacktestEngine

        with tmp_db.get_connection() as conn:
            for i in range(10):
                conn.execute(
                    """INSERT INTO candle_data
                       (instrument_key, timestamp, open, high, low, close, volume, oi, interval)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ["TEST|INS", f"2025-01-{i+1:02d}T09:15:00", 100, 105, 95, 100, 1000, 0, "1day"],
                )

        engine = BacktestEngine(tmp_db)
        # Strategy that buys but never sells
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        if self.bar_index == 0:
            self.buy(1)
"""
        result = engine.run(code, "TEST|INS", "candle_data", "1day")
        assert result["bars_processed"] == 10
        # Should have at least 1 trade (the force-close)
        assert len(result["trades"]) >= 1

    def test_progress_callback(self, tmp_db):
        from src.backtest.engine import BacktestEngine

        with tmp_db.get_connection() as conn:
            for i in range(5):
                conn.execute(
                    """INSERT INTO candle_data
                       (instrument_key, timestamp, open, high, low, close, volume, oi, interval)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    ["TEST|CB", f"2025-01-{i+1:02d}T09:15:00", 100, 105, 95, 100, 1000, 0, "1day"],
                )

        engine = BacktestEngine(tmp_db)
        code = """
class S(Strategy):
    def on_candle(self, timestamp, open, high, low, close, volume, oi):
        pass
"""
        progress_updates = []

        def cb(pct, msg):
            progress_updates.append((pct, msg))

        engine.run(code, "TEST|CB", "candle_data", "1day", progress_callback=cb)
        assert len(progress_updates) > 0
        # Should end with 100%
        assert progress_updates[-1][0] == 100


# ── Repository tests ──


class TestBacktestRepository:
    def test_strategy_crud(self, tmp_db):
        repo = tmp_db.backtests

        # Create
        sid = repo.save_strategy("Test", "class S(Strategy): pass", "A test")
        assert sid > 0

        # Read
        s = repo.get_strategy(sid)
        assert s["name"] == "Test"
        assert s["code"] == "class S(Strategy): pass"

        # Update
        repo.save_strategy("Updated", "class S2(Strategy): pass", "Updated", strategy_id=sid)
        s2 = repo.get_strategy(sid)
        assert s2["name"] == "Updated"

        # List
        strategies = repo.list_strategies()
        assert len(strategies) == 1

        # Delete
        repo.delete_strategy(sid)
        assert repo.get_strategy(sid) is None

    def test_result_crud(self, tmp_db):
        repo = tmp_db.backtests

        rid = repo.save_result(
            strategy_id=None,
            task_id="test-task-1",
            instrument_key="NSE_EQ|TEST",
            data_source="candle_data",
            interval="1day",
            from_date="2025-01-01",
            to_date="2025-12-31",
            initial_capital=100000,
        )
        assert rid > 0

        # Update with results
        repo.update_result(
            rid,
            status="completed",
            metrics={"total_pnl": 5000},
            trades=[{"pnl": 5000}],
            equity_curve=[{"equity": 105000}],
            bars_processed=100,
        )

        r = repo.get_result(rid)
        assert r["status"] == "completed"
        assert r["metrics"]["total_pnl"] == 5000
        assert r["bars_processed"] == 100

        # Get by task
        r2 = repo.get_result_by_task("test-task-1")
        assert r2["id"] == rid

        # List
        results = repo.list_results()
        assert len(results) == 1

        # Delete
        repo.delete_result(rid)
        assert repo.get_result(rid) is None


# ── Route tests ──


class TestBacktestRoutes:
    def test_backtest_page_loads(self, client):
        resp = client.get("/backtest")
        assert resp.status_code == 200
        assert b"Strategy Backtester" in resp.data

    def test_strategies_list_requires_auth(self, client):
        resp = client.get("/api/backtest/strategies")
        assert resp.status_code == 401

    def test_strategies_crud(self, authed_client):
        # List empty
        resp = authed_client.get("/api/backtest/strategies")
        assert resp.status_code == 200
        assert resp.json == []

        # Save
        resp = authed_client.post(
            "/api/backtest/strategies",
            json={"name": "Test", "code": "class S(Strategy): pass"},
        )
        assert resp.status_code == 200
        sid = resp.json["id"]

        # Get
        resp = authed_client.get(f"/api/backtest/strategies/{sid}")
        assert resp.status_code == 200
        assert resp.json["name"] == "Test"

        # Delete
        resp = authed_client.delete(f"/api/backtest/strategies/{sid}")
        assert resp.status_code == 200

    def test_validate_valid_code(self, authed_client):
        resp = authed_client.post(
            "/api/backtest/strategies/validate",
            json={"code": "class S(Strategy):\n    def on_init(self): pass"},
        )
        assert resp.status_code == 200
        assert resp.json["valid"] is True

    def test_validate_invalid_code(self, authed_client):
        resp = authed_client.post(
            "/api/backtest/strategies/validate",
            json={"code": "import os\nclass S(Strategy): pass"},
        )
        assert resp.status_code == 400
        assert resp.json["valid"] is False

    def test_seed_presets(self, authed_client):
        resp = authed_client.post("/api/backtest/seed-presets")
        assert resp.status_code == 200
        assert resp.json["added"] == 5

        # Second call should add 0
        resp = authed_client.post("/api/backtest/seed-presets")
        assert resp.json["added"] == 0

    def test_results_list(self, authed_client):
        resp = authed_client.get("/api/backtest/results")
        assert resp.status_code == 200

    def test_run_missing_fields(self, authed_client):
        resp = authed_client.post("/api/backtest/run", json={})
        assert resp.status_code == 400
