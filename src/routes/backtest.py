"""Backtest blueprint — strategy editor, run backtest, view results."""

import logging
import threading
import uuid
from datetime import datetime

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    render_template,
    request,
)

from src.routes.helpers import require_auth

backtest_bp = Blueprint("backtest", __name__)
logger = logging.getLogger(__name__)

# Lazy-initialized persistent tracker
_backtest_tracker = None


def _get_tracker():
    global _backtest_tracker
    if _backtest_tracker is None:
        from src.tasks import PersistentTaskTracker
        _backtest_tracker = PersistentTaskTracker("backtest", current_app.db_manager)
    return _backtest_tracker


# ── Page ──

@backtest_bp.route("/backtest")
def backtest_page() -> str:
    return render_template("backtest.html")


# ── Strategy CRUD ──

@backtest_bp.route("/api/backtest/strategies", methods=["GET"])
@require_auth
def api_list_strategies() -> Response:
    strategies = current_app.db_manager.backtests.list_strategies()
    return jsonify(strategies)


@backtest_bp.route("/api/backtest/strategies", methods=["POST"])
@require_auth
def api_save_strategy() -> tuple[Response, int] | Response:
    data = request.json or {}
    name = data.get("name", "").strip()
    code = data.get("code", "").strip()
    if not name or not code:
        return jsonify({"error": "Name and code are required"}), 400

    strategy_id = data.get("id")
    new_id = current_app.db_manager.backtests.save_strategy(
        name=name,
        code=code,
        description=data.get("description", ""),
        strategy_id=int(strategy_id) if strategy_id else None,
    )
    return jsonify({"id": new_id, "message": "Strategy saved"})


@backtest_bp.route("/api/backtest/strategies/<int:strategy_id>", methods=["GET"])
@require_auth
def api_get_strategy(strategy_id: int) -> tuple[Response, int] | Response:
    strategy = current_app.db_manager.backtests.get_strategy(strategy_id)
    if not strategy:
        return jsonify({"error": "Strategy not found"}), 404
    return jsonify(strategy)


@backtest_bp.route("/api/backtest/strategies/<int:strategy_id>", methods=["DELETE"])
@require_auth
def api_delete_strategy(strategy_id: int) -> Response:
    current_app.db_manager.backtests.delete_strategy(strategy_id)
    return jsonify({"message": "Strategy deleted"})


# ── Validate ──

@backtest_bp.route("/api/backtest/strategies/validate", methods=["POST"])
@require_auth
def api_validate_strategy() -> tuple[Response, int] | Response:
    data = request.json or {}
    code = data.get("code", "").strip()
    if not code:
        return jsonify({"error": "Code is required"}), 400

    from src.backtest.sandbox import SandboxError, validate_code

    violations = validate_code(code)
    if violations:
        return jsonify({"valid": False, "errors": violations}), 400

    # Try to compile
    try:
        from src.backtest.sandbox import compile_strategy
        compile_strategy(code)
    except SandboxError as e:
        return jsonify({"valid": False, "errors": [str(e)]}), 400

    return jsonify({"valid": True, "message": "Strategy code is valid"})


# ── Seed Presets ──

@backtest_bp.route("/api/backtest/seed-presets", methods=["POST"])
@require_auth
def api_seed_presets() -> Response:
    from src.backtest.presets import PRESET_STRATEGIES

    db = current_app.db_manager
    existing = db.backtests.list_strategies()
    existing_names = {s["name"] for s in existing}
    added = 0

    for preset in PRESET_STRATEGIES:
        if preset["name"] not in existing_names:
            db.backtests.save_strategy(
                name=preset["name"],
                code=preset["code"],
                description=preset["description"],
                is_preset=True,
            )
            added += 1

    return jsonify({"message": f"Added {added} preset strategies", "added": added})


# ── Run Backtest ──

@backtest_bp.route("/api/backtest/run", methods=["POST"])
@require_auth
def api_run_backtest() -> tuple[Response, int] | Response:
    data = request.json or {}
    code = data.get("code", "").strip()
    instrument_key = data.get("instrument_key", "").strip()

    if not code:
        return jsonify({"error": "Strategy code is required"}), 400
    if not instrument_key:
        return jsonify({"error": "Instrument key is required"}), 400

    data_source = data.get("data_source", "candle_data")
    interval = data.get("interval", "1day")
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    initial_capital = float(data.get("initial_capital", 100000))
    commission_rate = float(data.get("commission_rate", 0.0003))
    strategy_id = data.get("strategy_id")

    task_id = str(uuid.uuid4())
    tracker = _get_tracker()
    tracker.create(
        task_id,
        {
            "task_id": task_id,
            "status": "processing",
            "progress": 0,
            "status_message": "Starting backtest...",
            "error": None,
            "created_at": datetime.now().isoformat(),
        },
    )

    # Save initial result record
    db = current_app.db_manager
    result_id = db.backtests.save_result(
        strategy_id=int(strategy_id) if strategy_id else None,
        task_id=task_id,
        instrument_key=instrument_key,
        data_source=data_source,
        interval=interval,
        from_date=from_date,
        to_date=to_date,
        initial_capital=initial_capital,
        status="running",
    )

    def run_backtest():
        try:
            from src.backtest.engine import BacktestEngine

            engine = BacktestEngine(db)

            def on_progress(pct, msg):
                tracker.update(task_id, progress=pct, status_message=msg)

            result = engine.run(
                strategy_code=code,
                instrument_key=instrument_key,
                data_source=data_source,
                interval=interval,
                from_date=from_date,
                to_date=to_date,
                initial_capital=initial_capital,
                commission_rate=commission_rate,
                progress_callback=on_progress,
            )

            if result.get("error"):
                db.backtests.update_result(
                    result_id,
                    status="failed",
                    error_message=result["error"],
                )
                tracker.update(
                    task_id,
                    status="failed",
                    progress=100,
                    status_message=result["error"],
                    error=result["error"],
                )
                return

            db.backtests.update_result(
                result_id,
                status="completed",
                metrics=result["metrics"],
                trades=result["trades"],
                equity_curve=result["equity_curve"],
                bars_processed=result["bars_processed"],
            )
            tracker.update(
                task_id,
                status="completed",
                progress=100,
                status_message="Backtest complete!",
                result_id=result_id,
            )

        except Exception as e:
            import traceback
            logger.error(f"Backtest failed: {e}")
            logger.error(traceback.format_exc())
            try:
                db.backtests.update_result(
                    result_id, status="failed", error_message=str(e)
                )
            except Exception:
                pass
            tracker.update(
                task_id,
                status="failed",
                error=str(e),
                status_message=f"Backtest failed: {e}",
            )

    threading.Thread(target=run_backtest, daemon=True).start()
    return jsonify({"task_id": task_id, "result_id": result_id})


# ── Status ──

@backtest_bp.route("/api/backtest/status/<task_id>")
@require_auth
def api_backtest_status(task_id: str) -> tuple[Response, int] | Response:
    task = _get_tracker().get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


# ── Results ──

@backtest_bp.route("/api/backtest/results", methods=["GET"])
@require_auth
def api_list_results() -> Response:
    limit = min(request.args.get("limit", 50, type=int), 200)
    results = current_app.db_manager.backtests.list_results(limit=limit)
    return jsonify(results)


@backtest_bp.route("/api/backtest/results/<int:result_id>", methods=["GET"])
@require_auth
def api_get_result(result_id: int) -> tuple[Response, int] | Response:
    result = current_app.db_manager.backtests.get_result(result_id)
    if not result:
        return jsonify({"error": "Result not found"}), 404
    return jsonify(result)


@backtest_bp.route("/api/backtest/results/<int:result_id>", methods=["DELETE"])
@require_auth
def api_delete_result(result_id: int) -> Response:
    current_app.db_manager.backtests.delete_result(result_id)
    return jsonify({"message": "Result deleted"})


# ── Instruments ──

@backtest_bp.route("/api/backtest/instruments")
@require_auth
def api_backtest_instruments() -> Response:
    data_source = request.args.get("data_source", "candle_data")
    from src.backtest.engine import BacktestEngine

    engine = BacktestEngine(current_app.db_manager)
    instruments = engine.get_available_instruments(data_source)
    return jsonify(instruments)


# ── F&O Cascading Selectors ──

@backtest_bp.route("/api/backtest/fo/expiries")
@require_auth
def api_fo_expiries() -> tuple[Response, int] | Response:
    instrument_key = request.args.get("instrument_key", "").strip()
    if not instrument_key:
        return jsonify({"error": "instrument_key is required"}), 400

    from src.backtest.engine import BacktestEngine

    engine = BacktestEngine(current_app.db_manager)
    expiries = engine.get_fo_expiries(instrument_key)
    return jsonify(expiries)


@backtest_bp.route("/api/backtest/fo/contracts")
@require_auth
def api_fo_contracts() -> tuple[Response, int] | Response:
    instrument_key = request.args.get("instrument_key", "").strip()
    expiry_date = request.args.get("expiry_date", "").strip()
    if not instrument_key or not expiry_date:
        return jsonify({"error": "instrument_key and expiry_date are required"}), 400

    from src.backtest.engine import BacktestEngine

    engine = BacktestEngine(current_app.db_manager)
    contracts = engine.get_fo_contracts(instrument_key, expiry_date)
    return jsonify(contracts)
