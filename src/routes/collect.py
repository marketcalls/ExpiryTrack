"""Collect blueprint — collection wizard, start, status, tasks, expiry APIs."""

import asyncio
import logging

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from src.routes.helpers import require_auth
from src.routes.validators import CollectInput
from src.utils.instrument_mapper import get_instrument_key

logger = logging.getLogger(__name__)

collect_bp = Blueprint("collect", __name__)


@collect_bp.route("/collect")
def collect_page() -> str | Response:
    """Collection wizard page."""
    auth = current_app.auth_manager
    if not auth.has_credentials():
        session["error"] = "Please configure API credentials first"
        return redirect(url_for("auth.settings"))
    if not auth.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("collect_wizard.html")


@collect_bp.route("/api/expiries/<instrument>")
@require_auth
def api_expiries(instrument) -> Response:
    """Get expiries for a single instrument."""
    auth = current_app.auth_manager
    instrument_key = get_instrument_key(instrument)

    async def get_expiries():
        from src.collectors.expiry_tracker import ExpiryTracker

        tracker = ExpiryTracker(auth_manager=auth)
        async with tracker:
            return await tracker.get_expiries(instrument_key)

    loop = asyncio.new_event_loop()
    try:
        expiries = loop.run_until_complete(get_expiries())
    finally:
        loop.close()

    return jsonify(
        {
            "instrument": instrument,
            "instrument_key": instrument_key,
            "expiries": expiries,
        }
    )


@collect_bp.route("/api/instruments/expiries", methods=["POST"])
@require_auth
def api_instruments_expiries() -> tuple[Response, int] | Response:
    """Get expiries for multiple instruments."""
    auth = current_app.auth_manager
    data = request.json
    if not data or "instruments" not in data:
        return jsonify({"error": "Missing instruments list"}), 400

    instruments = data["instruments"]
    if not isinstance(instruments, list) or not instruments:
        return jsonify({"error": "Invalid instruments list"}), 400

    errors: list[str] = []

    async def get_all_expiries():
        from src.collectors.expiry_tracker import ExpiryTracker

        tracker = ExpiryTracker(auth_manager=auth)
        async with tracker:
            expiries_data = {}
            for instrument in instruments:
                try:
                    instrument_key = get_instrument_key(instrument)
                    expiries = await tracker.get_expiries(instrument_key)
                    expiries_data[instrument] = expiries
                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"Failed to fetch expiries for {instrument}: {error_msg}")
                    expiries_data[instrument] = []
                    if "401" in error_msg or "Invalid token" in error_msg:
                        errors.append("Upstox token is invalid or expired. Please re-login.")
                    elif "403" in error_msg:
                        errors.append("Access denied by Upstox API. Check your app permissions.")
                    else:
                        errors.append(f"Failed to fetch expiries for {instrument}: {error_msg}")
            return expiries_data

    loop = asyncio.new_event_loop()
    try:
        expiries_data = loop.run_until_complete(get_all_expiries())
    finally:
        loop.close()

    result: dict = {"expiries": expiries_data}
    # Deduplicate and include errors so the UI can display them
    unique_errors = list(dict.fromkeys(errors))
    if unique_errors:
        result["errors"] = unique_errors

    return jsonify(result)


@collect_bp.route("/api/collect/start", methods=["POST"])
@require_auth
def api_collect_start() -> tuple[Response, int] | Response:
    """Start a new collection task."""
    from src.routes.helpers import validate_json

    validated, err = validate_json(CollectInput)
    if err:
        return err

    from src.collectors.task_manager import task_manager

    try:
        task_id = task_manager.create_task(validated.model_dump())
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 409

    return jsonify(
        {
            "success": True,
            "task_id": task_id,
            "status": "started",
            "message": "Collection task started",
        }
    )


@collect_bp.route("/api/collect/status/<task_id>")
@require_auth
def api_collect_status(task_id) -> tuple[Response, int] | Response:
    """Get status of a collection task."""
    from src.collectors.task_manager import task_manager

    status = task_manager.get_task_status(task_id)
    if status:
        return jsonify(status)
    return jsonify({"error": "Task not found"}), 404


@collect_bp.route("/api/collect/tasks")
@require_auth
def api_collect_tasks() -> Response:
    """Get all collection tasks."""
    from src.collectors.task_manager import task_manager

    tasks = task_manager.get_all_tasks()
    return jsonify({"tasks": tasks})


@collect_bp.route("/api/collect/smart", methods=["POST"])
@require_auth
def api_collect_smart() -> tuple[Response, int] | Response:
    """Smart collect: auto-detect unfetched contracts across all active instruments."""
    from src.collectors.task_manager import task_manager

    db = current_app.db_manager
    active_instruments = db.get_active_instruments()

    if not active_instruments:
        return jsonify({"error": "No active instruments configured"}), 400

    # Find instruments with pending (unfetched) expiries
    unfetched = {}
    total_pending = 0
    for inst in active_instruments:
        inst_key = inst["instrument_key"]
        try:
            expiries = db.get_expiries_for_instrument(inst_key)
            pending_expiries = []
            for exp in expiries:
                contracts = db.get_contracts_for_expiry(inst_key, exp)
                pending = [c for c in contracts if not c.get("data_fetched") and not c.get("no_data")]
                if pending:
                    pending_expiries.append(exp)
                    total_pending += len(pending)
            if pending_expiries:
                unfetched[inst_key] = pending_expiries
        except Exception:
            continue

    if not unfetched:
        return jsonify({
            "success": True,
            "message": "All data is up to date! No unfetched contracts found.",
            "instruments": 0,
            "expiries": 0,
            "pending_contracts": 0,
        })

    # Build estimation
    instruments_list = list(unfetched.keys())
    estimation = task_manager.estimate_collection(instruments_list, unfetched)

    # Check if user wants to auto-start (from query param)
    auto_start = request.json and request.json.get("auto_start", False)

    result = {
        "success": True,
        "instruments": len(unfetched),
        "expiries": sum(len(v) for v in unfetched.values()),
        "pending_contracts": total_pending,
        "estimation": estimation,
        "unfetched": {k: v for k, v in unfetched.items()},
    }

    if auto_start:
        try:
            task_id = task_manager.create_task({
                "instruments": instruments_list,
                "contract_type": "both",
                "expiries": unfetched,
                "interval": "1minute",
                "workers": 5,
                "source": "smart_collect",
            })
            result["task_id"] = task_id
            result["message"] = f"Smart collection started for {total_pending} pending contracts"
        except ValueError as e:
            return jsonify({"success": False, "error": str(e)}), 409

    return jsonify(result)


@collect_bp.route("/api/collect/estimate", methods=["POST"])
@require_auth
def api_collect_estimate() -> tuple[Response, int] | Response:
    """Estimate collection work for given parameters."""
    from src.collectors.task_manager import task_manager

    data = request.json
    if not data:
        return jsonify({"error": "Request body required"}), 400

    instruments = data.get("instruments", [])
    expiries = data.get("expiries", {})
    contract_type = data.get("contract_type", "both")

    estimation = task_manager.estimate_collection(instruments, expiries, contract_type)
    return jsonify(estimation)
