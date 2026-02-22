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
                except Exception:
                    logger.debug(f"Failed to fetch expiries for {instrument}", exc_info=True)
                    expiries_data[instrument] = []
            return expiries_data

    loop = asyncio.new_event_loop()
    try:
        expiries_data = loop.run_until_complete(get_all_expiries())
    finally:
        loop.close()

    return jsonify({"expiries": expiries_data})


@collect_bp.route("/api/collect/start", methods=["POST"])
@require_auth
def api_collect_start() -> tuple[Response, int] | Response:
    """Start a new collection task."""
    from src.routes.helpers import validate_json

    validated, err = validate_json(CollectInput)
    if err:
        return err

    from src.collectors.task_manager import task_manager

    task_id = task_manager.create_task(validated.model_dump())

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
