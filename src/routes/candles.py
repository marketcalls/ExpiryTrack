"""Candles blueprint — collect, status, tasks, analytics, collection-status, data."""

import duckdb
from flask import Blueprint, Response, current_app, jsonify, request

from src.collectors.task_manager import task_manager
from src.routes.helpers import require_auth

VALID_INTERVALS = {
    "1minute",
    "3minute",
    "5minute",
    "10minute",
    "15minute",
    "30minute",
    "1hour",
    "1day",
    "1week",
    "1month",
}
MAX_INSTRUMENT_KEYS = 10000

candles_bp = Blueprint("candles", __name__)


def _translate_candle_task(task: dict) -> dict:
    """Translate a candle task dict to the instruments.html status format.

    Accepts output from either get_task_status() or get_candle_task_status().
    When _total/_processed/_skipped are present they are used directly (fast
    path); otherwise they are computed from instrument_progress (small tasks).
    instrument_progress is already trimmed to ≤100 active entries by
    get_candle_task_status() so the response payload stays small.
    """
    ip     = task.get("instrument_progress", {})
    total  = task.get("_total",     len(ip))
    processed = task.get("_processed",
                         sum(1 for v in ip.values() if v.get("status") not in ("pending", "running")))
    skipped   = task.get("_skipped",
                         sum(1 for v in ip.values() if v.get("status") == "skipped"))
    status = task["status"]
    return {
        "task_id":          task["task_id"],
        "status":           "processing" if status == "running" else status,
        "progress":         task["progress"],
        "processed":        processed,
        "total":            total,
        "candles_fetched":  task["stats"].get("candles", 0),
        "errors":           task["stats"].get("errors",  0),
        "skipped":          skipped,
        "current_instrument": task.get("current_action", ""),
        "current_action":   task.get("current_action", ""),
        "status_message":   task.get("current_action", ""),
        "instrument_progress": ip,          # already trimmed (non-pending, ≤100 entries)
        "created_at":       task.get("created_at"),
        "completed_at":     task.get("completed_at"),
    }


@candles_bp.route("/api/candles/collect", methods=["POST"])
@require_auth
def api_candles_collect() -> tuple[Response, int] | Response:
    """Start candle data collection for instruments."""
    data = request.json
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    instrument_keys = data.get("instrument_keys", [])
    if not instrument_keys:
        return jsonify({"error": "instrument_keys list is required"}), 400
    if len(instrument_keys) > MAX_INSTRUMENT_KEYS:
        return jsonify({"error": f"Maximum {MAX_INSTRUMENT_KEYS} instruments per collection"}), 400

    interval = data.get("interval", "1day")
    if interval not in VALID_INTERVALS:
        return jsonify({"error": "Invalid interval"}), 400

    params = {
        "instrument_keys": instrument_keys,
        "interval": interval,
        "from_date": data.get("from_date"),
        "to_date": data.get("to_date"),
        "workers": min(int(data.get("workers", 20)), 50),
        "incremental": data.get("incremental", False),
    }

    try:
        task_id = task_manager.create_candle_task(params)
        return jsonify({"task_id": task_id, "status": "started"})
    except ValueError as e:
        return jsonify({"error": str(e)}), 409


@candles_bp.route("/api/candles/status/<task_id>")
@require_auth
def api_candles_status(task_id) -> tuple[Response, int] | Response:
    """Get candle collection task status."""
    task = task_manager.get_candle_task_status(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(_translate_candle_task(task))


@candles_bp.route("/api/candles/tasks")
@require_auth
def api_candles_tasks() -> Response:
    """Get active candle collection tasks."""
    candle_tasks = []
    for tid, t in task_manager.tasks.items():
        if t.task_type == "candle":
            summary = task_manager.get_candle_task_status(tid)
            if summary:
                candle_tasks.append(_translate_candle_task(summary))
    return jsonify({"tasks": candle_tasks})


@candles_bp.route("/api/candles/analytics")
@require_auth
def api_candles_analytics() -> tuple[Response, int] | Response:
    """Get candle collection analytics summary."""
    try:
        summary = current_app.db_manager.get_candle_analytics_summary()
        return jsonify(summary)
    except duckdb.Error as e:
        return jsonify({"error": str(e)}), 500


@candles_bp.route("/api/candles/collection-status")
@require_auth
def api_candles_collection_status() -> Response:
    """Get candle collection status for instruments."""
    segment = request.args.get("segment")
    return jsonify({"status": current_app.db_manager.get_candle_collection_status(segment)})


@candles_bp.route("/api/candles/data/<path:instrument_key>")
@require_auth
def api_candles_data(instrument_key) -> tuple[Response, int] | Response:
    """Get candle data for an instrument."""
    interval = request.args.get("interval", "1day")
    if interval not in VALID_INTERVALS:
        return jsonify({"error": "Invalid interval"}), 400
    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    data = current_app.db_manager.get_candle_data(instrument_key, interval, from_date, to_date)
    return jsonify({"data": data, "count": len(data)})
