"""Candles blueprint — collect, status, tasks, analytics, collection-status, data."""

import asyncio
import threading
import uuid
from datetime import datetime

import duckdb
from flask import Blueprint, Response, current_app, jsonify, request

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
MAX_INSTRUMENT_KEYS = 5000

candles_bp = Blueprint("candles", __name__)

# Lazy-initialized persistent tracker
_candle_tracker = None


def _get_candle_tracker():
    global _candle_tracker
    if _candle_tracker is None:
        from src.tasks import PersistentTaskTracker

        _candle_tracker = PersistentTaskTracker("candles", current_app.db_manager)
    return _candle_tracker


@candles_bp.route("/api/candles/collect", methods=["POST"])
@require_auth
def api_candles_collect() -> tuple[Response, int] | Response:
    """Start candle data collection for instruments."""
    auth = current_app.auth_manager
    db = current_app.db_manager

    _get_candle_tracker().cleanup()

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
    from_date = data.get("from_date")
    to_date = data.get("to_date")
    batch_size = min(data.get("batch_size", 5), 10)
    incremental = data.get("incremental", False)

    task_id = str(uuid.uuid4())

    _get_candle_tracker().create(
        task_id,
        {
            "task_id": task_id,
            "status": "processing",
            "progress": 0,
            "total": len(instrument_keys),
            "processed": 0,
            "candles_fetched": 0,
            "errors": 0,
            "skipped": 0,
            "current_instrument": "",
            "incremental": incremental,
            "status_message": "Starting candle collection...",
            "created_at": datetime.now().isoformat(),
            "completed_at": None,
            "instrument_progress": {key: {"status": "pending", "candles": 0, "error": None} for key in instrument_keys},
        },
    )

    def _collect():
        async def _run():
            from src.collectors.candle_collector import CandleCollector

            def progress_cb(key, current, total, candles):
                update = {
                    "processed": current,
                    "progress": int(current / total * 100),
                    "current_instrument": key,
                    "status_message": f"Processing {current}/{total}: {key}",
                }
                # Build updated instrument_progress entry
                task = _get_candle_tracker().get(task_id)
                if task:
                    ip = task.get("instrument_progress", {})
                    if isinstance(candles, Exception):
                        update["errors"] = task.get("errors", 0) + 1
                        ip[key] = {"status": "failed", "candles": 0, "error": str(candles)}
                    elif isinstance(candles, int) and candles > 0:
                        update["candles_fetched"] = task.get("candles_fetched", 0) + candles
                        ip[key] = {"status": "completed", "candles": candles, "error": None}
                    else:
                        update["skipped"] = task.get("skipped", 0) + 1
                        ip[key] = {"status": "skipped", "candles": 0, "error": None}
                    update["instrument_progress"] = ip
                _get_candle_tracker().update(task_id, **update)

            collector = CandleCollector(auth_manager=auth, db_manager=db)
            async with collector:
                stats = await collector.collect(
                    instrument_keys=instrument_keys,
                    interval=interval,
                    from_date=from_date,
                    to_date=to_date,
                    batch_size=batch_size,
                    incremental=incremental,
                    progress_callback=progress_cb,
                )

            _get_candle_tracker().update(
                task_id,
                status="completed",
                progress=100,
                candles_fetched=stats["candles_fetched"],
                errors=stats["errors"],
                skipped=stats["skipped"],
                completed_at=datetime.now().isoformat(),
                status_message=(
                    f"Done: {stats['candles_fetched']:,} candles, {stats['errors']} errors, {stats['skipped']} skipped"
                ),
            )

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run())
        except Exception as e:
            _get_candle_tracker().update(
                task_id,
                status="failed",
                error=str(e),
                completed_at=datetime.now().isoformat(),
                status_message=f"Failed: {e}",
            )
        finally:
            loop.close()

    thread = threading.Thread(target=_collect)
    thread.start()

    return jsonify({"task_id": task_id, "status": "started"})


@candles_bp.route("/api/candles/status/<task_id>")
@require_auth
def api_candles_status(task_id) -> tuple[Response, int] | Response:
    """Get candle collection task status."""
    task = _get_candle_tracker().get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


@candles_bp.route("/api/candles/tasks")
@require_auth
def api_candles_tasks() -> Response:
    """Get active candle collection tasks."""
    return jsonify({"tasks": _get_candle_tracker().list_active()})


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
