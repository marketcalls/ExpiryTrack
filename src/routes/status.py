"""Status blueprint — download-status page, API, missing, resume, force-refetch, retry-failed, quality."""

import re

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

status_bp = Blueprint("status", __name__)


@status_bp.route("/status")
def status_page() -> str | Response:
    """Status page showing database statistics and recent tasks."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    db = current_app.db_manager
    stats = db.get_summary_stats()
    from src.collectors.task_manager import task_manager

    tasks = task_manager.get_all_tasks()
    tasks.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return render_template("status.html", stats=stats, tasks=tasks[:10])


@status_bp.route("/download-status")
def download_status_page() -> str | Response:
    """Download status page — per-expiry data coverage and resume."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("download_status.html")


@status_bp.route("/api/download-status")
@require_auth
def api_download_status() -> Response:
    """List all expiries with download status."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_download_status(instrument))


@status_bp.route("/api/download-status/<path:instrument_key>/<expiry_date>/missing")
@require_auth
def api_download_status_missing(instrument_key, expiry_date) -> tuple[Response, int] | Response:
    """Get missing (unfetched) contracts for a specific expiry."""
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date):
        return jsonify({"error": "Invalid expiry date format"}), 400
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_missing_contracts(instrument_key, expiry_date))


@status_bp.route("/api/download-status/resume", methods=["POST"])
@require_auth
def api_download_status_resume() -> tuple[Response, int] | Response:
    """Resume downloading for specific instrument+expiry combos."""
    data = request.json
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    instrument = data.get("instrument")
    expiries_list = data.get("expiries", [])

    if not instrument or not isinstance(instrument, str):
        return jsonify({"error": "instrument is required and must be a string"}), 400
    if not expiries_list or not isinstance(expiries_list, list):
        return jsonify({"error": "expiries is required and must be a list"}), 400
    if len(expiries_list) > 50:
        return jsonify({"error": "Maximum 50 expiries per request"}), 400

    for exp in expiries_list:
        if not isinstance(exp, str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", exp):
            return jsonify({"error": f"Invalid expiry date format: {exp}"}), 400

    from src.collectors.task_manager import task_manager

    task_params = {
        "instruments": [instrument],
        "contract_type": "both",
        "expiries": {instrument: expiries_list},
        "interval": "1minute",
        "workers": 5,
    }
    task_id = task_manager.create_task(task_params)
    return jsonify({"success": True, "task_id": task_id})


@status_bp.route("/api/download-status/force-refetch", methods=["POST"])
@require_auth
def api_download_status_force_refetch() -> tuple[Response, int] | Response:
    """Reset data_fetched flag for an instrument+expiry."""
    data = request.json
    if not data:
        return jsonify({"error": "Request body is required"}), 400

    instrument_key = data.get("instrument_key")
    expiry_date = data.get("expiry_date")

    if not instrument_key or not isinstance(instrument_key, str):
        return jsonify({"error": "instrument_key is required"}), 400
    if not expiry_date or not isinstance(expiry_date, str):
        return jsonify({"error": "expiry_date is required"}), 400
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date):
        return jsonify({"error": "Invalid expiry_date format"}), 400

    count = current_app.db_manager.reset_contracts_for_refetch(instrument_key, expiry_date)
    return jsonify({"success": True, "reset_count": count})


@status_bp.route("/api/download-status/retry-failed", methods=["POST"])
@require_auth
def api_download_status_retry_failed() -> Response:
    """Reset failed contracts for retry."""
    data = request.json or {}
    instrument_key = data.get("instrument_key")
    count = current_app.db_manager.reset_fetch_attempts(instrument_key or None)
    return jsonify({"success": True, "reset_count": count})


@status_bp.route("/api/quality/run", methods=["POST"])
@require_auth
def api_quality_run() -> Response:
    """Run data quality checks."""
    from src.quality.checker import DataQualityChecker

    instrument_key = None
    if request.json:
        instrument_key = request.json.get("instrument_key")
    checker = DataQualityChecker(current_app.db_manager)
    report = checker.run_all_checks(instrument_key)
    return jsonify(report.to_dict())
