"""Analytics blueprint — analytics page + all /api/analytics/* endpoints."""

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

from src.config import config
from src.routes.helpers import require_auth

analytics_bp = Blueprint("analytics", __name__)


@analytics_bp.route("/analytics")
def analytics_page() -> str | Response:
    """Analytics dashboard page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("analytics.html")


@analytics_bp.route("/api/analytics/summary")
@require_auth
def api_analytics_summary() -> Response:
    """Dashboard summary stats."""
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_dashboard_summary())


@analytics_bp.route("/api/analytics/candles-per-day")
@require_auth
def api_analytics_candles_per_day() -> Response:
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    limit = min(request.args.get("limit", 60, type=int), config.ANALYTICS_MAX_CHART_POINTS)
    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_candles_per_day(instrument, limit))


@analytics_bp.route("/api/analytics/contracts-by-type")
@require_auth
def api_analytics_contracts_by_type() -> Response:
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_contracts_by_type())


@analytics_bp.route("/api/analytics/contracts-by-instrument")
@require_auth
def api_analytics_contracts_by_instrument() -> Response:
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_contracts_by_instrument())


@analytics_bp.route("/api/analytics/data-coverage")
@require_auth
def api_analytics_data_coverage() -> Response:
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_data_coverage_by_expiry(instrument))


@analytics_bp.route("/api/analytics/volume-by-expiry")
@require_auth
def api_analytics_volume_by_expiry() -> Response:
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_volume_by_expiry(instrument))


@analytics_bp.route("/api/analytics/storage")
@require_auth
def api_analytics_storage() -> Response:
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    return jsonify(engine.get_storage_breakdown())
