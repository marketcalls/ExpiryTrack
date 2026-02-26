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


@analytics_bp.route("/option-chain")
def option_chain_page() -> str | Response:
    """Option chain explorer page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("option_chain.html")


@analytics_bp.route("/api/analytics/option-chain")
@require_auth
def api_analytics_option_chain() -> tuple[Response, int] | Response:
    """Get option chain data for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_option_chain_data(instrument, expiry)
    return jsonify({"data": data})


@analytics_bp.route("/api/analytics/expiry-dates")
@require_auth
def api_analytics_expiry_dates() -> tuple[Response, int] | Response:
    """Get available expiry dates for an instrument."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    if not instrument:
        return jsonify({"error": "instrument param required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    dates = engine.get_available_expiry_dates(instrument)
    return jsonify({"dates": dates})


@analytics_bp.route("/api/analytics/instruments-with-data")
@require_auth
def api_analytics_instruments_with_data() -> Response:
    """Get instruments that have contract data."""
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    instruments = engine.get_available_instruments_with_contracts()
    return jsonify({"instruments": instruments})


# ------------------------------------------------------------------
# Candlestick Chart Viewer (D5)
# ------------------------------------------------------------------


@analytics_bp.route("/charts")
def chart_viewer_page() -> str | Response:
    """Candlestick chart viewer page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("chart_viewer.html")


@analytics_bp.route("/api/analytics/candles")
@require_auth
def api_analytics_candles() -> tuple[Response, int] | Response:
    """Return OHLCV candle data for charting."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    if not instrument:
        return jsonify({"error": "instrument param required"}), 400

    contract = request.args.get("contract")
    interval = request.args.get("interval", "1minute")
    from_date = request.args.get("from")
    to_date = request.args.get("to")

    # Validate interval
    valid_intervals = ("1minute", "5minute", "15minute", "1hour", "1day")
    if interval not in valid_intervals:
        return jsonify({"error": f"Invalid interval. Must be one of: {', '.join(valid_intervals)}"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_candle_data(
        instrument_key=instrument,
        contract_key=contract,
        interval=interval,
        from_date=from_date,
        to_date=to_date,
    )
    return jsonify({"data": data, "count": len(data)})


@analytics_bp.route("/api/analytics/chart-instruments")
@require_auth
def api_analytics_chart_instruments() -> Response:
    """Get instruments that have candle data for the chart viewer."""
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    instruments = engine.get_instruments_for_chart()
    return jsonify({"instruments": instruments})


@analytics_bp.route("/api/analytics/chart-contracts")
@require_auth
def api_analytics_chart_contracts() -> tuple[Response, int] | Response:
    """Get contracts available for charting for a given instrument."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    if not instrument:
        return jsonify({"error": "instrument param required"}), 400

    expiry = request.args.get("expiry")
    engine = AnalyticsEngine(current_app.db_manager)
    contracts = engine.get_contracts_for_chart(instrument, expiry)
    return jsonify({"contracts": contracts})


# ------------------------------------------------------------------
# OI Analysis & Volume Profile (D6)
# ------------------------------------------------------------------


@analytics_bp.route("/oi-analysis")
def oi_analysis_page() -> str | Response:
    """OI Analysis page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("oi_analysis.html")


@analytics_bp.route("/api/analytics/oi-strike")
@require_auth
def api_analytics_oi_strike() -> tuple[Response, int] | Response:
    """OI by strike price for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_oi_by_strike(instrument, expiry)
    return jsonify(data)


@analytics_bp.route("/api/analytics/pcr")
@require_auth
def api_analytics_pcr() -> tuple[Response, int] | Response:
    """Put-Call Ratio trend for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_pcr_trend(instrument, expiry)
    return jsonify(data)


@analytics_bp.route("/api/analytics/max-pain")
@require_auth
def api_analytics_max_pain() -> tuple[Response, int] | Response:
    """Max pain calculation for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.calculate_max_pain(instrument, expiry)
    return jsonify(data)


@analytics_bp.route("/api/analytics/oi-heatmap")
@require_auth
def api_analytics_oi_heatmap() -> tuple[Response, int] | Response:
    """OI heatmap grid for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_oi_heatmap(instrument, expiry)
    return jsonify(data)


@analytics_bp.route("/api/analytics/volume-profile")
@require_auth
def api_analytics_volume_profile() -> tuple[Response, int] | Response:
    """Volume profile for an instrument+expiry."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiry = request.args.get("expiry")
    if not instrument or not expiry:
        return jsonify({"error": "instrument and expiry params required"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_volume_profile(instrument, expiry)
    return jsonify(data)


# ------------------------------------------------------------------
# Coverage Calendar (D14)
# ------------------------------------------------------------------


@analytics_bp.route("/calendar")
def calendar_page() -> str | Response:
    """Coverage calendar heatmap page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("calendar.html")


@analytics_bp.route("/api/analytics/calendar")
@require_auth
def api_analytics_calendar() -> tuple[Response, int] | Response:
    """Return coverage calendar data for an instrument+year."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    if not instrument:
        return jsonify({"error": "instrument param required"}), 400

    import datetime

    year = request.args.get("year", datetime.date.today().year, type=int)
    if year < 2000 or year > 2100:
        return jsonify({"error": "year must be between 2000 and 2100"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    calendar = engine.get_coverage_calendar(instrument, year)
    return jsonify({"instrument": instrument, "year": year, "calendar": calendar})


# ------------------------------------------------------------------
# Comparative Expiry Analysis (D11)
# ------------------------------------------------------------------


@analytics_bp.route("/expiry-comparison")
def expiry_comparison_page() -> str | Response:
    """Comparative expiry analysis page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("expiry_comparison.html")


@analytics_bp.route("/api/analytics/expiry-comparison")
@require_auth
def api_analytics_expiry_comparison() -> tuple[Response, int] | Response:
    """Compare metrics across multiple expiry dates for one instrument."""
    from src.analytics.engine import AnalyticsEngine

    instrument = request.args.get("instrument")
    expiries_raw = request.args.get("expiries", "")
    if not instrument or not expiries_raw:
        return jsonify({"error": "instrument and expiries params required"}), 400

    expiry_dates = [e.strip() for e in expiries_raw.split(",") if e.strip()]
    if len(expiry_dates) < 2:
        return jsonify({"error": "at least 2 expiry dates required"}), 400
    if len(expiry_dates) > 5:
        return jsonify({"error": "maximum 5 expiry dates allowed"}), 400

    engine = AnalyticsEngine(current_app.db_manager)
    data = engine.get_expiry_comparison(instrument, expiry_dates)
    return jsonify({"instrument": instrument, "comparison": data})
