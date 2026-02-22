"""Instruments blueprint — CRUD, FO import, master sync/browse/search/segments/types/keys, page."""

import threading
import uuid
from datetime import datetime

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

from src.routes.export import export_tracker
from src.routes.helpers import is_htmx_request, require_auth
from src.routes.validators import InstrumentInput

VALID_FO_CATEGORIES = {"stock", "commodity", "bse_stock"}

instruments_bp = Blueprint("instruments", __name__)


@instruments_bp.route("/instruments")
def instruments_page() -> str | Response:
    """Instrument browser page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("instruments.html")


# ── Partials (htmx) ──


def _render_instrument_list() -> str:
    """Render the instrument list partial grouped by category."""
    instruments = current_app.db_manager.get_active_instruments()
    grouped: dict[str, list] = {}
    for inst in instruments:
        cat = inst.get("category", "Index") if isinstance(inst, dict) else "Index"
        grouped.setdefault(cat, []).append(inst)
    return render_template(
        "partials/instrument_list.html",
        instruments=instruments,
        grouped=grouped,
    )


@instruments_bp.route("/partials/instruments/list")
def partials_instruments_list() -> str:
    """HTML fragment of the instrument list — for htmx."""
    return _render_instrument_list()


# ── CRUD ──


@instruments_bp.route("/api/instruments", methods=["GET"])
def api_instruments_list() -> Response:
    """List all instruments."""
    instruments = current_app.db_manager.get_active_instruments()
    return jsonify({"instruments": instruments})


@instruments_bp.route("/api/instruments", methods=["POST"])
@require_auth
def api_instruments_add() -> tuple[Response, int] | Response:
    """Add a new instrument."""
    from src.routes.helpers import validate_json

    validated, err = validate_json(InstrumentInput)
    if err:
        return err
    new_id = current_app.db_manager.add_instrument(
        validated.instrument_key,
        validated.symbol,
        validated.priority,
        validated.category,
    )
    if new_id:
        from src.utils.instrument_mapper import refresh_cache

        refresh_cache()
        if is_htmx_request():
            return _render_instrument_list()
        return jsonify({"success": True, "id": new_id})
    return jsonify({"error": "Instrument already exists"}), 409


@instruments_bp.route("/api/instruments/<int:instrument_id>", methods=["PATCH"])
@require_auth
def api_instruments_toggle(instrument_id) -> Response:
    """Toggle instrument active status."""
    data = request.json or {}
    is_active = data.get("is_active", True)
    current_app.db_manager.toggle_instrument(instrument_id, is_active)
    from src.utils.instrument_mapper import refresh_cache

    refresh_cache()
    return jsonify({"success": True})


@instruments_bp.route("/api/instruments/<int:instrument_id>", methods=["DELETE"])
@require_auth
def api_instruments_delete(instrument_id) -> Response | str:
    """Remove an instrument."""
    current_app.db_manager.remove_instrument(instrument_id)
    from src.utils.instrument_mapper import refresh_cache

    refresh_cache()
    if is_htmx_request():
        return _render_instrument_list()
    return jsonify({"success": True})


# ── F&O Import ──


@instruments_bp.route("/api/instruments/fo-available", methods=["GET"])
@require_auth
def api_instruments_fo_available() -> tuple[Response, int] | Response:
    """List F&O-eligible instruments not yet added."""
    category = request.args.get("category")
    if category and category not in VALID_FO_CATEGORIES:
        return jsonify({"error": "Invalid category"}), 400
    available = current_app.db_manager.get_fo_available_instruments(category)
    return jsonify({"instruments": available, "count": len(available)})


@instruments_bp.route("/api/instruments/import-fo", methods=["POST"])
@require_auth
def api_instruments_import_fo() -> tuple[Response, int] | Response:
    """Bulk import F&O instruments."""
    data = request.json or {}
    category = data.get("category")
    if category and category not in VALID_FO_CATEGORIES:
        return jsonify({"error": "Invalid category"}), 400
    result = current_app.db_manager.bulk_import_fo_instruments(category)
    from src.utils.instrument_mapper import refresh_cache

    refresh_cache()
    return jsonify(
        {
            "success": True,
            "added": result["added"],
            "skipped": result["skipped"],
            "total_available": result["total_available"],
        }
    )


# ── Master Sync/Browse/Search ──


@instruments_bp.route("/api/instruments/master/sync", methods=["POST"])
@require_auth
def api_instruments_master_sync() -> Response:
    """Sync instrument master data from Upstox."""
    from src.instruments.master import InstrumentMaster

    db = current_app.db_manager
    data = request.json or {}
    exchanges = data.get("exchanges")

    master = InstrumentMaster(db)
    task_id = str(uuid.uuid4())

    export_tracker.create(
        task_id,
        {
            "task_id": task_id,
            "status": "processing",
            "progress": 10,
            "status_message": "Downloading instrument master files...",
            "error": None,
            "created_at": datetime.now().isoformat(),
        },
    )

    def _sync():
        try:
            results = master.sync(exchanges)
            total = sum(v for v in results.values() if v > 0)
            export_tracker.update(
                task_id,
                status="completed",
                progress=100,
                status_message=f"Synced {total:,} instruments",
                results=results,
            )
        except Exception as e:
            export_tracker.update(task_id, status="failed", error=str(e), status_message=f"Sync failed: {e}")

    thread = threading.Thread(target=_sync)
    thread.start()

    return jsonify({"task_id": task_id, "status": "started"})


@instruments_bp.route("/api/instruments/master/segments")
@require_auth
def api_instruments_master_segments() -> Response:
    """Get available segments with counts."""
    db = current_app.db_manager
    segments = db.get_instrument_master_segments()
    last_sync = db.get_instrument_master_last_sync()
    return jsonify(
        {
            "segments": segments,
            "last_sync": last_sync.isoformat() if last_sync else None,
            "total": db.get_instrument_master_count(),
        }
    )


@instruments_bp.route("/api/instruments/master/types")
@require_auth
def api_instruments_master_types() -> tuple[Response, int] | Response:
    """Get distinct instrument types for a segment."""
    segment = request.args.get("segment")
    if not segment:
        return jsonify({"error": "segment is required"}), 400
    types = current_app.db_manager.get_instrument_types_by_segment(segment)
    return jsonify({"types": types})


@instruments_bp.route("/api/instruments/master/keys")
@require_auth
def api_instruments_master_keys() -> tuple[Response, int] | Response:
    """Get all instrument_keys matching segment/type filter."""
    segment = request.args.get("segment")
    if not segment:
        return jsonify({"error": "segment is required"}), 400
    instrument_type = request.args.get("type")
    keys = current_app.db_manager.get_instrument_keys_by_segment(segment, instrument_type)
    return jsonify({"keys": keys, "count": len(keys)})


@instruments_bp.route("/api/instruments/master/search")
@require_auth
def api_instruments_master_search() -> tuple[Response, int] | Response:
    """Search instrument master."""
    db = current_app.db_manager
    query = request.args.get("q", "")
    segment = request.args.get("segment")
    instrument_type = request.args.get("type")
    limit = min(request.args.get("limit", 50, type=int), 200)

    if not query and not segment:
        return jsonify({"error": "query (q) or segment parameter is required"}), 400

    if query:
        results = db.search_instrument_master(query, segment, instrument_type, limit)
    else:
        results = db.get_instruments_by_segment(segment, instrument_type, limit)
    return jsonify({"instruments": results, "count": len(results)})


@instruments_bp.route("/api/instruments/master/browse")
@require_auth
def api_instruments_master_browse() -> tuple[Response, int] | Response:
    """Browse instruments by segment with pagination."""
    db = current_app.db_manager
    segment = request.args.get("segment")
    instrument_type = request.args.get("type")
    limit = min(request.args.get("limit", 100, type=int), 500)
    offset = request.args.get("offset", 0, type=int)

    if not segment:
        return jsonify({"error": "segment parameter is required"}), 400

    results = db.get_instruments_by_segment(segment, instrument_type, limit, offset)
    total = db.get_instrument_master_count(segment, instrument_type)
    return jsonify(
        {
            "instruments": results,
            "count": len(results),
            "total": total,
            "offset": offset,
            "limit": limit,
        }
    )
