"""Watchlists blueprint — CRUD + items."""

from flask import Blueprint, Response, current_app, jsonify, request

from src.routes.helpers import require_auth
from src.routes.validators import WatchlistInput

watchlists_bp = Blueprint("watchlists", __name__)


@watchlists_bp.route("/api/watchlists", methods=["GET"])
@require_auth
def api_watchlists_list() -> Response:
    """List all watchlists."""
    return jsonify({"watchlists": current_app.db_manager.get_watchlists()})


@watchlists_bp.route("/api/watchlists", methods=["POST"])
@require_auth
def api_watchlists_create() -> tuple[Response, int] | Response:
    """Create a new watchlist."""
    from src.routes.helpers import validate_json

    validated, err = validate_json(WatchlistInput)
    if err:
        return err
    wl_id = current_app.db_manager.create_watchlist(validated.name, validated.segment)
    return jsonify({"success": True, "id": wl_id})


@watchlists_bp.route("/api/watchlists/<int:watchlist_id>", methods=["GET"])
@require_auth
def api_watchlist_items(watchlist_id) -> Response:
    """Get items in a watchlist."""
    items = current_app.db_manager.get_watchlist_items(watchlist_id)
    return jsonify({"items": items})


@watchlists_bp.route("/api/watchlists/<int:watchlist_id>/items", methods=["POST"])
@require_auth
def api_watchlist_add_items(watchlist_id) -> tuple[Response, int] | Response:
    """Add instruments to a watchlist."""
    data = request.json or {}
    keys = data.get("instrument_keys", [])
    if not keys:
        return jsonify({"error": "instrument_keys list is required"}), 400
    count = current_app.db_manager.add_to_watchlist(watchlist_id, keys)
    return jsonify({"success": True, "added": count})


@watchlists_bp.route("/api/watchlists/<int:watchlist_id>/items/<path:instrument_key>", methods=["DELETE"])
@require_auth
def api_watchlist_remove_item(watchlist_id, instrument_key) -> Response:
    """Remove an instrument from a watchlist."""
    current_app.db_manager.remove_from_watchlist(watchlist_id, instrument_key)
    return jsonify({"success": True})


@watchlists_bp.route("/api/watchlists/<int:watchlist_id>", methods=["DELETE"])
@require_auth
def api_watchlist_delete(watchlist_id) -> Response:
    """Delete a watchlist."""
    current_app.db_manager.delete_watchlist(watchlist_id)
    return jsonify({"success": True})
