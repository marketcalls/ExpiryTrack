"""Flask error handlers — JSON for API paths, HTML for browsers."""

import logging

from flask import jsonify, render_template, request

logger = logging.getLogger(__name__)


def _wants_json():
    """Return True if the client prefers JSON (API requests)."""
    return (
        request.path.startswith("/api/")
        or request.accept_mimetypes.best_match(["application/json", "text/html"]) == "application/json"
    )


def register_error_handlers(app):
    """Register 404, 405, and 500 error handlers on the Flask app."""

    @app.errorhandler(404)
    def not_found(e):
        if _wants_json():
            return jsonify({"error": "Not found", "status": 404}), 404
        return render_template("errors/404.html"), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        if _wants_json():
            return jsonify({"error": "Method not allowed", "status": 405}), 405
        return render_template("errors/404.html"), 405

    @app.errorhandler(500)
    def internal_error(e):
        logger.error(f"Internal server error: {e}", exc_info=True)
        if _wants_json():
            return jsonify({"error": "Internal server error", "status": 500}), 500
        return render_template("errors/500.html"), 500
