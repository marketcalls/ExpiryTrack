"""
API Key Authentication for ExpiryTrack REST API (#10)
"""

from functools import wraps

from flask import current_app, jsonify, request


def require_api_key(f):
    """Decorator that validates X-API-Key header against the api_keys table."""

    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            return jsonify({"error": "Missing X-API-Key header"}), 401

        key_info = current_app.db_manager.verify_api_key(api_key)
        if not key_info:
            return jsonify({"error": "Invalid or revoked API key"}), 401

        # Attach key info to request for downstream use
        request.api_key_info = key_info
        return f(*args, **kwargs)

    return decorated
