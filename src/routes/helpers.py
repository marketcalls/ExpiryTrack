"""Shared route helpers — auth decorator and standardized response builders."""

from functools import wraps

from flask import current_app, jsonify, request
from pydantic import ValidationError


def require_auth(f):
    """Decorator: checks Upstox token validity, returns 401 JSON if invalid."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_app.auth_manager.is_token_valid():
            return api_error("Not authenticated", 401)
        return f(*args, **kwargs)

    return decorated


def api_error(message, status_code=400, details=None):
    """Standardized error response: {"error": "...", "status": N}"""
    payload = {"error": message, "status": status_code}
    if details:
        payload.update(details)
    return jsonify(payload), status_code


def api_success(data=None, status_code=200):
    """Standardized success response: {"success": true, ...}"""
    payload = {"success": True}
    if data:
        payload.update(data)
    return jsonify(payload), status_code


def is_htmx_request() -> bool:
    """Check if the current request is an htmx request."""
    return request.headers.get("HX-Request") == "true"


def validate_json(model_class):
    """Parse and validate request JSON against a Pydantic model.

    Returns (model_instance, None) on success, or (None, error_response) on failure.
    """
    data = request.json
    if not data:
        return None, api_error("Request body is required")
    try:
        return model_class(**data), None
    except ValidationError as e:
        errors = [{"field": err["loc"][-1], "message": err["msg"]} for err in e.errors()]
        return None, api_error("Validation failed", 400, {"details": errors})
