"""Auth blueprint — login, callback, logout, credentials, token-status, settings page."""

import asyncio
import secrets

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from pydantic import ValidationError

from src.routes.validators import CredentialsInput

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/settings")
def settings() -> str:
    """Settings page for API credentials."""
    auth = current_app.auth_manager
    db = current_app.db_manager
    has_credentials = auth.has_credentials()
    credential = None
    if has_credentials:
        creds = db.get_credentials()
        if creds:
            credential = {
                "api_key": creds["api_key"],
                "api_secret": "***" + creds["api_secret"][-4:] if creds["api_secret"] else "",
                "redirect_url": creds["redirect_uri"],
            }
    return render_template("settings.html", credential=credential, has_credentials=has_credentials)


@auth_bp.route("/save_credentials", methods=["POST"])
def save_credentials() -> Response:
    """Save API credentials to database (encrypted)."""
    auth = current_app.auth_manager

    try:
        validated = CredentialsInput(
            api_key=request.form.get("api_key", "").strip(),
            api_secret=request.form.get("api_secret", "").strip(),
            redirect_url=request.form.get("redirect_url"),
        )
    except ValidationError as e:
        session["error"] = e.errors()[0]["msg"]
        return redirect(url_for("auth.settings"))

    if auth.save_credentials(validated.api_key, validated.api_secret, validated.redirect_url):
        session["message"] = "Credentials saved successfully!"
    else:
        session["error"] = "Failed to save credentials"

    return redirect(url_for("auth.settings"))


@auth_bp.route("/login")
def login() -> Response:
    """Start OAuth login flow."""
    auth = current_app.auth_manager
    if not auth.has_credentials():
        session["error"] = "Please configure API credentials first"
        return redirect(url_for("auth.settings"))

    try:
        auth_url = auth.get_authorization_url()
        session["oauth_state"] = auth._oauth_state
        return redirect(auth_url)
    except ValueError as e:
        session["error"] = str(e)
        return redirect(url_for("auth.settings"))


@auth_bp.route("/upstox/callback")
def upstox_callback() -> str | Response:
    """Handle OAuth callback."""
    auth = current_app.auth_manager
    auth_code = request.args.get("code")
    error = request.args.get("error")
    state = request.args.get("state")

    if error:
        from markupsafe import escape

        return f"Authentication failed: {escape(error)}", 400

    expected_state = session.pop("oauth_state", None)
    if not expected_state:
        expected_state = getattr(auth, "_oauth_state", None)
    if not expected_state or not state or not secrets.compare_digest(expected_state, state):
        auth.clear_oauth_state()
        return "Invalid OAuth state parameter.", 403

    auth.clear_oauth_state()

    if auth_code:
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(auth.exchange_code_for_token(auth_code))
        finally:
            loop.close()

        if success:
            # Publish token update via SSE
            try:
                from src.sse.stream import sse_broker

                sse_broker.publish("auth:token_updated", {"valid": True})
            except Exception:
                pass
            flash("Successfully authenticated with Upstox! You can now start collecting data.", "success")
            return redirect(url_for("index"))
        else:
            flash("Failed to authenticate with Upstox. Please try again.", "error")
            return redirect(url_for("auth.settings"))

    return "No authorization code received", 400


@auth_bp.route("/logout")
def logout() -> Response:
    """Logout and clear tokens."""
    current_app.auth_manager.clear_tokens()
    session.clear()
    return redirect(url_for("index"))


@auth_bp.route("/api/auth/token-status")
def api_auth_token_status() -> Response:
    """Return token validity and remaining time."""
    import time as _time

    auth = current_app.auth_manager
    valid = auth.is_token_valid()
    expiry = auth.token_expiry
    remaining = max(0, expiry - _time.time()) if expiry and valid else 0
    return jsonify(
        {
            "valid": valid,
            "expiry": expiry,
            "remaining_seconds": int(remaining),
        }
    )
