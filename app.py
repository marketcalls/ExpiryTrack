"""
ExpiryTrack Web Interface - Flask Application
"""

import os
import secrets
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, session
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect

from src.auth.manager import AuthManager
from src.config import config
from src.database.manager import DatabaseManager
from src.utils.logger import setup_logging

setup_logging()

app = Flask(__name__)
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# CSRF protection — all POST/PUT/PATCH/DELETE endpoints require a token
csrf = CSRFProtect(app)

# Rate limiting — default 60 requests/minute per IP
limiter = Limiter(get_remote_address, app=app, default_limits=["60 per minute"])

# Apply per-endpoint rate limits after blueprints are registered (below)

# Persist secret key across restarts
_secret_key_path = config.DATA_DIR / ".flask_secret_key"
if _secret_key_path.exists():
    app.secret_key = _secret_key_path.read_text().strip()
else:
    app.secret_key = secrets.token_hex(32)
    _secret_key_path.parent.mkdir(parents=True, exist_ok=True)
    _secret_key_path.write_text(app.secret_key)
    try:
        import stat

        os.chmod(_secret_key_path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass

# Initialize managers
auth_manager = AuthManager()
db_manager = DatabaseManager()

# Store managers on app for blueprint access via current_app
app.auth_manager = auth_manager
app.db_manager = db_manager


# Context processor to make is_authenticated available in all templates
@app.context_processor
def inject_auth_status():
    return {"is_authenticated": auth_manager.is_token_valid()}


# ── Register Blueprints ──
from src.routes.admin import admin_bp  # noqa: E402
from src.routes.analytics import analytics_bp  # noqa: E402
from src.routes.auth import auth_bp  # noqa: E402
from src.routes.candles import candles_bp  # noqa: E402
from src.routes.collect import collect_bp  # noqa: E402
from src.routes.export import export_bp  # noqa: E402
from src.routes.instruments import instruments_bp  # noqa: E402
from src.routes.sse import sse_bp  # noqa: E402
from src.routes.status import status_bp  # noqa: E402
from src.routes.tasks import tasks_bp  # noqa: E402
from src.routes.watchlists import watchlists_bp  # noqa: E402

app.register_blueprint(auth_bp)
app.register_blueprint(collect_bp)
app.register_blueprint(export_bp)
app.register_blueprint(instruments_bp)
app.register_blueprint(watchlists_bp)
app.register_blueprint(candles_bp)
app.register_blueprint(status_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(tasks_bp)
app.register_blueprint(sse_bp)

# REST API blueprints (exempt from CSRF — use API key auth, not session cookies)
from src.api.v1 import api_v1  # noqa: E402
from src.api.v2 import api_v2  # noqa: E402

app.register_blueprint(api_v1)
app.register_blueprint(api_v2)
csrf.exempt(api_v1)
csrf.exempt(api_v2)
csrf.exempt(sse_bp)

# Register error handlers (404, 405, 500)
from src.routes.errors import register_error_handlers  # noqa: E402

register_error_handlers(app)

# ── Per-endpoint rate limits ──
# Collection endpoints — each triggers Upstox API calls
limiter.limit("5 per minute")(app.view_functions["collect.api_collect_start"])
# Export endpoints — file generation is CPU-intensive
for _ep in ("export.api_export_start", "export.api_export_available_expiries"):
    if _ep in app.view_functions:
        limiter.limit("10 per minute")(app.view_functions[_ep])
# Backup — full DB copy
if "admin.api_backup_create" in app.view_functions:
    limiter.limit("3 per minute")(app.view_functions["admin.api_backup_create"])
# Auth login — prevent brute force
if "auth.login" in app.view_functions:
    limiter.limit("10 per minute")(app.view_functions["auth.login"])

# ── Core Routes (kept in app.py) ──


@app.route("/")
def index() -> str:
    """Home page."""
    is_authenticated = auth_manager.is_token_valid()
    stats = None
    if is_authenticated:
        try:
            stats = db_manager.get_summary_stats()
        except Exception:
            stats = None
    message = session.pop("message", None)
    error = session.pop("error", None)
    return render_template("index.html", is_authenticated=is_authenticated, stats=stats, message=message, error=error)


@app.route("/help")
def help_page() -> str:
    """Help page."""
    return render_template("help.html")


@app.route("/health")
def health_check() -> Response:
    """Health check endpoint."""
    checks = {"status": "ok", "timestamp": datetime.now(tz=None).isoformat()}
    try:
        count = db_manager.get_instrument_master_count()
        checks["database"] = "ok"
        checks["instrument_count"] = count
    except Exception:
        checks["database"] = "error"
        checks["status"] = "degraded"
    checks["auth"] = "valid" if auth_manager.is_token_valid() else "expired"
    return jsonify(checks)


# Setup default instruments if not already done
db_manager.setup_default_instruments()

# Crash recovery: mark any stale tasks from previous run as failed
try:
    db_manager.tasks_repo.mark_stale_tasks_failed()
except Exception:
    pass

# Start scheduler
from src.scheduler.scheduler import scheduler_manager  # noqa: E402

scheduler_manager.start()


if __name__ == "__main__":
    import sys

    if "--reload" not in sys.argv:
        app.run(debug=False, host=config.HOST, port=config.PORT)
    else:
        app.run(debug=True, use_reloader=False, host=config.HOST, port=config.PORT)
