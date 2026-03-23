"""Admin blueprint — backup, scheduler, api-keys, scheduler-history, data management, import, comparison."""

import json
import logging
import os
import re
import secrets
import tempfile

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from src.config import config
from src.routes.helpers import require_auth

logger = logging.getLogger(__name__)

# Maximum upload file size for import/compare (100 MB)
MAX_UPLOAD_SIZE = 100 * 1024 * 1024
ALLOWED_EXTENSIONS = {".csv", ".parquet"}

admin_bp = Blueprint("admin", __name__)


# ── API Key Management ──


@admin_bp.route("/api/api-keys", methods=["GET"])
@require_auth
def api_keys_list() -> Response:
    """List all API keys."""
    keys = current_app.db_manager.list_api_keys()
    return jsonify({"keys": keys})


@admin_bp.route("/api/api-keys", methods=["POST"])
@require_auth
def api_keys_create() -> tuple[Response, int] | Response:
    """Generate a new API key."""
    data = request.json or {}
    name = data.get("name", "Unnamed Key")
    key = current_app.db_manager.create_api_key(name)
    if key:
        return jsonify({"success": True, "key": key})
    return jsonify({"error": "Failed to create key"}), 500


@admin_bp.route("/api/api-keys/<int:key_id>", methods=["DELETE"])
@require_auth
def api_keys_revoke(key_id) -> Response:
    """Revoke an API key."""
    current_app.db_manager.revoke_api_key(key_id)
    return jsonify({"success": True})


# ── Backup & Restore ──


@admin_bp.route("/api/backup/create", methods=["POST"])
@require_auth
def api_backup_create() -> Response:
    """Create a database backup."""
    from src.backup.manager import BackupManager

    mgr = BackupManager(current_app.db_manager)
    result = mgr.create_backup()
    return jsonify({"success": True, **result})


@admin_bp.route("/api/backup/list")
@require_auth
def api_backup_list() -> Response:
    """List all backups."""
    from src.backup.manager import BackupManager

    mgr = BackupManager(current_app.db_manager)
    return jsonify({"backups": mgr.list_backups()})


@admin_bp.route("/api/backup/download/<filename>")
@require_auth
def api_backup_download(filename) -> tuple[Response, int] | Response:
    """Download a backup file."""
    from src.backup.manager import BackupManager

    mgr = BackupManager(current_app.db_manager)
    path = mgr.get_backup_path(filename)
    if not path:
        return jsonify({"error": "Backup not found"}), 404
    return send_file(str(path), as_attachment=True, download_name=filename)


@admin_bp.route("/api/backup/restore", methods=["POST"])
@require_auth
def api_backup_restore() -> tuple[Response, int] | Response:
    """Restore from an uploaded backup ZIP."""
    from src.backup.manager import BackupManager

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    uploaded = request.files["file"]
    if not uploaded.filename or not uploaded.filename.endswith(".zip"):
        return jsonify({"error": "File must be a .zip archive"}), 400

    mgr = BackupManager(current_app.db_manager)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        uploaded.save(tmp)
        tmp_path = tmp.name

    try:
        mgr.restore_backup(tmp_path)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        os.unlink(tmp_path)


@admin_bp.route("/api/backup/delete/<filename>", methods=["DELETE"])
@require_auth
def api_backup_delete(filename) -> tuple[Response, int] | Response:
    """Delete a backup."""
    from src.backup.manager import BackupManager

    mgr = BackupManager(current_app.db_manager)
    if mgr.delete_backup(filename):
        return jsonify({"success": True})
    return jsonify({"error": "Backup not found"}), 404


# ── Scheduler ──


@admin_bp.route("/api/scheduler/status")
@require_auth
def api_scheduler_status() -> Response:
    """Get scheduler status and job list."""
    from src.scheduler.scheduler import scheduler_manager

    return jsonify(scheduler_manager.get_status())


@admin_bp.route("/api/scheduler/toggle", methods=["POST"])
@require_auth
def api_scheduler_toggle() -> tuple[Response, int] | Response:
    """Start or stop the scheduler."""
    from src.scheduler.scheduler import scheduler_manager

    action = request.json.get("action") if request.json else None
    if action == "start":
        config.SCHEDULER_ENABLED = True
        scheduler_manager.start()
        return jsonify({"running": True, "message": "Scheduler started"})
    elif action == "stop":
        scheduler_manager.stop()
        config.SCHEDULER_ENABLED = False
        return jsonify({"running": False, "message": "Scheduler stopped"})
    return jsonify({"error": "action must be start or stop"}), 400


@admin_bp.route("/api/scheduler/jobs/<job_id>/pause", methods=["POST"])
@require_auth
def api_scheduler_pause_job(job_id) -> tuple[Response, int] | Response:
    """Pause a scheduled job."""
    from src.scheduler.scheduler import scheduler_manager

    if scheduler_manager.pause_job(job_id):
        return jsonify({"paused": True})
    return jsonify({"error": "Job not found"}), 404


@admin_bp.route("/api/scheduler/jobs/<job_id>/resume", methods=["POST"])
@require_auth
def api_scheduler_resume_job(job_id) -> tuple[Response, int] | Response:
    """Resume a paused job."""
    from src.scheduler.scheduler import scheduler_manager

    if scheduler_manager.resume_job(job_id):
        return jsonify({"paused": False})
    return jsonify({"error": "Job not found"}), 404


@admin_bp.route("/api/scheduler/jobs/<job_id>", methods=["DELETE"])
@require_auth
def api_scheduler_remove_job(job_id) -> tuple[Response, int] | Response:
    """Remove a scheduled job."""
    from src.scheduler.scheduler import scheduler_manager

    if scheduler_manager.remove_job(job_id):
        return jsonify({"removed": True})
    return jsonify({"error": "Job not found"}), 404


@admin_bp.route("/api/scheduler/history")
@require_auth
def api_scheduler_history() -> Response:
    """Get scheduler job execution history."""
    from src.scheduler.scheduler import scheduler_manager

    limit = request.args.get("limit", 20, type=int)
    return jsonify({"history": scheduler_manager.get_history(limit)})


# ── Data Management ──


@admin_bp.route("/data-management")
def data_management_page() -> str | Response:
    """Data management page — delete data by instrument, expiry, or date range."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("data_management.html")


@admin_bp.route("/api/admin/storage-estimate")
@require_auth
def api_storage_estimate() -> Response:
    """Get storage estimates, optionally filtered by instrument."""
    instrument_key = request.args.get("instrument_key")
    estimate = current_app.db_manager.historical.get_storage_estimate(instrument_key)
    return jsonify(estimate)


@admin_bp.route("/api/admin/delete-data", methods=["POST"])
@require_auth
def api_delete_data() -> tuple[Response, int] | Response:
    """Delete data by instrument, expiry, or date range."""
    data = request.json
    if not data:
        return jsonify({"error": "Request body required"}), 400

    delete_type = data.get("type")
    instrument_key = data.get("instrument_key")

    if delete_type == "instrument":
        if not instrument_key:
            return jsonify({"error": "instrument_key required"}), 400
        count = current_app.db_manager.historical.delete_by_instrument(instrument_key)
        return jsonify({"success": True, "deleted_rows": count})

    elif delete_type == "expiry":
        expiry_date = data.get("expiry_date")
        if not instrument_key or not expiry_date:
            return jsonify({"error": "instrument_key and expiry_date required"}), 400
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date):
            return jsonify({"error": "Invalid expiry_date format"}), 400
        count = current_app.db_manager.historical.delete_by_expiry(instrument_key, expiry_date)
        return jsonify({"success": True, "deleted_rows": count})

    elif delete_type == "date_range":
        from_date = data.get("from_date")
        to_date = data.get("to_date")
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400
        for d in [from_date, to_date]:
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
                return jsonify({"error": f"Invalid date format: {d}"}), 400
        count = current_app.db_manager.historical.delete_by_date_range(from_date, to_date, instrument_key)
        return jsonify({"success": True, "deleted_rows": count})

    return jsonify({"error": "type must be 'instrument', 'expiry', or 'date_range'"}), 400


@admin_bp.route("/api/admin/instruments-list")
@require_auth
def api_instruments_list() -> Response:
    """List all instruments with data for data management dropdowns."""
    from src.analytics.engine import AnalyticsEngine

    engine = AnalyticsEngine(current_app.db_manager)
    instruments = engine.get_available_instruments_with_contracts()
    return jsonify(instruments)


# ── Notifications ──


@admin_bp.route("/api/notifications")
@require_auth
def api_notifications_list() -> Response:
    """Get recent notifications."""
    from src.notifications.manager import NotificationManager

    limit = min(request.args.get("limit", 20, type=int), 100)
    mgr = NotificationManager(current_app.db_manager)
    return jsonify({"notifications": mgr.get_recent(limit)})


@admin_bp.route("/api/notifications/unread-count")
@require_auth
def api_notifications_unread_count() -> Response:
    """Get count of unread notifications."""
    from src.notifications.manager import NotificationManager

    mgr = NotificationManager(current_app.db_manager)
    return jsonify({"count": mgr.get_unread_count()})


@admin_bp.route("/api/notifications/mark-read", methods=["POST"])
@require_auth
def api_notifications_mark_read() -> tuple[Response, int] | Response:
    """Mark one or all notifications as read.

    Body: {"id": 123}  — mark single
    Body: {"all": true} — mark all
    """
    from src.notifications.manager import NotificationManager

    data = request.json or {}
    mgr = NotificationManager(current_app.db_manager)

    if data.get("all"):
        mgr.mark_all_read()
        return jsonify({"success": True})

    nid = data.get("id")
    if not nid:
        return jsonify({"error": "id or all=true required"}), 400

    mgr.mark_read(int(nid))
    return jsonify({"success": True})


# ── External Data Import (D15) ──


def _validate_upload_file(request_files, field_name: str = "file") -> tuple[None, None] | tuple:
    """Validate an uploaded file: presence, extension, size.

    Returns (uploaded_file, error_response) — one will be None.
    """
    if field_name not in request_files:
        return None, (jsonify({"error": "No file uploaded"}), 400)

    uploaded = request_files[field_name]
    if not uploaded.filename:
        return None, (jsonify({"error": "No file selected"}), 400)

    ext = os.path.splitext(uploaded.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return None, (jsonify({"error": f"Unsupported file type: {ext}. Allowed: .csv, .parquet"}), 400)

    # Check Content-Length header as a quick size check
    content_length = request.content_length or 0
    if content_length > MAX_UPLOAD_SIZE:
        return None, (jsonify({"error": "File too large. Maximum size is 100 MB."}), 400)

    return uploaded, None


@admin_bp.route("/import")
def import_page() -> str | Response:
    """Import external data page."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("import.html")


@admin_bp.route("/api/admin/import/preview", methods=["POST"])
@require_auth
def api_import_preview() -> tuple[Response, int] | Response:
    """Upload a file and return a preview with auto-detected column mapping."""
    from src.export.importer import DataImporter

    uploaded, error = _validate_upload_file(request.files)
    if error:
        return error

    ext = os.path.splitext(uploaded.filename)[1].lower()
    file_type = "parquet" if ext == ".parquet" else "csv"

    # Save to a temp file
    suffix = ext
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False, dir=tempfile.gettempdir()) as tmp:
        uploaded.save(tmp)
        tmp_path = tmp.name

    try:
        importer = DataImporter(current_app.db_manager)
        preview = importer.preview_file(tmp_path, file_type)
        # Store path server-side — send back only an opaque token to prevent path traversal
        file_token = secrets.token_urlsafe(32)
        session[f"import_file_{file_token}"] = tmp_path
        preview["file_token"] = file_token
        preview["file_type"] = file_type
        return jsonify(preview)
    except (FileNotFoundError, ValueError) as e:
        # Clean up on error
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        logger.exception("Import preview failed")
        return jsonify({"error": f"Failed to process file: {e}"}), 500


@admin_bp.route("/api/admin/import/execute", methods=["POST"])
@require_auth
def api_import_execute() -> tuple[Response, int] | Response:
    """Execute the import with the provided column mapping."""
    from src.export.importer import DataImporter

    data = request.json
    if not data:
        return jsonify({"error": "Request body required"}), 400

    file_token = data.get("file_token")
    column_mapping = data.get("column_mapping")
    instrument_key = data.get("instrument_key")

    if not file_token:
        return jsonify({"error": "file_token is required"}), 400
    if not column_mapping or not isinstance(column_mapping, dict):
        return jsonify({"error": "column_mapping is required (dict)"}), 400
    if not instrument_key:
        return jsonify({"error": "instrument_key is required"}), 400

    # Resolve server-side path from session token — never trust a client-supplied path
    real_path = session.pop(f"import_file_{file_token}", None)
    if not real_path:
        return jsonify({"error": "Invalid or expired file token. Please re-upload."}), 400

    if not os.path.exists(real_path):
        return jsonify({"error": "File not found. It may have been cleaned up. Please re-upload."}), 400

    ext = os.path.splitext(real_path)[1].lower()
    file_type = "parquet" if ext == ".parquet" else "csv"

    try:
        importer = DataImporter(current_app.db_manager)
        result = importer.import_file(real_path, column_mapping, instrument_key, file_type)
        return jsonify({"success": True, **result})
    except (FileNotFoundError, ValueError) as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception("Import execution failed")
        return jsonify({"error": f"Import failed: {e}"}), 500
    finally:
        # Clean up temp file after import
        try:
            os.unlink(real_path)
        except OSError:
            pass


# ── Data Comparison (D15) ──


@admin_bp.route("/data-comparison")
def data_comparison_page() -> str | Response:
    """Data comparison page — compare uploaded data against stored data."""
    if not current_app.auth_manager.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("data_comparison.html")


@admin_bp.route("/api/admin/compare", methods=["POST"])
@require_auth
def api_compare_data() -> tuple[Response, int] | Response:
    """Upload a reference file and compare it against stored data."""
    import pandas as pd

    from src.analytics.engine import AnalyticsEngine

    uploaded, error = _validate_upload_file(request.files)
    if error:
        return error

    instrument_key = request.form.get("instrument_key")
    expiry_date = request.form.get("expiry_date")

    if not instrument_key:
        return jsonify({"error": "instrument_key is required"}), 400
    if not expiry_date:
        return jsonify({"error": "expiry_date is required"}), 400
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date):
        return jsonify({"error": "Invalid expiry_date format (expected YYYY-MM-DD)"}), 400

    # Parse optional column mapping
    column_mapping_str = request.form.get("column_mapping", "{}")
    try:
        column_mapping = json.loads(column_mapping_str)
    except (json.JSONDecodeError, TypeError):
        column_mapping = {}

    ext = os.path.splitext(uploaded.filename)[1].lower()
    suffix = ext

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False, dir=tempfile.gettempdir()) as tmp:
        uploaded.save(tmp)
        tmp_path = tmp.name

    try:
        # Read the uploaded file
        if ext == ".parquet":
            df = pd.read_parquet(tmp_path)
        else:
            df = pd.read_csv(tmp_path)

        if df.empty:
            return jsonify({"error": "Uploaded file is empty"}), 400

        # Apply column mapping if provided (rename source columns to standard names)
        if column_mapping:
            rename_map = {}
            for target, source in column_mapping.items():
                if source and source in df.columns and target != source:
                    rename_map[source] = target
            if rename_map:
                df = df.rename(columns=rename_map)

        # Validate that the required columns exist
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            return jsonify({
                "error": f"Missing required columns in uploaded file: {sorted(missing)}. "
                         f"Available columns: {sorted(df.columns.tolist())}. "
                         "Use the column mapping to rename columns."
            }), 400

        engine = AnalyticsEngine(current_app.db_manager)
        result = engine.compare_datasets(df, instrument_key, expiry_date)
        return jsonify(result)

    except Exception as e:
        logger.exception("Data comparison failed")
        return jsonify({"error": f"Comparison failed: {e}"}), 500
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
