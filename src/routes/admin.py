"""Admin blueprint — backup, scheduler, api-keys, scheduler-history."""

import os
import tempfile

from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    request,
    send_file,
)

from src.config import config
from src.routes.helpers import require_auth

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
