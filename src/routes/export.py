"""Export blueprint — export wizard, available-expiries, start, status, download."""

import logging
import os
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
    send_file,
    session,
    url_for,
)

from src.routes.helpers import require_auth
from src.routes.validators import ExportInput

export_bp = Blueprint("export", __name__)

# Lazy-initialized persistent tracker (needs db_manager from app context)
_export_tracker = None


def _get_export_tracker():
    global _export_tracker
    if _export_tracker is None:
        from src.tasks import PersistentTaskTracker

        _export_tracker = PersistentTaskTracker("export", current_app.db_manager)
    return _export_tracker


# Backward-compat alias used by instruments blueprint
class _ExportTrackerProxy:
    """Proxy that lazily resolves the export tracker within app context."""

    def create(self, *a, **kw):
        return _get_export_tracker().create(*a, **kw)

    def update(self, *a, **kw):
        return _get_export_tracker().update(*a, **kw)

    def get(self, *a, **kw):
        return _get_export_tracker().get(*a, **kw)

    def list_active(self):
        return _get_export_tracker().list_active()

    def list_all(self):
        return _get_export_tracker().list_all()

    def cleanup(self):
        return _get_export_tracker().cleanup()


export_tracker = _ExportTrackerProxy()

logger = logging.getLogger(__name__)


@export_bp.route("/export")
def export_wizard() -> str | Response:
    """Export wizard page."""
    auth = current_app.auth_manager
    if not auth.has_credentials():
        session["error"] = "Please configure API credentials first"
        return redirect(url_for("auth.settings"))
    if not auth.is_token_valid():
        session["error"] = "Please authenticate first"
        return redirect(url_for("auth.login"))
    return render_template("export_wizard.html")


@export_bp.route("/api/export/available-expiries", methods=["POST"])
@require_auth
def api_export_available_expiries() -> Response:
    """Get available expiries for selected instruments."""
    from src.export.exporter import DataExporter

    data = request.json
    instruments = data.get("instruments", [])
    exporter = DataExporter(current_app.db_manager)
    expiries = exporter.get_available_expiries(instruments)
    return jsonify(expiries)


@export_bp.route("/api/export/start", methods=["POST"])
@require_auth
def api_export_start() -> tuple[Response, int] | Response:
    """Start export task."""
    db = current_app.db_manager

    from src.export.exporter import DataExporter
    from src.routes.helpers import validate_json

    validated, err = validate_json(ExportInput)
    if err:
        return err

    export_tracker.cleanup()

    task_id = str(uuid.uuid4())

    export_tracker.create(
        task_id,
        {
            "task_id": task_id,
            "status": "processing",
            "progress": 0,
            "status_message": "Preparing export...",
            "file_path": None,
            "error": None,
            "created_at": datetime.now().isoformat(),
        },
    )

    export_data = validated.model_dump()

    def run_export():
        try:
            exporter = DataExporter(db)

            export_tracker.update(task_id, progress=20, status_message="Gathering data...")

            fmt = export_data["format"]
            instruments = export_data["instruments"]
            expiries = export_data["expiries"]
            options = export_data["options"]

            export_tracker.update(task_id, progress=50, status_message=f"Exporting to {fmt.upper()}...")

            if fmt == "csv":
                file_path = exporter.export_to_csv(instruments, expiries, options, task_id)
            elif fmt == "json":
                file_path = exporter.export_to_json(instruments, expiries, options, task_id)
            elif fmt == "zip":
                file_path = exporter.export_to_zip(instruments, expiries, options, task_id)
            elif fmt == "parquet":
                file_path = exporter.export_to_parquet(instruments, expiries, options, task_id)
            else:
                raise ValueError(f"Unknown format: {fmt}")

            export_tracker.update(
                task_id,
                status="completed",
                progress=100,
                status_message="Export completed successfully!",
                file_path=file_path,
            )

        except Exception as e:
            import traceback

            logger.error(f"Export failed: {e}")
            logger.error(traceback.format_exc())
            export_tracker.update(task_id, status="failed", error=str(e), status_message=f"Export failed: {e}")

    thread = threading.Thread(target=run_export)
    thread.start()

    return jsonify({"task_id": task_id})


@export_bp.route("/api/export/status/<task_id>")
@require_auth
def api_export_status(task_id) -> tuple[Response, int] | Response:
    """Get export task status."""
    task = export_tracker.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


@export_bp.route("/api/export/download/<task_id>")
@require_auth
def api_export_download(task_id) -> tuple[Response, int] | Response:
    """Download exported file."""
    task = export_tracker.get(task_id)

    if not task:
        return jsonify({"error": "Task not found"}), 404
    if task["status"] != "completed":
        return jsonify({"error": "Export not completed"}), 400

    file_path = task["file_path"]
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    exports_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "exports"))
    real_path = os.path.realpath(file_path)
    if not real_path.startswith(exports_dir + os.sep):
        return jsonify({"error": "Access denied"}), 403

    filename = os.path.basename(real_path)
    return send_file(real_path, as_attachment=True, download_name=filename, mimetype="application/octet-stream")
