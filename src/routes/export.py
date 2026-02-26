"""Export blueprint — export wizard, available-expiries, start, status, download."""

import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path

from pydantic import ValidationError

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
    stream_with_context,
    url_for,
)

from src.routes.helpers import require_auth
from src.routes.validators import CandleExportInput, ExportInput, InstrumentMasterExportInput

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


def _cleanup_old_exports(db_manager, days: int = 7) -> None:
    """Delete export files and history records older than N days."""
    expired = db_manager.exports_repo.get_expired_exports(days=days)
    for record in expired:
        file_path = record.get("file_path")
        if file_path:
            try:
                Path(file_path).unlink(missing_ok=True)
            except OSError:
                pass
        db_manager.exports_repo.delete_export(record["id"])
    if expired:
        logger.info(f"Auto-cleaned {len(expired)} export records older than {days} days")


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
    try:
        _cleanup_old_exports(current_app.db_manager)
    except Exception as exc:
        logger.warning(f"Export cleanup failed: {exc}")
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

    try:
        _cleanup_old_exports(db)
    except Exception as exc:
        logger.warning(f"Export cleanup failed: {exc}")
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

            fmt_label = {
                "amibroker": "Amibroker CSV",
                "metatrader": "MetaTrader CSV",
            }.get(fmt, fmt.upper())
            export_tracker.update(task_id, progress=50, status_message=f"Exporting to {fmt_label}...")

            if fmt == "csv":
                file_path = exporter.export_to_csv(instruments, expiries, options, task_id)
            elif fmt == "json":
                file_path = exporter.export_to_json(instruments, expiries, options, task_id)
            elif fmt == "zip":
                file_path = exporter.export_to_zip(instruments, expiries, options, task_id)
            elif fmt == "parquet":
                file_path = exporter.export_to_parquet(instruments, expiries, options, task_id)
            elif fmt == "xlsx":
                file_path = exporter.export_to_xlsx(instruments, expiries, options, task_id)
            elif fmt == "amibroker":
                file_path = exporter.export_to_amibroker(instruments, expiries, options, task_id)
            elif fmt == "metatrader":
                file_path = exporter.export_to_metatrader(instruments, expiries, options, task_id)
            else:
                raise ValueError(f"Unknown format: {fmt}")

            # Save export history
            try:
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                instrument_names = ", ".join(
                    i.split("|")[1] if "|" in i else i for i in instruments
                )
                expiry_summary = ", ".join(
                    f"{k.split('|')[1] if '|' in k else k}: {len(v)}"
                    for k, v in expiries.items()
                )
                contract_types = ", ".join(options.get("contract_types") or []) or None
                # Row count: estimate from file for CSV, or 0 for binary formats
                row_count = 0
                if file_path and os.path.exists(file_path) and fmt in ("csv", "amibroker", "metatrader"):
                    # Count lines minus header
                    with open(file_path) as f:
                        row_count = max(sum(1 for _ in f) - 1, 0)

                db.exports_repo.save_export(
                    export_format=fmt,
                    instruments=instrument_names,
                    expiries=expiry_summary,
                    file_path=file_path,
                    file_size=file_size,
                    row_count=row_count,
                    contract_types=contract_types,
                )
            except Exception as hist_err:
                logger.warning(f"Failed to save export history: {hist_err}")

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


@export_bp.route("/api/export/history")
@require_auth
def api_export_history() -> Response:
    """Get recent export history."""
    db = current_app.db_manager
    limit = min(request.args.get("limit", 20, type=int), 100)
    exports = db.exports_repo.get_recent_exports(limit=limit)

    # Annotate each record with whether the file still exists on disk
    for export in exports:
        fp = export.get("file_path", "")
        export["file_exists"] = bool(fp and os.path.exists(fp))
        # Human-friendly file size
        size = export.get("file_size", 0) or 0
        if size >= 1_048_576:
            export["file_size_display"] = f"{size / 1_048_576:.1f} MB"
        elif size >= 1024:
            export["file_size_display"] = f"{size / 1024:.1f} KB"
        else:
            export["file_size_display"] = f"{size} B"
    return jsonify(exports)


@export_bp.route("/api/export/history/<int:export_id>", methods=["DELETE"])
@require_auth
def api_export_history_delete(export_id: int) -> tuple[Response, int] | Response:
    """Delete an export history record."""
    db = current_app.db_manager
    db.exports_repo.delete_export(export_id)
    return jsonify({"success": True})


@export_bp.route("/api/export/redownload/<int:export_id>")
@require_auth
def api_export_redownload(export_id: int) -> tuple[Response, int] | Response:
    """Re-download a past export file if it still exists."""
    db = current_app.db_manager
    exports = db.exports_repo.get_recent_exports(limit=1000)
    record = next((e for e in exports if e["id"] == export_id), None)

    if not record:
        return jsonify({"error": "Export record not found"}), 404

    file_path = record.get("file_path", "")
    if not file_path or not os.path.exists(file_path):
        return jsonify({"error": "File no longer exists on disk"}), 404

    exports_dir = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "exports"))
    real_path = os.path.realpath(file_path)
    if not real_path.startswith(exports_dir + os.sep):
        return jsonify({"error": "Access denied"}), 403

    filename = os.path.basename(real_path)
    return send_file(real_path, as_attachment=True, download_name=filename, mimetype="application/octet-stream")


@export_bp.route("/api/export/fo-instruments")
@require_auth
def api_export_fo_instruments() -> Response:
    """Return instruments that have F&O contracts in the DB."""
    with current_app.db_manager.get_read_connection() as conn:
        rows = conn.execute(
            """SELECT DISTINCT c.instrument_key,
                      COALESCE(im.name, c.instrument_key) AS display_name
               FROM contracts c
               LEFT JOIN instrument_master im USING (instrument_key)
               ORDER BY c.instrument_key"""
        ).fetchall()
    return jsonify({"instruments": [{"key": r[0], "name": r[1]} for r in rows]})


@export_bp.route("/api/export/candles/segments")
@require_auth
def api_export_candle_segments() -> Response:
    """Segments that have collected candle data (for UI dropdown)."""
    status = current_app.db_manager.get_candle_collection_status()
    segments = sorted({s.get("segment", "UNKNOWN") for s in status if s.get("segment")})
    return jsonify({"segments": segments})


@export_bp.route("/api/export/candles/instruments")
@require_auth
def api_export_candle_instruments() -> Response:
    """Instruments with candle data for a given segment."""
    segment = request.args.get("segment")
    status = current_app.db_manager.get_candle_collection_status(segment)
    instruments = [
        {"key": s["instrument_key"], "name": s.get("trading_symbol") or s["instrument_key"]}
        for s in status
    ]
    return jsonify({"instruments": instruments})


@export_bp.route("/api/export/candles/start", methods=["POST"])
@require_auth
def api_export_candles_start() -> tuple[Response, int] | Response:
    """Start candle data export task."""
    db = current_app.db_manager
    data = request.json or {}
    try:
        inp = CandleExportInput(**data)
    except ValidationError as e:
        return jsonify({"error": str(e)}), 400

    if not inp.instrument_keys:
        return jsonify({"error": "instrument_keys is required"}), 400

    tracker = _get_export_tracker()
    task_id = str(uuid.uuid4())
    tracker.create(
        task_id,
        {
            "task_id": task_id,
            "status": "processing",
            "progress": 0,
            "status_message": "Preparing candle export...",
            "file_path": None,
            "error": None,
            "created_at": datetime.now().isoformat(),
        },
    )

    export_params = inp.model_dump()

    def run() -> None:
        from src.export.exporter import DataExporter

        try:
            exporter = DataExporter(db)
            tracker.update(task_id, progress=10, status_message="Fetching candle data...")
            file_path, row_count = exporter.export_candles_bulk(
                export_params["instrument_keys"],
                export_params["interval"],
                export_params["format"],
                export_params.get("from_date"),
                export_params.get("to_date"),
            )
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            try:
                db.exports_repo.save_export(
                    export_format=export_params["format"],
                    instruments=", ".join(export_params["instrument_keys"][:5]),
                    expiries=export_params["interval"],
                    file_path=file_path,
                    file_size=file_size,
                    row_count=row_count,
                )
            except Exception as hist_err:
                logger.warning(f"Failed to save candle export history: {hist_err}")
            tracker.update(
                task_id,
                status="completed",
                progress=100,
                status_message=f"Done — {row_count:,} rows exported",
                file_path=file_path,
            )
        except Exception as exc:
            import traceback

            logger.error(f"Candle export failed: {exc}")
            logger.error(traceback.format_exc())
            tracker.update(task_id, status="failed", error=str(exc), status_message=f"Export failed: {exc}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"task_id": task_id, "status": "started"})


@export_bp.route("/api/export/instrument-master", methods=["POST"])
@require_auth
def api_export_instrument_master() -> tuple[Response, int] | Response:
    """Synchronous instrument master export — returns file directly."""
    data = request.json or {}
    try:
        inp = InstrumentMasterExportInput(**data)
    except ValidationError as e:
        return jsonify({"error": str(e)}), 400

    try:
        from src.export.exporter import DataExporter

        exporter = DataExporter(current_app.db_manager)
        file_path, row_count = exporter.export_instrument_master(inp.format, inp.segment)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Instrument master export failed: {e}")
        return jsonify({"error": "Export failed"}), 500

    exports_dir = os.path.realpath(
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "exports")
    )
    real_path = os.path.realpath(file_path)
    if not real_path.startswith(exports_dir + os.sep):
        return jsonify({"error": "Access denied"}), 403

    filename = os.path.basename(real_path)
    file_size = os.path.getsize(real_path) if os.path.exists(real_path) else 0
    try:
        segment_label = inp.segment or "all"
        current_app.db_manager.exports_repo.save_export(
            export_format=inp.format,
            instruments=f"instrument_master/{segment_label}",
            expiries="",
            file_path=file_path,
            file_size=file_size,
            row_count=row_count,
        )
    except Exception as hist_err:
        logger.warning(f"Failed to save instrument master export history: {hist_err}")

    return send_file(real_path, as_attachment=True, download_name=filename, mimetype="application/octet-stream")


@export_bp.route("/api/export/stream", methods=["POST"])
@require_auth
def api_export_stream() -> tuple[Response, int] | Response:
    """Streaming CSV export for large datasets.

    Returns a streamed response instead of writing to a file first.
    Falls back to the normal export path if the dataset is small.
    """
    from src.export.exporter import STREAMING_ROW_THRESHOLD, DataExporter
    from src.routes.helpers import validate_json

    validated, err = validate_json(ExportInput)
    if err:
        return err

    export_data = validated.model_dump()
    fmt = export_data.get("format", "csv")
    if fmt != "csv":
        return jsonify({"error": "Streaming export only supports CSV format"}), 400

    db = current_app.db_manager
    exporter = DataExporter(db)

    instruments = export_data["instruments"]
    expiries = export_data["expiries"]
    options = export_data["options"]

    # Check row count to decide streaming vs normal
    row_count = exporter.get_csv_row_count(instruments, expiries, options)

    if row_count < STREAMING_ROW_THRESHOLD:
        return jsonify({"error": "Dataset too small for streaming, use /api/export/start instead", "row_count": row_count}), 400

    def generate():
        yield from exporter.export_csv_streaming(instruments, expiries, options)

    filename = exporter._build_filename(instruments, "csv", options)

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Row-Count": str(row_count),
        },
    )
