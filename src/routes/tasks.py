"""Tasks blueprint — unified task list, status, cancel."""

from flask import Blueprint, Response, current_app, jsonify, request

from src.routes.helpers import require_auth

tasks_bp = Blueprint("tasks", __name__)


@tasks_bp.route("/api/tasks")
@require_auth
def api_tasks_list() -> Response:
    """List all tasks (filterable by type/status)."""
    task_type = request.args.get("type")
    status = request.args.get("status")
    limit = min(request.args.get("limit", 50, type=int), 200)

    tasks = current_app.db_manager.tasks_repo.list_tasks(
        task_type=task_type, status=status, limit=limit
    )
    return jsonify({"tasks": tasks, "count": len(tasks)})


@tasks_bp.route("/api/tasks/<task_id>")
@require_auth
def api_tasks_get(task_id: str) -> tuple[Response, int] | Response:
    """Get single task details."""
    task = current_app.db_manager.tasks_repo.get_task(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


@tasks_bp.route("/api/tasks/<task_id>/cancel", methods=["POST"])
@require_auth
def api_tasks_cancel(task_id: str) -> tuple[Response, int] | Response:
    """Cancel a running task."""
    # Try collection task manager first
    from src.collectors.task_manager import task_manager

    if task_manager.cancel_task(task_id):
        return jsonify({"success": True, "message": "Task cancelled"})

    # Update DB status
    task = current_app.db_manager.tasks_repo.get_task(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    if task.get("status") not in ("pending", "processing"):
        return jsonify({"error": "Task is not cancellable"}), 400

    current_app.db_manager.tasks_repo.update_task(
        task_id, status="failed", error_message="Cancelled by user"
    )
    return jsonify({"success": True, "message": "Task cancelled"})
