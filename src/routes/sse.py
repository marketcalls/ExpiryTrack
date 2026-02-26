"""SSE blueprint — Server-Sent Events endpoint."""

from flask import Blueprint, Response

from src.sse.stream import sse_broker

sse_bp = Blueprint("sse", __name__)


@sse_bp.route("/api/events")
def api_events() -> Response:
    """SSE stream endpoint. Clients connect here for real-time updates."""
    sid = sse_broker.subscribe()

    return Response(
        sse_broker.stream(sid),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
