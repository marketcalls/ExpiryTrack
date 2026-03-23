"""Cursor-based pagination utilities for API v2."""

from __future__ import annotations

import base64
import json
from typing import Any


def encode_cursor(data: dict[str, Any]) -> str:
    """Encode a cursor dict to a base64 string."""
    return base64.urlsafe_b64encode(json.dumps(data, default=str).encode()).decode()


def decode_cursor(cursor: str) -> dict[str, Any] | None:
    """Decode a base64 cursor string back to a dict. Returns None on invalid input."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        return json.loads(raw)
    except Exception:
        return None
