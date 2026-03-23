"""
ExpiryTrack REST API v2 Blueprint

Cursor-based pagination, field selection, unified task access.
"""

from flask import Blueprint, Response, current_app, jsonify, request

from .auth import require_api_key
from .pagination import decode_cursor, encode_cursor

api_v2 = Blueprint("api_v2", __name__, url_prefix="/api/v2")


@api_v2.route("/data")
@require_api_key
def get_data() -> tuple[Response, int] | Response:
    """Query historical data with cursor pagination.

    Query params:
        instrument_key: Filter by instrument
        expiry_date: Filter by expiry (YYYY-MM-DD)
        contract_type: CE, PE, or FUT
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        fields: Comma-separated list of fields to return
        limit: Max rows (default 100, max 1000)
        cursor: Pagination cursor from previous response
    """
    instrument_key = request.args.get("instrument_key")
    expiry_date = request.args.get("expiry_date")
    contract_type = request.args.get("contract_type")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    fields_param = request.args.get("fields")
    limit = min(request.args.get("limit", 100, type=int), 1000)
    cursor_str = request.args.get("cursor")

    # All available columns
    all_columns = [
        "instrument_key", "expiry_date", "contract_type", "strike_price",
        "trading_symbol", "openalgo_symbol", "timestamp",
        "open", "high", "low", "close", "volume", "oi",
    ]

    # Field selection
    if fields_param:
        requested = [f.strip() for f in fields_param.split(",")]
        columns = [c for c in requested if c in all_columns]
        if not columns:
            return jsonify({"error": "No valid fields specified"}), 400
    else:
        columns = all_columns

    # Build column mapping for SELECT
    col_map = {
        "instrument_key": "c.instrument_key",
        "expiry_date": "c.expiry_date",
        "contract_type": "c.contract_type",
        "strike_price": "c.strike_price",
        "trading_symbol": "c.trading_symbol",
        "openalgo_symbol": "c.openalgo_symbol",
        "timestamp": "h.timestamp",
        "open": "h.open",
        "high": "h.high",
        "low": "h.low",
        "close": "h.close",
        "volume": "h.volume",
        "oi": "h.oi",
    }

    select_cols = ", ".join(col_map[c] for c in columns)

    # Build WHERE conditions
    conditions = []
    params: list = []

    if instrument_key:
        conditions.append("c.instrument_key = ?")
        params.append(instrument_key)
    if expiry_date:
        conditions.append("c.expiry_date = ?")
        params.append(expiry_date)
    if contract_type:
        conditions.append("c.contract_type = ?")
        params.append(contract_type)
    if start_date:
        conditions.append("h.timestamp >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("h.timestamp <= ?")
        params.append(end_date + " 23:59:59")

    # Cursor-based pagination: use (timestamp, expired_instrument_key) as cursor
    if cursor_str:
        cursor_data = decode_cursor(cursor_str)
        if cursor_data and "timestamp" in cursor_data and "key" in cursor_data:
            conditions.append(
                "(h.timestamp > ? OR (h.timestamp = ? AND h.expired_instrument_key > ?))"
            )
            params.extend([cursor_data["timestamp"], cursor_data["timestamp"], cursor_data["key"]])
        elif cursor_data is None:
            return jsonify({"error": "Invalid cursor"}), 400

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Fetch limit+1 to detect has_more
    with current_app.db_manager.get_read_connection() as conn:
        query = f"""
            SELECT {select_cols}, h.timestamp AS _cursor_ts, h.expired_instrument_key AS _cursor_key
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            {where}
            ORDER BY h.timestamp, h.expired_instrument_key
            LIMIT ?
        """
        params.append(limit + 1)
        rows = conn.execute(query, params).fetchall()

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    # Build response data
    data = []
    last_row = None
    for row in rows:
        # row has columns + _cursor_ts + _cursor_key at the end
        record = {}
        for i, col in enumerate(columns):
            val = row[i]
            if col in ("expiry_date", "timestamp"):
                val = str(val) if val is not None else None
            record[col] = val
        data.append(record)
        last_row = row

    # Build next cursor
    next_cursor = None
    if has_more and last_row is not None:
        cursor_ts_idx = len(columns)
        cursor_key_idx = len(columns) + 1
        next_cursor = encode_cursor({
            "timestamp": str(last_row[cursor_ts_idx]),
            "key": str(last_row[cursor_key_idx]),
        })

    result = {
        "data": data,
        "has_more": has_more,
        "count": len(data),
    }
    if next_cursor:
        result["next_cursor"] = next_cursor

    return jsonify(result)


@api_v2.route("/instruments")
@require_api_key
def list_instruments() -> Response:
    """List instruments with contract counts and latest expiry."""
    with current_app.db_manager.get_read_connection() as conn:
        rows = conn.execute("""
            SELECT
                i.instrument_key,
                i.symbol,
                i.name,
                i.exchange,
                COUNT(DISTINCT c.expired_instrument_key) AS contract_count,
                MAX(c.expiry_date) AS latest_expiry
            FROM instruments i
            LEFT JOIN contracts c ON i.instrument_key = c.instrument_key
            GROUP BY i.instrument_key, i.symbol, i.name, i.exchange
            ORDER BY i.symbol
        """).fetchall()

        instruments = [
            {
                "instrument_key": r[0],
                "symbol": r[1],
                "name": r[2],
                "exchange": r[3],
                "contract_count": r[4],
                "latest_expiry": str(r[5]) if r[5] else None,
            }
            for r in rows
        ]

        return jsonify({"data": instruments, "count": len(instruments)})


@api_v2.route("/expiries/<path:instrument_key>")
@require_api_key
def list_expiries(instrument_key: str) -> Response:
    """List expiry dates for an instrument with contract counts."""
    with current_app.db_manager.get_read_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                c.expiry_date,
                COUNT(*) AS contract_count,
                COUNT(CASE WHEN c.contract_type = 'FUT' THEN 1 END) AS futures,
                COUNT(CASE WHEN c.contract_type IN ('CE', 'PE') THEN 1 END) AS options
            FROM contracts c
            WHERE c.instrument_key = ?
            GROUP BY c.expiry_date
            ORDER BY c.expiry_date DESC
            """,
            [instrument_key],
        ).fetchall()

        expiries = [
            {
                "expiry_date": str(r[0]),
                "contract_count": r[1],
                "futures": r[2],
                "options": r[3],
            }
            for r in rows
        ]

        return jsonify({
            "instrument_key": instrument_key,
            "data": expiries,
            "count": len(expiries),
        })


@api_v2.route("/tasks")
@require_api_key
def list_tasks() -> Response:
    """List tasks with optional filters."""
    task_type = request.args.get("type")
    status = request.args.get("status")
    limit = min(request.args.get("limit", 50, type=int), 200)

    tasks = current_app.db_manager.tasks_repo.list_tasks(
        task_type=task_type, status=status, limit=limit
    )

    return jsonify({
        "data": tasks,
        "count": len(tasks),
    })
