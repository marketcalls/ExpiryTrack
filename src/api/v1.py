"""
ExpiryTrack REST API v1 Blueprint (#10)

External access to collected data via API keys.
"""
import duckdb
from flask import Blueprint, request, jsonify, Response
import csv
import io

from ..config import config
from ..database.manager import DatabaseManager
from .auth import require_api_key

api_v1 = Blueprint('api_v1', __name__, url_prefix='/api/v1')


@api_v1.route('/data')
@require_api_key
def get_data():
    """Query historical data.

    Query params:
        instrument_key: Filter by instrument (e.g. NSE_INDEX|Nifty 50)
        expiry_date: Filter by expiry (YYYY-MM-DD)
        contract_type: CE, PE, or FUT
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        format: json (default) or csv
        limit: Max rows (default 1000, max 50000)
        offset: Pagination offset (default 0)
    """
    instrument_key = request.args.get('instrument_key')
    expiry_date = request.args.get('expiry_date')
    contract_type = request.args.get('contract_type')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    fmt = request.args.get('format', 'json')
    limit = min(request.args.get('limit', 1000, type=int), 50000)
    offset = request.args.get('offset', 0, type=int)

    # Build query
    conditions = []
    params = []

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
        params.append(end_date + ' 23:59:59')

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    conn = duckdb.connect(str(config.DB_PATH))
    try:
        query = f"""
            SELECT
                c.instrument_key,
                c.expiry_date,
                c.contract_type,
                c.strike_price,
                c.trading_symbol,
                c.openalgo_symbol,
                h.timestamp,
                h.open,
                h.high,
                h.low,
                h.close,
                h.volume,
                h.oi
            FROM historical_data h
            JOIN contracts c ON h.expired_instrument_key = c.expired_instrument_key
            {where}
            ORDER BY h.timestamp
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        rows = conn.execute(query, params).fetchall()
        columns = ['instrument_key', 'expiry_date', 'contract_type', 'strike_price',
                    'trading_symbol', 'openalgo_symbol', 'timestamp',
                    'open', 'high', 'low', 'close', 'volume', 'oi']

        if fmt == 'csv':
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([str(v) for v in row])
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={'Content-Disposition': 'attachment; filename=data.csv'}
            )

        # JSON format
        data = []
        for row in rows:
            data.append({col: (str(val) if col in ('expiry_date', 'timestamp') else val)
                         for col, val in zip(columns, row)})

        return jsonify({
            'count': len(data),
            'limit': limit,
            'offset': offset,
            'data': data,
        })
    finally:
        conn.close()


@api_v1.route('/instruments')
@require_api_key
def list_instruments():
    """List all instruments with data."""
    conn = duckdb.connect(str(config.DB_PATH))
    try:
        rows = conn.execute("""
            SELECT DISTINCT i.instrument_key, i.symbol, i.name, i.exchange
            FROM instruments i
            ORDER BY i.symbol
        """).fetchall()

        instruments = [{
            'instrument_key': r[0],
            'symbol': r[1],
            'name': r[2],
            'exchange': r[3],
        } for r in rows]

        return jsonify({'instruments': instruments})
    finally:
        conn.close()


@api_v1.route('/expiries/<path:instrument_key>')
@require_api_key
def list_expiries(instrument_key):
    """List all expiry dates for an instrument."""
    conn = duckdb.connect(str(config.DB_PATH))
    try:
        rows = conn.execute("""
            SELECT DISTINCT expiry_date
            FROM contracts
            WHERE instrument_key = ?
            ORDER BY expiry_date DESC
        """, [instrument_key]).fetchall()

        return jsonify({
            'instrument_key': instrument_key,
            'expiries': [str(r[0]) for r in rows],
        })
    finally:
        conn.close()
