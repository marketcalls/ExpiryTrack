"""
ExpiryTrack Web Interface - Flask Application
"""
import asyncio
import threading
from flask import Flask, render_template, redirect, url_for, request, session, jsonify, flash
from datetime import datetime, timedelta
import json

from src.auth.manager import AuthManager
from src.collectors.expiry_tracker import ExpiryTracker
from src.database.manager import DatabaseManager
from src.config import config
from src.utils.instrument_mapper import (
    get_instrument_key, get_display_name,
    get_all_display_names, INSTRUMENT_MAPPING
)
import os
import logging
import secrets

app = Flask(__name__)

# Persist secret key across restarts
_secret_key_path = config.DATA_DIR / '.flask_secret_key'
if _secret_key_path.exists():
    app.secret_key = _secret_key_path.read_text().strip()
else:
    app.secret_key = secrets.token_hex(32)
    _secret_key_path.parent.mkdir(parents=True, exist_ok=True)
    _secret_key_path.write_text(app.secret_key)
    try:
        import stat
        os.chmod(_secret_key_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass

# Initialize managers
auth_manager = AuthManager()
db_manager = DatabaseManager()

# Context processor to make is_authenticated available in all templates
@app.context_processor
def inject_auth_status():
    return {'is_authenticated': auth_manager.is_token_valid()}

@app.route('/')
def index():
    """Home page"""
    # Check if authenticated
    is_authenticated = auth_manager.is_token_valid()

    # Get database stats
    stats = None
    if is_authenticated:
        try:
            stats = db_manager.get_summary_stats()
        except Exception:
            stats = None

    # Get messages from session
    message = session.pop('message', None)
    error = session.pop('error', None)

    return render_template('index.html',
                         is_authenticated=is_authenticated,
                         stats=stats,
                         message=message,
                         error=error)

@app.route('/settings')
def settings():
    """Settings page for API credentials"""
    # Get credentials from database
    has_credentials = auth_manager.has_credentials()
    credential = None
    if has_credentials:
        creds = db_manager.get_credentials()
        if creds:
            credential = {
                'api_key': creds['api_key'],
                'api_secret': '***' + creds['api_secret'][-4:] if creds['api_secret'] else '',
                'redirect_url': creds['redirect_uri']
            }
    return render_template('settings.html', credential=credential, has_credentials=has_credentials)

@app.route('/save_credentials', methods=['POST'])
def save_credentials():
    """Save API credentials to database (encrypted)"""
    api_key = request.form.get('api_key', '').strip()
    api_secret = request.form.get('api_secret', '').strip()
    redirect_url = request.form.get('redirect_url')

    if not api_key or not api_secret:
        session['error'] = 'API Key and API Secret are required'
        return redirect(url_for('settings'))

    # Save to database using AuthManager (encrypted)
    if auth_manager.save_credentials(api_key, api_secret, redirect_url):
        session['message'] = 'Credentials saved successfully!'
    else:
        session['error'] = 'Failed to save credentials'

    return redirect(url_for('settings'))

@app.route('/login')
def login():
    """Start OAuth login flow"""
    if not auth_manager.has_credentials():
        session['error'] = 'Please configure API credentials first'
        return redirect(url_for('settings'))

    try:
        auth_url = auth_manager.get_authorization_url()
        # Store OAuth state in session for CSRF validation
        session['oauth_state'] = auth_manager._oauth_state
        return redirect(auth_url)
    except ValueError as e:
        session['error'] = str(e)
        return redirect(url_for('settings'))

@app.route('/upstox/callback')
def upstox_callback():
    """Handle OAuth callback"""
    auth_code = request.args.get('code')
    error = request.args.get('error')
    state = request.args.get('state')

    if error:
        from markupsafe import escape
        return f"Authentication failed: {escape(error)}", 400

    # Validate OAuth state parameter to prevent CSRF (timing-safe comparison)
    expected_state = session.pop('oauth_state', None)
    if not expected_state or not state or not secrets.compare_digest(expected_state, state):
        auth_manager.clear_oauth_state()
        return "Invalid OAuth state parameter.", 403

    auth_manager.clear_oauth_state()

    if auth_code:
        # Exchange code for token
        loop = asyncio.new_event_loop()
        try:
            success = loop.run_until_complete(
                auth_manager.exchange_code_for_token(auth_code)
            )
        finally:
            loop.close()

        if success:
            # Redirect to home page after successful login
            flash('Successfully authenticated with Upstox! You can now start collecting data.', 'success')
            return redirect(url_for('index'))
        else:
            flash('Failed to authenticate with Upstox. Please try again.', 'error')
            return redirect(url_for('settings'))

    return "No authorization code received", 400


@app.route('/api/expiries/<instrument>')
def api_expiries(instrument):
    """API endpoint to get expiries"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    # Convert display name to instrument key if needed
    instrument_key = get_instrument_key(instrument)

    async def get_expiries():
        tracker = ExpiryTracker(auth_manager=auth_manager)
        async with tracker:
            return await tracker.get_expiries(instrument_key)

    loop = asyncio.new_event_loop()
    try:
        expiries = loop.run_until_complete(get_expiries())
    finally:
        loop.close()

    return jsonify({
        'instrument': instrument,
        'instrument_key': instrument_key,
        'expiries': expiries
    })

@app.route('/api/instruments/expiries', methods=['POST'])
def api_instruments_expiries():
    """API endpoint to get expiries for multiple instruments"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.json
    if not data or 'instruments' not in data:
        return jsonify({'error': 'Missing instruments list'}), 400

    instruments = data['instruments']
    if not isinstance(instruments, list) or not instruments:
        return jsonify({'error': 'Invalid instruments list'}), 400

    async def get_all_expiries():
        tracker = ExpiryTracker(auth_manager=auth_manager)
        async with tracker:
            expiries_data = {}
            for instrument in instruments:
                try:
                    instrument_key = get_instrument_key(instrument)
                    expiries = await tracker.get_expiries(instrument_key)
                    expiries_data[instrument] = expiries
                except Exception as e:
                    expiries_data[instrument] = []
            return expiries_data

    loop = asyncio.new_event_loop()
    try:
        expiries_data = loop.run_until_complete(get_all_expiries())
    finally:
        loop.close()

    return jsonify({
        'expiries': expiries_data
    })

@app.route('/collect')
def collect_page():
    """Collection wizard page"""
    if not auth_manager.has_credentials():
        session['error'] = 'Please configure API credentials first'
        return redirect(url_for('settings'))

    if not auth_manager.is_token_valid():
        session['error'] = 'Please authenticate first'
        return redirect(url_for('login'))

    # Use the new wizard template
    return render_template('collect_wizard.html')

@app.route('/api/collect/start', methods=['POST'])
def api_collect_start():
    """Start a new collection task"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.json
    if not data or not isinstance(data.get('instruments'), list) or not data['instruments']:
        return jsonify({'error': 'instruments list is required and must be non-empty'}), 400

    # Import task manager
    from src.collectors.task_manager import task_manager

    # Create and start task
    task_id = task_manager.create_task(data)

    return jsonify({
        'success': True,
        'task_id': task_id,
        'status': 'started',
        'message': 'Collection task started'
    })

@app.route('/api/collect/status/<task_id>')
def api_collect_status(task_id):
    """Get status of a collection task"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.collectors.task_manager import task_manager

    status = task_manager.get_task_status(task_id)
    if status:
        return jsonify(status)
    else:
        return jsonify({'error': 'Task not found'}), 404

@app.route('/api/collect/tasks')
def api_collect_tasks():
    """Get all collection tasks"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.collectors.task_manager import task_manager

    tasks = task_manager.get_all_tasks()
    return jsonify({'tasks': tasks})

@app.route('/status')
def status_page():
    """Status page showing database statistics and recent tasks"""
    if not auth_manager.is_token_valid():
        session['error'] = 'Please authenticate first'
        return redirect(url_for('login'))

    # Get database stats
    stats = db_manager.get_summary_stats()

    # Get recent tasks
    from src.collectors.task_manager import task_manager
    tasks = task_manager.get_all_tasks()

    # Sort tasks by created_at (most recent first)
    tasks.sort(key=lambda x: x.get('created_at', ''), reverse=True)

    return render_template('status.html', stats=stats, tasks=tasks[:10])  # Show last 10 tasks

@app.route('/help')
def help_page():
    """Help page with CLI commands and OpenAlgo documentation"""
    return render_template('help.html')

# Export Routes
@app.route('/export')
def export_wizard():
    """Export wizard page"""
    if not auth_manager.has_credentials():
        session['error'] = 'Please configure API credentials first'
        return redirect(url_for('settings'))

    if not auth_manager.is_token_valid():
        session['error'] = 'Please authenticate first'
        return redirect(url_for('login'))

    return render_template('export_wizard.html')

@app.route('/api/export/available-expiries', methods=['POST'])
def api_export_available_expiries():
    """Get available expiries for selected instruments"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.export.exporter import DataExporter

    data = request.json
    instruments = data.get('instruments', [])

    exporter = DataExporter(db_manager)
    expiries = exporter.get_available_expiries(instruments)

    return jsonify(expiries)

# Global dictionary to store export tasks with thread lock
export_tasks = {}
export_tasks_lock = threading.Lock()

def _cleanup_export_tasks():
    """Remove completed/failed export tasks older than 1 hour"""
    cutoff = datetime.now() - timedelta(hours=1)
    with export_tasks_lock:
        to_remove = [
            tid for tid, t in export_tasks.items()
            if t.get('status') in ('completed', 'failed')
            and t.get('created_at')
            and datetime.fromisoformat(t['created_at']) < cutoff
        ]
        for tid in to_remove:
            export_tasks.pop(tid, None)

@app.route('/api/export/start', methods=['POST'])
def api_export_start():
    """Start export task"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    from src.export.exporter import DataExporter
    import uuid

    data = request.json
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    format_type = data.get('format', 'csv')
    if format_type not in ('csv', 'json', 'zip', 'parquet'):
        return jsonify({'error': f'Invalid format: {format_type}. Must be csv, json, zip, or parquet'}), 400

    # Clean up old tasks
    _cleanup_export_tasks()

    task_id = str(uuid.uuid4())

    # Initialize task status
    with export_tasks_lock:
        export_tasks[task_id] = {
            'task_id': task_id,
            'status': 'processing',
            'progress': 0,
            'status_message': 'Preparing export...',
            'file_path': None,
            'error': None,
            'created_at': datetime.now().isoformat()
        }

    # Run export in background thread
    def run_export():
        try:
            export_logger = logging.getLogger(__name__)

            export_logger.info(f"Starting export task {task_id}")
            export_logger.debug(f"Export data: instruments={data.get('instruments')}, expiries={data.get('expiries')}, options={data.get('options')}")

            exporter = DataExporter(db_manager)

            # Update progress
            with export_tasks_lock:
                export_tasks[task_id]['progress'] = 20
                export_tasks[task_id]['status_message'] = 'Gathering data...'

            format_type = data.get('format', 'csv')
            instruments = data.get('instruments', [])
            expiries = data.get('expiries', {})
            options = data.get('options', {})

            export_logger.info(f"Export parameters: format={format_type}, instruments={instruments}, expiries={expiries}")

            # Update progress
            with export_tasks_lock:
                export_tasks[task_id]['progress'] = 50
                export_tasks[task_id]['status_message'] = f'Exporting to {format_type.upper()}...'

            # Export based on format
            export_logger.info(f"Starting {format_type} export...")
            if format_type == 'csv':
                file_path = exporter.export_to_csv(instruments, expiries, options, task_id)
            elif format_type == 'json':
                file_path = exporter.export_to_json(instruments, expiries, options, task_id)
            elif format_type == 'zip':
                file_path = exporter.export_to_zip(instruments, expiries, options, task_id)
            elif format_type == 'parquet':
                file_path = exporter.export_to_parquet(instruments, expiries, options, task_id)
            else:
                raise ValueError(f"Unknown format: {format_type}")

            export_logger.info(f"Export completed: {file_path}")

            # Update task status
            with export_tasks_lock:
                export_tasks[task_id]['status'] = 'completed'
                export_tasks[task_id]['progress'] = 100
                export_tasks[task_id]['status_message'] = 'Export completed successfully!'
                export_tasks[task_id]['file_path'] = file_path

        except Exception as e:
            import traceback
            export_logger.error(f"Export failed: {str(e)}")
            export_logger.error(traceback.format_exc())
            with export_tasks_lock:
                export_tasks[task_id]['status'] = 'failed'
                export_tasks[task_id]['error'] = str(e)
                export_tasks[task_id]['status_message'] = f'Export failed: {str(e)}'

    thread = threading.Thread(target=run_export)
    thread.start()

    return jsonify({'task_id': task_id})

@app.route('/api/export/status/<task_id>')
def api_export_status(task_id):
    """Get export task status"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    with export_tasks_lock:
        task = export_tasks.get(task_id)

    if not task:
        return jsonify({'error': 'Task not found'}), 404

    return jsonify(task)

@app.route('/api/export/download/<task_id>')
def api_export_download(task_id):
    """Download exported file"""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from flask import send_file

    with export_tasks_lock:
        task = export_tasks.get(task_id)

    if not task:
        return jsonify({'error': 'Task not found'}), 404

    if task['status'] != 'completed':
        return jsonify({'error': 'Export not completed'}), 400

    file_path = task['file_path']
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404

    # Prevent path traversal — ensure file is within exports directory
    exports_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), 'exports'))
    real_path = os.path.realpath(file_path)
    if not real_path.startswith(exports_dir + os.sep):
        return jsonify({'error': 'Access denied'}), 403

    filename = os.path.basename(real_path)

    return send_file(
        real_path,
        as_attachment=True,
        download_name=filename,
        mimetype='application/octet-stream'
    )

@app.route('/logout')
def logout():
    """Logout and clear tokens"""
    auth_manager.clear_tokens()
    session.clear()
    return redirect(url_for('index'))


# ── Token Status (#7) ─────────────────────────────────────
@app.route('/api/auth/token-status')
def api_auth_token_status():
    """Return token validity and remaining time."""
    import time as _time
    valid = auth_manager.is_token_valid()
    expiry = auth_manager.token_expiry
    remaining = max(0, expiry - _time.time()) if expiry and valid else 0
    return jsonify({
        'valid': valid,
        'expiry': expiry,
        'remaining_seconds': int(remaining),
    })


# ── Instrument CRUD (#9) ──────────────────────────────────
@app.route('/api/instruments', methods=['GET'])
def api_instruments_list():
    """List all instruments."""
    instruments = db_manager.get_active_instruments()
    return jsonify({'instruments': instruments})


@app.route('/api/instruments', methods=['POST'])
def api_instruments_add():
    """Add a new instrument."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    data = request.json
    if not data or not data.get('instrument_key') or not data.get('symbol'):
        return jsonify({'error': 'instrument_key and symbol are required'}), 400
    new_id = db_manager.add_instrument(
        data['instrument_key'], data['symbol'], data.get('priority', 0)
    )
    if new_id:
        from src.utils.instrument_mapper import refresh_cache
        refresh_cache()
        return jsonify({'success': True, 'id': new_id})
    return jsonify({'error': 'Instrument already exists'}), 409


@app.route('/api/instruments/<int:instrument_id>', methods=['PATCH'])
def api_instruments_toggle(instrument_id):
    """Toggle instrument active status."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    data = request.json or {}
    is_active = data.get('is_active', True)
    db_manager.toggle_instrument(instrument_id, is_active)
    from src.utils.instrument_mapper import refresh_cache
    refresh_cache()
    return jsonify({'success': True})


@app.route('/api/instruments/<int:instrument_id>', methods=['DELETE'])
def api_instruments_delete(instrument_id):
    """Remove an instrument."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    db_manager.remove_instrument(instrument_id)
    from src.utils.instrument_mapper import refresh_cache
    refresh_cache()
    return jsonify({'success': True})


# ── Retry Failed Contracts (#15) ──────────────────────────
@app.route('/api/download-status/retry-failed', methods=['POST'])
def api_download_status_retry_failed():
    """Reset failed contracts and optionally start a re-download task."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.json or {}
    instrument_key = data.get('instrument_key')

    count = db_manager.reset_fetch_attempts(instrument_key or None)
    return jsonify({'success': True, 'reset_count': count})


# ── API Key Management (#10) ──────────────────────────────
@app.route('/api/api-keys', methods=['GET'])
def api_keys_list():
    """List all API keys."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    keys = db_manager.list_api_keys()
    return jsonify({'keys': keys})


@app.route('/api/api-keys', methods=['POST'])
def api_keys_create():
    """Generate a new API key."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    data = request.json or {}
    name = data.get('name', 'Unnamed Key')
    key = db_manager.create_api_key(name)
    if key:
        return jsonify({'success': True, 'key': key})
    return jsonify({'error': 'Failed to create key'}), 500


@app.route('/api/api-keys/<int:key_id>', methods=['DELETE'])
def api_keys_revoke(key_id):
    """Revoke an API key."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    db_manager.revoke_api_key(key_id)
    return jsonify({'success': True})

# ── Backup & Restore (#11) ─────────────────────────────────
@app.route('/api/backup/create', methods=['POST'])
def api_backup_create():
    """Create a database backup."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.backup.manager import BackupManager
    mgr = BackupManager()
    result = mgr.create_backup()
    return jsonify({'success': True, **result})


@app.route('/api/backup/list')
def api_backup_list():
    """List all backups."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.backup.manager import BackupManager
    mgr = BackupManager()
    return jsonify({'backups': mgr.list_backups()})


@app.route('/api/backup/download/<filename>')
def api_backup_download(filename):
    """Download a backup file."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from flask import send_file
    from src.backup.manager import BackupManager
    mgr = BackupManager()
    path = mgr.get_backup_path(filename)
    if not path:
        return jsonify({'error': 'Backup not found'}), 404
    return send_file(str(path), as_attachment=True, download_name=filename)


@app.route('/api/backup/restore', methods=['POST'])
def api_backup_restore():
    """Restore from an uploaded backup ZIP."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.backup.manager import BackupManager
    import tempfile

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    uploaded = request.files['file']
    if not uploaded.filename or not uploaded.filename.endswith('.zip'):
        return jsonify({'error': 'File must be a .zip archive'}), 400

    mgr = BackupManager()
    # Save uploaded file to temp location
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        uploaded.save(tmp)
        tmp_path = tmp.name

    try:
        mgr.restore_backup(tmp_path)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        os.unlink(tmp_path)


@app.route('/api/backup/delete/<filename>', methods=['DELETE'])
def api_backup_delete(filename):
    """Delete a backup."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.backup.manager import BackupManager
    mgr = BackupManager()
    if mgr.delete_backup(filename):
        return jsonify({'success': True})
    return jsonify({'error': 'Backup not found'}), 404


# ── REST API v1 Blueprint (#10) ────────────────────────────
from src.api.v1 import api_v1
app.register_blueprint(api_v1)


# Setup default instruments if not already done
db_manager.setup_default_instruments()

# ── Scheduler ──────────────────────────────────────────────
from src.scheduler.scheduler import scheduler_manager

# Start scheduler if enabled in config
scheduler_manager.start()


@app.route('/api/scheduler/status')
def api_scheduler_status():
    """Get scheduler status and job list."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    return jsonify(scheduler_manager.get_status())


@app.route('/api/scheduler/toggle', methods=['POST'])
def api_scheduler_toggle():
    """Start or stop the scheduler."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    action = request.json.get('action') if request.json else None
    if action == 'start':
        config.SCHEDULER_ENABLED = True
        scheduler_manager.start()
        return jsonify({'running': True, 'message': 'Scheduler started'})
    elif action == 'stop':
        scheduler_manager.stop()
        config.SCHEDULER_ENABLED = False
        return jsonify({'running': False, 'message': 'Scheduler stopped'})
    return jsonify({'error': 'action must be start or stop'}), 400


@app.route('/api/scheduler/jobs/<job_id>/pause', methods=['POST'])
def api_scheduler_pause_job(job_id):
    """Pause a scheduled job."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    if scheduler_manager.pause_job(job_id):
        return jsonify({'paused': True})
    return jsonify({'error': 'Job not found'}), 404


@app.route('/api/scheduler/jobs/<job_id>/resume', methods=['POST'])
def api_scheduler_resume_job(job_id):
    """Resume a paused job."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    if scheduler_manager.resume_job(job_id):
        return jsonify({'paused': False})
    return jsonify({'error': 'Job not found'}), 404


@app.route('/api/scheduler/jobs/<job_id>', methods=['DELETE'])
def api_scheduler_remove_job(job_id):
    """Remove a scheduled job."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    if scheduler_manager.remove_job(job_id):
        return jsonify({'removed': True})
    return jsonify({'error': 'Job not found'}), 404


# ── Analytics ──────────────────────────────────────────────
@app.route('/analytics')
def analytics_page():
    """Analytics dashboard page."""
    if not auth_manager.is_token_valid():
        session['error'] = 'Please authenticate first'
        return redirect(url_for('login'))
    return render_template('analytics.html')


@app.route('/api/analytics/summary')
def api_analytics_summary():
    """Dashboard summary stats."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    engine = AnalyticsEngine()
    return jsonify(engine.get_dashboard_summary())


@app.route('/api/analytics/candles-per-day')
def api_analytics_candles_per_day():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    instrument = request.args.get('instrument')
    limit = min(request.args.get('limit', 60, type=int), config.ANALYTICS_MAX_CHART_POINTS)
    engine = AnalyticsEngine()
    return jsonify(engine.get_candles_per_day(instrument, limit))


@app.route('/api/analytics/contracts-by-type')
def api_analytics_contracts_by_type():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    engine = AnalyticsEngine()
    return jsonify(engine.get_contracts_by_type())


@app.route('/api/analytics/contracts-by-instrument')
def api_analytics_contracts_by_instrument():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    engine = AnalyticsEngine()
    return jsonify(engine.get_contracts_by_instrument())


@app.route('/api/analytics/data-coverage')
def api_analytics_data_coverage():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    instrument = request.args.get('instrument')
    engine = AnalyticsEngine()
    return jsonify(engine.get_data_coverage_by_expiry(instrument))


@app.route('/api/analytics/volume-by-expiry')
def api_analytics_volume_by_expiry():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    instrument = request.args.get('instrument')
    engine = AnalyticsEngine()
    return jsonify(engine.get_volume_by_expiry(instrument))


@app.route('/api/analytics/storage')
def api_analytics_storage():
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    engine = AnalyticsEngine()
    return jsonify(engine.get_storage_breakdown())


# ── Download Status ────────────────────────────────────────
@app.route('/download-status')
def download_status_page():
    """Download status page — per-expiry data coverage and resume."""
    if not auth_manager.is_token_valid():
        session['error'] = 'Please authenticate first'
        return redirect(url_for('login'))
    return render_template('download_status.html')


@app.route('/api/download-status')
def api_download_status():
    """List all expiries with download status."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.analytics.engine import AnalyticsEngine
    instrument = request.args.get('instrument')
    engine = AnalyticsEngine()
    return jsonify(engine.get_download_status(instrument))


@app.route('/api/download-status/<path:instrument_key>/<expiry_date>/missing')
def api_download_status_missing(instrument_key, expiry_date):
    """Get missing (unfetched) contracts for a specific expiry."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', expiry_date):
        return jsonify({'error': 'Invalid expiry date format'}), 400

    from src.analytics.engine import AnalyticsEngine
    engine = AnalyticsEngine()
    return jsonify(engine.get_missing_contracts(instrument_key, expiry_date))


@app.route('/api/download-status/resume', methods=['POST'])
def api_download_status_resume():
    """Resume downloading for specific instrument+expiry combos."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.json
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    instrument = data.get('instrument')
    expiries_list = data.get('expiries', [])

    if not instrument or not isinstance(instrument, str):
        return jsonify({'error': 'instrument is required and must be a string'}), 400
    if not expiries_list or not isinstance(expiries_list, list):
        return jsonify({'error': 'expiries is required and must be a list'}), 400
    if len(expiries_list) > 50:
        return jsonify({'error': 'Maximum 50 expiries per request'}), 400

    import re
    for exp in expiries_list:
        if not isinstance(exp, str) or not re.match(r'^\d{4}-\d{2}-\d{2}$', exp):
            return jsonify({'error': f'Invalid expiry date format: {exp}'}), 400

    from src.collectors.task_manager import task_manager

    task_params = {
        'instruments': [instrument],
        'contract_type': 'both',
        'expiries': {instrument: expiries_list},
        'interval': '1minute',
        'workers': 5,
    }
    task_id = task_manager.create_task(task_params)

    return jsonify({'success': True, 'task_id': task_id})


@app.route('/api/download-status/force-refetch', methods=['POST'])
def api_download_status_force_refetch():
    """Reset data_fetched flag for an instrument+expiry so contracts get re-downloaded."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401

    data = request.json
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    instrument_key = data.get('instrument_key')
    expiry_date = data.get('expiry_date')

    if not instrument_key or not isinstance(instrument_key, str):
        return jsonify({'error': 'instrument_key is required'}), 400
    if not expiry_date or not isinstance(expiry_date, str):
        return jsonify({'error': 'expiry_date is required'}), 400

    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', expiry_date):
        return jsonify({'error': 'Invalid expiry_date format'}), 400

    count = db_manager.reset_contracts_for_refetch(instrument_key, expiry_date)
    return jsonify({'success': True, 'reset_count': count})


# ── Quality Checks ─────────────────────────────────────────
@app.route('/api/quality/run', methods=['POST'])
def api_quality_run():
    """Run data quality checks."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    from src.quality.checker import DataQualityChecker

    instrument_key = None
    if request.json:
        instrument_key = request.json.get('instrument_key')

    checker = DataQualityChecker()
    report = checker.run_all_checks(instrument_key)
    return jsonify(report.to_dict())


@app.route('/api/scheduler/history')
def api_scheduler_history():
    """Get scheduler job execution history."""
    if not auth_manager.is_token_valid():
        return jsonify({'error': 'Not authenticated'}), 401
    limit = request.args.get('limit', 20, type=int)
    return jsonify({'history': scheduler_manager.get_history(limit)})


if __name__ == '__main__':
    import sys
    # Disable auto-reload for exports directory
    extra_files = None
    if '--reload' not in sys.argv:
        # Run without auto-reload in production mode
        app.run(debug=False, host=config.HOST, port=config.PORT)
    else:
        # Development mode with auto-reload (exclude exports directory)
        app.run(debug=True, use_reloader=False, host=config.HOST, port=config.PORT)  # Disable reloader to prevent clearing export_tasks