"""
ExpiryTrack Web Interface - Flask Application
"""
import asyncio
from flask import Flask, render_template, redirect, url_for, request, session, jsonify, flash
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json

from src.auth.manager import AuthManager
from src.collectors.expiry_tracker import ExpiryTracker
from src.database.manager import DatabaseManager
from src.config import config
from src.utils.instrument_mapper import (
    get_instrument_key, get_display_name,
    get_all_display_names, INSTRUMENT_MAPPING
)
import secrets

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)  # Generate random secret key
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///upstox_app.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

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
        except:
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
    api_key = request.form.get('api_key')
    api_secret = request.form.get('api_secret')
    redirect_url = request.form.get('redirect_url')

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
        return redirect(auth_url)
    except ValueError as e:
        session['error'] = str(e)
        return redirect(url_for('settings'))

@app.route('/upstox/callback')
def upstox_callback():
    """Handle OAuth callback"""
    auth_code = request.args.get('code')
    error = request.args.get('error')

    if error:
        return f"Authentication failed: {error}", 400

    if auth_code:
        # Exchange code for token
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        success = loop.run_until_complete(
            auth_manager.exchange_code_for_token(auth_code)
        )
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
    asyncio.set_event_loop(loop)
    expiries = loop.run_until_complete(get_expiries())
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
    asyncio.set_event_loop(loop)
    expiries_data = loop.run_until_complete(get_all_expiries())
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
    from src.collectors.task_manager import task_manager

    status = task_manager.get_task_status(task_id)
    if status:
        # Debug log to see what's being returned
        logs_count = len(status.get('logs', []))
        print(f"DEBUG: Task {task_id} - Status: {status.get('status')}, Logs count: {logs_count}")
        if logs_count > 0:
            print(f"DEBUG: Sample log: {status['logs'][-1]}")
        return jsonify(status)
    else:
        return jsonify({'error': 'Task not found'}), 404

@app.route('/api/collect/tasks')
def api_collect_tasks():
    """Get all collection tasks"""
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
    from src.export.exporter import DataExporter

    data = request.json
    instruments = data.get('instruments', [])

    exporter = DataExporter(db_manager)
    expiries = exporter.get_available_expiries(instruments)

    return jsonify(expiries)

# Global dictionary to store export tasks
export_tasks = {}

@app.route('/api/export/start', methods=['POST'])
def api_export_start():
    """Start export task"""
    from src.export.exporter import DataExporter
    import uuid
    import threading

    data = request.json
    task_id = str(uuid.uuid4())

    # Initialize task status
    export_tasks[task_id] = {
        'task_id': task_id,
        'status': 'processing',
        'progress': 0,
        'status_message': 'Preparing export...',
        'file_path': None,
        'error': None
    }

    # Run export in background thread
    def run_export():
        try:
            import logging
            logging.basicConfig(level=logging.DEBUG)
            logger = logging.getLogger(__name__)

            logger.info(f"Starting export task {task_id}")
            logger.debug(f"Export data: instruments={data.get('instruments')}, expiries={data.get('expiries')}, options={data.get('options')}")

            exporter = DataExporter(db_manager)

            # Update progress
            export_tasks[task_id]['progress'] = 20
            export_tasks[task_id]['status_message'] = 'Gathering data...'

            format_type = data.get('format', 'csv')
            instruments = data.get('instruments', [])
            expiries = data.get('expiries', {})
            options = data.get('options', {})

            logger.info(f"Export parameters: format={format_type}, instruments={instruments}, expiries={expiries}")

            # Update progress
            export_tasks[task_id]['progress'] = 50
            export_tasks[task_id]['status_message'] = f'Exporting to {format_type.upper()}...'

            # Export based on format
            logger.info(f"Starting {format_type} export...")
            if format_type == 'csv':
                file_path = exporter.export_to_csv(instruments, expiries, options, task_id)
            elif format_type == 'json':
                file_path = exporter.export_to_json(instruments, expiries, options, task_id)
            elif format_type == 'zip':
                file_path = exporter.export_to_zip(instruments, expiries, options, task_id)
            else:
                raise ValueError(f"Unknown format: {format_type}")

            logger.info(f"Export completed: {file_path}")

            # Update task status
            export_tasks[task_id]['status'] = 'completed'
            export_tasks[task_id]['progress'] = 100
            export_tasks[task_id]['status_message'] = 'Export completed successfully!'
            export_tasks[task_id]['file_path'] = file_path

        except Exception as e:
            import traceback
            logger.error(f"Export failed: {str(e)}")
            logger.error(traceback.format_exc())
            export_tasks[task_id]['status'] = 'failed'
            export_tasks[task_id]['error'] = str(e)
            export_tasks[task_id]['status_message'] = f'Export failed: {str(e)}'

    thread = threading.Thread(target=run_export)
    thread.start()

    return jsonify({'task_id': task_id})

@app.route('/api/export/status/<task_id>')
def api_export_status(task_id):
    """Get export task status"""
    task = export_tasks.get(task_id)

    if not task:
        return jsonify({'error': 'Task not found'}), 404

    return jsonify(task)

@app.route('/api/export/download/<task_id>')
def api_export_download(task_id):
    """Download exported file"""
    from flask import send_file
    import os

    task = export_tasks.get(task_id)

    if not task:
        return jsonify({'error': 'Task not found'}), 404

    if task['status'] != 'completed':
        return jsonify({'error': 'Export not completed'}), 400

    file_path = task['file_path']
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404

    # Get filename for download
    filename = os.path.basename(file_path)

    return send_file(
        file_path,
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

# Create tables
with app.app_context():
    db.create_all()

    # Setup default instruments if not already done
    db_manager.setup_default_instruments()

if __name__ == '__main__':
    import sys
    # Disable auto-reload for exports directory
    extra_files = None
    if '--reload' not in sys.argv:
        # Run without auto-reload in production mode
        app.run(debug=False, host='127.0.0.1', port=5000)
    else:
        # Development mode with auto-reload (exclude exports directory)
        app.run(debug=True, use_reloader=False)  # Disable reloader to prevent clearing export_tasks