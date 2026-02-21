"""
Task Manager for handling async collection tasks
Supports parallel multi-instrument processing via asyncio.gather
"""
import asyncio
import copy
import uuid
import threading
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from enum import Enum

from .expiry_tracker import ExpiryTracker
from ..auth.manager import AuthManager
from ..database.manager import DatabaseManager
from ..config import config
from ..utils.instrument_mapper import get_instrument_key

logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class CollectionTask:
    """Represents a single collection task"""

    def __init__(self, task_id: str, params: Dict):
        self._lock = threading.Lock()
        self.task_id = task_id
        self.params = params
        self.status = TaskStatus.PENDING
        self.progress = 0
        self.stats = {
            'expiries': 0,
            'contracts': 0,
            'candles': 0,
            'errors': 0
        }
        self.instrument_progress = {}  # per-instrument progress tracking
        self.current_action = "Initializing..."
        self.logs = []
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.error_message = None

    def add_log(self, message: str, level: str = "info"):
        """Add a log entry"""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message
        }
        self.logs.append(log_entry)

        # Map custom levels to standard Python logging levels
        log_level_map = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'success': logging.INFO  # Map success to info level
        }

        log_level = log_level_map.get(level.lower(), logging.INFO)
        logger.log(log_level, f"[{self.task_id}] {message}")

    def to_dict(self) -> Dict:
        """Convert task to dictionary (thread-safe deep copy).
        Stats are computed dynamically from instrument_progress (#16)."""
        with self._lock:
            # Compute stats dynamically from instrument_progress for real-time accuracy
            ip = self.instrument_progress
            if ip:
                stats = {
                    'expiries': sum(p.get('expiries_done', 0) for p in ip.values()),
                    'contracts': sum(p.get('contracts', 0) for p in ip.values()),
                    'candles': sum(p.get('candles', 0) for p in ip.values()),
                    'errors': sum(p.get('errors', 0) for p in ip.values()),
                }
            else:
                stats = copy.deepcopy(self.stats)

            return {
                'task_id': self.task_id,
                'status': self.status.value,
                'progress': self.progress,
                'stats': stats,
                'instrument_progress': copy.deepcopy(self.instrument_progress),
                'current_action': self.current_action,
                'created_at': self.created_at.isoformat() if self.created_at else None,
                'started_at': self.started_at.isoformat() if self.started_at else None,
                'completed_at': self.completed_at.isoformat() if self.completed_at else None,
                'error_message': self.error_message,
                'logs': list(self.logs[-50:]),
            }

class TaskManager:
    """Manages collection tasks"""

    _instance = None
    _singleton_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.initialized = False
        return cls._instance

    def __init__(self):
        if not self.initialized:
            self.tasks = {}
            self.loop = None
            self.thread = None
            self.initialized = True
            self.auth_manager = AuthManager()
            self.db_manager = DatabaseManager()
            self._start_event_loop()

    def _start_event_loop(self):
        """Start async event loop in separate thread"""
        ready = threading.Event()

        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=run_loop, daemon=True)
        self.thread.start()

        if not ready.wait(timeout=5.0):
            raise RuntimeError("Event loop thread failed to start")

    def create_task(self, params: Dict) -> str:
        """Create a new collection task"""
        self._cleanup_old_tasks()

        task_id = str(uuid.uuid4())
        task = CollectionTask(task_id, params)
        self.tasks[task_id] = task

        # Schedule the task and store the future for cancellation
        future = asyncio.run_coroutine_threadsafe(
            self._run_collection(task),
            self.loop
        )
        task._future = future

        return task_id

    async def _run_collection(self, task: CollectionTask):
        """Run collection with parallel instrument processing.

        Instruments are processed concurrently up to MAX_PARALLEL_INSTRUMENTS.
        Within each instrument, expiries and contracts are still batched sequentially
        to stay within API rate limits.
        """
        try:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.add_log("Starting collection task", "info")
            task.add_log("Task initialization complete", "info")

            # Extract parameters
            instruments = task.params.get('instruments', [])
            contract_type = task.params.get('contract_type', 'both')
            expiries = task.params.get('expiries', {})
            interval = task.params.get('interval', '1minute')
            workers = task.params.get('workers', 5)

            # Create tracker
            tracker = ExpiryTracker(
                auth_manager=self.auth_manager,
                db_manager=self.db_manager
            )

            # Check authentication
            task.add_log("Checking authentication status", "info")
            if not self.auth_manager.is_token_valid():
                task.add_log("Not authenticated, attempting to refresh token", "warning")
                if not self.auth_manager.refresh_if_needed():
                    raise Exception("Authentication failed")
            else:
                task.add_log("Authentication valid, proceeding with collection", "info")

            async with tracker:
                total_work = sum(len(exp_list) for exp_list in expiries.values())
                task.add_log(
                    f"Starting collection for {len(instruments)} instruments "
                    f"with {total_work} total expiries", "info"
                )

                # Auto-detect expiries if none provided (e.g., scheduler jobs)
                if not expiries or all(len(v) == 0 for v in expiries.values()):
                    task.add_log("No expiries specified, auto-detecting from API...", "info")
                    for inst_name in instruments:
                        inst_key = get_instrument_key(inst_name)
                        try:
                            detected = await tracker.get_expiries(inst_key)
                            if detected:
                                # Filter to recent expiries (last N months)
                                cutoff = (datetime.now() - timedelta(days=config.HISTORICAL_MONTHS * 30)).strftime('%Y-%m-%d')
                                recent = [e for e in detected if e >= cutoff]
                                expiries[inst_name] = recent
                                task.add_log(f"Auto-detected {len(recent)} expiries for {inst_name}", "info")
                        except Exception as e:
                            task.add_log(f"Failed to auto-detect expiries for {inst_name}: {e}", "warning")

                # Initialize per-instrument progress
                for name in instruments:
                    task.instrument_progress[name] = {
                        'status': 'pending',
                        'progress': 0,
                        'expiries_done': 0,
                        'expiries_total': len(expiries.get(name, [])),
                        'contracts': 0,
                        'candles': 0,
                        'errors': 0,
                    }

                max_parallel = config.MAX_PARALLEL_INSTRUMENTS

                if len(instruments) > 1 and max_parallel > 1:
                    task.add_log(
                        f"Parallel mode: up to {max_parallel} instruments concurrently",
                        "info",
                    )

                # Process instruments in parallel batches
                for i in range(0, len(instruments), max_parallel):
                    if task.status == TaskStatus.CANCELLED:
                        break
                    batch = instruments[i:i + max_parallel]
                    coros = [
                        self._process_instrument(
                            tracker, name, expiries.get(name, []),
                            contract_type, interval, workers, task,
                        )
                        for name in batch
                    ]
                    await asyncio.gather(*coros, return_exceptions=True)

                # Aggregate overall progress
                task.progress = 100
                self._aggregate_stats(task)

            # Run post-collection quality checks if enabled
            if config.QUALITY_CHECK_AFTER_COLLECTION:
                task.current_action = "Running data quality checks..."
                task.add_log("Running post-collection quality checks", "info")
                try:
                    from ..quality.checker import DataQualityChecker
                    checker = DataQualityChecker()
                    qc_report = checker.run_all_checks()
                    task.add_log(
                        f"Quality check: {qc_report.checks_passed}/{qc_report.checks_run} passed, "
                        f"{qc_report.error_count} errors, {qc_report.warning_count} warnings",
                        "success" if qc_report.passed else "warning",
                    )
                except Exception as e:
                    task.add_log(f"Quality check failed: {e}", "warning")

            # Invalidate analytics cache after collection (#14)
            try:
                from ..analytics.engine import AnalyticsCache
                AnalyticsCache.invalidate_all()
            except Exception:
                pass

            # Mark as completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.current_action = "Collection completed"
            task.add_log("Collection completed successfully", "success")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now()
            task.error_message = str(e)
            task.current_action = f"Failed: {str(e)}"
            task.add_log(f"Collection failed: {str(e)}", "error")
            logger.exception(f"Task {task.task_id} failed")

    async def _process_instrument(
        self, tracker, instrument_name, instrument_expiries,
        contract_type, interval, workers, task
    ):
        """Process a single instrument's expiries and contracts."""
        instrument_key = get_instrument_key(instrument_name)
        ip = task.instrument_progress[instrument_name]
        ip['status'] = 'running'
        task.add_log(f"Starting collection for {instrument_name}", "info")

        try:
            for idx, expiry_date in enumerate(instrument_expiries):
                if task.status == TaskStatus.CANCELLED:
                    ip['status'] = 'cancelled'
                    return

                task.add_log(f"[{instrument_name}] Processing expiry {expiry_date}", "info")

                try:
                    contracts_data = await tracker.get_contracts(instrument_key, expiry_date)
                    contracts_to_process = []

                    if contract_type in ['options', 'both']:
                        options = contracts_data.get('options', [])
                        contracts_to_process.extend(options)
                        ip['contracts'] += len(options)

                    if contract_type in ['futures', 'both']:
                        futures = contracts_data.get('futures', [])
                        contracts_to_process.extend(futures)
                        ip['contracts'] += len(futures)

                    # Skip contracts already fetched
                    if contracts_to_process:
                        all_keys = [c.get('instrument_key', '') for c in contracts_to_process]
                        fetched_keys = self.db_manager.get_fetched_keys(all_keys)
                        if fetched_keys:
                            before = len(contracts_to_process)
                            contracts_to_process = [
                                c for c in contracts_to_process
                                if c.get('instrument_key', '') not in fetched_keys
                            ]
                            skipped = before - len(contracts_to_process)
                            if skipped:
                                task.add_log(
                                    f"[{instrument_name}] Skipped {skipped} already-fetched contracts",
                                    "info"
                                )

                    if contracts_to_process:
                        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
                        end_date = expiry_date
                        start_date = (expiry_dt - timedelta(days=90)).strftime('%Y-%m-%d')

                        batch_size = min(workers, 10)
                        for j in range(0, len(contracts_to_process), batch_size):
                            batch = contracts_to_process[j:j + batch_size]
                            coros = [
                                self._fetch_contract_data(
                                    tracker, c, start_date, end_date, interval, task
                                )
                                for c in batch
                            ]
                            results = await asyncio.gather(*coros, return_exceptions=True)
                            for result in results:
                                if isinstance(result, int):
                                    ip['candles'] += result
                                elif isinstance(result, Exception):
                                    ip['errors'] += 1
                                    task.add_log(
                                        f"[{instrument_name}] Error: {result}", "error"
                                    )

                except Exception as e:
                    ip['errors'] += 1
                    task.add_log(
                        f"[{instrument_name}] Error processing {expiry_date}: {e}", "error"
                    )

                ip['expiries_done'] = idx + 1
                if ip['expiries_total'] > 0:
                    ip['progress'] = int((ip['expiries_done'] / ip['expiries_total']) * 100)

                # Update overall progress
                self._aggregate_stats(task)

            ip['status'] = 'completed'
            task.add_log(f"[{instrument_name}] Collection completed", "success")

        except Exception as e:
            ip['status'] = 'failed'
            task.add_log(f"[{instrument_name}] Failed: {e}", "error")
            logger.exception(f"Instrument {instrument_name} failed in task {task.task_id}")

    def _aggregate_stats(self, task: CollectionTask):
        """Aggregate per-instrument stats into task-level stats and progress."""
        totals = {'expiries': 0, 'contracts': 0, 'candles': 0, 'errors': 0}
        total_expiries_done = 0
        total_expiries_all = 0

        for ip in task.instrument_progress.values():
            totals['expiries'] += ip.get('expiries_done', 0)
            totals['contracts'] += ip.get('contracts', 0)
            totals['candles'] += ip.get('candles', 0)
            totals['errors'] += ip.get('errors', 0)
            total_expiries_done += ip.get('expiries_done', 0)
            total_expiries_all += ip.get('expiries_total', 0)

        task.stats = totals
        if total_expiries_all > 0:
            task.progress = int((total_expiries_done / total_expiries_all) * 100)

        # Build current_action summary
        running = [
            name for name, ip in task.instrument_progress.items()
            if ip.get('status') == 'running'
        ]
        if running:
            task.current_action = f"Processing: {', '.join(running)}"

    async def _fetch_contract_data(self, tracker, contract, start_date, end_date, interval, task):
        """Fetch historical data for a single contract"""
        try:
            # The expired instrument key is stored in 'instrument_key' field for expired contracts
            expired_key = contract.get('instrument_key', '')
            symbol = contract.get('trading_symbol', expired_key)

            if not expired_key:
                task.add_log(f"Missing instrument_key for contract: {contract}", "error")
                return 0

            task.add_log(f"Fetching data for {symbol} ({expired_key}) from {start_date} to {end_date}", "debug")

            # Fetch historical data
            candles = await tracker.api_client.get_historical_data(
                expired_key,
                start_date,
                end_date,
                interval
            )

            if candles:
                # Store in database
                count = tracker.db_manager.insert_historical_data(expired_key, candles)
                task.add_log(f"Downloaded {count} candles for {symbol}", "info")
                return count
            else:
                # Mark contract as fetched but with no data available
                # This prevents re-processing the same contract on future runs
                tracker.db_manager.mark_contract_no_data(expired_key)
                task.add_log(f"No candles received for {symbol} — marked as no_data", "warning")

            return 0

        except Exception as e:
            task.add_log(f"Error fetching data for {contract.get('trading_symbol', 'unknown')}: {str(e)}", "error")
            raise

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get status of a task"""
        if task_id in self.tasks:
            return self.tasks[task_id].to_dict()
        return None

    def get_all_tasks(self) -> List[Dict]:
        """Get all tasks"""
        return [task.to_dict() for task in self.tasks.values()]

    def _cleanup_old_tasks(self):
        """Remove completed/failed/cancelled tasks older than 1 hour"""
        cutoff = datetime.now() - timedelta(hours=1)
        to_remove = [
            tid for tid, task in self.tasks.items()
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
            and task.completed_at and task.completed_at < cutoff
        ]
        for tid in to_remove:
            del self.tasks[tid]
        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old tasks")

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.CANCELLED
                task.completed_at = datetime.now()
                task.current_action = "Cancelled by user"
                task.add_log("Task cancelled by user", "warning")
                # Actually cancel the running coroutine
                future = getattr(task, '_future', None)
                if future and not future.done():
                    future.cancel()
                return True
        return False

# Singleton instance
task_manager = TaskManager()