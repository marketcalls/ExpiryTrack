"""
Task Manager for handling async collection tasks
"""
import asyncio
import uuid
import threading
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging
from enum import Enum

from .expiry_tracker import ExpiryTracker
from ..auth.manager import AuthManager
from ..database.manager import DatabaseManager
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
        """Convert task to dictionary"""
        return {
            'task_id': self.task_id,
            'status': self.status.value,
            'progress': self.progress,
            'stats': self.stats,
            'current_action': self.current_action,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'logs': self.logs[-50:]  # Last 50 log entries
        }

class TaskManager:
    """Manages collection tasks"""

    _instance = None

    def __new__(cls):
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
        def run_loop():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()

        self.thread = threading.Thread(target=run_loop, daemon=True)
        self.thread.start()

        # Wait for loop to be ready
        import time
        while self.loop is None:
            time.sleep(0.1)

    def create_task(self, params: Dict) -> str:
        """Create a new collection task"""
        task_id = str(uuid.uuid4())
        task = CollectionTask(task_id, params)
        self.tasks[task_id] = task

        # Schedule the task
        asyncio.run_coroutine_threadsafe(
            self._run_collection(task),
            self.loop
        )

        return task_id

    async def _run_collection(self, task: CollectionTask):
        """Run the actual collection"""
        try:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.add_log("Starting collection task", "info")

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
            if not self.auth_manager.is_token_valid():
                task.add_log("Not authenticated, attempting to refresh token", "warning")
                if not self.auth_manager.refresh_if_needed():
                    raise Exception("Authentication failed")

            async with tracker:
                total_work = sum(len(exp_list) for exp_list in expiries.values())
                work_done = 0

                # Process each instrument
                for instrument_name in instruments:
                    instrument_key = get_instrument_key(instrument_name)
                    instrument_expiries = expiries.get(instrument_name, [])

                    task.current_action = f"Processing {instrument_name}"
                    task.add_log(f"Starting collection for {instrument_name}", "info")

                    # Process each expiry
                    for expiry_date in instrument_expiries:
                        task.current_action = f"Fetching contracts for {instrument_name} - {expiry_date}"
                        task.add_log(f"Processing expiry {expiry_date}", "info")

                        try:
                            # Fetch contracts based on type
                            contracts_data = await tracker.get_contracts(instrument_key, expiry_date)

                            contracts_to_process = []

                            if contract_type in ['options', 'both']:
                                options = contracts_data.get('options', [])
                                contracts_to_process.extend(options)
                                task.stats['contracts'] += len(options)
                                task.add_log(f"Found {len(options)} option contracts", "info")

                            if contract_type in ['futures', 'both']:
                                futures = contracts_data.get('futures', [])
                                contracts_to_process.extend(futures)
                                task.stats['contracts'] += len(futures)
                                task.add_log(f"Found {len(futures)} futures contracts", "info")

                            # Fetch historical data
                            if contracts_to_process:
                                task.current_action = f"Downloading historical data for {len(contracts_to_process)} contracts"

                                # Calculate date range (3 months before expiry to expiry date)
                                expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d')
                                end_date = expiry_date
                                start_dt = expiry_dt - timedelta(days=90)
                                start_date = start_dt.strftime('%Y-%m-%d')

                                task.add_log(f"Date range for {instrument_name} {expiry_date}: {start_date} to {end_date}", "info")

                                # Batch process contracts
                                batch_size = min(workers, 10)
                                for i in range(0, len(contracts_to_process), batch_size):
                                    batch = contracts_to_process[i:i + batch_size]

                                    # Process batch concurrently
                                    tasks = []
                                    for contract in batch:
                                        tasks.append(
                                            self._fetch_contract_data(
                                                tracker, contract, start_date, end_date, interval, task
                                            )
                                        )

                                    results = await asyncio.gather(*tasks, return_exceptions=True)

                                    # Count successful candles
                                    for result in results:
                                        if isinstance(result, int):
                                            task.stats['candles'] += result
                                        elif isinstance(result, Exception):
                                            task.stats['errors'] += 1
                                            task.add_log(f"Error: {str(result)}", "error")

                            # Update progress
                            work_done += 1
                            task.progress = int((work_done / total_work) * 100)

                        except Exception as e:
                            task.stats['errors'] += 1
                            task.add_log(f"Error processing {expiry_date}: {str(e)}", "error")

                    task.stats['expiries'] += len(instrument_expiries)

            # Mark as completed
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.progress = 100
            task.current_action = "Collection completed"
            task.add_log("Collection completed successfully", "success")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now()
            task.error_message = str(e)
            task.current_action = f"Failed: {str(e)}"
            task.add_log(f"Collection failed: {str(e)}", "error")
            logger.exception(f"Task {task.task_id} failed")

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
                task.add_log(f"No candles received for {symbol}", "warning")

            return 0

        except Exception as e:
            task.add_log(f"Error fetching data for {contract.get('trading_symbol', 'unknown')}: {str(e)}", "error")
            raise e

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get status of a task"""
        if task_id in self.tasks:
            return self.tasks[task_id].to_dict()
        return None

    def get_all_tasks(self) -> List[Dict]:
        """Get all tasks"""
        return [task.to_dict() for task in self.tasks.values()]

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.status == TaskStatus.RUNNING:
                task.status = TaskStatus.CANCELLED
                task.completed_at = datetime.now()
                task.current_action = "Cancelled by user"
                task.add_log("Task cancelled by user", "warning")
                return True
        return False

# Singleton instance
task_manager = TaskManager()