"""
Task Manager for handling async collection tasks
Supports parallel multi-instrument processing via asyncio.gather
"""

import asyncio
import copy
import logging
import math
import threading
import uuid
from datetime import date, datetime, timedelta
from enum import Enum

from ..auth.manager import AuthManager
from ..config import config
from ..database.manager import DatabaseManager
from ..utils.instrument_mapper import get_display_name, get_instrument_key
from .expiry_tracker import ExpiryTracker

logger = logging.getLogger(__name__)


MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2  # seconds: 2, 8, 32


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CollectionTask:
    """Represents a single collection task"""

    def __init__(self, task_id: str, params: dict):
        self._lock = threading.Lock()
        self.task_id = task_id
        self.params = params
        self.task_type: str = "fo"  # "fo" for F&O collection, "candle" for candle collection
        self.status = TaskStatus.PENDING
        self.progress = 0
        self.stats = {"expiries": 0, "contracts": 0, "candles": 0, "errors": 0}
        self.failed_contracts: list[dict] = []  # contracts that failed all retries
        self.instrument_progress = {}  # per-instrument progress tracking
        self.current_action = "Initializing..."
        self.logs = []
        self.created_at = datetime.now()
        self.started_at = None
        self.completed_at = None
        self.error_message = None

    def add_log(self, message: str, level: str = "info"):
        """Add a log entry"""
        log_entry = {"timestamp": datetime.now().isoformat(), "level": level, "message": message}
        self.logs.append(log_entry)

        # Map custom levels to standard Python logging levels
        log_level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "success": logging.INFO,  # Map success to info level
        }

        log_level = log_level_map.get(level.lower(), logging.INFO)
        logger.log(log_level, f"[{self.task_id}] {message}")

    def to_dict(self) -> dict:
        """Convert task to dictionary (thread-safe deep copy).
        Stats are computed dynamically from instrument_progress (#16)."""
        with self._lock:
            # Compute stats dynamically from instrument_progress for real-time accuracy
            ip = self.instrument_progress
            if ip:
                stats = {
                    "expiries": sum(p.get("expiries_done", 0) for p in ip.values()),
                    "contracts": sum(p.get("contracts", 0) for p in ip.values()),
                    "candles": sum(p.get("candles", 0) for p in ip.values()),
                    "errors": sum(p.get("errors", 0) for p in ip.values()),
                }
            else:
                stats = copy.deepcopy(self.stats)

            return {
                "task_id": self.task_id,
                "task_type": self.task_type,
                "status": self.status.value,
                "progress": self.progress,
                "stats": stats,
                "instrument_progress": copy.deepcopy(self.instrument_progress),
                "current_action": self.current_action,
                "created_at": self.created_at.isoformat() if self.created_at else None,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "completed_at": self.completed_at.isoformat() if self.completed_at else None,
                "error_message": self.error_message,
                "failed_contracts": list(self.failed_contracts),
                "logs": list(self.logs[-50:]),
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

    def has_running_task(self) -> bool:
        """Check if any F&O collection task is currently running."""
        return any(t.status == TaskStatus.RUNNING and t.task_type == "fo" for t in self.tasks.values())

    def get_running_task_id(self) -> str | None:
        """Return the ID of the currently running F&O task, if any."""
        for tid, t in self.tasks.items():
            if t.status == TaskStatus.RUNNING and t.task_type == "fo":
                return tid
        return None

    def create_task(self, params: dict) -> str:
        """Create a new collection task. Raises ValueError if another task is running."""
        self._cleanup_old_tasks()

        # Concurrent guard: reject if a task is already running
        running_id = self.get_running_task_id()
        if running_id:
            raise ValueError(f"A collection task is already running (ID: {running_id[:8]}...)")

        task_id = str(uuid.uuid4())
        task = CollectionTask(task_id, params)
        self.tasks[task_id] = task

        # Schedule the task and store the future for cancellation
        future = asyncio.run_coroutine_threadsafe(self._run_collection(task), self.loop)
        task._future = future

        return task_id

    def create_candle_task(self, params: dict) -> str:
        """Create a candle data collection task. Only one candle task can run at a time."""
        self._cleanup_old_tasks()
        running = [tid for tid, t in self.tasks.items()
                   if t.status == TaskStatus.RUNNING and t.task_type == "candle"]
        if running:
            raise ValueError(f"A candle task is already running (ID: {running[0][:8]}...)")

        task_id = str(uuid.uuid4())
        task = CollectionTask(task_id, params)
        task.task_type = "candle"
        task.instrument_progress = {
            key: {"status": "pending", "candles": 0, "errors": 0, "chunks_done": 0, "chunks_total": 0}
            for key in params.get("instrument_keys", [])
        }
        self.tasks[task_id] = task
        future = asyncio.run_coroutine_threadsafe(self._run_candle_collection(task), self.loop)
        task._future = future
        return task_id

    async def _run_candle_collection(self, task: CollectionTask):
        """Run candle data collection in explicit batches (batch_size instruments at a time)."""
        try:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.add_log("Starting candle collection", "info")

            # Auth check
            if not self.auth_manager.is_token_valid():
                task.add_log("Not authenticated, attempting to refresh token", "warning")
                if not self.auth_manager.refresh_if_needed():
                    raise Exception("Authentication failed")
            else:
                task.add_log("Authentication valid, proceeding with collection", "info")

            # Extract params
            instrument_keys = task.params.get("instrument_keys", [])
            interval = task.params.get("interval", "1day")
            from_date = task.params.get("from_date")
            to_date = task.params.get("to_date")
            batch_size = max(1, int(task.params.get("workers", 20)))
            incremental = task.params.get("incremental", False)

            from ..api.client import DATA_AVAILABLE_FROM
            from .candle_collector import INTERVAL_MAP, CandleCollector

            if interval not in INTERVAL_MAP:
                raise ValueError(f"Invalid interval '{interval}'")
            unit, interval_val = INTERVAL_MAP[interval]

            # Use full available history as default when user doesn't specify
            if to_date is None:
                to_date = date.today().isoformat()
            if from_date is None:
                from_date = DATA_AVAILABLE_FROM.get(unit, "2000-01-01")

            # Incremental: per-instrument start dates
            incremental_dates: dict = {}
            if incremental:
                incremental_dates = self.db_manager.get_last_candle_timestamps(instrument_keys, interval)
                if incremental_dates:
                    task.add_log(f"Incremental mode: {len(incremental_dates)} instruments have existing data", "info")

            total = len(instrument_keys)
            total_batches = math.ceil(total / batch_size) if total else 1
            task.stats["total_batches"] = total_batches
            task.stats["current_batch"] = 0

            task.add_log(
                f"Collecting {total} instruments in {total_batches} batches of {batch_size}, "
                f"interval={interval}, {from_date} to {to_date}", "info"
            )

            collector = CandleCollector(auth_manager=self.auth_manager, db_manager=self.db_manager)

            async with collector:
                async def collect_one(key: str) -> None:
                    ip = task.instrument_progress[key]

                    # Resolve effective from_date
                    inst_last = incremental_dates.get(key)
                    if inst_last:
                        eff_from = (date.fromisoformat(inst_last) + timedelta(days=1)).isoformat()
                    else:
                        eff_from = from_date

                    # Skip if already up-to-date
                    if eff_from > to_date:  # type: ignore[operator]
                        ip["status"] = "skipped"
                        ip["candles"] = 0
                        self._aggregate_candle_stats(task)
                        return

                    try:
                        ip["status"] = "running"
                        self._aggregate_candle_stats(task)

                        def _on_chunk(chunks_done, chunks_total, candles_in_chunk, _ip=ip):
                            _ip["chunks_done"] = chunks_done
                            _ip["chunks_total"] = chunks_total
                            _ip["candles"] = _ip.get("candles", 0) + candles_in_chunk
                            self._aggregate_candle_stats(task)

                        count = await collector._collect_single(
                            key, unit, interval_val, interval, eff_from, to_date,  # type: ignore[arg-type]
                            on_chunk_done=_on_chunk,
                        )
                        ip["candles"] = count  # Final accurate count from DB insert
                        ip["status"] = "completed"
                    except Exception as e:
                        ip["status"] = "failed"
                        ip["error"] = str(e)
                        ip["errors"] = 1
                        task.add_log(f"Failed {key}: {e}", "error")
                    finally:
                        self._aggregate_candle_stats(task)

                # Process in explicit batches — complete one batch before starting next
                for batch_idx in range(total_batches):
                    if task.status == TaskStatus.FAILED:
                        break
                    batch_start = batch_idx * batch_size
                    batch_keys = instrument_keys[batch_start: batch_start + batch_size]
                    task.stats["current_batch"] = batch_idx + 1
                    task.current_action = (
                        f"Batch {batch_idx + 1}/{total_batches}: "
                        f"collecting {len(batch_keys)} instruments"
                    )
                    await asyncio.gather(*[collect_one(k) for k in batch_keys], return_exceptions=True)

            task.progress = 100
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            task.current_action = f"Completed — {total} instruments in {total_batches} batches"
            task.add_log("Candle collection completed", "success")

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now()
            task.error_message = str(e)
            task.current_action = f"Failed: {e}"
            task.add_log(f"Candle collection failed: {e}", "error")
            logger.exception(f"Candle task {task.task_id} failed")

    def _aggregate_candle_stats(self, task: CollectionTask) -> None:
        """Update task-level progress/action from per-instrument candle progress."""
        ip = task.instrument_progress
        task.stats["candles"] = sum(v.get("candles", 0) for v in ip.values())
        task.stats["errors"] = sum(v.get("errors", 0) for v in ip.values())
        running = [k for k, v in ip.items() if v.get("status") == "running"]
        cur_batch = task.stats.get("current_batch", 0)
        tot_batch = task.stats.get("total_batches", 0)
        batch_prefix = f"[{cur_batch}/{tot_batch}] " if tot_batch > 1 else ""
        if running:
            suffix = f" +{len(running) - 1} more" if len(running) > 1 else ""
            task.current_action = f"{batch_prefix}Collecting: {running[0]}{suffix}"
        done = sum(1 for v in ip.values() if v.get("status") not in ("pending", "running"))
        total = len(ip)
        if total:
            task.progress = int(done / total * 100)

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
            instruments = task.params.get("instruments", [])
            contract_type = task.params.get("contract_type", "both")
            expiries = task.params.get("expiries", {})
            interval = task.params.get("interval", "1minute")
            workers = task.params.get("workers", 3)  # conservative default to avoid 429s

            # Create tracker
            tracker = ExpiryTracker(auth_manager=self.auth_manager, db_manager=self.db_manager)

            # Pre-collection token expiry check
            task.add_log("Checking authentication status", "info")
            if self.auth_manager.is_token_valid():
                # Check if token has enough remaining time
                try:
                    cred = self.auth_manager.db_manager.get_credentials()
                    if cred and cred.get("token_expiry"):
                        import time
                        remaining = cred["token_expiry"] - time.time()
                        if remaining < 3600:  # less than 1 hour
                            mins = int(remaining / 60)
                            task.add_log(
                                f"Warning: Token expires in {mins} minutes. Collection may be interrupted.",
                                "warning",
                            )
                except Exception:
                    pass  # Don't block collection on check failure

            if not self.auth_manager.is_token_valid():
                task.add_log("Not authenticated, attempting to refresh token", "warning")
                if not self.auth_manager.refresh_if_needed():
                    raise Exception("Authentication failed")
            else:
                task.add_log("Authentication valid, proceeding with collection", "info")

            async with tracker:
                total_work = sum(len(exp_list) for exp_list in expiries.values())
                task.add_log(
                    f"Starting collection for {len(instruments)} instruments with {total_work} total expiries", "info"
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
                                cutoff = (datetime.now() - timedelta(days=config.HISTORICAL_MONTHS * 30)).strftime(
                                    "%Y-%m-%d"
                                )
                                recent = [e for e in detected if e >= cutoff]
                                expiries[inst_name] = recent
                                task.add_log(f"Auto-detected {len(recent)} expiries for {inst_name}", "info")
                        except Exception as e:
                            task.add_log(f"Failed to auto-detect expiries for {inst_name}: {e}", "warning")

                # Initialize per-instrument progress
                for name in instruments:
                    task.instrument_progress[name] = {
                        "status": "pending",
                        "progress": 0,
                        "expiries_done": 0,
                        "expiries_total": len(expiries.get(name, [])),
                        "contracts": 0,
                        "candles": 0,
                        "errors": 0,
                    }

                max_parallel = config.MAX_PARALLEL_INSTRUMENTS

                # Shared semaphore — limits total concurrent historical-data API calls
                # across ALL instruments so workers=3 truly means ≤3 in-flight at once
                fetch_sem = asyncio.Semaphore(workers)

                if len(instruments) > 1 and max_parallel > 1:
                    task.add_log(
                        f"Parallel mode: up to {max_parallel} instruments concurrently, "
                        f"{workers} concurrent API calls total",
                        "info",
                    )

                # Process instruments in parallel batches
                for i in range(0, len(instruments), max_parallel):
                    if task.status == TaskStatus.CANCELLED:
                        break
                    batch = instruments[i : i + max_parallel]
                    coros = [
                        self._process_instrument(
                            tracker,
                            name,
                            expiries.get(name, []),
                            contract_type,
                            interval,
                            fetch_sem,
                            task,
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
                logger.debug("Failed to invalidate analytics cache", exc_info=True)

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

    def _apply_strike_filter(
        self, contracts: list[dict], strike_filter: dict | None, task: CollectionTask, display_name: str
    ) -> list[dict]:
        """Filter contracts by strike range.

        Args:
            contracts: List of contract dicts with 'strike_price' and 'contract_type' keys.
            strike_filter: Dict with keys: type ("all"|"atm_range"|"custom"),
                           atm_range (int), min_strike (float), max_strike (float).
        """
        if not strike_filter or strike_filter.get("type") == "all":
            return contracts

        filter_type = strike_filter.get("type")

        # Only filter options (CE/PE), keep futures untouched
        options = [c for c in contracts if c.get("contract_type") in ("CE", "PE")]
        non_options = [c for c in contracts if c.get("contract_type") not in ("CE", "PE")]

        if not options:
            return contracts

        if filter_type == "atm_range":
            atm_n = strike_filter.get("atm_range", 10)
            # Find the ATM strike (the most common/middle strike)
            strikes = sorted(set(float(c.get("strike_price", 0)) for c in options))
            if not strikes:
                return contracts
            # ATM is the median strike
            mid_idx = len(strikes) // 2
            # Get N strikes above and below ATM
            lower_idx = max(0, mid_idx - atm_n)
            upper_idx = min(len(strikes), mid_idx + atm_n + 1)
            allowed_strikes = set(strikes[lower_idx:upper_idx])

            before_count = len(options)
            options = [c for c in options if float(c.get("strike_price", 0)) in allowed_strikes]
            filtered_count = before_count - len(options)
            if filtered_count > 0:
                task.add_log(
                    f"[{display_name}] Strike filter (ATM +/-{atm_n}): kept {len(options)}, "
                    f"filtered out {filtered_count} contracts",
                    "info",
                )

        elif filter_type == "custom":
            min_strike = strike_filter.get("min_strike")
            max_strike = strike_filter.get("max_strike")
            before_count = len(options)
            if min_strike is not None:
                options = [c for c in options if float(c.get("strike_price", 0)) >= min_strike]
            if max_strike is not None:
                options = [c for c in options if float(c.get("strike_price", 0)) <= max_strike]
            filtered_count = before_count - len(options)
            if filtered_count > 0:
                range_desc = []
                if min_strike is not None:
                    range_desc.append(f"min={min_strike}")
                if max_strike is not None:
                    range_desc.append(f"max={max_strike}")
                task.add_log(
                    f"[{display_name}] Strike filter ({', '.join(range_desc)}): kept {len(options)}, "
                    f"filtered out {filtered_count} contracts",
                    "info",
                )

        return non_options + options

    async def _process_instrument(
        self, tracker, instrument_name, instrument_expiries, contract_type, interval, fetch_sem, task
    ):
        """Process a single instrument's expiries and contracts."""
        instrument_key = get_instrument_key(instrument_name)
        display_name = get_display_name(instrument_name)
        ip = task.instrument_progress[instrument_name]
        ip["status"] = "running"
        task.add_log(f"Starting collection for {display_name}", "info")

        # Extract strike filter from task params
        strike_filter = task.params.get("strike_filter")

        try:
            for idx, expiry_date in enumerate(instrument_expiries):
                if task.status == TaskStatus.CANCELLED:
                    ip["status"] = "cancelled"
                    return

                task.add_log(f"[{display_name}] Processing expiry {expiry_date}", "info")

                try:
                    contracts_data = await tracker.get_contracts(instrument_key, expiry_date)
                    contracts_to_process = []

                    if contract_type in ["options", "both"]:
                        options = contracts_data.get("options", [])
                        contracts_to_process.extend(options)
                        ip["contracts"] += len(options)

                    if contract_type in ["futures", "both"]:
                        futures = contracts_data.get("futures", [])
                        contracts_to_process.extend(futures)
                        ip["contracts"] += len(futures)

                    # Apply strike range filter
                    if strike_filter:
                        before = len(contracts_to_process)
                        contracts_to_process = self._apply_strike_filter(
                            contracts_to_process, strike_filter, task, display_name
                        )
                        filtered = before - len(contracts_to_process)
                        if filtered > 0:
                            ip["contracts"] -= filtered

                    # Skip contracts already fetched (unless force_refetch)
                    force_refetch = task.params.get("force_refetch", False)
                    if contracts_to_process and not force_refetch:
                        all_keys = [c.get("instrument_key", "") for c in contracts_to_process]
                        fetched_keys = self.db_manager.get_fetched_keys(all_keys)
                        if fetched_keys:
                            before = len(contracts_to_process)
                            contracts_to_process = [
                                c for c in contracts_to_process if c.get("instrument_key", "") not in fetched_keys
                            ]
                            skipped = before - len(contracts_to_process)
                            if skipped:
                                task.add_log(f"[{display_name}] Skipped {skipped} already-fetched contracts", "info")
                    elif force_refetch and contracts_to_process:
                        task.add_log(f"[{display_name}] Force re-download: processing all {len(contracts_to_process)} contracts", "info")

                    if contracts_to_process:
                        expiry_dt = datetime.strptime(expiry_date, "%Y-%m-%d")
                        end_date = expiry_date
                        start_date = (expiry_dt - timedelta(days=90)).strftime("%Y-%m-%d")

                        # Semaphore-based concurrency: at most `workers` requests
                        # in-flight at any moment across ALL instruments
                        async def _fetch_guarded(contract):
                            async with fetch_sem:
                                return await self._fetch_contract_data(
                                    tracker, contract, start_date, end_date, interval, task
                                )

                        coros = [_fetch_guarded(c) for c in contracts_to_process]
                        results = await asyncio.gather(*coros, return_exceptions=True)
                        for result in results:
                            if isinstance(result, int):
                                ip["candles"] += result
                            elif isinstance(result, Exception):
                                ip["errors"] += 1
                                task.add_log(f"[{display_name}] Error: {result}", "error")

                except Exception as e:
                    ip["errors"] += 1
                    task.add_log(f"[{display_name}] Error processing {expiry_date}: {e}", "error")

                ip["expiries_done"] = idx + 1
                if ip["expiries_total"] > 0:
                    ip["progress"] = int((ip["expiries_done"] / ip["expiries_total"]) * 100)

                # Update overall progress
                self._aggregate_stats(task)

            ip["status"] = "completed"
            task.add_log(f"[{display_name}] Collection completed", "success")

        except Exception as e:
            ip["status"] = "failed"
            task.add_log(f"[{display_name}] Failed: {e}", "error")
            logger.exception(f"Instrument {display_name} failed in task {task.task_id}")

    def _aggregate_stats(self, task: CollectionTask):
        """Aggregate per-instrument stats into task-level stats and progress."""
        totals = {"expiries": 0, "contracts": 0, "candles": 0, "errors": 0}
        total_expiries_done = 0
        total_expiries_all = 0

        for ip in task.instrument_progress.values():
            totals["expiries"] += ip.get("expiries_done", 0)
            totals["contracts"] += ip.get("contracts", 0)
            totals["candles"] += ip.get("candles", 0)
            totals["errors"] += ip.get("errors", 0)
            total_expiries_done += ip.get("expiries_done", 0)
            total_expiries_all += ip.get("expiries_total", 0)

        task.stats = totals
        if total_expiries_all > 0:
            task.progress = int((total_expiries_done / total_expiries_all) * 100)

        # Build current_action summary
        running = [
            get_display_name(name) for name, ip in task.instrument_progress.items() if ip.get("status") == "running"
        ]
        if running:
            task.current_action = f"Processing: {', '.join(running)}"

    async def _fetch_contract_data(self, tracker, contract, start_date, end_date, interval, task):
        """Fetch historical data for a single contract with retry logic."""
        expired_key = contract.get("instrument_key", "")
        symbol = contract.get("trading_symbol", expired_key)

        if not expired_key:
            task.add_log(f"Missing instrument_key for contract: {contract}", "error")
            return 0

        last_error = None
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                task.add_log(
                    f"Fetching data for {symbol} ({expired_key}) from {start_date} to {end_date}"
                    + (f" (attempt {attempt})" if attempt > 1 else ""),
                    "debug",
                )

                candles = await tracker.api_client.get_historical_data(
                    expired_key, start_date, end_date, interval
                )

                if candles:
                    count = tracker.db_manager.insert_historical_data(expired_key, candles)
                    task.add_log(f"Downloaded {count} candles for {symbol}", "info")
                    return count
                else:
                    tracker.db_manager.mark_contract_no_data(expired_key)
                    task.add_log(f"No candles received for {symbol} — marked as no_data", "warning")
                    return 0

            except Exception as e:
                last_error = e
                if attempt < MAX_RETRY_ATTEMPTS:
                    _backoff_steps = [2, 8, 32]
                    backoff = _backoff_steps[min(attempt - 1, len(_backoff_steps) - 1)]
                    task.add_log(
                        f"Attempt {attempt}/{MAX_RETRY_ATTEMPTS} failed for {symbol}: {e}. "
                        f"Retrying in {backoff}s...",
                        "warning",
                    )
                    await asyncio.sleep(backoff)

        # All retries exhausted — record as failed
        task.add_log(
            f"All {MAX_RETRY_ATTEMPTS} attempts failed for {symbol}: {last_error}",
            "error",
        )
        with task._lock:
            task.failed_contracts.append({
                "instrument_key": expired_key,
                "trading_symbol": symbol,
                "error": str(last_error),
            })
        raise last_error  # type: ignore[misc]

    def estimate_collection(self, instruments: list[str], expiries: dict[str, list[str]], contract_type: str = "both") -> dict:
        """Estimate the work required for a collection.

        Returns dict with: api_calls, est_time_seconds, est_storage_bytes, contracts_count
        """
        total_contracts = 0
        for inst_key in instruments:
            for expiry_date in expiries.get(inst_key, []):
                try:
                    contracts = self.db_manager.get_contracts_for_expiry(inst_key, expiry_date)
                    if contract_type == "options":
                        contracts = [c for c in contracts if c.get("contract_type") in ("CE", "PE")]
                    elif contract_type == "futures":
                        contracts = [c for c in contracts if c.get("contract_type") not in ("CE", "PE")]
                    # Subtract already-fetched
                    pending = [c for c in contracts if not c.get("data_fetched")]
                    total_contracts += len(pending)
                except Exception:
                    # Estimate average contracts per expiry if we can't look up
                    total_contracts += 200  # rough estimate

        # Each contract = 1 API call for historical data
        api_calls = total_contracts
        # Rate limit: ~45 req/sec effective
        est_time = api_calls / 40.0  # seconds, conservative
        # Average 375 candles per contract * ~100 bytes each
        est_storage = total_contracts * 375 * 100

        return {
            "contracts_count": total_contracts,
            "api_calls": api_calls,
            "est_time_seconds": round(est_time),
            "est_time_display": f"{int(est_time // 60)}m {int(est_time % 60)}s" if est_time >= 60 else f"{int(est_time)}s",
            "est_storage_bytes": est_storage,
            "est_storage_display": f"{est_storage / (1024*1024):.1f} MB" if est_storage > 1024*1024 else f"{est_storage / 1024:.0f} KB",
        }

    def get_task_status(self, task_id: str) -> dict | None:
        """Get status of a task"""
        if task_id in self.tasks:
            return self.tasks[task_id].to_dict()
        return None

    def get_candle_task_status(self, task_id: str) -> dict | None:
        """Optimised status read for candle tasks.

        Avoids deep-copying the full instrument_progress dict (which can have
        thousands of entries).  Aggregate counts are computed from all entries
        but only the non-pending rows (≤ 100) are included in the response so
        poll payloads stay small regardless of collection size.
        """
        task = self.tasks.get(task_id)
        if not task:
            return None
        with task._lock:
            ip = task.instrument_progress
            total = len(ip)
            processed = sum(1 for v in ip.values() if v.get("status") not in ("pending", "running"))
            skipped   = sum(1 for v in ip.values() if v.get("status") == "skipped")
            candles   = sum(v.get("candles", 0) for v in ip.values())
            errors    = sum(v.get("errors",  0) for v in ip.values())

            # Trim to active entries only — running first, then failed/completed/skipped
            _ORDER = {"running": 0, "failed": 1, "completed": 2, "skipped": 3}
            active = {k: v for k, v in ip.items() if v.get("status") != "pending"}
            trimmed_keys = sorted(active, key=lambda k: _ORDER.get(active[k].get("status", ""), 3))[:100]
            trimmed_ip = {
                k: {
                    "status":  active[k].get("status"),
                    "candles": active[k].get("candles", 0),
                    "chunks_done": active[k].get("chunks_done", 0),
                    "chunks_total": active[k].get("chunks_total", 0),
                    "error":   active[k].get("error"),
                }
                for k in trimmed_keys
            }

            return {
                "task_id":        task_id,
                "task_type":      task.task_type,
                "status":         task.status.value,
                "progress":       task.progress,
                "stats":          {"candles": candles, "errors": errors},
                "instrument_progress": trimmed_ip,
                "current_action": task.current_action,
                "created_at":     task.created_at.isoformat() if task.created_at else None,
                "completed_at":   task.completed_at.isoformat() if task.completed_at else None,
                # Pre-computed aggregate counts (from full ip, not trimmed)
                "_total":     total,
                "_processed": processed,
                "_skipped":   skipped,
            }

    def get_all_tasks(self) -> list[dict]:
        """Get all tasks"""
        return [task.to_dict() for task in self.tasks.values()]

    def _cleanup_old_tasks(self):
        """Remove completed/failed/cancelled tasks older than 1 hour"""
        cutoff = datetime.now() - timedelta(hours=1)
        to_remove = [
            tid
            for tid, task in self.tasks.items()
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)
            and task.completed_at
            and task.completed_at < cutoff
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
                future = getattr(task, "_future", None)
                if future and not future.done():
                    future.cancel()
                return True
        return False


# Singleton instance
task_manager = TaskManager()
