"""
Rate limiter implementation for Upstox API compliance
Limits: 50 req/sec, 500 req/min, 2000 req/30min
"""

import asyncio
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class UpstoxRateLimiter:
    """
    Rate limiter that enforces Upstox API limits with safety margins
    """

    def __init__(self, max_per_second: int = 45, max_per_minute: int = 450, max_per_30min: int = 1800):
        """
        Initialize rate limiter with conservative limits

        Args:
            max_per_second: Maximum requests per second (default 45, limit is 50)
            max_per_minute: Maximum requests per minute (default 450, limit is 500)
            max_per_30min: Maximum requests per 30 minutes (default 1800, limit is 2000)
        """
        self.limits = {
            "second": (max_per_second, 1.0),
            "minute": (max_per_minute, 60.0),
            "half_hour": (max_per_30min, 1800.0),
        }

        self.windows = {"second": deque(), "minute": deque(), "half_hour": deque()}

        self.lock = asyncio.Lock()
        self.request_count = 0
        self.error_count = 0
        self.backoff_factor = 1.0
        self._backoff_until: float = 0.0  # epoch seconds; all workers pause until this time

    async def acquire(self, priority: int = 1) -> None:
        """
        Wait if necessary to respect rate limits.

        Long waits (e.g. 30-minute window) are broken into shorter sleeps
        so the lock is released periodically and progress is visible.

        Args:
            priority: Request priority (lower = higher priority)
        """
        while True:
            async with self.lock:
                now = time.time()

                # Honour global backoff set by a previous 429 response
                if now < self._backoff_until:
                    wait_time = self._backoff_until - now
                    logger.debug(f"Global backoff active, waiting {wait_time:.1f}s")
                    await asyncio.sleep(min(wait_time, 30.0))
                    if wait_time <= 30.0:
                        now = time.time()
                    else:
                        continue  # Re-acquire lock and re-check

                # Clean old timestamps from windows
                for window_name, (_limit, duration) in self.limits.items():
                    window = self.windows[window_name]
                    while window and now - window[0] > duration:
                        window.popleft()

                # Check if we need to wait
                wait_time = 0.0
                blocking_window = ""
                for window_name, (limit, duration) in self.limits.items():
                    window = self.windows[window_name]
                    effective_limit = int(limit / self.backoff_factor)

                    if len(window) >= effective_limit:
                        oldest = window[0]
                        wait_needed = duration - (now - oldest) + 0.01
                        if wait_needed > wait_time:
                            wait_time = wait_needed
                            blocking_window = window_name

                if wait_time <= 0:
                    # No wait needed — record the request and return
                    for window in self.windows.values():
                        window.append(now)
                    self.request_count += 1
                    return

                # Cap the sleep to avoid holding the lock for too long.
                # For short waits (< 5s), sleep inline. For long waits,
                # release the lock, sleep a bit, and re-check.
                if wait_time <= 5.0:
                    logger.info(f"Rate limit reached ({blocking_window}), waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    now = time.time()
                    # Record and return
                    for window in self.windows.values():
                        window.append(now)
                    self.request_count += 1
                    return

            # Long wait: release the lock, sleep briefly, then re-check
            sleep_chunk = min(wait_time, 30.0)
            logger.info(
                f"Rate limit reached ({blocking_window}), need {wait_time:.0f}s — "
                f"sleeping {sleep_chunk:.0f}s then re-checking"
            )
            await asyncio.sleep(sleep_chunk)
            # Loop back to re-acquire lock and re-evaluate

    async def handle_response(self, status_code: int, headers: dict | None = None) -> None:
        """
        Handle API response and adjust rate limiting if needed

        Args:
            status_code: HTTP status code
            headers: Response headers
        """
        if status_code == 429:  # Rate limit exceeded
            self.error_count += 1
            self.backoff_factor = min(2.0, 1.0 + (self.error_count * 0.1))

            # Get retry-after header if available, capped to avoid excessive stalls
            retry_after = 10  # Default wait time
            if headers and "retry-after" in headers:
                try:
                    retry_after = int(headers["retry-after"])
                except (ValueError, TypeError):
                    pass

            # Cap to 30s max — API may send 600s but that stalls bulk collection
            retry_after = min(retry_after, 30)

            # Set a global backoff gate so ALL workers slow down together
            new_backoff = time.time() + retry_after
            if new_backoff > self._backoff_until:
                self._backoff_until = new_backoff

            logger.warning(f"Rate limit exceeded (429), all workers backing off for {retry_after}s")

        elif status_code < 400:  # Successful request
            if self.error_count > 0:
                self.error_count -= 1
                self.backoff_factor = max(1.0, self.backoff_factor - 0.05)
            # _backoff_until is left to expire naturally; don't clear on single success

    def get_usage_stats(self) -> dict[str, dict]:
        """
        Get current rate limit usage statistics

        Returns:
            Dictionary with usage stats for each time window
        """
        now = time.time()
        stats = {}

        for window_name, (limit, duration) in self.limits.items():
            window = self.windows[window_name]
            recent = sum(1 for ts in window if now - ts <= duration)

            stats[window_name] = {
                "used": recent,
                "limit": int(limit / self.backoff_factor),
                "original_limit": limit,
                "percentage": (recent / limit) * 100 if limit > 0 else 0,
                "remaining": max(0, int(limit / self.backoff_factor) - recent),
            }

        stats["total_requests"] = self.request_count
        stats["error_count"] = self.error_count
        stats["backoff_factor"] = self.backoff_factor

        return stats

    def reset(self) -> None:
        """Reset all rate limit windows and counters"""
        for window in self.windows.values():
            window.clear()
        self.request_count = 0
        self.error_count = 0
        self.backoff_factor = 1.0
        logger.info("Rate limiter reset")

    def print_dashboard(self) -> None:
        """Print current rate limit status"""
        stats = self.get_usage_stats()

        lines = ["Rate Limit Status Dashboard"]
        for window_name in ["second", "minute", "half_hour"]:
            if window_name in stats:
                s = stats[window_name]
                window_display = window_name.replace("_", " ").title()
                lines.append(f"  {window_display}: {s['used']}/{s['limit']} ({s['percentage']:.1f}%)")
        lines.append(f"  Total: {stats['total_requests']:,} | Errors: {stats['error_count']}")
        if stats["backoff_factor"] > 1.0:
            lines.append(f"  Backoff Active: {stats['backoff_factor']:.2f}x")
        logger.info(" | ".join(lines))


class PriorityRateLimiter(UpstoxRateLimiter):
    """
    Rate limiter with priority queue support
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.priority_queue = asyncio.PriorityQueue()
        self._processing_lock = asyncio.Lock()
        self._processing = False
        self._counter = 0

    async def acquire_with_priority(self, priority: int = 5) -> None:
        """
        Acquire rate limit slot with priority

        Args:
            priority: Request priority (1=highest, 10=lowest)
        """
        event = asyncio.Event()
        counter = self._counter
        self._counter += 1
        await self.priority_queue.put((priority, time.time(), counter, event))

        async with self._processing_lock:
            if not self._processing:
                self._processing = True
                asyncio.create_task(self._process_queue())

        await event.wait()

    async def _process_queue(self) -> None:
        """Process priority queue"""
        try:
            while True:
                try:
                    priority, timestamp, counter, event = self.priority_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await self.acquire()
                event.set()
        finally:
            async with self._processing_lock:
                self._processing = False
                # Re-check: if items were added during processing, restart
                if not self.priority_queue.empty():
                    self._processing = True
                    asyncio.create_task(self._process_queue())
