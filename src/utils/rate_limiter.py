"""
Rate limiter implementation for Upstox API compliance
Limits: 50 req/sec, 500 req/min, 2000 req/30min
"""
import asyncio
import time
from collections import deque
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class UpstoxRateLimiter:
    """
    Rate limiter that enforces Upstox API limits with safety margins
    """

    def __init__(self,
                 max_per_second: int = 45,
                 max_per_minute: int = 450,
                 max_per_30min: int = 1800):
        """
        Initialize rate limiter with conservative limits

        Args:
            max_per_second: Maximum requests per second (default 45, limit is 50)
            max_per_minute: Maximum requests per minute (default 450, limit is 500)
            max_per_30min: Maximum requests per 30 minutes (default 1800, limit is 2000)
        """
        self.limits = {
            'second': (max_per_second, 1.0),
            'minute': (max_per_minute, 60.0),
            'half_hour': (max_per_30min, 1800.0)
        }

        self.windows = {
            'second': deque(),
            'minute': deque(),
            'half_hour': deque()
        }

        self.lock = asyncio.Lock()
        self.request_count = 0
        self.error_count = 0
        self.backoff_factor = 1.0

    async def acquire(self, priority: int = 1) -> None:
        """
        Wait if necessary to respect rate limits

        Args:
            priority: Request priority (lower = higher priority)
        """
        async with self.lock:
            now = time.time()

            # Clean old timestamps from windows
            for window_name, (limit, duration) in self.limits.items():
                window = self.windows[window_name]
                while window and now - window[0] > duration:
                    window.popleft()

            # Check if we need to wait
            wait_time = 0.0
            for window_name, (limit, duration) in self.limits.items():
                window = self.windows[window_name]
                effective_limit = int(limit / self.backoff_factor)

                if len(window) >= effective_limit:
                    oldest = window[0]
                    wait_needed = duration - (now - oldest) + 0.01
                    wait_time = max(wait_time, wait_needed)

                    logger.debug(f"Rate limit {window_name}: {len(window)}/{effective_limit}, "
                               f"waiting {wait_needed:.2f}s")

            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                now = time.time()

            # Record the request timestamp
            for window in self.windows.values():
                window.append(now)

            self.request_count += 1

    async def handle_response(self, status_code: int, headers: Optional[Dict] = None) -> None:
        """
        Handle API response and adjust rate limiting if needed

        Args:
            status_code: HTTP status code
            headers: Response headers
        """
        if status_code == 429:  # Rate limit exceeded
            self.error_count += 1
            self.backoff_factor = min(2.0, 1.0 + (self.error_count * 0.1))

            # Get retry-after header if available
            retry_after = 60  # Default wait time
            if headers and 'retry-after' in headers:
                retry_after = int(headers['retry-after'])

            logger.warning(f"Rate limit exceeded (429), backing off for {retry_after}s")
            await asyncio.sleep(retry_after)

        elif status_code < 400:  # Successful request
            if self.error_count > 0:
                self.error_count -= 1
                self.backoff_factor = max(1.0, self.backoff_factor - 0.05)

    def get_usage_stats(self) -> Dict[str, Dict]:
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
                'used': recent,
                'limit': int(limit / self.backoff_factor),
                'original_limit': limit,
                'percentage': (recent / limit) * 100 if limit > 0 else 0,
                'remaining': max(0, int(limit / self.backoff_factor) - recent)
            }

        stats['total_requests'] = self.request_count
        stats['error_count'] = self.error_count
        stats['backoff_factor'] = self.backoff_factor

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

        print("\n" + "="*50)
        print("Rate Limit Status Dashboard")
        print("="*50)

        for window_name in ['second', 'minute', 'half_hour']:
            if window_name in stats:
                s = stats[window_name]
                bar_length = 20
                filled = int(bar_length * s['percentage'] / 100)
                bar = '█' * filled + '░' * (bar_length - filled)

                window_display = window_name.replace('_', ' ').title()
                print(f"{window_display:10s}: [{bar}] {s['used']}/{s['limit']} "
                      f"({s['percentage']:.1f}%)")

        print("-"*50)
        print(f"Total Requests: {stats['total_requests']:,}")
        print(f"Errors: {stats['error_count']}")
        if stats['backoff_factor'] > 1.0:
            print(f"⚠️  Backoff Active: {stats['backoff_factor']:.2f}x")
        print("="*50 + "\n")

class PriorityRateLimiter(UpstoxRateLimiter):
    """
    Rate limiter with priority queue support
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.priority_queue = asyncio.PriorityQueue()
        self.processing = False
        self._counter = 0

    async def acquire_with_priority(self, priority: int = 5) -> None:
        """
        Acquire rate limit slot with priority

        Args:
            priority: Request priority (1=highest, 10=lowest)
        """
        event = asyncio.Event()
        # Add a unique counter to ensure tuples are always comparable
        counter = self._counter
        self._counter += 1
        await self.priority_queue.put((priority, time.time(), counter, event))

        if not self.processing:
            asyncio.create_task(self._process_queue())

        await event.wait()

    async def _process_queue(self) -> None:
        """Process priority queue"""
        self.processing = True

        while not self.priority_queue.empty():
            priority, timestamp, counter, event = await self.priority_queue.get()
            await self.acquire()
            event.set()

        self.processing = False