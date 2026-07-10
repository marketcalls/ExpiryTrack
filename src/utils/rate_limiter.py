"""
Rate limiter implementation for Upstox API compliance
Limits (Standard APIs, per user): 50 req/sec, 500 req/min, 2000 req/30min
"""
import asyncio
import threading
import time
from collections import deque
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)

class UpstoxRateLimiter:
    """
    Rate limiter that enforces Upstox API limits with safety margins.

    Upstox applies rate limits per user, not per connection, so all limiter
    instances in this process share one set of sliding windows (class-level
    state). The state is guarded by a threading.Lock that is only held for
    bookkeeping - never across a sleep - which makes the limiter safe to use
    from multiple event loops and threads (Flask routes, the task manager
    loop, CLI scripts).

    A 429 from the server starts a shared cooldown that pauses every request
    in the process until it expires, instead of each coroutine sleeping on
    its own while others keep firing.
    """

    _state_lock = threading.Lock()
    _windows: Dict[str, deque] = {
        'second': deque(),
        'minute': deque(),
        'half_hour': deque(),
    }
    _cooldown_until: float = 0.0
    _consecutive_429: int = 0
    _counters = {
        'request_count': 0,
        'error_count': 0,
    }

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

    async def acquire(self, priority: int = 1) -> None:
        """
        Wait if necessary to respect rate limits and any active 429 cooldown.

        Args:
            priority: Request priority (kept for API compatibility)
        """
        cls = UpstoxRateLimiter
        reported = False

        while True:
            with cls._state_lock:
                now = time.monotonic()
                wait = cls._cooldown_until - now
                reason = "server cooldown after 429"

                if wait <= 0:
                    wait = 0.0
                    for window_name, (limit, duration) in self.limits.items():
                        window = cls._windows[window_name]
                        while window and now - window[0] > duration:
                            window.popleft()

                        if len(window) >= limit:
                            wait_needed = duration - (now - window[0]) + 0.05
                            if wait_needed > wait:
                                wait = wait_needed
                                reason = (f"{window_name} window full "
                                          f"({len(window)}/{limit})")

                    if wait <= 0:
                        for window in cls._windows.values():
                            window.append(now)
                        cls._counters['request_count'] += 1
                        return

            if not reported and wait > 5:
                logger.info(f"Rate limit: {reason}, waiting {wait:.0f}s")
                reported = True
            else:
                logger.debug(f"Rate limit: {reason}, waiting {wait:.2f}s")

            # Sleep outside the lock, in bounded chunks, then re-check.
            await asyncio.sleep(min(wait, 30.0))

    def register_rate_limit_hit(self, retry_after: Optional[float] = None) -> float:
        """
        Record a 429 response and start a process-wide cooldown.

        Args:
            retry_after: Server-provided Retry-After seconds, if any

        Returns:
            The cooldown duration in seconds
        """
        cls = UpstoxRateLimiter
        with cls._state_lock:
            cls._counters['error_count'] += 1
            cls._consecutive_429 += 1

            if retry_after is None or retry_after <= 0:
                # Exponential backoff: 15s, 30s, 60s... capped at 5 minutes
                retry_after = min(15.0 * (2 ** (cls._consecutive_429 - 1)), 300.0)

            until = time.monotonic() + retry_after
            if until > cls._cooldown_until:
                cls._cooldown_until = until

            return retry_after

    def register_success(self) -> None:
        """Record a successful response, ending any escalating backoff."""
        cls = UpstoxRateLimiter
        with cls._state_lock:
            cls._consecutive_429 = 0

    async def handle_response(self, status_code: int, headers: Optional[Dict] = None) -> None:
        """
        Handle API response and adjust rate limiting if needed.
        Non-blocking: a 429 starts the shared cooldown that acquire() honors.

        Args:
            status_code: HTTP status code
            headers: Response headers
        """
        if status_code == 429:
            retry_after = None
            if headers:
                for key, value in headers.items():
                    if key.lower() == 'retry-after':
                        try:
                            retry_after = float(value)
                        except (TypeError, ValueError):
                            pass
                        break
            wait = self.register_rate_limit_hit(retry_after)
            logger.warning(f"Rate limit exceeded (429), cooling down for {wait:.0f}s")
        elif status_code < 400:
            self.register_success()

    def get_usage_stats(self) -> Dict[str, Dict]:
        """
        Get current rate limit usage statistics

        Returns:
            Dictionary with usage stats for each time window
        """
        cls = UpstoxRateLimiter
        stats = {}

        with cls._state_lock:
            now = time.monotonic()
            for window_name, (limit, duration) in self.limits.items():
                window = cls._windows[window_name]
                recent = sum(1 for ts in window if now - ts <= duration)

                stats[window_name] = {
                    'used': recent,
                    'limit': limit,
                    'original_limit': limit,
                    'percentage': (recent / limit) * 100 if limit > 0 else 0,
                    'remaining': max(0, limit - recent)
                }

            stats['total_requests'] = cls._counters['request_count']
            stats['error_count'] = cls._counters['error_count']
            stats['cooldown_seconds'] = round(max(0.0, cls._cooldown_until - now), 1)

        return stats

    def reset(self) -> None:
        """Reset all rate limit windows and counters"""
        cls = UpstoxRateLimiter
        with cls._state_lock:
            for window in cls._windows.values():
                window.clear()
            cls._counters['request_count'] = 0
            cls._counters['error_count'] = 0
            cls._consecutive_429 = 0
            cls._cooldown_until = 0.0
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
                window_display = window_name.replace('_', ' ').title()
                print(f"{window_display:10s}: {s['used']}/{s['limit']} "
                      f"({s['percentage']:.1f}%)")

        print("-"*50)
        print(f"Total Requests: {stats['total_requests']:,}")
        print(f"Errors: {stats['error_count']}")
        if stats['cooldown_seconds'] > 0:
            print(f"Cooldown Active: {stats['cooldown_seconds']}s remaining")
        print("="*50 + "\n")

class PriorityRateLimiter(UpstoxRateLimiter):
    """
    Backward-compatible subclass.

    The previous priority-queue implementation could strand a waiter forever
    when the queue processor exited between a put() and the processing-flag
    check, hanging the collection. Requests now go straight to acquire(),
    which already serializes waiters fairly.
    """

    async def acquire_with_priority(self, priority: int = 5) -> None:
        """
        Acquire rate limit slot

        Args:
            priority: Request priority (kept for API compatibility)
        """
        await self.acquire(priority)
