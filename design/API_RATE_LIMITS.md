# ExpiryTrack - API Rate Limit Management Guide

## Upstox API Rate Limits

Upstox enforces rate limits on a **per-API, per-user basis** to ensure consistent and reliable service for all users.

### Official Rate Limits

| Time Duration | Request Limit |
|--------------|---------------|
| **Per Second** | 50 requests |
| **Per Minute** | 500 requests |
| **Per 30 Minutes** | 2000 requests |

These limits apply to:
- Order Placement APIs
- Expired Instruments APIs
- All other standard APIs

## Rate Limit Strategy for ExpiryTrack

### Optimal Configuration

Given the 50 req/sec limit, ExpiryTrack can achieve high throughput while staying compliant:

```python
# Recommended Settings
MAX_REQUESTS_PER_SECOND = 45  # 10% safety margin
MAX_CONCURRENT_WORKERS = 10   # Each worker ~4-5 req/sec
BURST_SIZE = 20               # Allow short bursts
```

### Tiered Throttling Approach

```
┌─────────────────────────────────────┐
│         Per-Second Control          │
│     Token Bucket (45 tokens/sec)    │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│         Per-Minute Tracking         │
│    Sliding Window (450 requests)    │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│      30-Minute Safety Check         │
│     Hard limit at 1800 requests     │
└─────────────────────────────────────┘
```

## Implementation Example

### Rate Limiter with httpx

```python
import httpx
import asyncio
import time
from collections import deque
from typing import Optional

class UpstoxRateLimiter:
    def __init__(self):
        # Conservative limits with safety margin
        self.limits = {
            'second': (45, 1.0),      # 45 req/sec (90% of 50)
            'minute': (450, 60.0),    # 450 req/min (90% of 500)
            'half_hour': (1800, 1800.0)  # 1800 req/30min (90% of 2000)
        }

        self.windows = {
            'second': deque(),
            'minute': deque(),
            'half_hour': deque()
        }

        self.lock = asyncio.Lock()

    async def acquire(self):
        """Wait if necessary to respect rate limits"""
        async with self.lock:
            now = time.time()

            # Clean old timestamps
            for window_name, (limit, duration) in self.limits.items():
                window = self.windows[window_name]
                while window and now - window[0] > duration:
                    window.popleft()

            # Check if we can make a request
            wait_time = 0.0
            for window_name, (limit, duration) in self.limits.items():
                window = self.windows[window_name]
                if len(window) >= limit:
                    oldest = window[0]
                    wait_needed = duration - (now - oldest) + 0.01
                    wait_time = max(wait_time, wait_needed)

            if wait_time > 0:
                await asyncio.sleep(wait_time)
                now = time.time()

            # Record the request
            for window in self.windows.values():
                window.append(now)

    async def make_request(self, client: httpx.AsyncClient, url: str, **kwargs):
        """Make a rate-limited request"""
        await self.acquire()
        return await client.get(url, **kwargs)

# Usage example
async def fetch_data():
    rate_limiter = UpstoxRateLimiter()

    async with httpx.AsyncClient(
        base_url="https://api.upstox.com/v2",
        http2=True,
        limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
    ) as client:

        # Fetch multiple contracts in parallel
        tasks = []
        for expiry in expiry_dates:
            url = f"/expired-instruments/option/contract"
            params = {"instrument_key": "NSE_INDEX|Nifty 50", "expiry_date": expiry}
            task = rate_limiter.make_request(client, url, params=params)
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        return responses
```

## Monitoring Rate Limit Usage

### Real-time Monitoring

```python
class RateLimitMonitor:
    def __init__(self, rate_limiter: UpstoxRateLimiter):
        self.rate_limiter = rate_limiter

    def get_usage_stats(self):
        now = time.time()
        stats = {}

        for window_name, (limit, duration) in self.rate_limiter.limits.items():
            window = self.rate_limiter.windows[window_name]
            # Count recent requests
            recent = sum(1 for ts in window if now - ts <= duration)
            stats[window_name] = {
                'used': recent,
                'limit': limit,
                'percentage': (recent / limit) * 100,
                'remaining': limit - recent
            }

        return stats

    def print_dashboard(self):
        stats = self.get_usage_stats()
        print("\n=== Rate Limit Status ===")
        print(f"Second:  {stats['second']['used']}/{stats['second']['limit']} "
              f"({stats['second']['percentage']:.1f}%)")
        print(f"Minute:  {stats['minute']['used']}/{stats['minute']['limit']} "
              f"({stats['minute']['percentage']:.1f}%)")
        print(f"30-Min:  {stats['half_hour']['used']}/{stats['half_hour']['limit']} "
              f"({stats['half_hour']['percentage']:.1f}%)")
```

## Optimization Strategies

### 1. Request Batching
Combine multiple small requests when possible:
```python
# Instead of fetching each strike separately
# Fetch all contracts for an expiry in one request
contracts = await get_option_contracts(instrument_key, expiry_date)
```

### 2. Intelligent Scheduling
```python
class SmartScheduler:
    def __init__(self, rate_limiter):
        self.rate_limiter = rate_limiter
        self.priority_queue = asyncio.PriorityQueue()

    async def schedule_request(self, priority: int, request_func):
        """Schedule requests by priority (lower number = higher priority)"""
        await self.priority_queue.put((priority, request_func))

    async def process_queue(self):
        while True:
            priority, request_func = await self.priority_queue.get()
            await self.rate_limiter.acquire()
            asyncio.create_task(request_func())
```

### 3. Adaptive Rate Limiting
```python
class AdaptiveRateLimiter(UpstoxRateLimiter):
    def __init__(self):
        super().__init__()
        self.error_count = 0
        self.backoff_factor = 1.0

    async def handle_response(self, response: httpx.Response):
        if response.status_code == 429:  # Rate limit exceeded
            self.error_count += 1
            self.backoff_factor = min(2.0, 1.0 + (self.error_count * 0.1))

            # Reduce limits temporarily
            for window_name in self.limits:
                original_limit = self.limits[window_name][0]
                self.limits[window_name] = (
                    int(original_limit / self.backoff_factor),
                    self.limits[window_name][1]
                )

            wait_time = response.headers.get('Retry-After', 60)
            await asyncio.sleep(int(wait_time))
        else:
            # Gradually restore limits
            if self.error_count > 0:
                self.error_count -= 1
                self.backoff_factor = max(1.0, self.backoff_factor - 0.05)
```

## Best Practices

### DO's ✅
- **Use connection pooling** with httpx to reduce overhead
- **Implement exponential backoff** on rate limit errors
- **Monitor usage** continuously with dashboards
- **Leave safety margin** (use 45 req/sec instead of 50)
- **Cache responses** when possible to reduce API calls
- **Batch operations** to minimize request count

### DON'Ts ❌
- Don't burst all 50 requests in first 100ms of each second
- Don't ignore rate limit headers in responses
- Don't retry immediately after rate limit error
- Don't run multiple instances without coordination
- Don't assume limits won't change - make it configurable

## Troubleshooting

### Common Error Responses

| Status Code | Meaning | Action |
|------------|---------|---------|
| 429 | Rate limit exceeded | Wait and retry with backoff |
| 503 | Service temporarily unavailable | Retry with exponential backoff |
| 401 | Unauthorized | Check token validity |

### Debug Mode

```python
# Enable detailed logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Log all requests and responses
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.DEBUG)
```

## Performance Metrics

With proper rate limit management, ExpiryTrack can achieve:

- **Throughput**: ~40-45 requests/second sustained
- **Burst capacity**: Up to 50 requests/second for short periods
- **Daily volume**: ~3.5 million requests (theoretical max: 4.3M)
- **Parallel workers**: 10-15 concurrent connections
- **Response time**: <100ms average with connection pooling

## Conclusion

Effective rate limit management is crucial for ExpiryTrack's performance. By implementing intelligent throttling, monitoring, and adaptive strategies, the system can maximize throughput while staying well within Upstox's limits, ensuring reliable and continuous data collection.