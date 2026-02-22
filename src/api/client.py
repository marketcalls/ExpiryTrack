"""
Upstox API Client using httpx for expired instruments and historical candle data
"""

import asyncio
import logging
from datetime import date, datetime, timedelta

import httpx

from ..auth.manager import AuthManager
from ..config import config
from ..utils.rate_limiter import PriorityRateLimiter

logger = logging.getLogger(__name__)

# V3 API interval limits for date chunking
_V3_MAX_RANGE = {
    # (unit, interval_range) -> max days per request
    "minutes_fine": 30,  # 1-15 minute intervals: max 1 month
    "minutes_coarse": 90,  # 16-300 minute intervals: max 1 quarter
    "hours": 90,  # 1-5 hours: max 1 quarter
    "days": 3650,  # daily: max 1 decade
    "weeks": 36500,  # weekly: unlimited (10 years practical)
    "months": 36500,  # monthly: unlimited
}


def _get_max_days(unit: str, interval: int) -> int:
    """Get max days per request for a given unit and interval"""
    if unit == "minutes":
        return _V3_MAX_RANGE["minutes_fine"] if interval <= 15 else _V3_MAX_RANGE["minutes_coarse"]
    return _V3_MAX_RANGE.get(unit, 90)


def _chunk_date_range(from_date: str, to_date: str, max_days: int) -> list[tuple]:
    """Split a date range into chunks respecting API limits"""
    start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.strptime(to_date, "%Y-%m-%d").date()
    chunks = []

    while start < end:
        chunk_end = min(start + timedelta(days=max_days - 1), end)
        chunks.append((start.isoformat(), chunk_end.isoformat()))
        start = chunk_end + timedelta(days=1)

    return chunks


class UpstoxAPIClient:
    """
    Async HTTP client for Upstox Expired Instruments API and V3 Historical Candles
    """

    def __init__(self, auth_manager: AuthManager | None = None):
        """
        Initialize API client

        Args:
            auth_manager: Authentication manager instance
        """
        self.auth_manager = auth_manager or AuthManager()
        self.base_url = config.UPSTOX_BASE_URL
        self.base_url_v3 = config.UPSTOX_BASE_URL_V3
        self.rate_limiter = PriorityRateLimiter(
            max_per_second=config.MAX_REQUESTS_SEC,
            max_per_minute=config.MAX_REQUESTS_MIN,
            max_per_30min=config.MAX_REQUESTS_30MIN,
        )

        # HTTP client configuration
        self.client_config = {
            "base_url": self.base_url,
            "timeout": httpx.Timeout(config.REQUEST_TIMEOUT),
            "limits": httpx.Limits(max_keepalive_connections=5, max_connections=10, keepalive_expiry=30),
            "http2": False,  # Disable HTTP/2 to avoid potential issues
        }

        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def connect(self) -> None:
        """Initialize HTTP client"""
        if not self._client:
            self._client = httpx.AsyncClient(**self.client_config)
            logger.info("HTTP client connected")

    async def close(self) -> None:
        """Close HTTP client"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("HTTP client closed")

    async def _make_request(
        self, method: str, endpoint: str, params: dict | None = None, data: dict | None = None, priority: int = 5
    ) -> httpx.Response:
        """
        Make rate-limited HTTP request

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            priority: Request priority (1=highest)

        Returns:
            HTTP response
        """
        if not self._client:
            await self.connect()

        # Ensure authentication
        if not self.auth_manager.is_token_valid():
            raise ValueError("Invalid or expired token. Please authenticate first.")

        # Get headers with auth token
        headers = self.auth_manager.get_headers()

        # Apply rate limiting
        await self.rate_limiter.acquire_with_priority(priority)

        try:
            response = await self._client.request(
                method=method, url=endpoint, params=params, json=data, headers=headers
            )

            # Handle rate limit response
            await self.rate_limiter.handle_response(response.status_code, dict(response.headers))

            return response

        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise

    async def get_expiries(self, instrument_key: str) -> list[str]:
        """
        Get all available expiry dates for an instrument
        Always fetches from API - no hardcoding

        Args:
            instrument_key: Instrument key (e.g., "NSE_INDEX|Nifty 50")

        Returns:
            List of expiry dates in YYYY-MM-DD format
        """
        endpoint = "/expired-instruments/expiries"
        params = {"instrument_key": instrument_key}

        logger.info(f"Fetching expiries for {instrument_key}")

        response = await self._make_request("GET", endpoint, params=params, priority=2)

        if response.status_code == 200:
            data = response.json()
            expiries = data.get("data", [])
            logger.info(f"Found {len(expiries)} expiry dates for {instrument_key}")
            return expiries
        else:
            logger.error(f"Failed to fetch expiries: {response.status_code} - {response.text}")
            return []

    async def get_option_contracts(self, instrument_key: str, expiry_date: str) -> list[dict]:
        """
        Get expired option contracts for given expiry

        Args:
            instrument_key: Instrument key
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            List of option contract details (CE and PE)
        """
        endpoint = "/expired-instruments/option/contract"
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}

        logger.info(f"Fetching option contracts for {instrument_key} expiry {expiry_date}")

        response = await self._make_request("GET", endpoint, params=params, priority=3)

        if response.status_code == 200:
            data = response.json()
            contracts = data.get("data", [])
            logger.info(f"Found {len(contracts)} option contracts")
            return contracts
        else:
            logger.error(f"Failed to fetch option contracts: {response.status_code}")
            return []

    async def get_future_contracts(self, instrument_key: str, expiry_date: str) -> list[dict]:
        """
        Get expired future contracts for given expiry

        Args:
            instrument_key: Instrument key
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            List of future contract details
        """
        endpoint = "/expired-instruments/future/contract"
        params = {"instrument_key": instrument_key, "expiry_date": expiry_date}

        logger.info(f"Fetching future contracts for {instrument_key} expiry {expiry_date}")

        response = await self._make_request("GET", endpoint, params=params, priority=3)

        if response.status_code == 200:
            data = response.json()
            contracts = data.get("data", [])
            logger.info(f"Found {len(contracts)} future contracts")
            return contracts
        else:
            logger.error(f"Failed to fetch future contracts: {response.status_code}")
            return []

    async def get_historical_data(
        self, expired_instrument_key: str, from_date: str, to_date: str, interval: str = "1minute"
    ) -> list[list]:
        """
        Get historical candle data for expired contract

        Args:
            expired_instrument_key: Expired instrument key (e.g., "NSE_FO|71706|28-08-2025")
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            interval: Candle interval (1minute, 3minute, 5minute, etc.)

        Returns:
            List of OHLCV candles
        """
        endpoint = f"/expired-instruments/historical-candle/{expired_instrument_key}/{interval}/{to_date}/{from_date}"

        logger.info(f"Fetching {interval} data for {expired_instrument_key} from {from_date} to {to_date}")
        logger.debug(f"Full endpoint URL: {endpoint}")

        response = await self._make_request("GET", endpoint, priority=5)

        if response.status_code == 200:
            data = response.json()
            candles = data.get("data", {}).get("candles", [])
            logger.info(f"Received {len(candles)} candles for {expired_instrument_key}")
            return candles
        else:
            logger.error(
                f"Failed to fetch historical data for {expired_instrument_key}: {response.status_code} - {response.text[:200]}"
            )
            return []

    async def get_all_contracts_for_expiry(self, instrument_key: str, expiry_date: str) -> dict[str, list[dict]]:
        """
        Get all contracts (options and futures) for a given expiry

        Args:
            instrument_key: Instrument key
            expiry_date: Expiry date

        Returns:
            Dictionary with 'options' and 'futures' lists
        """
        # Fetch options and futures in parallel
        tasks = [
            self.get_option_contracts(instrument_key, expiry_date),
            self.get_future_contracts(instrument_key, expiry_date),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        contracts = {
            "options": results[0] if not isinstance(results[0], Exception) else [],
            "futures": results[1] if not isinstance(results[1], Exception) else [],
        }

        total = len(contracts["options"]) + len(contracts["futures"])
        logger.info(f"Total contracts for {expiry_date}: {total}")

        return contracts

    async def test_connection(self) -> bool:
        """
        Test API connection and authentication

        Returns:
            True if connection successful
        """
        try:
            # Try to fetch expiries for a known instrument
            expiries = await self.get_expiries("NSE_INDEX|Nifty 50")
            if expiries:
                logger.info("API connection test successful")
                return True
            return False
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False

    def get_rate_limit_status(self) -> dict:
        """Get current rate limit status"""
        return self.rate_limiter.get_usage_stats()

    def print_rate_limit_dashboard(self) -> None:
        """Print rate limit dashboard"""
        self.rate_limiter.print_dashboard()

    async def check_rate_limits(self) -> dict[str, str]:
        """
        Check current rate limit usage

        Returns:
            Dictionary with usage info
        """
        stats = self.get_rate_limit_status()
        return {
            "per_second": f"{stats['second']['used']}/{stats['second']['limit']}",
            "per_minute": f"{stats['minute']['used']}/{stats['minute']['limit']}",
            "per_30min": f"{stats['half_hour']['used']}/{stats['half_hour']['limit']}",
        }

    # ── V3 Historical Candle Methods ─────────────────────────

    async def _make_request_v3(
        self, method: str, endpoint: str, params: dict | None = None, priority: int = 5
    ) -> httpx.Response:
        """Make rate-limited request to V3 API"""
        if not self._client:
            await self.connect()

        if not self.auth_manager.is_token_valid():
            raise ValueError("Invalid or expired token. Please authenticate first.")

        headers = self.auth_manager.get_headers()
        await self.rate_limiter.acquire_with_priority(priority)

        try:
            # V3 uses a different base URL
            url = f"{self.base_url_v3}{endpoint}"
            response = await self._client.request(method=method, url=url, params=params, headers=headers)

            await self.rate_limiter.handle_response(response.status_code, dict(response.headers))

            return response

        except httpx.TimeoutException as e:
            logger.error(f"V3 request timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"V3 request failed: {e}")
            raise

    async def get_historical_candles_v3(
        self,
        instrument_key: str,
        unit: str = "days",
        interval: int = 1,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> list[list]:
        """
        Get historical candle data via V3 API with auto date-chunking.

        Args:
            instrument_key: Instrument key (e.g., 'NSE_EQ|INE848E01016')
            unit: 'minutes', 'hours', 'days', 'weeks', 'months'
            interval: Interval value (1-300 for minutes, 1-5 for hours, 1 for days/weeks/months)
            from_date: Start date YYYY-MM-DD (optional)
            to_date: End date YYYY-MM-DD (defaults to today)

        Returns:
            List of candles [timestamp, open, high, low, close, volume, oi]
        """
        if to_date is None:
            to_date = date.today().isoformat()
        if from_date is None:
            # Default: 1 year for daily, 1 month for minutes
            days_back = 365 if unit in ("days", "weeks", "months") else 30
            from_date = (date.today() - timedelta(days=days_back)).isoformat()

        max_days = _get_max_days(unit, interval)
        chunks = _chunk_date_range(from_date, to_date, max_days)

        all_candles = []
        for chunk_from, chunk_to in chunks:
            endpoint = f"/historical-candle/{instrument_key}/{unit}/{interval}/{chunk_to}/{chunk_from}"

            logger.info(f"V3 candles: {instrument_key} {unit}/{interval} {chunk_from} to {chunk_to}")

            try:
                response = await self._make_request_v3("GET", endpoint, priority=5)

                if response.status_code == 200:
                    data = response.json()
                    candles = data.get("data", {}).get("candles", [])
                    all_candles.extend(candles)
                    logger.info(f"Received {len(candles)} candles for chunk {chunk_from}-{chunk_to}")
                else:
                    logger.error(
                        f"V3 candle fetch failed for {instrument_key}: {response.status_code} - {response.text[:200]}"
                    )
            except Exception as e:
                logger.error(f"V3 candle fetch error for {instrument_key} ({chunk_from}-{chunk_to}): {e}")

        logger.info(f"Total V3 candles for {instrument_key}: {len(all_candles)}")
        return all_candles

    async def get_intraday_candles_v3(
        self, instrument_key: str, unit: str = "minutes", interval: int = 1
    ) -> list[list]:
        """
        Get intraday candle data via V3 API (current day).

        Args:
            instrument_key: Instrument key
            unit: 'minutes', 'hours', or 'days'
            interval: Interval value

        Returns:
            List of candles
        """
        endpoint = f"/historical-candle/intraday/{instrument_key}/{unit}/{interval}"

        logger.info(f"V3 intraday: {instrument_key} {unit}/{interval}")

        response = await self._make_request_v3("GET", endpoint, priority=3)

        if response.status_code == 200:
            data = response.json()
            candles = data.get("data", {}).get("candles", [])
            logger.info(f"Received {len(candles)} intraday candles for {instrument_key}")
            return candles
        else:
            logger.error(f"V3 intraday fetch failed: {response.status_code} - {response.text[:200]}")
            return []

    def set_max_rate(self, max_per_second: int) -> None:
        """
        Adjust maximum request rate

        Args:
            max_per_second: New maximum requests per second
        """
        self.rate_limiter.limits["second"] = (max_per_second, 1.0)
        logger.info(f"Adjusted rate limit to {max_per_second} req/sec")
