"""
Upstox API Client using httpx for expired instruments data
"""
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

import httpx

from ..auth.manager import AuthManager
from ..utils.rate_limiter import PriorityRateLimiter
from ..config import config

logger = logging.getLogger(__name__)

class UpstoxAPIClient:
    """
    Async HTTP client for Upstox Expired Instruments API
    """

    def __init__(self, auth_manager: Optional[AuthManager] = None):
        """
        Initialize API client

        Args:
            auth_manager: Authentication manager instance
        """
        self.auth_manager = auth_manager or AuthManager()
        self.base_url = config.UPSTOX_BASE_URL
        self.rate_limiter = PriorityRateLimiter(
            max_per_second=config.MAX_REQUESTS_SEC,
            max_per_minute=config.MAX_REQUESTS_MIN,
            max_per_30min=config.MAX_REQUESTS_30MIN
        )

        # HTTP client configuration
        self.client_config = {
            'base_url': self.base_url,
            'timeout': httpx.Timeout(config.REQUEST_TIMEOUT),
            'limits': httpx.Limits(
                max_keepalive_connections=20,
                max_connections=50,
                keepalive_expiry=30
            ),
            'http2': True  # Enable HTTP/2
        }

        self._client: Optional[httpx.AsyncClient] = None

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

    async def _make_request(self,
                          method: str,
                          endpoint: str,
                          params: Optional[Dict] = None,
                          data: Optional[Dict] = None,
                          priority: int = 5) -> httpx.Response:
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
                method=method,
                url=endpoint,
                params=params,
                json=data,
                headers=headers
            )

            # Handle rate limit response
            await self.rate_limiter.handle_response(
                response.status_code,
                dict(response.headers)
            )

            return response

        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise

    async def get_expiries(self, instrument_key: str) -> List[str]:
        """
        Get all available expiry dates for an instrument
        Always fetches from API - no hardcoding

        Args:
            instrument_key: Instrument key (e.g., "NSE_INDEX|Nifty 50")

        Returns:
            List of expiry dates in YYYY-MM-DD format
        """
        endpoint = "/expired-instruments/expiries"
        params = {'instrument_key': instrument_key}

        logger.info(f"Fetching expiries for {instrument_key}")

        response = await self._make_request('GET', endpoint, params=params, priority=2)

        if response.status_code == 200:
            data = response.json()
            expiries = data.get('data', [])
            logger.info(f"Found {len(expiries)} expiry dates for {instrument_key}")
            return expiries
        else:
            logger.error(f"Failed to fetch expiries: {response.status_code} - {response.text}")
            return []

    async def get_option_contracts(self,
                                  instrument_key: str,
                                  expiry_date: str) -> List[Dict]:
        """
        Get expired option contracts for given expiry

        Args:
            instrument_key: Instrument key
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            List of option contract details (CE and PE)
        """
        endpoint = "/expired-instruments/option/contract"
        params = {
            'instrument_key': instrument_key,
            'expiry_date': expiry_date
        }

        logger.info(f"Fetching option contracts for {instrument_key} expiry {expiry_date}")

        response = await self._make_request('GET', endpoint, params=params, priority=3)

        if response.status_code == 200:
            data = response.json()
            contracts = data.get('data', [])
            logger.info(f"Found {len(contracts)} option contracts")
            return contracts
        else:
            logger.error(f"Failed to fetch option contracts: {response.status_code}")
            return []

    async def get_future_contracts(self,
                                  instrument_key: str,
                                  expiry_date: str) -> List[Dict]:
        """
        Get expired future contracts for given expiry

        Args:
            instrument_key: Instrument key
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            List of future contract details
        """
        endpoint = "/expired-instruments/future/contract"
        params = {
            'instrument_key': instrument_key,
            'expiry_date': expiry_date
        }

        logger.info(f"Fetching future contracts for {instrument_key} expiry {expiry_date}")

        response = await self._make_request('GET', endpoint, params=params, priority=3)

        if response.status_code == 200:
            data = response.json()
            contracts = data.get('data', [])
            logger.info(f"Found {len(contracts)} future contracts")
            return contracts
        else:
            logger.error(f"Failed to fetch future contracts: {response.status_code}")
            return []

    async def get_historical_data(self,
                                 expired_instrument_key: str,
                                 from_date: str,
                                 to_date: str,
                                 interval: str = '1minute') -> List[List]:
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

        response = await self._make_request('GET', endpoint, priority=5)

        if response.status_code == 200:
            data = response.json()
            candles = data.get('data', {}).get('candles', [])
            logger.info(f"Received {len(candles)} candles for {expired_instrument_key}")
            return candles
        else:
            logger.error(f"Failed to fetch historical data for {expired_instrument_key}: {response.status_code} - {response.text[:200]}")
            return []

    async def get_all_contracts_for_expiry(self,
                                          instrument_key: str,
                                          expiry_date: str) -> Dict[str, List[Dict]]:
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
            self.get_future_contracts(instrument_key, expiry_date)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        contracts = {
            'options': results[0] if not isinstance(results[0], Exception) else [],
            'futures': results[1] if not isinstance(results[1], Exception) else []
        }

        total = len(contracts['options']) + len(contracts['futures'])
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

    def get_rate_limit_status(self) -> Dict:
        """Get current rate limit status"""
        return self.rate_limiter.get_usage_stats()

    def print_rate_limit_dashboard(self) -> None:
        """Print rate limit dashboard"""
        self.rate_limiter.print_dashboard()

    async def check_rate_limits(self) -> Dict[str, str]:
        """
        Check current rate limit usage

        Returns:
            Dictionary with usage info
        """
        stats = self.get_rate_limit_status()
        return {
            'per_second': f"{stats['second']['used']}/{stats['second']['limit']}",
            'per_minute': f"{stats['minute']['used']}/{stats['minute']['limit']}",
            'per_30min': f"{stats['half_hour']['used']}/{stats['half_hour']['limit']}"
        }

    def set_max_rate(self, max_per_second: int) -> None:
        """
        Adjust maximum request rate

        Args:
            max_per_second: New maximum requests per second
        """
        self.rate_limiter.limits['second'] = (max_per_second, 1.0)
        logger.info(f"Adjusted rate limit to {max_per_second} req/sec")