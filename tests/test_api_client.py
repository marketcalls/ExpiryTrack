"""Tests for UpstoxAPIClient — date chunking, rate limiter, API methods."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.client import _chunk_date_range, _get_max_days

# ── Date chunking utilities ──


def test_chunk_date_range_single_chunk():
    """Date range within max_days produces one chunk."""
    chunks = _chunk_date_range("2025-01-01", "2025-01-20", max_days=30)
    assert len(chunks) == 1
    assert chunks[0] == ("2025-01-01", "2025-01-20")


def test_chunk_date_range_multiple_chunks():
    """Date range exceeding max_days is split into multiple chunks."""
    chunks = _chunk_date_range("2025-01-01", "2025-03-01", max_days=30)
    assert len(chunks) >= 2
    # First chunk should be 30 days
    assert chunks[0][0] == "2025-01-01"
    # Last chunk should end at target
    assert chunks[-1][1] == "2025-03-01"


def test_chunk_date_range_exact_boundary():
    """Date range exactly equal to max_days produces one chunk."""
    chunks = _chunk_date_range("2025-01-01", "2025-01-30", max_days=30)
    assert len(chunks) == 1


def test_chunk_date_range_no_gap():
    """Consecutive chunks have no gaps (start of next = day after end of prev)."""
    chunks = _chunk_date_range("2025-01-01", "2025-04-01", max_days=30)
    for i in range(len(chunks) - 1):
        from datetime import datetime, timedelta

        end = datetime.strptime(chunks[i][1], "%Y-%m-%d").date()
        next_start = datetime.strptime(chunks[i + 1][0], "%Y-%m-%d").date()
        assert next_start == end + timedelta(days=1)


# ── Max days per unit ──


def test_get_max_days_minutes_fine():
    """1-15 minute intervals use minutes_fine limit (30 days)."""
    assert _get_max_days("minutes", 1) == 30
    assert _get_max_days("minutes", 15) == 30


def test_get_max_days_minutes_coarse():
    """16+ minute intervals use minutes_coarse limit (90 days)."""
    assert _get_max_days("minutes", 30) == 90
    assert _get_max_days("minutes", 300) == 90


def test_get_max_days_daily():
    """Daily interval allows up to 3650 days."""
    assert _get_max_days("days", 1) == 3650


def test_get_max_days_unknown_unit():
    """Unknown unit falls back to 90 days."""
    assert _get_max_days("unknown_unit", 1) == 90


# ── UpstoxAPIClient methods (mocked HTTP) ──


@pytest.fixture
def api_client(mock_auth_manager):
    """UpstoxAPIClient with mocked auth and HTTP client."""
    with patch("src.api.client.AuthManager", return_value=mock_auth_manager):
        from src.api.client import UpstoxAPIClient

        client = UpstoxAPIClient(auth_manager=mock_auth_manager)
    return client


@pytest.mark.asyncio
async def test_get_expiries_success(api_client):
    """Successful get_expiries returns list of dates."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": ["2025-01-30", "2025-02-27"]}

    api_client._client = AsyncMock()
    api_client._client.request = AsyncMock(return_value=mock_response)
    api_client.rate_limiter.acquire_with_priority = AsyncMock()
    api_client.rate_limiter.handle_response = AsyncMock()

    result = await api_client.get_expiries("NSE_INDEX|Nifty 50")
    assert result == ["2025-01-30", "2025-02-27"]


@pytest.mark.asyncio
async def test_get_expiries_failure(api_client):
    """Failed get_expiries returns empty list."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Server Error"

    api_client._client = AsyncMock()
    api_client._client.request = AsyncMock(return_value=mock_response)
    api_client.rate_limiter.acquire_with_priority = AsyncMock()
    api_client.rate_limiter.handle_response = AsyncMock()

    result = await api_client.get_expiries("NSE_INDEX|Nifty 50")
    assert result == []


@pytest.mark.asyncio
async def test_get_option_contracts_success(api_client):
    """Successful get_option_contracts returns contract list."""
    contracts = [
        {"instrument_key": "NSE_FO|123", "trading_symbol": "NIFTY25JAN50CE"},
    ]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": contracts}

    api_client._client = AsyncMock()
    api_client._client.request = AsyncMock(return_value=mock_response)
    api_client.rate_limiter.acquire_with_priority = AsyncMock()
    api_client.rate_limiter.handle_response = AsyncMock()

    result = await api_client.get_option_contracts("NSE_INDEX|Nifty 50", "2025-01-30")
    assert len(result) == 1
    assert result[0]["trading_symbol"] == "NIFTY25JAN50CE"


@pytest.mark.asyncio
async def test_get_historical_data_success(api_client):
    """Successful get_historical_data returns candle list."""
    candles = [
        ["2025-01-15T09:15:00", 22000.0, 22050.0, 21990.0, 22030.0, 1000, 0],
    ]
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"candles": candles}}

    api_client._client = AsyncMock()
    api_client._client.request = AsyncMock(return_value=mock_response)
    api_client.rate_limiter.acquire_with_priority = AsyncMock()
    api_client.rate_limiter.handle_response = AsyncMock()

    result = await api_client.get_historical_data(
        "NSE_FO|123|30-01-2025", "2025-01-01", "2025-01-30"
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_make_request_unauthenticated_raises(api_client):
    """_make_request raises ValueError when token is invalid."""
    api_client.auth_manager.is_token_valid.return_value = False
    api_client._client = AsyncMock()

    with pytest.raises(ValueError, match="Invalid or expired token"):
        await api_client._make_request("GET", "/test")


@pytest.mark.asyncio
async def test_get_rate_limit_status(api_client):
    """get_rate_limit_status returns dict with usage info."""
    status = api_client.get_rate_limit_status()
    assert "second" in status
    assert "minute" in status
    assert "half_hour" in status


@pytest.mark.asyncio
async def test_check_rate_limits(api_client):
    """check_rate_limits returns formatted usage strings."""
    result = await api_client.check_rate_limits()
    assert "per_second" in result
    assert "/" in result["per_second"]
