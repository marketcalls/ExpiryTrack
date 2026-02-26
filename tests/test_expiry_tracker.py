"""Tests for ExpiryTracker — orchestration of data collection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.collectors.expiry_tracker import ExpiryTracker


@pytest.fixture
def tracker(tmp_db, mock_auth_manager):
    """ExpiryTracker with isolated DB and mocked auth."""
    mock_auth_manager.is_token_valid.return_value = True
    t = ExpiryTracker(auth_manager=mock_auth_manager, db_manager=tmp_db)
    t.api_client = MagicMock()
    t.api_client.connect = AsyncMock()
    t.api_client.close = AsyncMock()
    return t


# ── get_expiries ──


@pytest.mark.asyncio
async def test_get_expiries_stores_data(tracker, tmp_db):
    """get_expiries fetches from API and stores instrument + expiries in DB."""
    tracker.api_client.get_expiries = AsyncMock(
        return_value=["2025-01-30", "2025-02-27"]
    )

    # Insert instrument first (needed for foreign key)
    result = await tracker.get_expiries("NSE_INDEX|Nifty 50")

    assert result == ["2025-01-30", "2025-02-27"]
    assert tracker.stats["expiries_fetched"] == 2


@pytest.mark.asyncio
async def test_get_expiries_empty_response(tracker):
    """get_expiries with empty API response returns empty list."""
    tracker.api_client.get_expiries = AsyncMock(return_value=[])
    result = await tracker.get_expiries("NSE_INDEX|Nifty 50")
    assert result == []
    assert tracker.stats["expiries_fetched"] == 0


@pytest.mark.asyncio
async def test_get_expiries_unauthenticated_raises(tracker):
    """get_expiries raises when not authenticated."""
    tracker.auth_manager.is_token_valid.return_value = False
    with pytest.raises(ValueError, match="Not authenticated"):
        await tracker.get_expiries("NSE_INDEX|Nifty 50")


# ── get_contracts ──


@pytest.mark.asyncio
async def test_get_contracts_stores_data(tracker, tmp_db):
    """get_contracts fetches and stores contracts in DB."""
    contracts = {
        "options": [
            {
                "instrument_key": "NSE_FO|71706|28-08-2025",
                "trading_symbol": "NIFTY25JAN50CE",
                "contract_type": "CE",
                "strike_price": 22000,
                "expiry": "2025-01-30",
                "lot_size": 50,
                "tick_size": 0.05,
            }
        ],
        "futures": [],
    }
    tracker.api_client.get_all_contracts_for_expiry = AsyncMock(
        return_value=contracts
    )

    # Need instrument in DB first
    tmp_db.insert_instrument(
        {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "symbol": "Nifty 50",
            "segment": "NSE_INDEX",
        }
    )
    tmp_db.insert_expiries("NSE_INDEX|Nifty 50", ["2025-01-30"])

    result = await tracker.get_contracts("NSE_INDEX|Nifty 50", "2025-01-30")
    assert len(result["options"]) == 1
    assert tracker.stats["contracts_fetched"] == 1


# ── collect_historical_data ──


@pytest.mark.asyncio
async def test_collect_historical_data(tracker, tmp_db):
    """collect_historical_data fetches candles and stores them."""
    candles = [
        ["2025-01-15T09:15:00", 22000.0, 22050.0, 21990.0, 22030.0, 1000, 0],
        ["2025-01-15T09:16:00", 22030.0, 22060.0, 22020.0, 22040.0, 800, 0],
    ]
    tracker.api_client.get_historical_data = AsyncMock(return_value=candles)

    # Insert instrument, expiry, contract for FK
    tmp_db.insert_instrument(
        {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "symbol": "Nifty 50",
            "segment": "NSE_INDEX",
        }
    )
    tmp_db.insert_expiries("NSE_INDEX|Nifty 50", ["2025-01-30"])
    expired_key = "NSE_FO|123|30-01-2025"
    tmp_db.insert_contracts(
        [
            {
                "instrument_key": expired_key,
                "trading_symbol": "NIFTY25JAN50CE",
                "instrument_type": "CE",
                "strike_price": 22000,
                "expiry": "2025-01-30",
                "lot_size": 50,
                "tick_size": 0.05,
                "underlying_key": "NSE_INDEX|Nifty 50",
            }
        ]
    )

    contracts = [{"instrument_key": expired_key, "trading_symbol": "NIFTY25JAN50CE"}]
    total = await tracker.collect_historical_data(
        contracts, "2025-01-01", "2025-01-30"
    )
    assert total == 2
    assert tracker.stats["candles_fetched"] == 2


# ── get_database_stats ──


def test_get_database_stats(tracker, tmp_db):
    """get_database_stats returns summary from DB."""
    stats = tracker.get_database_stats()
    assert "total_instruments" in stats
    assert "total_contracts" in stats
    assert "total_candles" in stats


# ── test_connection ──


@pytest.mark.asyncio
async def test_test_connection_success(tracker):
    """test_connection returns True when API is reachable."""
    tracker.api_client.test_connection = AsyncMock(return_value=True)
    result = await tracker.test_connection()
    assert result is True
