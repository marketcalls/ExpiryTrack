"""
Main ExpiryTracker class that orchestrates data collection
"""
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, date
import logging
from tqdm.asyncio import tqdm

from ..api.client import UpstoxAPIClient
from ..auth.manager import AuthManager
from ..database.manager import DatabaseManager
from ..config import config

logger = logging.getLogger(__name__)

class ExpiryTracker:
    """
    Main orchestrator for expired contract data collection
    """

    def __init__(self,
                 auth_manager: Optional[AuthManager] = None,
                 db_manager: Optional[DatabaseManager] = None):
        """
        Initialize ExpiryTracker

        Args:
            auth_manager: Authentication manager
            db_manager: Database manager
        """
        self.auth_manager = auth_manager or AuthManager()
        self.db_manager = db_manager or DatabaseManager()
        self.api_client = UpstoxAPIClient(self.auth_manager)

        self.stats = {
            'expiries_fetched': 0,
            'contracts_fetched': 0,
            'candles_fetched': 0,
            'errors': 0
        }

    async def __aenter__(self):
        """Async context manager entry"""
        await self.api_client.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.api_client.close()

    def authenticate(self) -> bool:
        """
        Authenticate with Upstox

        Returns:
            True if authentication successful
        """
        return self.auth_manager.authenticate()

    async def get_expiries(self, instrument_key: str) -> List[str]:
        """
        Fetch and store expiry dates for an instrument

        Args:
            instrument_key: Instrument identifier

        Returns:
            List of expiry dates
        """
        # Ensure authenticated
        if not self.auth_manager.is_token_valid():
            raise ValueError("Not authenticated. Please call authenticate() first.")

        # Fetch expiries from API (never hardcoded)
        expiries = await self.api_client.get_expiries(instrument_key)

        if expiries:
            # Parse instrument key
            parts = instrument_key.split('|')
            instrument_data = {
                'instrument_key': instrument_key,
                'symbol': parts[1] if len(parts) > 1 else instrument_key,
                'segment': parts[0] if len(parts) > 0 else 'UNKNOWN'
            }

            # Store instrument
            self.db_manager.insert_instrument(instrument_data)

            # Store expiries
            self.db_manager.insert_expiries(instrument_key, expiries)
            self.stats['expiries_fetched'] += len(expiries)

        return expiries

    async def get_contracts(self,
                           instrument: str,
                           expiry_date: str) -> Dict[str, List[Dict]]:
        """
        Fetch and store contracts for a specific expiry

        Args:
            instrument: Instrument key
            expiry_date: Expiry date in YYYY-MM-DD format

        Returns:
            Dictionary with options and futures contracts
        """
        # Fetch all contracts
        contracts = await self.api_client.get_all_contracts_for_expiry(
            instrument, expiry_date
        )

        # Store in database
        all_contracts = contracts['options'] + contracts['futures']
        if all_contracts:
            self.db_manager.insert_contracts(all_contracts)
            self.stats['contracts_fetched'] += len(all_contracts)
            # Mark this expiry's contracts as fetched
            self.db_manager.mark_expiry_contracts_fetched(instrument, expiry_date)

        return contracts

    async def collect_historical_data(self,
                                     contracts: List[Dict],
                                     from_date: str,
                                     to_date: str,
                                     interval: str = '1minute') -> int:
        """
        Collect historical data for contracts

        Args:
            contracts: List of contract dictionaries
            from_date: Start date
            to_date: End date
            interval: Data interval

        Returns:
            Number of candles collected
        """
        total_candles = 0

        # Create progress bar
        pbar = tqdm(contracts, desc="Fetching historical data", unit="contract")

        for contract in pbar:
            try:
                expired_key = contract.get('instrument_key', '')
                pbar.set_description(f"Processing {contract.get('trading_symbol', expired_key)}")

                # Fetch historical data
                candles = await self.api_client.get_historical_data(
                    expired_key,
                    from_date,
                    to_date,
                    interval
                )

                if candles:
                    # Store in database
                    count = self.db_manager.insert_historical_data(expired_key, candles)
                    total_candles += count
                    self.stats['candles_fetched'] += count

                    pbar.set_postfix({'candles': total_candles})

            except Exception as e:
                logger.error(f"Failed to fetch data for {expired_key}: {e}")
                self.stats['errors'] += 1

        pbar.close()
        return total_candles

    async def auto_collect(self,
                          instrument: str,
                          months_back: int = 6,
                          interval: str = '1minute') -> Dict:
        """
        Automatically collect all data for an instrument

        Args:
            instrument: Instrument key
            months_back: How many months of history to collect
            interval: Data interval

        Returns:
            Collection statistics
        """
        logger.info(f"Starting auto-collection for {instrument}")

        # Step 1: Get all expiries
        logger.info("Step 1: Fetching expiries...")
        expiries = await self.get_expiries(instrument)
        logger.info(f"Found {len(expiries)} expiry dates")

        # Filter expiries based on months_back
        cutoff_date = (datetime.now() - timedelta(days=months_back * 30)).date()
        filtered_expiries = [
            exp for exp in expiries
            if datetime.strptime(exp, '%Y-%m-%d').date() >= cutoff_date
        ]
        logger.info(f"Processing {len(filtered_expiries)} expiries within {months_back} months")

        # Step 2: Fetch contracts for each expiry
        logger.info("Step 2: Fetching contracts...")
        all_contracts = []

        for expiry_date in tqdm(filtered_expiries, desc="Fetching contracts"):
            try:
                contracts = await self.get_contracts(instrument, expiry_date)
                all_contracts.extend(contracts['options'])
                all_contracts.extend(contracts['futures'])
            except Exception as e:
                logger.error(f"Failed to fetch contracts for {expiry_date}: {e}")
                self.stats['errors'] += 1

        logger.info(f"Total contracts to process: {len(all_contracts)}")

        # Step 3: Collect historical data
        if all_contracts:
            logger.info("Step 3: Fetching historical data...")

            # Batch contracts for efficient processing
            batch_size = 50
            for i in range(0, len(all_contracts), batch_size):
                batch = all_contracts[i:i + batch_size]

                # Calculate date range for each contract
                for contract in batch:
                    expiry_date = contract.get('expiry', '')
                    if expiry_date:
                        # Fetch data from 3 months before expiry to expiry date
                        end_date = expiry_date
                        start_date = (
                            datetime.strptime(expiry_date, '%Y-%m-%d') -
                            timedelta(days=90)
                        ).strftime('%Y-%m-%d')

                        await self.collect_historical_data(
                            [contract],
                            start_date,
                            end_date,
                            interval
                        )

                # Show rate limit status
                self.api_client.print_rate_limit_dashboard()

        # Print summary
        logger.info("=" * 50)
        logger.info("Collection Summary")
        logger.info("=" * 50)
        logger.info(f"Expiries fetched: {self.stats['expiries_fetched']}")
        logger.info(f"Contracts fetched: {self.stats['contracts_fetched']}")
        logger.info(f"Candles fetched: {self.stats['candles_fetched']:,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 50)

        return self.stats

    async def resume_collection(self, checkpoint_file: Optional[str] = None) -> Dict:
        """
        Resume data collection from checkpoint

        Args:
            checkpoint_file: Path to checkpoint file

        Returns:
            Collection statistics
        """
        # Get pending contracts from database
        pending_contracts = self.db_manager.get_pending_contracts(limit=1000)
        logger.info(f"Resuming collection for {len(pending_contracts)} pending contracts")

        if pending_contracts:
            # Group by expiry for efficient date range calculation
            contracts_by_expiry = {}
            for contract in pending_contracts:
                expiry = contract.get('expiry_date', '')
                if expiry not in contracts_by_expiry:
                    contracts_by_expiry[expiry] = []
                contracts_by_expiry[expiry].append(contract)

            # Process each expiry group
            for expiry_date, contracts in contracts_by_expiry.items():
                if expiry_date:
                    # Calculate date range
                    end_date = expiry_date
                    start_date = (
                        datetime.strptime(expiry_date, '%Y-%m-%d') -
                        timedelta(days=90)
                    ).strftime('%Y-%m-%d')

                    await self.collect_historical_data(
                        contracts,
                        start_date,
                        end_date
                    )

        return self.stats

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        return self.db_manager.get_summary_stats()

    def print_summary(self) -> None:
        """Print collection summary"""
        db_stats = self.get_database_stats()

        print("\n" + "="*60)
        print("ExpiryTrack Database Summary")
        print("="*60)
        print(f"Instruments: {db_stats['total_instruments']}")
        print(f"Expiries: {db_stats['total_expiries']}")
        print(f"Contracts: {db_stats['total_contracts']:,}")
        print(f"Historical Candles: {db_stats['total_candles']:,}")
        print("-"*60)
        print(f"Pending Expiries: {db_stats['pending_expiries']}")
        print(f"Pending Contracts: {db_stats['pending_contracts']}")
        print("="*60)

    async def test_connection(self) -> bool:
        """Test API connection"""
        return await self.api_client.test_connection()