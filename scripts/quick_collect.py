#!/usr/bin/env python
"""
Quick collection script for latest expiry
"""
import sys
import asyncio
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.collectors.expiry_tracker import ExpiryTracker
from src.config import config

async def quick_collect():
    """Collect data for the latest expiry"""
    tracker = ExpiryTracker()

    print("🚀 Quick Collection - Latest Expiry")
    print("="*40)

    # Authenticate
    if not tracker.authenticate():
        print("❌ Authentication failed!")
        return

    async with tracker:
        # Get expiries
        instrument = config.DEFAULT_INSTRUMENT
        print(f"Instrument: {instrument}")

        expiries = await tracker.get_expiries(instrument)

        if not expiries:
            print("No expiries found!")
            return

        # Filter future expiries only
        today = datetime.now().date()
        future_expiries = [
            exp for exp in expiries
            if datetime.strptime(exp, '%Y-%m-%d').date() >= today
        ]

        if not future_expiries:
            # Use most recent past expiry
            latest_expiry = expiries[-1]
            print(f"No future expiries, using most recent: {latest_expiry}")
        else:
            # Use nearest future expiry
            latest_expiry = future_expiries[0]
            print(f"Next expiry: {latest_expiry}")

        # Fetch contracts
        print("\nFetching contracts...")
        contracts = await tracker.get_contracts(instrument, latest_expiry)

        options = contracts.get('options', [])
        futures = contracts.get('futures', [])
        total = len(options) + len(futures)

        print(f"  Options: {len(options)}")
        print(f"  Futures: {len(futures)}")
        print(f"  Total: {total}")

        if total > 0:
            # Collect first 10 contracts as sample
            sample_contracts = (options[:5] + futures[:2])[:10]

            print(f"\nCollecting data for {len(sample_contracts)} sample contracts...")

            # Calculate date range (1 month before expiry)
            end_date = latest_expiry
            start_date = (
                datetime.strptime(latest_expiry, '%Y-%m-%d').
                replace(day=1)
            ).strftime('%Y-%m-%d')

            candles = await tracker.collect_historical_data(
                sample_contracts,
                start_date,
                end_date,
                '1minute'
            )

            print(f"\n✅ Collected {candles:,} candles!")

        # Show summary
        tracker.print_summary()

if __name__ == "__main__":
    asyncio.run(quick_collect())