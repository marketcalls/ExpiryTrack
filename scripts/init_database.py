#!/usr/bin/env python
"""
Initialize ExpiryTrack database
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.manager import DatabaseManager
from src.config import config

def init_database():
    """Initialize database with schema"""
    print("Initializing ExpiryTrack database...")
    print(f"   Database Type: {config.DB_TYPE}")
    print(f"   Database Path: {config.DB_PATH}")

    try:
        db_manager = DatabaseManager()
        print("Database initialized successfully!")

        # Show initial stats
        stats = db_manager.get_summary_stats()
        print("\nDatabase Status:")
        print(f"  Instruments: {stats['total_instruments']}")
        print(f"  Expiries: {stats['total_expiries']}")
        print(f"  Contracts: {stats['total_contracts']}")
        print(f"  Historical Data: {stats['total_candles']}")

    except Exception as e:
        print(f"Failed to initialize database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    init_database()