"""
Script to update existing OpenAlgo symbols in the database
Fixes the incorrectly formatted symbols to use the proper format
"""

import sqlite3
import json
from src.utils.openalgo_symbol import to_openalgo_symbol

def update_existing_symbols():
    """Update all existing OpenAlgo symbols in the database"""
    print("\n" + "="*70)
    print("Updating OpenAlgo Symbols in Database")
    print("="*70)

    # Connect to database
    conn = sqlite3.connect('data/expirytrack.db')
    cursor = conn.cursor()

    try:
        # Get all contracts with their metadata
        cursor.execute("SELECT expired_instrument_key, metadata FROM contracts")
        contracts = cursor.fetchall()

        print(f"\nFound {len(contracts)} contracts to update")

        updates = []
        for expired_key, metadata_json in contracts:
            if metadata_json:
                try:
                    # Parse the contract metadata
                    contract = json.loads(metadata_json)

                    # Generate correct OpenAlgo symbol
                    openalgo_symbol = to_openalgo_symbol(contract)

                    updates.append((openalgo_symbol, expired_key))

                except json.JSONDecodeError:
                    print(f"Error parsing metadata for {expired_key}")
                except Exception as e:
                    print(f"Error generating symbol for {expired_key}: {e}")

        # Update all symbols
        if updates:
            print(f"\nUpdating {len(updates)} symbols...")
            cursor.executemany(
                "UPDATE contracts SET openalgo_symbol = ? WHERE expired_instrument_key = ?",
                updates
            )
            conn.commit()
            print(f"Successfully updated {len(updates)} symbols")

            # Show some examples
            print("\nExample updated symbols:")
            cursor.execute("""
                SELECT trading_symbol, openalgo_symbol
                FROM contracts
                LIMIT 10
            """)

            for trading_symbol, openalgo_symbol in cursor.fetchall():
                print(f"  {trading_symbol:40} -> {openalgo_symbol}")

        else:
            print("No symbols to update")

    finally:
        conn.close()

    print("\n" + "="*70)
    print("Update Complete!")
    print("="*70)

if __name__ == "__main__":
    update_existing_symbols()