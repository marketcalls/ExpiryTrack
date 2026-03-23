import duckdb
import sys

def delete_expiries():
    try:
        # Connect to DuckDB (requires write access)
        con = duckdb.connect('data/expirytrack.duckdb')
    except Exception as e:
        print("ERROR: Could not connect to the database.")
        print("The ExpiryTrack app is likely running and locking the database.")
        print("Please stop 'uv run app.py', run this script, and then start it again.")
        print(f"Details: {e}")
        sys.exit(1)

    expiries_to_delete = [
        '2025-02-06',
        '2025-02-13',
        '2025-02-20',
        '2025-02-27',
        '2025-03-06',
        '2025-03-13',
        '2025-03-20',
        '2025-03-27',
        '2026-03-02',
        '2026-03-10',
        '2026-03-17'
    ]
    
    # Create the SQL IN clause string
    in_clause = ', '.join([f"'{d}'" for d in expiries_to_delete])
    
    print(f"Preparing to delete data for {len(expiries_to_delete)} expiry dates...")
    
    # 1. Start Transaction
    con.execute("BEGIN TRANSACTION")
    
    try:
        # 2. Get the number of historical rows to be deleted
        cursor = con.execute(f"""
            SELECT COUNT(*) FROM historical_data 
            WHERE expired_instrument_key IN (
                SELECT expired_instrument_key FROM contracts WHERE expiry_date IN ({in_clause})
            )
        """).fetchone()
        hist_count = cursor[0] if cursor else 0
        
        # 3. Delete from historical_data
        print(f"Deleting {hist_count:,} rows from historical_data...")
        con.execute(f"""
            DELETE FROM historical_data 
            WHERE expired_instrument_key IN (
                SELECT expired_instrument_key FROM contracts WHERE expiry_date IN ({in_clause})
            )
        """)
        
        # 4. Get contracts count
        cursor = con.execute(f"SELECT COUNT(*) FROM contracts WHERE expiry_date IN ({in_clause})").fetchone()
        contracts_count = cursor[0] if cursor else 0
        
        # 5. Delete from contracts
        print(f"Deleting {contracts_count:,} rows from contracts...")
        con.execute(f"DELETE FROM contracts WHERE expiry_date IN ({in_clause})")
        
        # 6. Get expiries count
        cursor = con.execute(f"SELECT COUNT(*) FROM expiries WHERE expiry_date IN ({in_clause})").fetchone()
        expiries_count = cursor[0] if cursor else 0
        
        # 7. Delete from expiries
        print(f"Deleting {expiries_count:,} rows from expiries master table...")
        con.execute(f"DELETE FROM expiries WHERE expiry_date IN ({in_clause})")
        
        # 8. Commit
        con.execute("COMMIT")
        print("✅ Deletion successful and committed!")
        
        # Clean up database space
        print("Vacuuming database to reclaim space...")
        con.execute("VACUUM")
        print("Database optimized.")
        
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"❌ Error occurred during deletion. Rolled back changes. Error: {e}")
        
    finally:
        con.close()

if __name__ == "__main__":
    delete_expiries()
