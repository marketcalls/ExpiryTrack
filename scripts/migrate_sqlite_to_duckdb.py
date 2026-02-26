"""
Migrate ExpiryTrack data from SQLite to DuckDB

Uses DuckDB's native sqlite_scan extension for fast, direct table copy.
Run once after switching to the DuckDB-based DatabaseManager.

Usage:
    python scripts/migrate_sqlite_to_duckdb.py
"""
import sys
import os
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb

SQLITE_PATH = project_root / "data" / "expirytrack.db"
DUCKDB_PATH = project_root / "data" / "expirytrack.duckdb"

# Small tables copied in one shot; historical_data gets batched
SMALL_TABLES = [
    "credentials",
    "default_instruments",
    "instruments",
    "expiries",
    "contracts",
    "job_status",
]

BATCH_SIZE = 200  # contracts per batch for historical_data (~1.5M rows each)


def fmt_size(b):
    if b >= 1024 * 1024 * 1024:
        return f"{b / (1024**3):.2f} GB"
    elif b >= 1024 * 1024:
        return f"{b / (1024*1024):.1f} MB"
    elif b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b} bytes"


def copy_small_table(conn, table, sqlite_path_str):
    """Copy a small table in one shot."""
    exists = conn.execute(
        f"SELECT COUNT(*) FROM sqlite_scan('{sqlite_path_str}', '{table}')"
    ).fetchone()

    if exists is None:
        print(f"       {table:25s} -- not found in SQLite, skipping")
        return None

    src_count = exists[0]
    if src_count == 0:
        print(f"       {table:25s} -- empty, skipping")
        return (0, 0)

    conn.execute(f"DELETE FROM {table}")
    conn.execute(f"""
        INSERT INTO {table}
        SELECT * FROM sqlite_scan('{sqlite_path_str}', '{table}')
    """)
    conn.commit()

    dst_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    match = "OK" if src_count == dst_count else "MISMATCH"
    print(f"       {table:25s} -- {src_count:>10,} rows -> {dst_count:>10,} rows  [{match}]")
    return (src_count, dst_count)


def copy_historical_data_batched(conn, sqlite_path_str):
    """Copy historical_data in batches to avoid OOM."""
    table = "historical_data"

    src_count = conn.execute(
        f"SELECT COUNT(*) FROM sqlite_scan('{sqlite_path_str}', '{table}')"
    ).fetchone()[0]

    if src_count == 0:
        print(f"       {table:25s} -- empty, skipping")
        return (0, 0)

    print(f"       {table:25s} -- {src_count:>10,} rows (batched copy)...")

    # Tune DuckDB for large bulk load — keep memory usage low
    conn.execute("SET preserve_insertion_order=false")
    conn.execute("SET threads=2")
    conn.execute("SET memory_limit='4GB'")

    # Get all distinct expired_instrument_keys from SQLite to batch by contract
    keys = conn.execute(f"""
        SELECT DISTINCT expired_instrument_key
        FROM sqlite_scan('{sqlite_path_str}', '{table}')
    """).fetchall()
    all_keys = [row[0] for row in keys]

    print(f"       Found {len(all_keys):,} contracts to copy...")

    # Clear any partial data from a previous failed attempt
    existing = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if existing > 0:
        print(f"       Clearing {existing:,} rows from previous attempt...")
        conn.execute(f"DELETE FROM {table}")
        conn.commit()

    total_inserted = 0
    start_time = time.time()

    # Process in batches of keys
    for i in range(0, len(all_keys), BATCH_SIZE):
        batch_keys = all_keys[i:i + BATCH_SIZE]

        # Create a temp table with the batch keys
        conn.execute("CREATE OR REPLACE TEMP TABLE _batch_keys (k TEXT)")
        # Insert keys via VALUES
        for k in batch_keys:
            conn.execute("INSERT INTO _batch_keys VALUES (?)", (k,))

        conn.execute(f"""
            INSERT INTO {table}
            SELECT s.*
            FROM sqlite_scan('{sqlite_path_str}', '{table}') s
            WHERE s.expired_instrument_key IN (SELECT k FROM _batch_keys)
        """)
        conn.commit()

        batch_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        elapsed = time.time() - start_time
        rate = batch_count / elapsed if elapsed > 0 else 0
        total_inserted = batch_count

        print(f"       Progress: {total_inserted:>12,} / {src_count:>12,} rows  "
              f"({total_inserted*100/src_count:.1f}%)  "
              f"[{rate:,.0f} rows/sec]")

        conn.execute("DROP TABLE IF EXISTS _batch_keys")

    # Reset settings
    conn.execute("RESET preserve_insertion_order")
    conn.execute("RESET threads")
    conn.execute("RESET memory_limit")

    dst_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    match = "OK" if src_count == dst_count else "MISMATCH"
    elapsed = time.time() - start_time
    print(f"       {table:25s} -- {src_count:>10,} rows -> {dst_count:>10,} rows  [{match}] ({elapsed:.1f}s)")
    return (src_count, dst_count)


def migrate():
    print("=" * 70)
    print("ExpiryTrack: SQLite -> DuckDB Migration")
    print("=" * 70)

    # Pre-flight checks
    if not SQLITE_PATH.exists():
        print(f"\nNo SQLite database found at {SQLITE_PATH}")
        print("Nothing to migrate. The app will create a fresh DuckDB on first run.")
        return

    if DUCKDB_PATH.exists():
        print(f"\nDuckDB database already exists at {DUCKDB_PATH}")
        resp = input("Overwrite? [y/N] ").strip().lower()
        if resp != "y":
            print("Aborted.")
            return
        DUCKDB_PATH.unlink()
        # Also remove WAL file if present
        wal_path = DUCKDB_PATH.with_suffix('.duckdb.wal')
        if wal_path.exists():
            wal_path.unlink()

    # Initialize DuckDB with the schema from DatabaseManager
    print("\n[1/4] Initializing DuckDB schema...")
    from src.database.manager import DatabaseManager
    dm = DatabaseManager(db_path=DUCKDB_PATH)
    print("       Schema created.")

    # Open DuckDB and install sqlite extension
    print("[2/4] Loading SQLite extension...")
    conn = duckdb.connect(str(DUCKDB_PATH))
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    print("       Extension loaded.")

    # Copy tables
    print("[3/4] Copying tables...")
    sqlite_path_str = str(SQLITE_PATH)
    row_counts = {}

    # Copy small tables first
    for table in SMALL_TABLES:
        try:
            result = copy_small_table(conn, table, sqlite_path_str)
            if result is not None:
                row_counts[table] = result
        except Exception as e:
            print(f"       {table:25s} -- ERROR: {e}")
            row_counts[table] = (0, -1)

    # Copy historical_data with batching
    try:
        result = copy_historical_data_batched(conn, sqlite_path_str)
        row_counts["historical_data"] = result
    except Exception as e:
        print(f"       {'historical_data':25s} -- ERROR: {e}")
        row_counts["historical_data"] = (0, -1)

    # Size comparison
    print("\n[4/4] Size comparison:")
    sqlite_size = SQLITE_PATH.stat().st_size
    conn.execute("CHECKPOINT")
    conn.close()
    duckdb_size = DUCKDB_PATH.stat().st_size

    ratio = duckdb_size / sqlite_size if sqlite_size > 0 else 0
    print(f"       SQLite:  {fmt_size(sqlite_size)}")
    print(f"       DuckDB:  {fmt_size(duckdb_size)}")
    print(f"       Ratio:   {ratio:.2f}x ({1/ratio:.1f}x smaller)" if ratio > 0 else "")

    # Summary
    print("\n" + "=" * 70)
    mismatches = [t for t, (s, d) in row_counts.items() if s != d]
    if mismatches:
        print(f"WARNING: Row count mismatches in: {', '.join(mismatches)}")
        print("Please investigate before removing the SQLite database.")
    else:
        print("Migration completed successfully!")
        print(f"DuckDB database ready at: {DUCKDB_PATH}")
        print(f"\nYou can now safely archive or delete {SQLITE_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    migrate()
