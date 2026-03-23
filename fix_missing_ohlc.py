"""
ExpiryTrack Missing OHLC Fix
==============================
Finds all contracts that exist in the contracts table but have ZERO rows
in historical_data, resets their data_fetched flag to FALSE, and then
runs ExpiryTrack's existing resume_collection() to re-fetch the data.

This uses your existing ExpiryTrack infrastructure — no custom API calls.

Usage:
    # Step 1: Preview what will be reset (dry run)
    uv run python fix_missing_ohlc.py --dry-run

    # Step 2: Reset contracts and trigger re-fetch
    uv run python fix_missing_ohlc.py

    # Step 3: Only reset specific expiry dates
    uv run python fix_missing_ohlc.py --expiry 2024-12-26
    uv run python fix_missing_ohlc.py --expiry 2025-01-02

    # Step 4: After reset, run ExpiryTrack resume to fetch data
    python main.py resume
"""

import asyncio
import sys
from pathlib import Path

import click
import duckdb

# ── Point at your ExpiryTrack project root ──
EXPITRACK_ROOT = Path(__file__).parent
EXPITRACK_DB   = EXPITRACK_ROOT / "data" / "expirytrack.duckdb"

# ── Add ExpiryTrack src to path so we can use its classes ──
sys.path.insert(0, str(EXPITRACK_ROOT))


# ─────────────────────────────────────────────────────────────
# Step 1: Find all missing contracts
# ─────────────────────────────────────────────────────────────

def find_missing_contracts(con, expiry_filter: str | None = None) -> list[dict]:
    """Return all contracts that exist in contracts table but have 0 OHLC rows."""

    expiry_clause = ""
    params = []
    if expiry_filter:
        expiry_clause = "AND c.expiry_date = ?"
        params.append(expiry_filter)

    rows = con.execute(f"""
        SELECT
            c.expired_instrument_key,
            c.trading_symbol,
            c.expiry_date,
            c.contract_type,
            c.strike_price,
            c.data_fetched,
            c.fetch_attempts,
            c.no_data,
            COALESCE(h.row_count, 0) AS ohlc_rows
        FROM contracts c
        LEFT JOIN (
            SELECT expired_instrument_key, COUNT(*) AS row_count
            FROM historical_data
            GROUP BY expired_instrument_key
        ) h ON c.expired_instrument_key = h.expired_instrument_key
        WHERE c.instrument_key = 'NSE_INDEX|Nifty 50'
          AND (h.row_count IS NULL OR h.row_count = 0)
          AND c.no_data = FALSE
          {expiry_clause}
        ORDER BY c.expiry_date, c.strike_price, c.contract_type
    """, params).fetchall()

    return [
        {
            "key":           row[0],
            "symbol":        row[1],
            "expiry":        str(row[2]),
            "type":          row[3],
            "strike":        float(row[4]) if row[4] is not None else 0,
            "data_fetched":  row[5],
            "attempts":      row[6],
            "no_data":       row[7],
            "ohlc_rows":     row[8],
        }
        for row in rows
        if row[0] is not None  # skip rows with null expired_instrument_key
    ]


# ─────────────────────────────────────────────────────────────
# Step 2: Reset contracts so ExpiryTrack re-fetches them
# ─────────────────────────────────────────────────────────────

def reset_contracts(con, contracts: list[dict]) -> int:
    """
    Reset data_fetched = FALSE and fetch_attempts = 0 for missing contracts.
    This makes them appear as 'pending' to ExpiryTrack's resume_collection().
    Also resets the parent expiry's data_fetched flag so the expiry is re-processed.
    """
    if not contracts:
        return 0

    keys = [c["key"] for c in contracts]

    # Reset individual contracts
    placeholders = ", ".join("?" * len(keys))
    con.execute(f"""
        UPDATE contracts
        SET data_fetched   = FALSE,
            fetch_attempts = 0,
            no_data        = FALSE,
            last_attempted_at = NULL
        WHERE expired_instrument_key IN ({placeholders})
    """, keys)

    # Also reset the parent expiry rows so ExpiryTrack re-processes them
    # (ExpiryTrack checks expiry.data_fetched to decide whether to skip)
    expiry_dates = list({c["expiry"] for c in contracts})
    exp_placeholders = ", ".join("?" * len(expiry_dates))
    con.execute(f"""
        UPDATE expiries
        SET data_fetched = FALSE
        WHERE instrument_key = 'NSE_INDEX|Nifty 50'
          AND expiry_date IN ({exp_placeholders})
    """, expiry_dates)

    con.commit()
    return len(keys)


# ─────────────────────────────────────────────────────────────
# Step 3: Trigger ExpiryTrack's own resume_collection()
# ─────────────────────────────────────────────────────────────

async def run_resume():
    """Use ExpiryTrack's existing resume_collection() to re-fetch data."""
    from src.auth.manager import AuthManager
    from src.collectors.expiry_tracker import ExpiryTracker

    auth = AuthManager()
    if not auth.is_token_valid():
        print("\nNot authenticated. Run first:")
        print("  python main.py authenticate")
        return False

    tracker = ExpiryTracker()
    if not tracker.authenticate():
        print("Authentication failed!")
        return False

    print("\nRunning ExpiryTrack resume_collection()...")
    print("This fetches OHLC data for all pending contracts.\n")

    async with tracker:
        stats = await tracker.resume_collection()

    print(f"\nResume complete!")
    print(f"  Candles fetched : {stats.get('candles_fetched', 0):,}")
    print(f"  Errors          : {stats.get('errors', 0)}")
    return True


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

@click.command()
@click.option("--dry-run",  is_flag=True, help="Preview what would be reset without changing anything")
@click.option("--expiry",   default=None, help="Only fix a specific expiry date (YYYY-MM-DD)")
@click.option("--reset-only", is_flag=True, help="Reset contracts but don't trigger re-fetch")
@click.option("--fetch-only", is_flag=True, help="Skip reset, just run resume_collection()")
def main(dry_run, expiry, reset_only, fetch_only):
    """Fix missing OHLC data in ExpiryTrack database."""

    print("=" * 65)
    print("  ExpiryTrack Missing OHLC Fix")
    print("=" * 65)

    if not EXPITRACK_DB.exists():
        print(f"\nERROR: Database not found at {EXPITRACK_DB}")
        print("Check EXPITRACK_ROOT at the top of this script.")
        sys.exit(1)

    con = duckdb.connect(str(EXPITRACK_DB))

    # ── Find missing contracts ──
    if not fetch_only:
        print(f"\nScanning for contracts with zero OHLC data...")
        if expiry:
            print(f"  Filter: expiry = {expiry}")

        missing = find_missing_contracts(con, expiry_filter=expiry)

        if not missing:
            print("\nNo missing contracts found — database looks complete!")
            con.close()
            return

        # Group by expiry for summary
        by_expiry: dict[str, list] = {}
        for c in missing:
            by_expiry.setdefault(c["expiry"], []).append(c)

        print(f"\nFound {len(missing)} contracts with zero OHLC data across "
              f"{len(by_expiry)} expiries:\n")
        print(f"  {'Expiry':<14} {'Missing':>8}  Sample strikes missing")
        print(f"  {'-'*55}")
        for exp_date in sorted(by_expiry.keys()):
            contracts = by_expiry[exp_date]
            sample_strikes = sorted({c['strike'] for c in contracts[:5]})
            sample_str = ", ".join(f"{int(s)}" for s in sample_strikes[:3])
            if len(sample_strikes) > 3:
                sample_str += "..."
            print(f"  {exp_date:<14} {len(contracts):>8}  {sample_str}")

        if dry_run:
            print(f"\n[DRY RUN] Would reset {len(missing)} contracts.")
            print("Run without --dry-run to apply the fix.")
            con.close()
            return

        # ── Reset contracts ──
        print(f"\nResetting {len(missing)} contracts to data_fetched=FALSE...")
        reset_count = reset_contracts(con, missing)
        print(f"  Reset {reset_count} contracts ✓")
        print(f"  Reset {len(by_expiry)} expiry rows ✓")

        print(f"\nContracts are now pending re-fetch in ExpiryTrack.")

    con.close()

    if reset_only:
        print("\n[reset-only mode] Skipping re-fetch.")
        print("Run re-fetch manually:")
        print("  python main.py resume")
        return

    # ── Trigger re-fetch ──
    print("\n" + "=" * 65)
    print("  Triggering ExpiryTrack resume_collection()")
    print("=" * 65)

    success = asyncio.run(run_resume())

    if success:
        print("\nAll done! Run the diagnostic again to verify:")
        print("  uv run python find_missing_ohlc.py")
    else:
        print("\nRe-fetch failed. Reset was applied — run manually:")
        print("  python main.py resume")


if __name__ == "__main__":
    main()
