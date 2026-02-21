"""
ExpiryTrack - Main Application Entry Point
"""
import asyncio
import click
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.collectors.expiry_tracker import ExpiryTracker
from src.auth.manager import AuthManager
from src.database.manager import DatabaseManager
from src.config import config
from src.utils.logger import setup_logging
from src.utils.instrument_mapper import get_instrument_key, get_display_name, get_all_display_names

# Setup logging
logger = setup_logging()

@click.group()
def cli():
    """ExpiryTrack - Automated Historical Data Collection for Expired Derivatives"""
    pass

@cli.command()
def setup():
    """Setup Upstox API credentials (stored encrypted in database)"""
    auth_manager = AuthManager()

    click.echo("\n" + "="*50)
    click.echo("ExpiryTrack Setup - API Credentials")
    click.echo("="*50)
    click.echo("\nPlease enter your Upstox API credentials.")
    click.echo("These will be stored encrypted in the database.\n")

    api_key = click.prompt("API Key", type=str)
    api_secret = click.prompt("API Secret", type=str, hide_input=True)

    # Optional redirect URI
    click.echo(f"\nRedirect URI (press Enter for default: {config.UPSTOX_REDIRECT_URI})")
    redirect_uri = click.prompt("Redirect URI", default="", show_default=False)

    if not api_key or not api_secret:
        click.echo("\nError: API Key and Secret are required!")
        return

    # Save credentials
    if auth_manager.save_credentials(api_key, api_secret, redirect_uri or None):
        click.echo("\n" + "="*50)
        click.echo("Credentials saved successfully!")
        click.echo("="*50)
        click.echo("\nYou can now run:")
        click.echo("  python main.py authenticate - to login")
        click.echo("  python main.py collect - to start data collection")
    else:
        click.echo("\nError: Failed to save credentials")

@cli.command()
def authenticate():
    """Authenticate with Upstox API"""
    auth_manager = AuthManager()

    # Check if credentials are configured
    if not auth_manager.has_credentials():
        click.echo("\nNo API credentials found!")
        click.echo("Please run: python main.py setup")
        return

    if auth_manager.is_token_valid():
        click.echo("Already authenticated with valid token")
        remaining = (auth_manager.token_expiry - datetime.now().timestamp()) / 3600
        click.echo(f"Token expires in {remaining:.1f} hours")
    else:
        click.echo("Starting authentication flow...")
        if auth_manager.authenticate():
            click.echo("Authentication successful!")
        else:
            click.echo("Authentication failed!")
            sys.exit(1)

@cli.command()
@click.option('--instrument', type=click.Choice(get_all_display_names()), required=True, help='Select instrument')
def get_expiries(instrument):
    """Fetch all available expiries for an instrument"""
    async def _get_expiries():
        tracker = ExpiryTracker()

        # Authenticate
        if not tracker.authenticate():
            click.echo("Authentication failed!")
            return

        # Convert display name to instrument key
        instrument_key = get_instrument_key(instrument)

        async with tracker:
            expiries = await tracker.get_expiries(instrument_key)

            if expiries:
                click.echo(f"\nFound {len(expiries)} expiry dates for {instrument}:")
                click.echo("-" * 40)

                # Group by month
                months = {}
                for expiry in expiries:
                    month = expiry[:7]  # YYYY-MM
                    if month not in months:
                        months[month] = []
                    months[month].append(expiry)

                for month, dates in months.items():
                    click.echo(f"\n{month}:")
                    for date in dates:
                        day = datetime.strptime(date, '%Y-%m-%d').strftime('%a, %d %b %Y')
                        click.echo(f"  - {day}")
            else:
                click.echo(f"No expiries found for {instrument}")

    asyncio.run(_get_expiries())

@cli.command()
@click.option('--instrument', type=click.Choice(get_all_display_names()), required=True, help='Select instrument')
@click.option('--expiry', required=True, help='Expiry date (YYYY-MM-DD)')
def get_contracts(instrument, expiry):
    """Fetch contracts for a specific expiry"""
    async def _get_contracts():
        tracker = ExpiryTracker()

        if not tracker.authenticate():
            click.echo("Authentication failed!")
            return

        # Convert display name to instrument key
        instrument_key = get_instrument_key(instrument)

        async with tracker:
            contracts = await tracker.get_contracts(instrument_key, expiry)

            options = contracts.get('options', [])
            futures = contracts.get('futures', [])

            click.echo(f"\nContracts for {instrument} expiring {expiry}:")
            click.echo("="*50)
            click.echo(f"Options (CE+PE): {len(options)}")
            click.echo(f"Futures: {len(futures)}")
            click.echo(f"Total: {len(options) + len(futures)}")

            if options:
                # Show strike price range
                strikes = sorted(set(opt['strike_price'] for opt in options if 'strike_price' in opt))
                if strikes:
                    click.echo(f"\nStrike Range: {strikes[0]} - {strikes[-1]}")
                    click.echo(f"Total Strikes: {len(strikes) // 2}")  # CE and PE

    asyncio.run(_get_contracts())

@cli.command()
@click.option('--instruments', '-i', multiple=True, type=click.Choice(get_all_display_names()),
              help='Select instruments to collect (can specify multiple: -i "Nifty 50" -i "Bank Nifty")')
@click.option('--all', 'collect_all', is_flag=True, help='Collect all available instruments')
@click.option('--months', default=config.HISTORICAL_MONTHS, help='Months of history')
@click.option('--interval', default='1minute', help='Data interval')
@click.option('--concurrent', default=10, help='Number of concurrent workers')
def collect(instruments, collect_all, months, interval, concurrent):
    """Collect all expired contract data for instruments"""
    async def _collect():
        tracker = ExpiryTracker()
        auth_manager = AuthManager()
        db_manager = DatabaseManager()

        # Check credentials
        if not auth_manager.has_credentials():
            click.echo("\nNo API credentials found!")
            click.echo("Please run: python main.py setup")
            return

        if not tracker.authenticate():
            click.echo("Authentication failed!")
            return

        # Determine which instruments to collect
        if collect_all:
            # Collect all available instruments
            selected_instruments = get_all_display_names()
            click.echo(f"\nCollecting all instruments: {', '.join(selected_instruments)}")
        elif instruments:
            # Use specified instruments
            selected_instruments = list(instruments)
        else:
            # Interactive selection
            click.echo("\nNo instruments specified. Please select instruments to collect:")
            available = get_all_display_names()
            for i, name in enumerate(available, 1):
                click.echo(f"{i}. {name}")
            click.echo(f"{len(available) + 1}. All instruments")

            choice = click.prompt("\nEnter your choice (number or comma-separated numbers)", type=str)

            if choice.strip() == str(len(available) + 1):
                selected_instruments = available
            else:
                try:
                    indices = [int(x.strip()) - 1 for x in choice.split(',')]
                    selected_instruments = [available[i] for i in indices if 0 <= i < len(available)]
                except (ValueError, IndexError):
                    click.echo("Invalid selection. Exiting.")
                    return

        if not selected_instruments:
            click.echo("No instruments selected.")
            return

        # Convert display names to instrument keys
        instrument_keys = [get_instrument_key(name) for name in selected_instruments]

        click.echo(f"\nStarting data collection")
        click.echo(f"Instruments: {', '.join(selected_instruments)}")
        click.echo(f"Months back: {months}")
        click.echo(f"Interval: {interval}")
        click.echo(f"Concurrent Workers: {concurrent}")
        click.echo("="*50)

        total_stats = {
            'expiries_fetched': 0,
            'contracts_fetched': 0,
            'candles_fetched': 0,
            'errors': 0
        }

        async with tracker:
            for inst_key, inst_name in zip(instrument_keys, selected_instruments):
                click.echo(f"\n" + "-"*40)
                click.echo(f"Collecting: {inst_name}")
                click.echo("-"*40)

                stats = await tracker.auto_collect(inst_key, months, interval)

                # Aggregate stats
                for key in total_stats:
                    total_stats[key] += stats.get(key, 0)

        click.echo("\n" + "="*50)
        click.echo("Collection Complete!")
        click.echo(f"Total Expiries: {total_stats['expiries_fetched']}")
        click.echo(f"Total Contracts: {total_stats['contracts_fetched']}")
        click.echo(f"Total Candles: {total_stats['candles_fetched']:,}")
        click.echo(f"Total Errors: {total_stats['errors']}")
        click.echo("="*50)

    asyncio.run(_collect())

@cli.command()
def resume():
    """Resume incomplete data collection"""
    async def _resume():
        tracker = ExpiryTracker()

        if not tracker.authenticate():
            click.echo("Authentication failed!")
            return

        click.echo("\nResuming incomplete collection...")

        async with tracker:
            stats = await tracker.resume_collection()

            click.echo("\nResume Complete!")
            click.echo(f"   Candles fetched: {stats['candles_fetched']:,}")
            click.echo(f"   Errors: {stats['errors']}")

    asyncio.run(_resume())

@cli.command()
def status():
    """Show database status and statistics"""
    db_manager = DatabaseManager()
    stats = db_manager.get_summary_stats()

    click.echo("\n" + "="*50)
    click.echo("ExpiryTrack Database Status")
    click.echo("="*50)
    click.echo(f"Instruments: {stats['total_instruments']}")
    click.echo(f"Expiries: {stats['total_expiries']}")
    click.echo(f"Contracts: {stats['total_contracts']:,}")
    click.echo(f"Historical Candles: {stats['total_candles']:,}")
    click.echo("-"*50)
    click.echo(f"Pending Expiries: {stats['pending_expiries']}")
    click.echo(f"Pending Contracts: {stats['pending_contracts']}")
    click.echo("="*50)

    # Calculate database size
    if config.DB_PATH.exists():
        size_mb = config.DB_PATH.stat().st_size / (1024 * 1024)
        click.echo(f"\nDatabase Size: {size_mb:.2f} MB")

@cli.command()
def test():
    """Test API connection"""
    async def _test():
        tracker = ExpiryTracker()

        click.echo("Testing connection...")

        if not tracker.authenticate():
            click.echo("Authentication failed!")
            return

        async with tracker:
            if await tracker.test_connection():
                click.echo("API connection successful!")
                # Show rate limit status
                limits = await tracker.api_client.check_rate_limits()
                click.echo("\nRate Limit Status:")
                for key, value in limits.items():
                    click.echo(f"  {key}: {value}")
            else:
                click.echo("API connection failed!")

    asyncio.run(_test())

@cli.command()
def optimize():
    """Optimize database (CHECKPOINT for DuckDB)"""
    db_manager = DatabaseManager()
    click.echo("Optimizing database...")
    db_manager.vacuum()
    click.echo("Database optimized!")

@cli.command()
def clear_auth():
    """Clear stored authentication tokens"""
    auth_manager = AuthManager()
    auth_manager.clear_tokens()
    click.echo("Authentication tokens cleared!")

@cli.command()
@click.option('--instrument', default=None, help='Filter to specific instrument key')
def quality_check(instrument):
    """Run data quality checks on collected data"""
    from src.quality.checker import DataQualityChecker

    click.echo("\nRunning data quality checks...")
    checker = DataQualityChecker()
    report = checker.run_all_checks(instrument)

    click.echo(f"\nChecks run: {report.checks_run}")
    click.echo(f"Checks passed: {report.checks_passed}")
    click.echo(f"Errors: {report.error_count}")
    click.echo(f"Warnings: {report.warning_count}")
    click.echo(f"Overall: {'PASSED' if report.passed else 'FAILED'}")

    if report.violations:
        click.echo(f"\nViolations ({len(report.violations)}):")
        click.echo("-" * 60)
        for v in report.violations[:20]:
            icon = 'E' if v.severity == 'error' else 'W' if v.severity == 'warning' else 'I'
            click.echo(f"  [{icon}] {v.check}: {v.message}")
        if len(report.violations) > 20:
            click.echo(f"  ... and {len(report.violations) - 20} more")

@cli.command()
@click.option('--enable/--disable', default=True, help='Enable or disable scheduler')
def scheduler(enable):
    """Start the background scheduler for automated collection"""
    import signal
    import time
    from src.scheduler.scheduler import scheduler_manager

    if enable:
        config.SCHEDULER_ENABLED = True
        scheduler_manager.start()

        click.echo("Scheduler started. Jobs:")
        for job in scheduler_manager.get_jobs():
            click.echo(f"  - {job['name']} (next: {job['next_run'] or 'paused'})")

        click.echo("\nPress Ctrl+C to stop...")

        def handle_signal(sig, frame):
            click.echo("\nStopping scheduler...")
            scheduler_manager.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        while True:
            time.sleep(1)
    else:
        scheduler_manager.stop()
        click.echo("Scheduler disabled")

if __name__ == '__main__':
    cli()