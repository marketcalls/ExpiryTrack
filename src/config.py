"""
Zero-Configuration management for ExpiryTrack
All settings have sensible defaults - no .env required
"""
import os
from pathlib import Path
from typing import Optional

class Config:
    """Application configuration with zero-config defaults"""

    # Application paths (auto-created)
    BASE_DIR: Path = Path(__file__).parent.parent
    DATA_DIR: Path = BASE_DIR / 'data'
    LOGS_DIR: Path = BASE_DIR / 'logs'
    DB_PATH: Path = DATA_DIR / 'expirytrack.db'

    # Database settings
    DB_TYPE: str = 'sqlite'  # Default to SQLite

    # Upstox API (loaded from database, not .env)
    UPSTOX_BASE_URL: str = 'https://api.upstox.com/v2'
    UPSTOX_REDIRECT_URI: str = 'http://127.0.0.1:5000/upstox/callback'

    # Rate Limiting defaults (Conservative for safety)
    MAX_WORKERS: int = 10
    MAX_REQUESTS_SEC: int = 45  # Safety margin below 50
    MAX_REQUESTS_MIN: int = 450  # Safety margin below 500
    MAX_REQUESTS_30MIN: int = 1800  # Safety margin below 2000
    REQUEST_TIMEOUT: int = 30

    # Performance defaults
    BATCH_SIZE: int = 5000
    CHECKPOINT_INTERVAL: int = 100

    # Collection defaults
    HISTORICAL_MONTHS: int = 6
    DATA_INTERVAL: str = '1minute'

    # Logging defaults
    LOG_LEVEL: str = 'INFO'
    LOG_FILE: Path = LOGS_DIR / 'expirytrack.log'

    # Default instruments (no hardcoding to single instrument)
    DEFAULT_INSTRUMENTS = [
        'NSE_INDEX|Nifty 50',
        'NSE_INDEX|Nifty Bank',
        'BSE_INDEX|SENSEX'
    ]

    def __init__(self):
        """Initialize configuration"""
        # Create necessary directories
        self.DATA_DIR.mkdir(exist_ok=True, parents=True)
        self.LOGS_DIR.mkdir(exist_ok=True, parents=True)

        # Optional: Override with environment variables if present
        self._load_env_overrides()

    def _load_env_overrides(self):
        """Load any environment variable overrides (optional)"""
        # Only override non-credential settings from environment
        if os.getenv('MAX_WORKERS'):
            self.MAX_WORKERS = int(os.getenv('MAX_WORKERS'))
        if os.getenv('BATCH_SIZE'):
            self.BATCH_SIZE = int(os.getenv('BATCH_SIZE'))
        if os.getenv('LOG_LEVEL'):
            self.LOG_LEVEL = os.getenv('LOG_LEVEL')
        if os.getenv('HISTORICAL_MONTHS'):
            self.HISTORICAL_MONTHS = int(os.getenv('HISTORICAL_MONTHS'))

    @classmethod
    def validate(cls) -> bool:
        """Validate configuration"""
        # No need to check for API credentials in .env anymore
        # They'll be stored in database

        # Just ensure directories exist
        instance = cls()
        return True

    @classmethod
    def get_db_url(cls) -> str:
        """Get database connection URL"""
        instance = cls()
        if instance.DB_TYPE == 'sqlite':
            return f"sqlite:///{instance.DB_PATH}"
        elif instance.DB_TYPE == 'duckdb':
            return f"duckdb:///{instance.DB_PATH}"
        else:
            raise ValueError(f"Unsupported database type: {instance.DB_TYPE}")

# Create singleton instance
config = Config()