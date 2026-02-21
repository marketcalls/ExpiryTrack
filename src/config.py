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
    DB_PATH: Path = DATA_DIR / 'expirytrack.duckdb'

    # Database settings
    DB_TYPE: str = 'duckdb'

    # Upstox API (loaded from database, not .env)
    UPSTOX_BASE_URL: str = 'https://api.upstox.com/v2'
    UPSTOX_REDIRECT_URI: str = 'http://127.0.0.1:5005/upstox/callback'

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
    DEFAULT_INSTRUMENTS = (
        'NSE_INDEX|Nifty 50',
        'NSE_INDEX|Nifty Bank',
        'BSE_INDEX|SENSEX',
    )

    # Scheduler settings
    SCHEDULER_ENABLED: bool = False
    SCHEDULER_MISFIRE_GRACE_TIME: int = 300  # seconds

    # Parallel processing
    MAX_PARALLEL_INSTRUMENTS: int = 3

    # Data quality
    QUALITY_CHECK_AFTER_COLLECTION: bool = True
    QUALITY_VIOLATION_THRESHOLD: float = 0.05  # 5% tolerance

    # Analytics
    ANALYTICS_QUERY_TIMEOUT: int = 30  # seconds
    ANALYTICS_MAX_CHART_POINTS: int = 500

    # Server defaults
    HOST: str = '127.0.0.1'
    PORT: int = 5005

    def __init__(self):
        """Initialize configuration"""
        # Load environment variables from .env file
        from dotenv import load_dotenv
        load_dotenv()

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
        
        # Database type override
        if os.getenv('DB_TYPE'):
            self.DB_TYPE = os.getenv('DB_TYPE')
            if self.DB_TYPE == 'duckdb':
                self.DB_PATH = self.DATA_DIR / 'expirytrack.duckdb'
            else:
                self.DB_PATH = self.DATA_DIR / 'expirytrack.db'

        # Scheduler settings
        if os.getenv('SCHEDULER_ENABLED'):
            self.SCHEDULER_ENABLED = os.getenv('SCHEDULER_ENABLED', '').lower() in ('true', '1', 'yes')
        if os.getenv('SCHEDULER_MISFIRE_GRACE_TIME'):
            self.SCHEDULER_MISFIRE_GRACE_TIME = int(os.getenv('SCHEDULER_MISFIRE_GRACE_TIME'))

        # Parallel processing
        if os.getenv('MAX_PARALLEL_INSTRUMENTS'):
            self.MAX_PARALLEL_INSTRUMENTS = int(os.getenv('MAX_PARALLEL_INSTRUMENTS'))

        # Data quality
        if os.getenv('QUALITY_CHECK_AFTER_COLLECTION'):
            self.QUALITY_CHECK_AFTER_COLLECTION = os.getenv('QUALITY_CHECK_AFTER_COLLECTION', '').lower() in ('true', '1', 'yes')
        if os.getenv('QUALITY_VIOLATION_THRESHOLD'):
            self.QUALITY_VIOLATION_THRESHOLD = float(os.getenv('QUALITY_VIOLATION_THRESHOLD'))

        # Analytics
        if os.getenv('ANALYTICS_QUERY_TIMEOUT'):
            self.ANALYTICS_QUERY_TIMEOUT = int(os.getenv('ANALYTICS_QUERY_TIMEOUT'))
        if os.getenv('ANALYTICS_MAX_CHART_POINTS'):
            self.ANALYTICS_MAX_CHART_POINTS = int(os.getenv('ANALYTICS_MAX_CHART_POINTS'))

        # Server settings
        if os.getenv('HOST'):
            self.HOST = os.getenv('HOST')
        if os.getenv('PORT'):
            self.PORT = int(os.getenv('PORT'))

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