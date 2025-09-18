"""
ExpiryTrack - Automated Historical Data Collection for Expired Derivatives
"""

__version__ = "1.0.0"
__author__ = "ExpiryTrack Team"

from .api.client import UpstoxAPIClient
from .auth.manager import AuthManager
from .database.manager import DatabaseManager
from .collectors.expiry_tracker import ExpiryTracker

__all__ = [
    "UpstoxAPIClient",
    "AuthManager",
    "DatabaseManager",
    "ExpiryTracker"
]