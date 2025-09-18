"""
User-friendly instrument name mapping
"""

# Mapping of user-friendly names to Upstox instrument keys
INSTRUMENT_MAPPING = {
    "Nifty 50": "NSE_INDEX|Nifty 50",
    "Bank Nifty": "NSE_INDEX|Nifty Bank",
    "Sensex": "BSE_INDEX|SENSEX"
}

# Reverse mapping for display purposes
INSTRUMENT_DISPLAY_NAMES = {v: k for k, v in INSTRUMENT_MAPPING.items()}

# Default instruments to show (user-friendly names)
DEFAULT_DISPLAY_INSTRUMENTS = ["Nifty 50", "Bank Nifty", "Sensex"]

def get_instrument_key(display_name: str) -> str:
    """
    Get the Upstox instrument key from user-friendly display name

    Args:
        display_name: User-friendly name like "Nifty 50"

    Returns:
        Upstox instrument key like "NSE_INDEX|Nifty 50"
    """
    return INSTRUMENT_MAPPING.get(display_name, display_name)

def get_display_name(instrument_key: str) -> str:
    """
    Get user-friendly display name from Upstox instrument key

    Args:
        instrument_key: Upstox key like "NSE_INDEX|Nifty 50"

    Returns:
        User-friendly name like "Nifty 50"
    """
    return INSTRUMENT_DISPLAY_NAMES.get(instrument_key, instrument_key)

def get_all_display_names() -> list:
    """Get all available user-friendly instrument names"""
    return list(INSTRUMENT_MAPPING.keys())

def get_all_instrument_keys() -> list:
    """Get all available Upstox instrument keys"""
    return list(INSTRUMENT_MAPPING.values())