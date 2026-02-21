"""
Dynamic instrument name mapping — reads from database (#9)

Falls back to hardcoded defaults when DB is not available.
"""
import logging

logger = logging.getLogger(__name__)

# Hardcoded fallback (used before DB is initialized)
_FALLBACK_MAPPING = {
    "Nifty 50": "NSE_INDEX|Nifty 50",
    "Bank Nifty": "NSE_INDEX|Nifty Bank",
    "Sensex": "BSE_INDEX|SENSEX",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "BANKEX": "BSE_INDEX|BANKEX",
}

# Module-level cache
_cached_mapping = None
_cached_reverse = None


def _load_from_db():
    """Load active instruments from database and build mappings."""
    global _cached_mapping, _cached_reverse
    try:
        from ..database.manager import DatabaseManager
        db = DatabaseManager()
        instruments = db.get_active_instruments()
        if instruments:
            _cached_mapping = {inst['symbol']: inst['instrument_key'] for inst in instruments}
            _cached_reverse = {inst['instrument_key']: inst['symbol'] for inst in instruments}
            return
    except Exception as e:
        logger.debug(f"Could not load instruments from DB, using fallback: {e}")

    _cached_mapping = dict(_FALLBACK_MAPPING)
    _cached_reverse = {v: k for k, v in _FALLBACK_MAPPING.items()}


def _ensure_loaded():
    if _cached_mapping is None:
        _load_from_db()


def refresh_cache():
    """Force reload from database."""
    global _cached_mapping, _cached_reverse
    _cached_mapping = None
    _cached_reverse = None
    _ensure_loaded()


# Public API — same interface as before

def get_instrument_key(display_name: str) -> str:
    """Get the Upstox instrument key from user-friendly display name."""
    _ensure_loaded()
    return _cached_mapping.get(display_name, display_name)


def get_display_name(instrument_key: str) -> str:
    """Get user-friendly display name from Upstox instrument key."""
    _ensure_loaded()
    return _cached_reverse.get(instrument_key, instrument_key)


def get_all_display_names() -> list:
    """Get all available user-friendly instrument names."""
    _ensure_loaded()
    return list(_cached_mapping.keys())


def get_all_instrument_keys() -> list:
    """Get all available Upstox instrument keys."""
    _ensure_loaded()
    return list(_cached_mapping.values())


# Keep INSTRUMENT_MAPPING as a lazy property for backward compatibility
class _MappingProxy(dict):
    """Dict-like proxy that lazy-loads from DB."""
    def __init__(self):
        super().__init__()
        self._loaded = False

    def _ensure(self):
        if not self._loaded:
            _ensure_loaded()
            self.update(_cached_mapping or {})
            self._loaded = True

    def __getitem__(self, key):
        self._ensure()
        return super().__getitem__(key)

    def __contains__(self, key):
        self._ensure()
        return super().__contains__(key)

    def keys(self):
        self._ensure()
        return super().keys()

    def values(self):
        self._ensure()
        return super().values()

    def items(self):
        self._ensure()
        return super().items()

    def get(self, key, default=None):
        self._ensure()
        return super().get(key, default)

    def __len__(self):
        self._ensure()
        return super().__len__()


INSTRUMENT_MAPPING = _MappingProxy()
