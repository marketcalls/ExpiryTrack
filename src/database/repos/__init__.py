"""Repository classes for domain-specific database operations."""

from .candles import CandleRepository
from .contracts import ContractRepository
from .credentials import CredentialRepository
from .historical import HistoricalDataRepository
from .instrument_master import InstrumentMasterRepository
from .instruments import InstrumentRepository
from .jobs import JobRepository
from .tasks import TaskRepository
from .watchlists import WatchlistRepository

__all__ = [
    "CredentialRepository",
    "InstrumentRepository",
    "ContractRepository",
    "HistoricalDataRepository",
    "InstrumentMasterRepository",
    "CandleRepository",
    "WatchlistRepository",
    "JobRepository",
    "TaskRepository",
]
