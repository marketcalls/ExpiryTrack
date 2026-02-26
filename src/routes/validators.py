"""Pydantic models for POST endpoint input validation."""

from pydantic import BaseModel, Field, field_validator


class CredentialsInput(BaseModel):
    api_key: str = Field(min_length=5)
    api_secret: str = Field(min_length=5)
    redirect_url: str | None = None


class InstrumentInput(BaseModel):
    instrument_key: str = Field(min_length=3)
    symbol: str = Field(min_length=1)
    priority: int = 0
    category: str = "Index"


class StrikeFilter(BaseModel):
    type: str = "all"  # "all", "atm_range", "custom"
    atm_range: int | None = None  # Number of strikes around ATM
    min_strike: float | None = None  # Custom range minimum
    max_strike: float | None = None  # Custom range maximum

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        allowed = {"all", "atm_range", "custom"}
        if v not in allowed:
            raise ValueError(f"strike_filter type must be one of {allowed}")
        return v


class CollectInput(BaseModel):
    instruments: list[str] = Field(min_length=1)
    expiries: dict[str, list[str]] = Field(default_factory=dict)
    contract_type: str = "both"
    interval: str = "1minute"
    workers: int = 5
    strike_filter: StrikeFilter | None = None
    force_refetch: bool = False


class ExportInput(BaseModel):
    format: str = "csv"
    instruments: list[str] = Field(default_factory=list)
    expiries: dict[str, list[str]] = Field(default_factory=dict)
    options: dict = Field(default_factory=dict)

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        allowed = {"csv", "json", "zip", "parquet", "xlsx", "amibroker", "metatrader"}
        if v not in allowed:
            raise ValueError(f"format must be one of {allowed}")
        return v


class WatchlistInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    segment: str | None = None


_VALID_CANDLE_INTERVALS = {
    "1minute", "3minute", "5minute", "10minute", "15minute",
    "30minute", "1hour", "1day", "1week", "1month",
}
_VALID_CANDLE_FORMATS = {"csv", "json", "parquet", "xlsx", "zip"}


class CandleExportInput(BaseModel):
    format: str = "csv"
    instrument_keys: list[str] = Field(default_factory=list)
    interval: str = "1day"
    from_date: str | None = None
    to_date: str | None = None

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in _VALID_CANDLE_FORMATS:
            raise ValueError(f"format must be one of {_VALID_CANDLE_FORMATS}")
        return v

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: str) -> str:
        if v not in _VALID_CANDLE_INTERVALS:
            raise ValueError(f"interval must be one of {_VALID_CANDLE_INTERVALS}")
        return v


class InstrumentMasterExportInput(BaseModel):
    format: str = "csv"
    segment: str | None = None

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        if v not in {"csv", "json", "parquet"}:
            raise ValueError("format must be csv, json, or parquet")
        return v
