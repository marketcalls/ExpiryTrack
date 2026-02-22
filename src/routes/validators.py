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


class CollectInput(BaseModel):
    instruments: list[str] = Field(min_length=1)
    expiries: dict[str, list[str]] = Field(default_factory=dict)
    contract_type: str = "both"
    interval: str = "1minute"
    workers: int = 5


class ExportInput(BaseModel):
    format: str = "csv"
    instruments: list[str] = Field(default_factory=list)
    expiries: dict[str, list[str]] = Field(default_factory=dict)
    options: dict = Field(default_factory=dict)

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        allowed = {"csv", "json", "zip", "parquet"}
        if v not in allowed:
            raise ValueError(f"format must be one of {allowed}")
        return v


class WatchlistInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    segment: str | None = None
