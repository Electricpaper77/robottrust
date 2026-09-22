"""Local ledger settings and future transport coordinates; no broker client."""
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

SQLITE_MAX_INTEGER = 2**63 - 1


class LedgerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    database_path: Path
    busy_timeout_s: float = Field(default=5.0, gt=0, le=60)

    @field_validator("database_path")
    @classmethod
    def durable_path(cls, value: Path) -> Path:
        if str(value) in (".", ":memory:"):
            raise ValueError("a persistent database file path is required")
        return value


class TransportPosition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    topic: str = Field(strict=True, min_length=1)
    partition: int = Field(strict=True, ge=0, le=SQLITE_MAX_INTEGER)
    offset: int = Field(strict=True, ge=0, lt=SQLITE_MAX_INTEGER)

    @field_validator("topic")
    @classmethod
    def nonblank_topic(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("topic must not be blank")
        return value
