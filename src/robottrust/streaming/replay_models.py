"""Finite replay contracts, independent of Kafka and live checkpoint state."""
from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, model_validator
from robottrust.streaming.config import SQLITE_MAX_INTEGER


class ReplayFailure(str, Enum):
    EXPIRED = "FAIL_EXPIRED_HISTORY"
    IDENTITY = "FAIL_TOPIC_IDENTITY_MISMATCH"
    TOPOLOGY = "FAIL_PARTITION_TOPOLOGY_CHANGED"
    HISTORY = "FAIL_INCONSISTENT_HISTORY"
    DEADLINE = "FAIL_REPLAY_DEADLINE"
    INTERRUPTED = "FAIL_REPLAY_INTERRUPTED"
    TRANSPORT = "FAIL_REPLAY_TRANSPORT"


class ReplayError(RuntimeError):
    def __init__(self, code: ReplayFailure, reason: str):
        self.code, self.reason = code, reason
        super().__init__(f"{code.value}: {reason}")


class Boundary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    partition: int = Field(strict=True, ge=0)
    start_offset: int = Field(strict=True, ge=0, le=SQLITE_MAX_INTEGER)
    end_offset: int = Field(strict=True, ge=0, le=SQLITE_MAX_INTEGER)

    @model_validator(mode="after")
    def ordered(self) -> Boundary:
        if self.start_offset > self.end_offset:
            raise ValueError("start must not exceed frozen end")
        return self


class ReplayManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    session_id: str = Field(min_length=1)
    topic: str = Field(min_length=1)
    broker_id: str = Field(min_length=1)
    topic_id: str = Field(min_length=1)
    partition_count: int = Field(strict=True, ge=1, le=128)
    boundaries: tuple[Boundary, ...] = Field(min_length=1, max_length=128)
    created_at: str = Field(min_length=1)

    @model_validator(mode="after")
    def partitions(self) -> ReplayManifest:
        ids = [b.partition for b in self.boundaries]
        if len(ids) != len(set(ids)) or any(p >= self.partition_count for p in ids):
            raise ValueError("unique partitions inside topology required")
        return self
