"""Local ledger settings and future transport coordinates; no broker client."""
from pathlib import Path
from typing import Literal

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


class BrokerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    bootstrap_servers: str = Field(default="127.0.0.1:19092", min_length=1)
    topic: str = Field(default="robottrust.episodes.v1", min_length=1)

    @field_validator("bootstrap_servers", "topic")
    @classmethod
    def nonblank_setting(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("broker settings must not be blank")
        return value


class ProducerConfig(BrokerConfig):
    delivery_timeout_ms: int = Field(default=10000, strict=True, ge=1000, le=120000)
    flush_timeout_s: float = Field(default=15, gt=0, le=180)

    def client_settings(self) -> dict[str, str | int | bool]:
        return {"bootstrap.servers": self.bootstrap_servers, "client.id": "robottrust-producer",
                "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": self.delivery_timeout_ms,
                "allow.auto.create.topics": False}


class ConsumerConfig(BrokerConfig):
    group_id: str = Field(min_length=1)
    bootstrap_policy: Literal["earliest", "latest", "explicit"] | None = None
    expected_partitions: int = Field(default=3, strict=True, ge=1)
    startup_timeout_s: float = Field(default=30, gt=0, le=120)
    poll_timeout_s: float = Field(default=1, gt=0, le=10)
    socket_timeout_ms: int = Field(default=10000, strict=True, ge=1000, le=60000)

    @field_validator("group_id")
    @classmethod
    def nonblank_group(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("group_id must not be blank")
        return value

    def client_settings(self) -> dict[str, str | int | bool]:
        return {"bootstrap.servers": self.bootstrap_servers, "group.id": self.group_id,
                "enable.auto.commit": False, "enable.auto.offset.store": False,
                "auto.offset.reset": "error", "allow.auto.create.topics": False,
                "group.protocol": "classic", "socket.timeout.ms": self.socket_timeout_ms}
