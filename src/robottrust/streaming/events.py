"""Version-1 identity and canonical JSON, independent of delivery metadata.

Identity bytes: UTF-8 JSON array ["robottrust.event-id", 1, run_id, source_id,
episode_id], compact separators and ensure_ascii=True, no trailing newline.
Event ID: "rt-event-v1:" plus the lowercase SHA-256 hex digest of those bytes.
Payload bytes: the complete validated envelope, sorted keys, compact JSON,
ensure_ascii=True, allow_nan=False, and episode timestamp normalized to UTC
with six fractional digits. Numeric fields use the canonical Episode types;
negative float zero becomes 0.0. Identifiers retain exact Unicode code points.
"""
from __future__ import annotations

from datetime import timezone
import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from robottrust.models import Episode
from robottrust.streaming.config import SQLITE_MAX_INTEGER


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def derive_event_id(schema_version: int, run_id: str, source_id: str, episode_id: str) -> str:
    if type(schema_version) is not int or schema_version != 1:
        raise ValueError("unsupported schema_version; expected integer 1")
    for value in (run_id, source_id, episode_id):
        if not isinstance(value, str) or not value.strip():
            raise ValueError("identity components must be nonblank strings")
    identity = _json_bytes(["robottrust.event-id", schema_version, run_id, source_id, episode_id])
    return "rt-event-v1:" + hashlib.sha256(identity).hexdigest()


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: Literal[1]
    event_type: Literal["robot_episode_completed"]
    event_id: str = Field(strict=True, min_length=1)
    run_id: str = Field(strict=True, min_length=1)
    source_id: str = Field(strict=True, min_length=1)
    sequence: int = Field(strict=True, ge=0, le=SQLITE_MAX_INTEGER)
    episode: Episode

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be an integer")
        return value

    @field_validator("run_id", "source_id")
    @classmethod
    def nonblank_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("run_id and source_id must not be blank")
        return value

    @field_validator("episode", mode="before")
    @classmethod
    def copy_episode(cls, value: object) -> object:
        return value.model_dump() if isinstance(value, Episode) else value

    @model_validator(mode="after")
    def verify_identity(self) -> EventEnvelope:
        expected = derive_event_id(self.schema_version, self.run_id, self.source_id, self.episode.episode_id)
        if self.event_id != expected:
            raise ValueError("event_id does not match the derived identity")
        return self


def create_event(episode: Episode, run_id: str, source_id: str, sequence: int) -> EventEnvelope:
    # Revalidate a copy: B1 Episode objects themselves permit later mutation.
    return EventEnvelope.model_validate(dict(schema_version=1, event_type="robot_episode_completed",
        event_id=derive_event_id(1, run_id, source_id, episode.episode_id), run_id=run_id,
        source_id=source_id, sequence=sequence, episode=episode.model_dump()))


def canonical_payload_bytes(event: EventEnvelope) -> bytes:
    validated = EventEnvelope.model_validate(event.model_dump())
    payload = validated.model_dump(mode="json")
    payload["episode"]["timestamp"] = validated.episode.timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds")
    for key, value in payload["episode"].items():
        if isinstance(value, float) and value == 0:
            payload["episode"][key] = 0.0
    return _json_bytes(payload)


def canonical_payload_hash(event: EventEnvelope) -> str:
    return hashlib.sha256(canonical_payload_bytes(event)).hexdigest()


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def decode_event_json(raw: bytes) -> object:
    """Reject ambiguous duplicate JSON keys rather than choosing the last one."""
    return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_keys)
