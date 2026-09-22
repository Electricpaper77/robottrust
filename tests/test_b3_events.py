"""Canonical identity and payload contract, without transport dependencies."""
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest
from pydantic import ValidationError

from robottrust.generator import generate_episodes
from robottrust.models import Episode
from robottrust.streaming.events import (EventEnvelope, canonical_payload_bytes,
                                        canonical_payload_hash, create_event, derive_event_id)


@pytest.fixture
def episode():
    record = next(generate_episodes(1, 42)).model_dump()
    record["episode_id"] = "ep-1"
    return Episode.model_validate(record)


@pytest.fixture
def event(episode):
    return create_event(episode, "run-a", "source-a", 0)


def test_deterministic_event_id():
    identity = b'["robottrust.event-id",1,"run-a","source-a","ep-1"]'
    expected = "rt-event-v1:" + hashlib.sha256(identity).hexdigest()
    assert derive_event_id(1, "run-a", "source-a", "ep-1") == expected
    assert derive_event_id(1, "run-a", "source-a", "ep-1") == expected


def test_event_id_changes_with_run(event):
    assert derive_event_id(1, "run-b", event.source_id, event.episode.episode_id) != event.event_id


def test_event_id_changes_with_source(event):
    assert derive_event_id(1, event.run_id, "source-b", event.episode.episode_id) != event.event_id


def test_event_id_changes_with_episode(event):
    assert derive_event_id(1, event.run_id, event.source_id, "ep-2") != event.event_id


def test_identity_encoding_has_unambiguous_boundaries():
    assert derive_event_id(1, "a/b", "c", "d") != derive_event_id(1, "a", "b/c", "d")


def test_valid_envelope_reuses_episode(event):
    assert isinstance(event.episode, Episode)
    assert EventEnvelope.model_validate_json(event.model_dump_json()) == event


def test_payload_hash_deterministic_and_key_order_independent(event):
    reordered = dict(reversed(list(event.model_dump(mode="json").items())))
    other = EventEnvelope.model_validate_json(json.dumps(reordered, indent=4))
    assert canonical_payload_hash(event) == canonical_payload_hash(other)
    assert canonical_payload_hash(event) == hashlib.sha256(canonical_payload_bytes(event)).hexdigest()
    assert not canonical_payload_bytes(event).endswith(b"\n")


def test_equivalent_timezones_have_same_payload_hash(event):
    alternate = event.model_dump(mode="json")
    alternate["episode"]["timestamp"] = "2025-12-31T16:00:00-08:00"
    assert canonical_payload_hash(event) == canonical_payload_hash(EventEnvelope.model_validate(alternate))
    assert json.loads(canonical_payload_bytes(event))["episode"]["timestamp"] == "2026-01-01T00:00:00.000000+00:00"


def test_numeric_equivalence(event):
    first = event.model_dump(mode="json")
    first["episode"]["duration_ms"] = 0
    second = event.model_dump(mode="json")
    second["episode"]["duration_ms"] = -0.0
    assert canonical_payload_hash(EventEnvelope.model_validate(first)) == canonical_payload_hash(EventEnvelope.model_validate(second))


def test_sequence_changes_payload_not_identity(event):
    changed = EventEnvelope.model_validate({**event.model_dump(), "sequence": 1})
    assert changed.event_id == event.event_id
    assert canonical_payload_hash(changed) != canonical_payload_hash(event)


def test_episode_content_changes_payload_not_identity(event):
    record = event.model_dump()
    record["episode"]["duration_ms"] += 1
    changed = EventEnvelope.model_validate(record)
    assert changed.event_id == event.event_id
    assert canonical_payload_hash(changed) != canonical_payload_hash(event)


@pytest.mark.parametrize("value", [2, "1", True, 1.0])
def test_unsupported_or_noninteger_schema(event, value):
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate({**event.model_dump(), "schema_version": value})


def test_unsupported_event_type(event):
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate({**event.model_dump(), "event_type": "robot_started"})


@pytest.mark.parametrize("field,value", [("run_id", ""), ("source_id", ""), ("run_id", "  "),
                                         ("source_id", "\t"), ("sequence", -1), ("sequence", True),
                                         ("sequence", 1.5), ("sequence", 2**63)])
def test_invalid_identity_or_sequence(event, field, value):
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate({**event.model_dump(), field: value})


def test_incorrect_derived_id_rejected(event):
    with pytest.raises(ValidationError, match="derived identity"):
        EventEnvelope.model_validate({**event.model_dump(), "event_id": "claimed"})


def test_invalid_episode_rejected(event):
    record = event.model_dump()
    record["episode"]["collision_count"] = -1
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(record)


def test_mutated_episode_instance_revalidated(event):
    event.episode.duration_ms = -1
    with pytest.raises(ValidationError):
        canonical_payload_hash(event)
    with pytest.raises(ValidationError):
        create_event(event.episode, "run-a", "source-a", 0)


@pytest.mark.parametrize("field", ["offset", "retry_timestamp", "ingestion_timestamp"])
def test_transport_fields_forbidden_in_envelope(event, field):
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate({**event.model_dump(), field: 1})


def test_unicode_identity_is_deterministic_and_not_normalized(episode):
    composed = create_event(episode, "caf\u00e9", "robot", 0)
    decomposed = create_event(episode, "cafe\u0301", "robot", 0)
    assert composed.event_id != decomposed.event_id
    assert canonical_payload_bytes(composed).isascii()
