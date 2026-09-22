"""Durable SQLite transactions and invariants; no broker or simulated Kafka."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import sqlite3
from threading import Barrier

import pytest
from pydantic import ValidationError

from robottrust.evaluator import evaluate
from robottrust.generator import generate_episodes
from robottrust.streaming.config import LedgerConfig, TransportPosition
from robottrust.streaming.events import EventEnvelope, canonical_payload_hash, create_event
from robottrust.streaming.store import Disposition, IngestionStore


@pytest.fixture
def event():
    return create_event(next(generate_episodes(1, 42)), "run-a", "source-a", 0)


@pytest.fixture
def path(tmp_path):
    return tmp_path / "ingestion.sqlite3"


@pytest.fixture
def store(path):
    with IngestionStore(path) as ledger:
        yield ledger


def position(topic="episodes", partition=0, offset=0):
    return TransportPosition(topic=topic, partition=partition, offset=offset)


def test_first_ingest_accepted(store, event):
    result = store.ingest(event)
    assert result.disposition == Disposition.ACCEPTED
    assert result.event_id == event.event_id
    assert result.payload_hash == canonical_payload_hash(event)
    assert datetime.fromisoformat(result.ingested_at).utcoffset().total_seconds() == 0
    assert store.lookup_event(event.event_id) == event


def test_exact_retry_duplicate(store, event):
    first = store.ingest(event)
    retry = store.ingest(event.model_dump(mode="json"))
    assert retry.disposition == Disposition.DUPLICATE
    assert retry.receipt_id != first.receipt_id
    assert store.accepted_events(event.run_id) == [event]


def test_same_identity_different_payload_conflict(store, event):
    store.ingest(event)
    changed = EventEnvelope.model_validate({**event.model_dump(), "sequence": 1})
    result = store.ingest(changed)
    assert result.disposition == Disposition.CONFLICT
    assert result.rejection_reason
    assert store.lookup_event(event.event_id) == event


def test_episode_collision_within_run_fails_closed(store, event):
    store.ingest(event)
    colliding = create_event(event.episode, event.run_id, "source-b", 0)
    assert colliding.event_id != event.event_id
    assert store.ingest(colliding).disposition == Disposition.CONFLICT
    assert store.lookup_event(colliding.event_id) is None
    assert store.accepted_events(event.run_id) == [event]


def test_same_episode_in_separate_run_allowed(store, event):
    store.ingest(event)
    other = create_event(event.episode, "run-b", event.source_id, 0)
    assert store.ingest(other).disposition == Disposition.ACCEPTED


def test_position_key_includes_topic(store, event):
    store.ingest(event, position("topic-a"), checkpoint_next_offset=1)
    other = create_event(event.episode, "run-b", event.source_id, 0)
    assert store.ingest(other, position("topic-b"), checkpoint_next_offset=1).disposition == Disposition.ACCEPTED
    assert store.lookup_position(position("topic-a")).event_id == event.event_id
    assert store.lookup_position(position("topic-b")).event_id == other.event_id
    assert store.latest_checkpoint("topic-a", 0) == store.latest_checkpoint("topic-b", 0) == 1


def test_same_position_retry_preserves_original_receipt(store, event, path):
    first = store.ingest(event, position())
    assert store.ingest(event, position()).disposition == Disposition.DUPLICATE
    assert store.lookup_position(position()) == first
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM transport_positions").fetchone()[0] == 1
    assert store.counts_by_disposition()[Disposition.DUPLICATE] == 1


def test_position_reuse_with_different_event_conflicts(store, event):
    first = store.ingest(event, position())
    other = create_event(event.episode, "run-b", event.source_id, 0)
    assert store.ingest(other, position()).disposition == Disposition.CONFLICT
    assert store.lookup_position(position()) == first
    assert store.lookup_event(other.event_id) is None


def test_duplicate_at_new_offset_is_audited(store, event):
    store.ingest(event, position(offset=0), checkpoint_next_offset=1)
    assert store.ingest(event, position(offset=1), checkpoint_next_offset=2).disposition == Disposition.DUPLICATE
    assert store.latest_checkpoint("episodes", 0) == 2
    assert len(store.accepted_events(event.run_id)) == 1


@pytest.mark.parametrize("raw", [b"not-json", b"\xff", b"{}", b"null", b'{"run_id":"a","run_id":"b"}'])
def test_malformed_payload_durably_rejected(store, raw, path):
    result = store.ingest(raw, position(), checkpoint_next_offset=1)
    assert result.disposition == Disposition.REJECTED
    assert result.payload_hash is None
    assert result.rejection_reason
    assert store.latest_checkpoint("episodes", 0) == 1
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT raw_payload FROM ingest_receipts").fetchone()[0] == raw


def test_invalid_episode_durably_rejected(store, event):
    payload = event.model_dump(mode="json")
    payload["episode"]["sensor_dropout_rate"] = 2
    result = store.ingest(payload)
    assert result.disposition == Disposition.REJECTED
    assert result.event_id == event.event_id
    assert store.lookup_event(event.event_id) is None


def test_unsupported_schema_durably_rejected(store, event):
    assert store.ingest({**event.model_dump(mode="json"), "schema_version": 2}).disposition == Disposition.REJECTED


def test_disposition_counters_and_run_filter(store, event):
    store.ingest(event)
    store.ingest(event)
    store.ingest(EventEnvelope.model_validate({**event.model_dump(), "sequence": 1}))
    store.ingest({**event.model_dump(mode="json"), "event_type": "invalid"})
    assert store.counts_by_disposition() == {d: 1 for d in Disposition}
    assert store.counts_by_disposition(event.run_id) == {d: 1 for d in Disposition}
    assert store.counts_by_disposition("absent") == {d: 0 for d in Disposition}


def test_deterministic_order_and_b1_evaluation(store):
    episodes = list(generate_episodes(5, 42))
    events = [create_event(episode, "run", source, seq) for episode, source, seq in
              zip(episodes, ["b", "a", "a", "a", "b"], [0, 2, 0, 0, 1])]
    for event in reversed(events):
        store.ingest(event)
    expected = sorted(events, key=lambda e: (e.source_id, e.sequence, e.episode.episode_id, e.event_id))
    assert store.accepted_events("run") == expected
    assert evaluate([e.episode for e in store.accepted_events("run")]) == evaluate([e.episode for e in expected])


def test_wal_active(store):
    assert store.durability_settings()["journal_mode"] == "wal"


def test_synchronous_full(store):
    assert store.durability_settings()["synchronous"] == 2


def test_foreign_keys_enabled(store):
    assert store.durability_settings()["foreign_keys"] == 1


def test_reopen_preserves_records_checkpoints_and_settings(path, event):
    with IngestionStore(path) as ledger:
        first = ledger.ingest(event, position(), checkpoint_next_offset=1)
    with IngestionStore(path) as ledger:
        assert ledger.accepted_events(event.run_id) == [event]
        assert ledger.lookup_position(position()) == first
        assert ledger.latest_checkpoint("episodes", 0) == 1
        assert ledger.durability_settings() == {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1}
        assert ledger.ingest(event).disposition == Disposition.DUPLICATE


@pytest.mark.parametrize("operation", ["INSERT", "UPDATE"])
def test_transaction_rollback_leaves_no_partial_state(path, event, operation):
    with IngestionStore(path) as ledger:
        if operation == "UPDATE":
            old = create_event(event.episode, "old-run", event.source_id, 0)
            ledger.ingest(old, position(offset=0), checkpoint_next_offset=1)
        prior_counts = ledger.counts_by_disposition()
        prior_checkpoint = ledger.latest_checkpoint("episodes", 0)
        with sqlite3.connect(path) as db:
            # Fail on the last write, after acceptance, audit and position writes.
            db.execute(f"CREATE TRIGGER fail_checkpoint BEFORE {operation} ON checkpoints BEGIN SELECT RAISE(ABORT, 'injected checkpoint failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match="injected checkpoint failure"):
            ledger.ingest(event, position(offset=1), checkpoint_next_offset=2)
        assert ledger.lookup_event(event.event_id) is None
        assert ledger.accepted_events(event.run_id) == []
        assert ledger.lookup_position(position(offset=1)) is None
        assert ledger.counts_by_disposition() == prior_counts
        assert ledger.latest_checkpoint("episodes", 0) == prior_checkpoint
        with sqlite3.connect(path) as db:
            db.execute("DROP TRIGGER fail_checkpoint")
    with IngestionStore(path) as ledger:
        assert ledger.lookup_event(event.event_id) is None
        assert ledger.counts_by_disposition() == prior_counts
        assert ledger.ingest(event, position(offset=1), checkpoint_next_offset=2).disposition == Disposition.ACCEPTED


def test_checkpoint_is_explicit_not_inferred(store, event):
    store.ingest(event, position(offset=100))
    assert store.latest_checkpoint("episodes", 0) is None
    assert store.lookup_position(position(offset=100)) is not None


def test_checkpoint_cannot_rewind(store, event):
    store.ingest(event, position(offset=5), checkpoint_next_offset=6)
    with pytest.raises(ValueError, match="backwards"):
        store.ingest(event, position(offset=0), checkpoint_next_offset=1)
    assert store.counts_by_disposition()[Disposition.DUPLICATE] == 0
    assert store.latest_checkpoint("episodes", 0) == 6


@pytest.mark.parametrize("coordinates,next_offset", [(None, 1), (position(), 2), (position(), True)])
def test_invalid_checkpoint_has_no_writes(store, event, coordinates, next_offset):
    with pytest.raises(ValueError):
        store.ingest(event, coordinates, checkpoint_next_offset=next_offset)
    assert store.counts_by_disposition() == {d: 0 for d in Disposition}


def test_concurrent_retries_accept_only_once(path, event):
    with IngestionStore(path):
        pass
    barrier = Barrier(2)
    def writer():
        with IngestionStore(path) as ledger:
            barrier.wait(timeout=10)
            return ledger.ingest(event).disposition
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: writer(), range(2)))
    assert sorted(d.value for d in results) == ["ACCEPTED", "DUPLICATE"]
    with IngestionStore(path) as ledger:
        assert ledger.accepted_events(event.run_id) == [event]


def test_closed_store_rejects_access(path, event):
    store = IngestionStore(path)
    store.close()
    store.close()
    with pytest.raises(RuntimeError, match="closed"):
        store.lookup_event(event.event_id)


@pytest.mark.parametrize("path_value", ["", ":memory:"])
def test_in_memory_or_empty_path_rejected(path_value):
    with pytest.raises(ValidationError):
        LedgerConfig(database_path=path_value)


@pytest.mark.parametrize("fields", [{"topic": " "}, {"partition": -1}, {"offset": -1}, {"offset": True}])
def test_invalid_transport_position(fields):
    with pytest.raises(ValidationError):
        TransportPosition.model_validate({"topic": "episodes", "partition": 0, "offset": 0, **fields})


def test_unrelated_database_rejected(path):
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
    with pytest.raises(RuntimeError, match="unrelated"):
        IngestionStore(path)
