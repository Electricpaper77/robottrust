# B3.1 durable ingestion core

This milestone implements transport-independent envelopes and a local SQLite ledger. No Kafka client, Redpanda, consumer, producer, Compose service or B3.2 implementation is included. B1/B2 evaluators and package dependencies remain unchanged.

## Event contract

EventEnvelope requires schema_version (integer 1), event_type (robot_episode_completed), event_id, run_id, source_id, sequence and the existing canonical Episode. Extra envelope fields, blank identifiers, unsupported versions/types, incorrect event IDs and invalid Episode data fail validation. Sequence is a nonnegative signed-64-bit SQLite integer. The factory copies and revalidates mutable B1 Episode instances; canonical hashing also revalidates its input.

Identity encoding v1 is the UTF-8 representation of the compact JSON array:

```json
["robottrust.event-id",1,"run_id","source_id","episode_id"]
```

JSON uses ensure_ascii=True, separators=(",", ":") and no trailing newline. derive_event_id returns rt-event-v1: followed by the lowercase SHA-256 of these exact bytes. This domain-separated encoding avoids delimiter ambiguity. Identifiers use exact Unicode code points without Unicode normalization or whitespace trimming; blank identifiers are invalid.

Canonical payload hashing uses the entire validated envelope, including sequence and episode content, with sorted JSON keys, compact separators, ASCII escaping and finite numbers only. The episode timestamp is normalized to UTC with exactly six fractional digits and +00:00; B1 numeric validation supplies numeric types, and negative floating zero normalizes to 0.0. No ingestion timestamp, retry timestamp or transport coordinate enters either identity or payload hash. Equivalent timezone representations and JSON key ordering produce the same hash. Changing sequence or episode content preserves identity but changes payload hash. This is a documented RobotTrust v1 encoding, not a claim to implement RFC 8785.

## Store API

```python
from robottrust.generator import generate_episodes
from robottrust.streaming.events import create_event
from robottrust.streaming.config import TransportPosition
from robottrust.streaming.store import IngestionStore

episode = next(generate_episodes(1, seed=42))
event = create_event(episode, run_id="run-42", source_id="synthetic", sequence=0)
with IngestionStore("work/ingestion.sqlite3") as store:
    result = store.ingest(event)  # ACCEPTED, then DUPLICATE on an exact retry
    accepted = store.accepted_events("run-42")
    counts = store.counts_by_disposition("run-42")
    original = store.lookup_event(event.event_id)
```

The store accepts envelopes, JSON-compatible mappings, JSON strings or UTF-8 bytes. Unsupported Python objects or non-JSON-compatible mappings raise before writes. Malformed/invalid raw inputs that can be represented durably produce REJECTED receipts containing original bytes, raw SHA-256, recoverable identity fields and a reason. Their canonical payload_hash is null: invalid input is not assigned a valid canonical content hash. Duplicate JSON object keys are rejected. A valid event may be ingested after an earlier rejection; rejections never reserve logical acceptance identity.

Disposition rules:

- ACCEPTED: first valid logical event without a run/episode collision.
- DUPLICATE: an already accepted event_id with identical canonical payload hash.
- CONFLICT: accepted identity with changed content, a distinct event claiming an already accepted (run_id, episode_id), or reuse of a transport position for different content.
- REJECTED: invalid input with sufficient raw evidence to persist classification.

Accepted rows are inserted only and never updated by the API. A conflicting candidate is not accepted. lookup_event returns the accepted original, or None; accepted_events returns only accepted envelopes. Each ingestion call produces an append-only audit receipt, so disposition counts describe committed ingestion calls, including duplicate observations, rather than unique transport deliveries. Unparseable rejections without run identity appear in global counters, not a run-filtered count.

Accepted events are ordered by source_id (SQLite BINARY), numeric sequence, episode_id (BINARY), then event_id (BINARY). Sequence gaps and equal sequences are permitted in B3.1; ordering and completion guarantees for a transport stream are future work. Evaluators receive [event.episode for event in accepted_events(run_id)] and retain the B1 duplicate episode invariant. Runs are separate evaluation scopes.

## SQLite durability and transactions

Use sqlite3 only, with journal_mode=WAL, synchronous=FULL and foreign_keys=ON verified on the active connection. LedgerConfig requires a file path and bounds SQLite lock waiting (default 5 seconds, maximum 60). Instances are single-threaded; independent connections serialize writers with BEGIN IMMEDIATE. Unrelated or unsupported-version databases fail closed.

Tables separate immutable accepted_events, append-only ingest_receipts, unique transport_positions and checkpoints. accepted_events enforces UNIQUE(run_id, episode_id) and PRIMARY KEY(event_id). transport_positions uses PRIMARY KEY(topic, partition, offset), with a foreign key to its first audit receipt. Re-observing that position adds an audit receipt but not a second position row or a replacement first binding. A different payload at that position is CONFLICT, even if its logical event would otherwise be new.

One explicit transaction persists acceptance if applicable, the disposition receipt, first transport binding and any supplied checkpoint. Exceptions roll back all writes and propagate; database failures never masquerade as a successful disposition. Tests inject SQLite failures at checkpoint insertion and update, after earlier writes have executed, then verify no partial state survives or reappears on reopen.

## Explicit checkpoint contract

TransportPosition is only a typed coordinate: it has no Kafka dependency. Its three required fields are topic, partition and offset. Numeric values must fit SQLite; offsets reserve room for offset+1.

store.ingest(event, position, checkpoint_next_offset=position.offset + 1) requests an atomic checkpoint update. Checkpoints contain the NEXT offset, are scoped by (topic, partition), and cannot move backwards. Supplying a position alone records that position but does not infer a checkpoint. latest_checkpoint returns the next offset or None. lookup_position returns the original receipt for a coordinate.

The caller must establish that an explicitly supplied checkpoint represents its completely processed prefix. B3.1 cannot prove broker offset continuity, topic generation, ordering or retention. It does not infer completeness from the highest observed offset. Rejected/conflicting dispositions can be checkpointed only through the same explicit caller request. Durable classification is not successful episode acceptance.

## Boundaries and validation

Tests cover canonical encoding, identity/content conflicts, raw rejection evidence, topic-qualified coordinates, explicit checkpoints, rollback, ordering, concurrent retries and database reopen. Existing B1/B2 tests remain unchanged. No synthetic Kafka integration tests are claimed.

WAL/FULL is local durability, not replication or an exactly-once broker guarantee. Keep the database and WAL files on suitable local storage; use SQLite-aware backup rather than copying only the main file while live. No JSONL exporter, broker acknowledgement, retention policy, compaction, schema migration or consumer recovery loop is implemented in B3.1. Topic recreation must be addressed before transport integration. State should live under ignored work/ for local development, not be committed.

## Local B3.1 validation

Validated with the project-local Python 3.11.9 virtual environment and SQLite 3.45.1. Focused B3.1 tests: 69 passed. Full suite: 158 passed, including all unchanged 89 B1/B2 cases. Existing dependency warnings remain visible. Imports and Python wheel build passed. Tests used the requested work/pytest base directory, with TEMP and TMP under work/tmp. No broker, Kafka client or Compose service was installed or started.
