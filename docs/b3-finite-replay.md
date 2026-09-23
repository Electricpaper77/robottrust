# B3.3.2 finite replay and idempotent re-ingestion

Replay is a separate, finite session against an existing ledger with verified live partition provenance. It does not initialize a fresh ledger for reconstruction. Canonical event identity, validation and B1/B2 evaluation are unchanged. Live and replay consumers now share the transport validation function; both ultimately use the same B3.1 preparation/classification and immutable acceptance rules.

## Frozen history

Capture records broker cluster ID, topic ID, topic name, expected partition topology, selected partitions, UTC creation time and per-partition [start,end) offsets. The default start is earliest retained; API callers can request explicit starts and a subset of partitions. Ends are broker high-water offsets captured before replay, never extended when new data arrives. Capture is a per-partition vector, not a globally atomic broker snapshot.

The vector is committed to SQLite before assignment/polling. The replay client uses a dedicated generated group, manual assignment, explicit seeks, disabled auto commit/offset storage and auto.offset.reset=error. Replay never calls the Kafka commit API. The group is separate from live ingestion even though manual assignment does not join the live group.

Every step revalidates identity, topology and watermarks before polling and again before durable ingestion. A requested start below the current low watermark fails with FAIL_EXPIRED_HISTORY; a shrinking end fails inconsistent history. Identity/topology changes fail explicitly. Even already-traversed starts are checked conservatively until the whole session finishes. Required history is never silently reset to a newer start.

Completion is per partition. A durable last in-range record, a delivered boundary-or-later record, or a partition EOF at/beyond the frozen end provides traversal evidence. An initially empty interval is complete by construction. Empty polls never complete a partition. Offset gaps are allowed: the engine follows ordered broker delivery and EOF rather than equating offset distance with record count. Completed partitions are paused only to end their finite traversal, not as a backpressure system. All selected partitions must finish before the session becomes COMPLETE. A deadline failure or explicit close leaves a FAILED session with a reason; abrupt process termination can leave ACTIVE state, which is still incomplete. Reopening preserves this state; automatic session resumption is not implemented.

## Durable separation and evidence

Schema version 3 adds replay_sessions, replay_partitions and replay_observations, preserving v1/v2 data through deterministic migrations. Every replay observation, its B3.1 receipt and replay progress commit together. Live starting boundaries, live next offsets and legacy checkpoints are never written by replay. Transport-position uniqueness remains (topic,partition,offset). Existing position bindings remain immutable; observations and audit receipts are separate counts.

Identical accepted history classifies as DUPLICATE and adds zero accepted events. Conflicts/rejections remain auditable. Previously uncovered retained events may be accepted into an existing ledger; zero accepted delta is guaranteed only for already-ingested identical history. Cross-partition receipt interleaving is not a global event order, and this feature does not reconstruct conflicting first-arrival winners into a new ledger.

Replay records raw key bytes as nullable base64, explicit payload-null presence, raw payload base64/hash, event identity/hash where valid, disposition and actual coordinates. Empty bytes and Kafka tombstones remain distinguishable. Evidence JSONL is streamed via keyset pages (default 100, maximum 1000 rows); accepted ID/hash access is also paginated. Counts use SQL aggregates. There is no growing replay result list. Individual broker records and client buffers still consume memory; full byte-based backpressure is deferred.

Use exclusive ownership of the local ledger during replay. Publication to the broker may continue. The session snapshots and verifies live checkpoint state, but this is not a multi-process ledger lock or a distributed ownership protocol. Live group offsets are externally compared in the real integration test.

## Commands

```powershell
# Existing ledger must already contain live provenance for the selected topic.
.\.venv\Scripts\python.exe -m robottrust.streaming.replay --ledger work/existing.sqlite3 --output-dir work/replay-evidence --topic robottrust.episodes.v1 --timeout 60

# Self-contained integration: populate a new test ledger by LIVE ingestion first.
docker compose up -d --wait --wait-timeout 180
.\.venv\Scripts\python.exe scripts/b3_replay_integration.py --ledger work/new-replay-test.sqlite3 --output-dir work/replay-test-evidence
docker compose stop
```

The integration uses an isolated UUID-named three-partition topic on the pinned local Redpanda broker, replication factor one. It live-ingests 12 valid events plus a duplicate, conflict and three invalid/tombstone/empty records. It freezes ends, begins replay, then publishes three additional records. Assertions verify 17 original observations, 13 duplicates, one conflict, three rejections, zero new accepted/missing/unexpected events, all three later events excluded, exact accepted IDs/hashes preserved, and unchanged live SQLite/broker offsets. Explicit producer partitions guarantee fixture coverage; all deliveries use real checked broker acknowledgements.

Checked-in evidence/b3_3_2 contains session.json, observations.jsonl, integration.json and SHA-256 checksums. Earlier evidence is untouched. CI preserves the Python matrix and runs the finite scenario in the separate real-broker job with existing transport and startup-reconciliation checks. No reconstruction, queue backpressure, subprocess crash testing, exactly-once or HA claim is included.
