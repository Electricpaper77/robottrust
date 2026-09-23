"""SQLite acceptance ledger and append-only disposition audit.

One BEGIN IMMEDIATE transaction records each ingest call, immutable acceptance,
first transport receipt, and any explicit checkpoint. Checkpoints are NEXT
offsets, not inferred high-water marks; the future transport caller must assert
that the supplied position completes its processed prefix. No broker is used.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import Enum
import base64
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from robottrust.streaming.config import LedgerConfig, SQLITE_MAX_INTEGER, TransportPosition
from robottrust.streaming.replay_models import ReplayManifest
from robottrust.streaming.reconciliation import LiveCheckpoint, Provenance, RecoveryDecision
from robottrust.streaming.events import EventEnvelope, canonical_payload_bytes, decode_event_json


class Disposition(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class IngestResult:
    receipt_id: int
    disposition: Disposition
    event_id: str | None
    payload_hash: str | None
    rejection_reason: str | None
    ingested_at: str


@dataclass(frozen=True)
class _Prepared:
    raw: bytes
    raw_hash: str
    metadata: dict[str, Any]
    event: EventEnvelope | None
    payload: bytes | None
    payload_hash: str | None
    rejection_reason: str | None

    @property
    def fingerprint(self) -> str:
        return "event:" + self.payload_hash if self.payload_hash is not None else "raw:" + self.raw_hash


_SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS accepted_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    episode_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    event_type TEXT NOT NULL CHECK(event_type = 'robot_episode_completed'),
    payload_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    UNIQUE(run_id, episode_id)
);
CREATE TABLE IF NOT EXISTS ingest_receipts (
    receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT, run_id TEXT, source_id TEXT, sequence INTEGER, episode_id TEXT,
    payload_hash TEXT, schema_version INTEGER, event_type TEXT,
    disposition TEXT NOT NULL CHECK(disposition IN ('ACCEPTED','DUPLICATE','CONFLICT','REJECTED')),
    rejection_reason TEXT,
    topic TEXT, partition INTEGER, offset INTEGER,
    ingested_at TEXT NOT NULL,
    raw_payload BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL,
    CHECK((topic IS NULL AND partition IS NULL AND offset IS NULL)
       OR (topic IS NOT NULL AND partition >= 0 AND offset >= 0))
);
CREATE INDEX IF NOT EXISTS receipts_run ON ingest_receipts(run_id, disposition);
CREATE TABLE IF NOT EXISTS transport_positions (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK(partition >= 0),
    offset INTEGER NOT NULL CHECK(offset >= 0),
    fingerprint TEXT NOT NULL,
    receipt_id INTEGER NOT NULL REFERENCES ingest_receipts(receipt_id),
    PRIMARY KEY(topic, partition, offset)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK(partition >= 0),
    next_offset INTEGER NOT NULL CHECK(next_offset >= 0),
    receipt_id INTEGER NOT NULL REFERENCES ingest_receipts(receipt_id),
    PRIMARY KEY(topic, partition)
);
PRAGMA user_version=1;
COMMIT;
"""


_PROVENANCE_SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE live_partitions (
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL CHECK(partition >= 0),
    broker_id TEXT NOT NULL,
    topic_id TEXT NOT NULL,
    partition_count INTEGER NOT NULL CHECK(partition_count > partition),
    mode TEXT NOT NULL CHECK(mode = 'LIVE'),
    start_offset INTEGER NOT NULL CHECK(start_offset >= 0),
    next_offset INTEGER NOT NULL CHECK(next_offset >= start_offset),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(topic, partition)
);
CREATE TABLE startup_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    result_json TEXT NOT NULL,
    status TEXT NOT NULL
);
PRAGMA user_version=2;
COMMIT;
"""


_REPLAY_SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE replay_sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    manifest_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('ACTIVE','COMPLETE','FAILED')),
    failure_reason TEXT,
    updated_at TEXT NOT NULL,
    accepted_before INTEGER NOT NULL,
    accepted_after INTEGER,
    live_before_json TEXT NOT NULL,
    live_after_json TEXT
);
CREATE TABLE replay_partitions (
    session_id TEXT NOT NULL REFERENCES replay_sessions(session_id),
    partition INTEGER NOT NULL,
    start_offset INTEGER NOT NULL,
    end_offset INTEGER NOT NULL CHECK(end_offset >= start_offset),
    next_offset INTEGER NOT NULL CHECK(next_offset >= start_offset AND next_offset <= end_offset),
    complete INTEGER NOT NULL CHECK(complete IN (0,1)),
    completion_proof TEXT,
    PRIMARY KEY(session_id, partition)
);
CREATE TABLE replay_observations (
    session_id TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    receipt_id INTEGER NOT NULL REFERENCES ingest_receipts(receipt_id),
    message_key BLOB,
    payload_is_null INTEGER NOT NULL CHECK(payload_is_null IN (0,1)),
    PRIMARY KEY(session_id,partition,offset),
    FOREIGN KEY(session_id,partition) REFERENCES replay_partitions(session_id,partition)
);
CREATE INDEX replay_receipts ON replay_observations(session_id,receipt_id);
PRAGMA user_version=3;
COMMIT;
"""


def _prepare(value: EventEnvelope | Mapping[str, Any] | bytes | str) -> _Prepared:
    if isinstance(value, EventEnvelope):
        raw = value.model_dump_json().encode("utf-8")
    elif isinstance(value, bytes):
        raw = value
    elif isinstance(value, str):
        raw = value.encode("utf-8", errors="surrogatepass")
    elif isinstance(value, Mapping):
        # JSON-compatible mappings only. Invalid JSON numeric constants remain
        # available as raw evidence and are rejected by the canonical schema.
        raw = json.dumps(dict(value), ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    else:
        raise TypeError("expected an EventEnvelope, JSON-compatible mapping, UTF-8 bytes or JSON string")
    parsed: object = None
    event = None
    payload = None
    reason = None
    try:
        parsed = decode_event_json(raw)
        event = EventEnvelope.model_validate(parsed)
        payload = canonical_payload_bytes(event)
    except (ValueError, TypeError, OverflowError) as exc:
        event, payload = None, None
        reason = f"invalid event: {exc}"
    metadata: dict[str, Any] = {}
    if isinstance(parsed, dict):
        for key in ("event_id", "run_id", "source_id", "event_type"):
            metadata[key] = parsed.get(key) if isinstance(parsed.get(key), str) else None
        for key in ("schema_version", "sequence"):
            val = parsed.get(key)
            metadata[key] = val if type(val) is int and -SQLITE_MAX_INTEGER <= val <= SQLITE_MAX_INTEGER else None
        episode = parsed.get("episode")
        metadata["episode_id"] = episode.get("episode_id") if isinstance(episode, dict) and isinstance(episode.get("episode_id"), str) else None
    return _Prepared(raw, hashlib.sha256(raw).hexdigest(), metadata, event, payload,
                     hashlib.sha256(payload).hexdigest() if payload is not None else None, reason)


class IngestionStore:
    """File-backed, single-thread-per-instance ledger; separate writers serialize.

    Repeated transport delivery creates a new audit receipt but never a second
    transport_positions row. counts_by_disposition counts committed ingest calls,
    not unique broker positions. Accepted queries return only immutable originals.
    """
    def __init__(self, config: LedgerConfig | str | Path):
        self.config = config if isinstance(config, LedgerConfig) else LedgerConfig(database_path=config)
        path = self.config.database_path.expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: sqlite3.Connection | None = sqlite3.connect(
            path, timeout=self.config.busy_timeout_s, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        try:
            db = self._db()
            version = db.execute("PRAGMA user_version").fetchone()[0]
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            expected = {"accepted_events", "ingest_receipts", "transport_positions", "checkpoints"}
            if (version not in (0, 1, 2, 3) or (version == 0 and tables) or
                    (version == 1 and tables != expected) or
                    (version == 2 and tables != expected | {"live_partitions", "startup_actions"}) or
                    (version == 3 and tables != expected | {"live_partitions", "startup_actions", "replay_sessions", "replay_partitions", "replay_observations"})):
                raise RuntimeError("unsupported or unrelated ledger schema")
            mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            if mode.lower() != "wal" or self.durability_settings() != {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1}:
                raise RuntimeError("required SQLite durability settings are unavailable")
            if version == 0:
                db.executescript(_SCHEMA)
            if version in (0, 1):
                # Preserve legacy rows exactly; never invent their starting boundary.
                db.executescript(_PROVENANCE_SCHEMA)
            if version in (0, 1, 2):
                db.executescript(_REPLAY_SCHEMA)
        except BaseException:
            self.close()
            raise

    def _db(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("ingestion store is closed")
        return self._connection

    def __enter__(self) -> IngestionStore:
        self._db()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def durability_settings(self) -> dict[str, str | int]:
        db = self._db()
        return {"journal_mode": db.execute("PRAGMA journal_mode").fetchone()[0].lower(),
                "synchronous": db.execute("PRAGMA synchronous").fetchone()[0],
                "foreign_keys": db.execute("PRAGMA foreign_keys").fetchone()[0]}

    def ingest(self, value: EventEnvelope | Mapping[str, Any] | bytes | str,
               position: TransportPosition | None = None, *, checkpoint_next_offset: int | None = None) -> IngestResult:
        return self._ingest_prepared(_prepare(value), position, checkpoint_next_offset)

    def reject(self, value: EventEnvelope | Mapping[str, Any] | bytes | str, reason: str,
               position: TransportPosition | None = None, *, checkpoint_next_offset: int | None = None) -> IngestResult:
        """Durably classify external validation failure without altering raw evidence.

        This generic boundary is transport-independent. A rejected payload has
        raw evidence/hash, not a canonical accepted payload hash. All transaction
        and position-conflict rules are shared with ingest.
        """
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("a nonblank rejection reason is required")
        prepared = replace(_prepare(value), event=None, payload=None, payload_hash=None, rejection_reason=reason)
        return self._ingest_prepared(prepared, position, checkpoint_next_offset)

    def _ingest_prepared(self, prepared: _Prepared, position: TransportPosition | None,
                         checkpoint_next_offset: int | None) -> IngestResult:
        if position is not None:
            position = TransportPosition.model_validate(position.model_dump())
        if checkpoint_next_offset is not None:
            if (position is None or type(checkpoint_next_offset) is not int
                    or checkpoint_next_offset != position.offset + 1):
                raise ValueError("checkpoint requires a transport position and must equal offset + 1")
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            if checkpoint_next_offset is not None:
                previous = self.latest_checkpoint(position.topic, position.partition)
                if previous is not None and checkpoint_next_offset < previous:
                    raise ValueError("checkpoint cannot move backwards")
            live = self.live_checkpoint(position.topic, position.partition) if position else None
            if live is not None:
                if position.offset < live.start_offset:
                    raise ValueError("message precedes declared live starting boundary")
                if position.offset < live.next_offset:
                    if self.lookup_position(position) is None:
                        raise ValueError("old delivery has no durable position in covered interval")
                    if checkpoint_next_offset is not None:
                        raise ValueError("old delivery must not rewrite live progress")
                elif position.offset != live.next_offset or checkpoint_next_offset != position.offset + 1:
                    raise ValueError("live checkpoint would skip unfinished durable work")
            result = self._persist(prepared, position, checkpoint_next_offset)
            if live is not None and checkpoint_next_offset is not None:
                db.execute("UPDATE live_partitions SET next_offset=?, updated_at=? WHERE topic=? AND partition=?",
                           (checkpoint_next_offset, result.ingested_at, position.topic, position.partition))
            db.execute("COMMIT")
            return result
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def _persist(self, prepared: _Prepared, position: TransportPosition | None,
                 checkpoint: int | None) -> IngestResult:
        db = self._db()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        coordinates = (position.topic, position.partition, position.offset) if position else (None, None, None)
        prior_position = db.execute("SELECT fingerprint FROM transport_positions WHERE topic=? AND partition=? AND offset=?",
                                    coordinates).fetchone() if position else None
        event = prepared.event
        disposition = Disposition.REJECTED
        reason = prepared.rejection_reason
        if prior_position is not None and prior_position[0] != prepared.fingerprint:
            disposition, reason = Disposition.CONFLICT, "transport position already belongs to different content"
        elif event is not None:
            existing = db.execute("SELECT payload_hash FROM accepted_events WHERE event_id=?", (event.event_id,)).fetchone()
            collision = db.execute("SELECT event_id FROM accepted_events WHERE run_id=? AND episode_id=?",
                                   (event.run_id, event.episode.episode_id)).fetchone()
            if existing is not None:
                disposition = Disposition.DUPLICATE if existing[0] == prepared.payload_hash else Disposition.CONFLICT
                reason = None if disposition == Disposition.DUPLICATE else "event identity has a different accepted payload"
            elif collision is not None:
                disposition, reason = Disposition.CONFLICT, "episode_id already accepted under a different event within this run"
            else:
                disposition, reason = Disposition.ACCEPTED, None
                db.execute("""INSERT INTO accepted_events
                    (event_id,run_id,source_id,sequence,episode_id,payload_hash,schema_version,event_type,payload_json,ingested_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""", (event.event_id, event.run_id, event.source_id, event.sequence,
                    event.episode.episode_id, prepared.payload_hash, event.schema_version, event.event_type,
                    prepared.payload.decode("utf-8"), timestamp))
        meta = prepared.metadata
        cursor = db.execute("""INSERT INTO ingest_receipts
            (event_id,run_id,source_id,sequence,episode_id,payload_hash,schema_version,event_type,disposition,
             rejection_reason,topic,partition,offset,ingested_at,raw_payload,raw_sha256)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            meta.get("event_id"), meta.get("run_id"), meta.get("source_id"), meta.get("sequence"),
            meta.get("episode_id"), prepared.payload_hash, meta.get("schema_version"), meta.get("event_type"),
            disposition.value, reason, *coordinates, timestamp, prepared.raw, prepared.raw_hash))
        receipt_id = cursor.lastrowid
        if position is not None and prior_position is None:
            db.execute("INSERT INTO transport_positions(topic,partition,offset,fingerprint,receipt_id) VALUES (?,?,?,?,?)",
                       (*coordinates, prepared.fingerprint, receipt_id))
        if checkpoint is not None:
            db.execute("""INSERT INTO checkpoints(topic,partition,next_offset,receipt_id) VALUES (?,?,?,?)
                ON CONFLICT(topic,partition) DO UPDATE SET next_offset=excluded.next_offset, receipt_id=excluded.receipt_id""",
                (position.topic, position.partition, checkpoint, receipt_id))
        return IngestResult(receipt_id, disposition, meta.get("event_id"), prepared.payload_hash, reason, timestamp)

    def accepted_events(self, run_id: str) -> list[EventEnvelope]:
        rows = self._db().execute("""SELECT payload_json FROM accepted_events WHERE run_id=?
            ORDER BY source_id COLLATE BINARY, sequence, episode_id COLLATE BINARY, event_id COLLATE BINARY""", (run_id,))
        return [EventEnvelope.model_validate_json(row[0]) for row in rows]

    def lookup_event(self, event_id: str) -> EventEnvelope | None:
        row = self._db().execute("SELECT payload_json FROM accepted_events WHERE event_id=?", (event_id,)).fetchone()
        return EventEnvelope.model_validate_json(row[0]) if row else None

    def counts_by_disposition(self, run_id: str | None = None) -> dict[Disposition, int]:
        query = "SELECT disposition,COUNT(*) FROM ingest_receipts"
        args: tuple[str, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=?"
            args = (run_id,)
        rows = self._db().execute(query + " GROUP BY disposition", args)
        result = {disposition: 0 for disposition in Disposition}
        result.update({Disposition(row[0]): row[1] for row in rows})
        return result

    def latest_checkpoint(self, topic: str, partition: int) -> int | None:
        TransportPosition(topic=topic, partition=partition, offset=0)
        row = self._db().execute("SELECT next_offset FROM checkpoints WHERE topic=? AND partition=?", (topic, partition)).fetchone()
        return row[0] if row else None

    def lookup_position(self, position: TransportPosition) -> IngestResult | None:
        row = self._db().execute("""SELECT r.receipt_id,r.disposition,r.event_id,r.payload_hash,r.rejection_reason,r.ingested_at
            FROM transport_positions p JOIN ingest_receipts r ON r.receipt_id=p.receipt_id
            WHERE p.topic=? AND p.partition=? AND p.offset=?""", (position.topic, position.partition, position.offset)).fetchone()
        return IngestResult(row[0], Disposition(row[1]), *row[2:]) if row else None


    def live_checkpoint(self, topic: str, partition: int) -> LiveCheckpoint | None:
        row = self._db().execute("SELECT * FROM live_partitions WHERE topic=? AND partition=?", (topic, partition)).fetchone()
        if row is None:
            return None
        return LiveCheckpoint(Provenance(row["topic"], row["partition"], row["broker_id"], row["topic_id"], row["partition_count"]),
                              row["start_offset"], row["next_offset"], row["mode"], row["created_at"], row["updated_at"])

    def can_bootstrap(self, topic: str, partition: int) -> bool:
        db = self._db()
        return (self.live_checkpoint(topic, partition) is None and
                self.latest_checkpoint(topic, partition) is None and
                db.execute("SELECT 1 FROM transport_positions WHERE topic=? AND partition=? LIMIT 1", (topic, partition)).fetchone() is None)

    def initialize_live(self, provenance: Provenance, start: int) -> LiveCheckpoint:
        if (type(start) is not int or not 0 <= start < SQLITE_MAX_INTEGER or not provenance.broker_id or
                not provenance.topic_id or not provenance.topic or not 0 <= provenance.partition < provenance.partition_count):
            raise ValueError("invalid live provenance or starting boundary")
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            if not self.can_bootstrap(provenance.topic, provenance.partition):
                raise ValueError("legacy or existing checkpoint cannot be bootstrapped without verified provenance")
            timestamp = datetime.now(timezone.utc).isoformat(timespec="microseconds")
            db.execute("INSERT INTO live_partitions VALUES (?,?,?,?,?,'LIVE',?,?,?,?)",
                       (provenance.topic, provenance.partition, provenance.broker_id, provenance.topic_id,
                        provenance.partition_count, start, start, timestamp, timestamp))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        return self.live_checkpoint(provenance.topic, provenance.partition)

    def verify_live_coverage(self, checkpoint: LiveCheckpoint) -> bool:
        """B3.3.1 conservatively requires contiguous recorded offsets.

        Topics with logical offset gaps fail closed; gap-aware traversal is not
        inferred from max(offset). Legacy checkpoints are never promoted.
        """
        p = checkpoint.provenance
        start, end = checkpoint.start_offset, checkpoint.next_offset
        if not 0 <= start <= end or checkpoint.mode != "LIVE":
            return False
        count = self._db().execute("SELECT COUNT(*) FROM transport_positions WHERE topic=? AND partition=? AND offset>=? AND offset<?",
                                   (p.topic, p.partition, start, end)).fetchone()[0]
        old = self.latest_checkpoint(p.topic, p.partition)
        return count == end - start and (old == end if end > start else old is None)

    def record_startup(self, decision: RecoveryDecision, status: str) -> None:
        self._db().execute("INSERT INTO startup_actions(recorded_at,result_json,status) VALUES (?,?,?)",
                           (datetime.now(timezone.utc).isoformat(timespec="microseconds"), json.dumps(asdict(decision), sort_keys=True), status))

    def startup_actions(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._db().execute("SELECT * FROM startup_actions ORDER BY action_id")]


    def accepted_count(self) -> int:
        return self._db().execute("SELECT COUNT(*) FROM accepted_events").fetchone()[0]

    def accepted_page(self, *, after_id: str = "", limit: int = 100) -> list[dict[str, str]]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("page limit must be between 1 and 1000")
        return [dict(row) for row in self._db().execute(
            "SELECT event_id,payload_hash FROM accepted_events WHERE event_id>? ORDER BY event_id LIMIT ?", (after_id,limit))]

    def create_replay(self, manifest: ReplayManifest) -> None:
        manifest = ReplayManifest.model_validate(manifest.model_dump())
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            for boundary in manifest.boundaries:
                live = self.live_checkpoint(manifest.topic,boundary.partition)
                if live is None:
                    raise ValueError("replay requires existing live partition provenance; fresh-ledger reconstruction is not supported")
                p = live.provenance
                if (p.broker_id,p.topic_id,p.partition_count) != (manifest.broker_id,manifest.topic_id,manifest.partition_count):
                    raise ValueError("replay manifest differs from existing ledger provenance")
            snapshot = {str(b.partition):asdict(self.live_checkpoint(manifest.topic,b.partition)) for b in manifest.boundaries}
            db.execute("INSERT INTO replay_sessions VALUES (?,?,'ACTIVE',NULL,?,?,NULL,?,NULL)",
                       (manifest.session_id,manifest.model_dump_json(),manifest.created_at,self.accepted_count(),json.dumps(snapshot,sort_keys=True)))
            for b in manifest.boundaries:
                db.execute("INSERT INTO replay_partitions VALUES (?,?,?,?,?,?,?)",
                           (manifest.session_id,b.partition,b.start_offset,b.end_offset,b.start_offset,
                            int(b.start_offset==b.end_offset),"EMPTY_INTERVAL" if b.start_offset==b.end_offset else None))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def replay_state(self, session_id: str) -> dict[str, Any]:
        row = self._db().execute("SELECT * FROM replay_sessions WHERE session_id=?",(session_id,)).fetchone()
        if row is None:
            raise ValueError("unknown replay session")
        result = dict(row)
        result["manifest"] = json.loads(result.pop("manifest_json"))
        result["live_before"] = json.loads(result.pop("live_before_json"))
        after = result.pop("live_after_json")
        result["live_after"] = json.loads(after) if after is not None else None
        result["partitions"] = [dict(r) for r in self._db().execute(
            "SELECT * FROM replay_partitions WHERE session_id=? ORDER BY partition",(session_id,))]
        return result

    def _active_replay_partition(self, session_id: str, partition: int) -> sqlite3.Row:
        row = self._db().execute("""SELECT p.*,s.status,s.manifest_json FROM replay_partitions p
            JOIN replay_sessions s USING(session_id) WHERE p.session_id=? AND p.partition=?""",(session_id,partition)).fetchone()
        if row is None or row["status"] != "ACTIVE":
            raise ValueError("replay session/partition is not active")
        return row

    def ingest_replay(self, session_id: str, value: bytes, key: bytes | None, payload_is_null: bool,
                      position: TransportPosition, rejection: str | None = None) -> IngestResult:
        """Receipt, observation and replay progress share a transaction; no live writes."""
        prepared = _prepare(value)
        if rejection is not None:
            prepared = replace(prepared,event=None,payload=None,payload_hash=None,rejection_reason=rejection)
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            progress = self._active_replay_partition(session_id,position.partition)
            manifest = ReplayManifest.model_validate_json(progress["manifest_json"])
            if (position.topic != manifest.topic or progress["complete"] or
                    not progress["next_offset"] <= position.offset < progress["end_offset"]):
                raise ValueError("replay observation outside unfinished frozen interval")
            result = self._persist(prepared,position,None)
            db.execute("INSERT INTO replay_observations VALUES (?,?,?,?,?,?)",
                       (session_id,position.partition,position.offset,result.receipt_id,key,int(payload_is_null)))
            next_offset = position.offset + 1
            db.execute("UPDATE replay_partitions SET next_offset=?,complete=?,completion_proof=? WHERE session_id=? AND partition=?",
                       (next_offset,int(next_offset==progress["end_offset"]),
                        "DURABLE_RECORD" if next_offset==progress["end_offset"] else None,session_id,position.partition))
            db.execute("COMMIT")
            return result
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def complete_replay_partition(self, session_id: str, partition: int, traversed_offset: int, proof: str) -> None:
        if proof not in ("PARTITION_EOF","BOUNDARY_RECORD"):
            raise ValueError("explicit broker traversal proof is required")
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            progress = self._active_replay_partition(session_id,partition)
            if traversed_offset < progress["end_offset"]:
                raise ValueError("broker has not traversed frozen end")
            db.execute("UPDATE replay_partitions SET next_offset=end_offset,complete=1,completion_proof=? WHERE session_id=? AND partition=?",
                       (proof,session_id,partition))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def finish_replay(self, session_id: str, failure_reason: str | None = None) -> None:
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            state = self.replay_state(session_id)
            if state["status"] != "ACTIVE":
                raise ValueError("replay is already terminal")
            if failure_reason is None and not all(p["complete"] for p in state["partitions"]):
                raise ValueError("not all replay partitions completed")
            manifest = state["manifest"]
            snapshot = {str(b["partition"]):asdict(self.live_checkpoint(manifest["topic"],b["partition"])) for b in manifest["boundaries"]}
            if snapshot != state["live_before"] and failure_reason is None:
                failure_reason = "FAIL_LIVE_STATE_CHANGED: live ingestion must not run concurrently against the replay ledger"
            db.execute("UPDATE replay_sessions SET status=?,failure_reason=?,updated_at=?,accepted_after=?,live_after_json=? WHERE session_id=?",
                       ("FAILED" if failure_reason else "COMPLETE",failure_reason,
                        datetime.now(timezone.utc).isoformat(timespec="microseconds"),self.accepted_count(),json.dumps(snapshot,sort_keys=True),session_id))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def replay_counts(self, session_id: str) -> dict[str, int]:
        counts = {d.value:0 for d in Disposition}
        counts.update({row[0]:row[1] for row in self._db().execute("""SELECT r.disposition,COUNT(*)
            FROM replay_observations o JOIN ingest_receipts r USING(receipt_id)
            WHERE o.session_id=? GROUP BY r.disposition""",(session_id,))})
        return counts

    def replay_page(self, session_id: str, *, after_receipt: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        if type(limit) is not int or not 1 <= limit <= 1000 or type(after_receipt) is not int or after_receipt < 0:
            raise ValueError("valid cursor and page limit 1..1000 required")
        rows = self._db().execute("""SELECT r.receipt_id,r.topic,r.partition,r.offset,r.event_id,r.payload_hash,
            r.disposition,r.rejection_reason,r.raw_sha256,r.raw_payload,o.message_key,o.payload_is_null
            FROM replay_observations o JOIN ingest_receipts r USING(receipt_id)
            WHERE o.session_id=? AND r.receipt_id>? ORDER BY r.receipt_id LIMIT ?""",(session_id,after_receipt,limit))
        result = []
        for row in rows:
            item = dict(row)
            item["payload_base64"] = base64.b64encode(item.pop("raw_payload")).decode("ascii")
            key = item.pop("message_key")
            item["key_base64"] = base64.b64encode(key).decode("ascii") if key is not None else None
            item["payload_is_null"] = bool(item["payload_is_null"])
            result.append(item)
        return result
