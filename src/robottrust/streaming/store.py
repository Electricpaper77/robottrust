"""SQLite acceptance ledger and append-only disposition audit.

One BEGIN IMMEDIATE transaction records each ingest call, immutable acceptance,
first transport receipt, and any explicit checkpoint. Checkpoints are NEXT
offsets, not inferred high-water marks; the future transport caller must assert
that the supplied position completes its processed prefix. No broker is used.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from robottrust.streaming.config import LedgerConfig, SQLITE_MAX_INTEGER, TransportPosition
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
            if version not in (0, 1) or (version == 0 and tables) or (version == 1 and tables != expected):
                raise RuntimeError("unsupported or unrelated ledger schema")
            mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            if mode.lower() != "wal" or self.durability_settings() != {"journal_mode": "wal", "synchronous": 2, "foreign_keys": 1}:
                raise RuntimeError("required SQLite durability settings are unavailable")
            if version == 0:
                db.executescript(_SCHEMA)
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
        if position is not None:
            position = TransportPosition.model_validate(position.model_dump())
        if checkpoint_next_offset is not None:
            if (position is None or type(checkpoint_next_offset) is not int
                    or checkpoint_next_offset != position.offset + 1):
                raise ValueError("checkpoint requires a transport position and must equal offset + 1")
        prepared = _prepare(value)
        db = self._db()
        db.execute("BEGIN IMMEDIATE")
        try:
            if checkpoint_next_offset is not None:
                previous = self.latest_checkpoint(position.topic, position.partition)
                if previous is not None and checkpoint_next_offset < previous:
                    raise ValueError("checkpoint cannot move backwards")
            result = self._persist(prepared, position, checkpoint_next_offset)
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
