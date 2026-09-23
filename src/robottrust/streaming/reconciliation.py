"""Pure startup decisions; no Kafka client, SQLite calls, or automatic resets."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from enum import Enum
import json


class Action(str, Enum):
    RESUME_AT_D = "RESUME_AT_D"
    REPAIR = "RESUME_AT_D_AND_REPAIR_BROKER_COMMIT"
    BOOTSTRAP = "BOOTSTRAP_FROM_EXPLICIT_START_POLICY"
    BOOTSTRAP_REQUIRED = "FAIL_BOOTSTRAP_REQUIRED"
    EXPIRED = "FAIL_EXPIRED_HISTORY"
    INCONSISTENT = "FAIL_INCONSISTENT_HISTORY"
    IDENTITY = "FAIL_TOPIC_IDENTITY_MISMATCH"
    TOPOLOGY = "FAIL_PARTITION_TOPOLOGY_CHANGED"
    NOT_READY = "FAIL_PARTITION_NOT_RECONCILED"
    TRANSPORT = "FAIL_RECOVERY_TRANSPORT"


@dataclass(frozen=True)
class Provenance:
    topic: str
    partition: int
    broker_id: str
    topic_id: str
    partition_count: int


@dataclass(frozen=True)
class LiveCheckpoint:
    provenance: Provenance
    start_offset: int
    next_offset: int
    mode: str = "LIVE"
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class RecoveryDecision:
    topic: str
    partition: int
    D: int | None
    K: int | None
    earliest: int | None
    end: int | None
    decision: Action
    reason: str
    seek_offset: int | None = None

    @property
    def failed(self) -> bool:
        return self.decision.value.startswith("FAIL_")


class ConsumerError(RuntimeError):
    """Transport/recovery failure; the consumer must stop."""


class RecoveryError(ConsumerError):
    def __init__(self, result: RecoveryDecision):
        self.result = result
        super().__init__(json.dumps(asdict(result), sort_keys=True))


def reconcile(durable: LiveCheckpoint | None, observed: Provenance, committed: int | None,
              earliest: int, end: int, *, expected_partitions: int = 3,
              coverage_verified: bool = False, explicit_start: int | None = None,
              new_stream: bool = False) -> RecoveryDecision:
    """Coverage is independently checked by the store before calling this function.

    A caller must explicitly authorize bootstrap of a new local stream/group.
    Broker commits are diagnostic; they never replace missing local coverage.
    """
    d = durable.next_offset if durable else None
    def result(action: Action, reason: str, seek: int | None = None) -> RecoveryDecision:
        return RecoveryDecision(observed.topic, observed.partition, d, committed, earliest, end, action, reason, seek)
    if (observed.partition_count != expected_partitions or
            durable and durable.provenance.partition_count != observed.partition_count):
        return result(Action.TOPOLOGY, "partition count differs from configured or durable expectation")
    if (not observed.broker_id or not observed.topic_id or not observed.topic or
            not 0 <= observed.partition < observed.partition_count or
            durable and durable.provenance != observed):
        return result(Action.IDENTITY, "broker, topic incarnation, or partition provenance differs")
    if (type(earliest) is not int or type(end) is not int or not 0 <= earliest <= end or
            committed is not None and (type(committed) is not int or committed < 0 or committed > end)):
        return result(Action.INCONSISTENT, "invalid broker boundaries or commit beyond broker end")
    if durable is None:
        if committed is not None or not new_stream or explicit_start is None:
            return result(Action.BOOTSTRAP_REQUIRED, "missing durable provenance; explicit new-stream bootstrap required")
        if type(explicit_start) is not int or not earliest <= explicit_start <= end:
            return result(Action.INCONSISTENT, "explicit bootstrap start is outside retained history")
        return result(Action.BOOTSTRAP, "new stream explicitly starts at the selected boundary", explicit_start)
    if (durable.mode != "LIVE" or type(d) is not int or type(durable.start_offset) is not int or
            not 0 <= durable.start_offset <= d or not coverage_verified):
        return result(Action.INCONSISTENT, "invalid or unverified durable covered interval")
    if d < earliest:
        return result(Action.EXPIRED, "required durable next offset is no longer retained")
    if d > end:
        return result(Action.INCONSISTENT, "durable checkpoint exceeds broker end")
    if committed is not None and d > committed:
        return result(Action.REPAIR, "verified durable interval leads broker commit", d)
    return result(Action.RESUME_AT_D, "resume from verified durable progress, never from an ahead broker commit", d)
