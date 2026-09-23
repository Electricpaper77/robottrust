"""Shared broker identity lookup and transport validation for live and replay."""
from __future__ import annotations
from confluent_kafka import TopicCollection
from confluent_kafka.admin import AdminClient
from robottrust.streaming.config import TransportPosition
from robottrust.streaming.events import EventEnvelope, decode_event_json
from robottrust.streaming.producer import source_key
from robottrust.streaming.store import IngestResult, IngestionStore


def broker_identity(bootstrap: str, topic: str, timeout: float) -> tuple[str, str, int]:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    cluster = admin.describe_cluster(request_timeout=timeout).result(timeout)
    description = admin.describe_topics(TopicCollection([topic]), request_timeout=timeout)[topic].result(timeout)
    topic_id = str(description.topic_id)
    if not cluster.cluster_id or not topic_id or topic_id == "AAAAAAAAAAAAAAAAAAAAAA":
        raise ValueError("broker does not expose a usable cluster/topic incarnation identity")
    return cluster.cluster_id, topic_id, len(description.partitions)


def ingest_transport(store: IngestionStore, value: bytes | None, key: bytes | None,
                     position: TransportPosition, *, checkpoint: int | None = None,
                     replay_id: str | None = None) -> IngestResult:
    """One validation boundary; canonical identity and classification remain B3.1."""
    payload_is_null = value is None
    rejection = None
    if payload_is_null:
        value, rejection = b"", "tombstone/null payload (no value bytes)"
    else:
        try:
            event = EventEnvelope.model_validate(decode_event_json(value))
        except (ValueError, TypeError):
            event = None  # The ledger preserves the precise schema rejection.
        if event is not None and key != source_key(event):
            rejection = "message key mismatch with stable run/source identity"
    if replay_id is not None:
        if checkpoint is not None:
            raise ValueError("replay cannot update a live checkpoint")
        return store.ingest_replay(replay_id, value, key, payload_is_null, position, rejection)
    if rejection is not None:
        return store.reject(value, rejection, position, checkpoint_next_offset=checkpoint)
    return store.ingest(value, position, checkpoint_next_offset=checkpoint)
