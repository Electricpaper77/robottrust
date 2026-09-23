"""Startup reconciliation invariants and real SQLite provenance."""
from dataclasses import replace
import sqlite3
from unittest.mock import Mock
import pytest
from confluent_kafka import TopicPartition, OFFSET_INVALID
from robottrust.generator import generate_episodes
from robottrust.streaming.events import create_event
from robottrust.streaming.config import ConsumerConfig, TransportPosition
from robottrust.streaming.consumer import EpisodeConsumer
from robottrust.streaming.reconciliation import Action, Provenance, LiveCheckpoint, RecoveryError, reconcile
from robottrust.streaming.store import IngestionStore, _SCHEMA
from test_b3_transport import message

TOPIC = "robottrust.episodes.v1"
P = Provenance(TOPIC, 0, "cluster", "topic-id", 3)

@pytest.mark.parametrize("d,k,low,end,action", [
    (10,10,0,20,Action.RESUME_AT_D), (10,5,0,20,Action.REPAIR),
    (5,10,0,20,Action.RESUME_AT_D), (5,10,6,20,Action.EXPIRED),
    (10,None,0,20,Action.RESUME_AT_D), (None,10,0,20,Action.BOOTSTRAP_REQUIRED),
    (5,5,6,20,Action.EXPIRED), (21,10,0,20,Action.INCONSISTENT),
    (None,None,0,20,Action.BOOTSTRAP_REQUIRED), (10,21,0,20,Action.INCONSISTENT),
])
def test_reconciliation_matrix(d, k, low, end, action):
    result = reconcile(LiveCheckpoint(P, 0, d) if d is not None else None, P, k, low, end, coverage_verified=True)
    assert result.decision == action
    assert (result.D, result.K, result.earliest, result.end) == (d,k,low,end)
    if not result.failed:
        assert result.seek_offset == d

def test_explicit_new_stream():
    result = reconcile(None, P, None, 4, 20, explicit_start=4, new_stream=True)
    assert result.decision == Action.BOOTSTRAP and result.seek_offset == 4

@pytest.mark.parametrize("field,value,action", [("broker_id","other",Action.IDENTITY), ("topic_id","other",Action.IDENTITY),
    ("partition",1,Action.IDENTITY), ("partition_count",4,Action.TOPOLOGY)])
def test_provenance_mismatch(field, value, action):
    assert reconcile(LiveCheckpoint(P,0,10), replace(P, **{field:value}),10,0,20,coverage_verified=True).decision == action

@pytest.mark.parametrize("start,end,verified", [(11,10,True), (-1,10,True), (0,10,False)])
def test_invalid_coverage(start, end, verified):
    assert reconcile(LiveCheckpoint(P,start,end),P,10,0,20,coverage_verified=verified).decision == Action.INCONSISTENT

def test_fail_closed_config():
    assert ConsumerConfig(group_id="test").client_settings()["auto.offset.reset"] == "error"

@pytest.fixture
def store(tmp_path):
    with IngestionStore(tmp_path / "ledger.db") as ledger:
        yield ledger

@pytest.fixture
def event():
    return create_event(next(generate_episodes(1,42)),"test","robot",0)

def make_worker(store, k=None):
    client = Mock()
    client.committed.return_value = [TopicPartition(TOPIC,0,OFFSET_INVALID if k is None else k)]
    client.get_watermark_offsets.return_value = (0,200)
    client.commit.side_effect = lambda *, offsets, asynchronous: offsets
    worker = EpisodeConsumer(ConsumerConfig(group_id="test",bootstrap_policy="explicit"), store,client=client,
                             subscribe=False,metadata_provider=lambda:(P.broker_id,P.topic_id,P.partition_count))
    return worker,client

def boot(worker, start=0):
    return worker.reconcile_assignment([TopicPartition(TOPIC,0)],explicit_starts={0:start})

def test_seek_precedes_processing(store,event):
    worker,client=make_worker(store)
    client.seek.side_effect=lambda position: (store.lookup_event(event.event_id) is None) or pytest.fail("premature ingest")
    boot(worker)
    worker.handle_message(message(event))
    calls=[call[0] for call in client.method_calls]
    assert calls.index("seek") < calls.index("commit")

def test_processing_without_startup_rejected(store,event):
    worker,client=make_worker(store)
    with pytest.raises(RecoveryError) as exc:
        worker.handle_message(message(event))
    assert exc.value.result.decision == Action.NOT_READY
    assert store.lookup_event(event.event_id) is None
    client.commit.assert_not_called()

def test_repair_after_seek(store,event):
    store.initialize_live(P,0)
    store.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=0),checkpoint_next_offset=1)
    worker,client=make_worker(store,k=0)
    assert boot(worker)[0].decision == Action.REPAIR
    assert client.seek.call_args.args[0].offset == 1
    assert client.commit.call_args.kwargs["offsets"][0].offset == 1
    assert store.startup_actions()[-1]["status"] == "REPAIRED"
    calls=[c[0] for c in client.method_calls]
    assert calls.index("seek") < calls.index("commit")

def test_unconfirmed_repair_blocks_processing(store,event):
    store.initialize_live(P,0)
    store.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=0),checkpoint_next_offset=1)
    worker,client=make_worker(store,k=0)
    client.commit.side_effect=None
    client.commit.return_value=[]
    with pytest.raises(RecoveryError) as exc:
        boot(worker)
    assert exc.value.result.decision == Action.TRANSPORT
    assert exc.value.result.D == 1 and exc.value.result.K == 0
    with pytest.raises(RuntimeError):
        worker.handle_message(message(event))

def test_old_redelivery_never_commits_51(store,event):
    store.initialize_live(P,50)
    for offset in range(50,101):
        store.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=offset),checkpoint_next_offset=offset+1)
    worker,client=make_worker(store,k=101)
    boot(worker)
    result=worker.handle_message(message(event,offset=50))
    assert result.ingestion.disposition == "DUPLICATE"
    assert result.committed_next_offset == 101
    assert client.commit.call_args.kwargs["offsets"][0].offset == 101
    assert store.live_checkpoint(TOPIC,0).next_offset == 101

def test_ahead_commit_does_not_skip_or_regress(store,event):
    store.initialize_live(P,0)
    worker,client=make_worker(store,k=2)
    boot(worker)
    assert client.seek.call_args.args[0].offset == 0
    result=worker.handle_message(message(event))
    assert store.live_checkpoint(TOPIC,0).next_offset == 1
    client.commit.assert_not_called()
    assert result.committed_next_offset == 2

def test_reopen_provenance(store):
    store.initialize_live(P,7)
    original=store.live_checkpoint(TOPIC,0)
    path=store.config.database_path
    store.close()
    with IngestionStore(path) as reopened:
        assert reopened.live_checkpoint(TOPIC,0) == original
        assert reopened.verify_live_coverage(original)

def test_legacy_migration_preserves_but_does_not_invent_provenance(tmp_path,event):
    path=tmp_path / "legacy.db"
    with IngestionStore(path) as original:
        original.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=4),checkpoint_next_offset=5)
    # Reproduce the exact v1 table set, retaining original acceptance/receipts.
    with sqlite3.connect(path) as db:
        db.executescript("DROP TABLE replay_observations; DROP TABLE replay_partitions; DROP TABLE replay_sessions; DROP TABLE live_partitions; DROP TABLE startup_actions; PRAGMA user_version=1;")
    with IngestionStore(path) as store:
        assert store.live_checkpoint(TOPIC,0) is None
        worker,client=make_worker(store,k=5)
        with pytest.raises(RecoveryError) as exc:
            boot(worker)
        assert exc.value.result.decision == Action.BOOTSTRAP_REQUIRED
        assert store.lookup_event(event.event_id) == event
        assert store.latest_checkpoint(TOPIC,0) == 5
        client.seek.assert_not_called()

def test_live_gap_rolls_back(store,event):
    store.initialize_live(P,0)
    with pytest.raises(ValueError,match="skip"):
        store.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=2),checkpoint_next_offset=3)
    assert store.lookup_event(event.event_id) is None
    assert store.live_checkpoint(TOPIC,0).next_offset == 0

def test_retained_start_can_pass_covered_start():
    assert reconcile(LiveCheckpoint(P,0,10),P,5,7,20,coverage_verified=True).decision == Action.REPAIR


def test_coverage_detects_missing_position(store,event):
    store.initialize_live(P,0)
    store.ingest(event,TransportPosition(topic=TOPIC,partition=0,offset=0),checkpoint_next_offset=1)
    with sqlite3.connect(store.config.database_path) as db:
        db.execute("DELETE FROM transport_positions")
    worker,client=make_worker(store,k=0)
    with pytest.raises(RecoveryError) as exc:
        boot(worker)
    assert exc.value.result.decision == Action.INCONSISTENT
    client.commit.assert_not_called()
    client.seek.assert_not_called()

def test_revoke_blocks_processing(store,event):
    worker,client=make_worker(store)
    boot(worker)
    worker._on_revoke(client,[TopicPartition(TOPIC,0)])
    with pytest.raises(RecoveryError):
        worker.handle_message(message(event))
    assert store.lookup_event(event.event_id) is None

def test_seek_failure_is_durable_diagnostic(store):
    worker,client=make_worker(store)
    client.seek.side_effect=RuntimeError("seek failed")
    with pytest.raises(RecoveryError) as exc:
        boot(worker)
    assert exc.value.result.decision == Action.TRANSPORT
    assert store.startup_actions()[-1]["status"] == "FAILED"
    client.commit.assert_not_called()
