"""Finite replay unit boundaries and real SQLite invariants; no simulated broker claims."""
from dataclasses import asdict
import json
import sqlite3
import time
from unittest.mock import Mock
import pytest
from confluent_kafka import KafkaError
from robottrust.generator import generate_episodes
from robottrust.streaming.config import ReplayConfig, TransportPosition
from robottrust.streaming.events import create_event, canonical_payload_bytes
from robottrust.streaming.producer import source_key
from robottrust.streaming.reconciliation import Provenance
from robottrust.streaming.replay import ReplaySession, write_replay_evidence
from robottrust.streaming.replay_models import ReplayError, ReplayFailure
from robottrust.streaming.store import IngestionStore
from robottrust.streaming.transport import ingest_transport

TOPIC="robottrust.episodes.v1"

def msg(event=None, *, p=0, offset=0, value="event", eof=False):
    result=Mock()
    result.topic.return_value=TOPIC
    result.partition.return_value=p
    result.offset.return_value=offset
    result.key.return_value=source_key(event) if event else b"key"
    result.value.return_value=canonical_payload_bytes(event) if value=="event" else value
    result.error.return_value=KafkaError(KafkaError._PARTITION_EOF) if eof else None
    return result

@pytest.fixture
def fixture(tmp_path):
    with IngestionStore(tmp_path/"ledger.db") as store:
        events=[create_event(ep,"replay-test",str(i),i) for i,ep in enumerate(generate_episodes(3,42))]
        for p,event in enumerate(events):
            store.initialize_live(Provenance(TOPIC,p,"cluster","topic-id",3),0)
            ingest_transport(store,canonical_payload_bytes(event),source_key(event),
                             TransportPosition(topic=TOPIC,partition=p,offset=0),checkpoint=1)
        yield store,events

def session(fixture, messages=(), *, ends=None):
    store,_=fixture
    client=Mock()
    ends=ends or {0:1,1:1,2:1}
    client.get_watermark_offsets.side_effect=lambda target,**kw:(0,ends[target.partition])
    iterator=iter(messages)
    client.poll.side_effect=lambda timeout:next(iterator,None)
    worker=ReplaySession(ReplayConfig(),store,client=client,metadata_provider=lambda:("cluster","topic-id",3))
    return worker,client

def test_frozen_capture_and_seek(fixture):
    worker,client=session(fixture)
    manifest=worker.capture()
    assert [(b.start_offset,b.end_offset) for b in manifest.boundaries]==[(0,1)]*3
    assert [(c.args[0].partition,c.args[0].offset) for c in client.seek.call_args_list]==[(0,0),(1,0),(2,0)]
    client.poll.assert_not_called()
    assert fixture[0].replay_state(worker.session_id)["status"]=="ACTIVE"
    worker.close()

def test_identical_replay_and_live_state_unchanged(fixture):
    store,events=fixture
    before=[asdict(store.live_checkpoint(TOPIC,p)) for p in range(3)]
    checkpoints=[store.latest_checkpoint(TOPIC,p) for p in range(3)]
    worker,client=session(fixture,[msg(e,p=p) for p,e in enumerate(events)])
    with worker:
        worker.capture()
        result=worker.run()
    assert result["status"]=="COMPLETE"
    assert result["accepted_after"]-result["accepted_before"]==0
    assert store.replay_counts(worker.session_id)=={"ACCEPTED":0,"DUPLICATE":3,"CONFLICT":0,"REJECTED":0}
    assert [asdict(store.live_checkpoint(TOPIC,p)) for p in range(3)]==before
    assert [store.latest_checkpoint(TOPIC,p) for p in range(3)]==checkpoints
    client.commit.assert_not_called()
    assert client.close.called

def test_empty_poll_not_completion(fixture):
    worker,_=session(fixture)
    with worker:
        worker.capture()
        assert worker.step() is False
        assert fixture[0].replay_state(worker.session_id)["status"]=="ACTIVE"

@pytest.mark.parametrize("offset",[1,7])
def test_at_or_beyond_end_excluded(fixture,offset):
    store,events=fixture
    worker,_=session(fixture,[msg(events[0],offset=offset)])
    with worker:
        worker.capture(partitions=(0,))
        assert worker.step()
    assert store.replay_page(worker.session_id)==[]
    assert store.accepted_count()==3
    assert store.replay_state(worker.session_id)["partitions"][0]["completion_proof"]=="BOUNDARY_RECORD"

def test_all_partitions_must_finish(fixture):
    store,events=fixture
    worker,_=session(fixture,[msg(events[0])])
    with worker:
        worker.capture()
        assert not worker.step()
        state=store.replay_state(worker.session_id)
        assert [p["complete"] for p in state["partitions"]]==[1,0,0]
        with pytest.raises(ValueError,match="not all"):
            store.finish_replay(worker.session_id)

def test_offset_gaps_and_eof_proof(fixture):
    store,events=fixture
    worker,_=session(fixture,[msg(events[0],offset=2),msg(events[0],offset=5),msg(p=0,offset=8,value=b"",eof=True)],ends={0:8})
    with worker:
        worker.capture(partitions=(0,))
        worker.run()
    assert [r["offset"] for r in store.replay_page(worker.session_id)]==[2,5]
    assert store.replay_state(worker.session_id)["partitions"][0]["next_offset"]==8
    assert store.live_checkpoint(TOPIC,0).next_offset==1

def test_eof_below_end_not_completion(fixture):
    worker,_=session(fixture,[msg(value=b"",eof=True,offset=0)])
    with worker:
        worker.capture(partitions=(0,))
        assert not worker.step()

@pytest.mark.parametrize("change,code",[("expired",ReplayFailure.EXPIRED),("identity",ReplayFailure.IDENTITY),
    ("topology",ReplayFailure.TOPOLOGY),("truncated",ReplayFailure.HISTORY)])
def test_history_changes_fail_closed(fixture,change,code):
    store,_=fixture
    worker,client=session(fixture)
    worker.capture()
    if change=="expired":
        client.get_watermark_offsets.side_effect=lambda *a,**kw:(1,1)
    elif change=="truncated":
        client.get_watermark_offsets.side_effect=lambda *a,**kw:(0,0)
    else:
        worker._metadata_provider=lambda:("cluster","changed" if change=="identity" else "topic-id",4 if change=="topology" else 3)
    with pytest.raises(ReplayError) as exc:
        worker.step()
    assert exc.value.code==code
    assert store.replay_state(worker.session_id)["status"]=="FAILED"
    client.poll.assert_not_called()
    worker.close()

def test_expired_requested_start_before_capture(fixture):
    worker,client=session(fixture)
    client.get_watermark_offsets.side_effect=lambda *a,**kw:(1,3)
    with pytest.raises(ReplayError) as exc:
        worker.capture(starts={0:0})
    assert exc.value.code==ReplayFailure.EXPIRED
    client.assign.assert_not_called()
    worker.close()

def test_post_capture_high_growth_does_not_extend_end(fixture):
    store,events=fixture
    worker,client=session(fixture,[msg(events[0])])
    with worker:
        worker.capture(partitions=(0,))
        client.get_watermark_offsets.side_effect=lambda *a,**kw:(0,9)
        assert worker.step()
    assert store.replay_state(worker.session_id)["partitions"][0]["end_offset"]==1

def test_deadline_is_incomplete(fixture):
    store,_=fixture
    worker,_=session(fixture)
    worker.capture()
    worker._deadline=time.monotonic()-1
    with pytest.raises(ReplayError) as exc:
        worker.run()
    assert exc.value.code==ReplayFailure.DEADLINE
    assert store.replay_state(worker.session_id)["status"]=="FAILED"
    worker.close()

def test_close_and_reopen_preserves_incomplete(fixture):
    store,_=fixture
    worker,_=session(fixture)
    worker.capture()
    worker.close()
    original=store.replay_state(worker.session_id)
    assert original["status"]=="FAILED" and "INTERRUPTED" in original["failure_reason"]
    with IngestionStore(store.config.database_path) as reopened:
        assert reopened.replay_state(worker.session_id)==original

def test_keyset_pages_and_evidence(fixture,tmp_path):
    store,events=fixture
    worker,_=session(fixture,[msg(e,p=p) for p,e in enumerate(events)])
    with worker:
        worker.capture()
        worker.run()
    first=store.replay_page(worker.session_id,limit=2)
    second=store.replay_page(worker.session_id,limit=2,after_receipt=first[-1]["receipt_id"])
    assert len(first)==2 and len(second)==1
    assert len({r["receipt_id"] for r in first+second})==3
    write_replay_evidence(store,worker.session_id,tmp_path/"evidence",page_size=1)
    assert len((tmp_path/"evidence/observations.jsonl").read_text().splitlines())==3
    assert len(store.accepted_page(limit=2))==2

@pytest.mark.parametrize("limit",[0,1001,-1,True])
def test_invalid_page_limits(fixture,limit):
    with pytest.raises(ValueError):
        fixture[0].replay_page("unused",limit=limit)

def test_tombstone_distinct_from_empty_bytes(fixture):
    store,_=fixture
    worker,_=session(fixture,[msg(value=None,offset=0),msg(value=b"",offset=1)],ends={0:2})
    with worker:
        worker.capture(partitions=(0,))
        worker.run()
    rows=store.replay_page(worker.session_id)
    assert [r["payload_is_null"] for r in rows]==[True,False]
    assert [r["payload_base64"] for r in rows]==["",""]
    assert all(r["key_base64"]=="a2V5" for r in rows)

def test_replay_observation_failure_rolls_back(fixture):
    store,events=fixture
    worker,_=session(fixture,[msg(events[0])])
    worker.capture()
    before=store.counts_by_disposition()
    with sqlite3.connect(store.config.database_path) as db:
        db.execute("CREATE TRIGGER fail_replay BEFORE INSERT ON replay_observations BEGIN SELECT RAISE(ABORT,'injected'); END")
    with pytest.raises(sqlite3.IntegrityError):
        worker.step()
    assert store.counts_by_disposition()==before
    assert store.replay_page(worker.session_id)==[]
    assert store.replay_state(worker.session_id)["partitions"][0]["next_offset"]==0
    worker.close()

def test_no_fresh_ledger_reconstruction(tmp_path):
    with IngestionStore(tmp_path/"empty.db") as store:
        worker,_=session((store,[]))
        with pytest.raises(ReplayError,match="reconstruction"):
            worker.capture()
        worker.close()

def test_empty_interval_completes_without_idle_heuristic(fixture):
    worker,client=session(fixture,ends={0:0})
    with worker:
        worker.capture(partitions=(0,))
        assert worker.step()
    client.poll.assert_not_called()


def test_retention_moves_during_poll_before_durable_ingest(fixture):
    store,events=fixture
    worker,client=session(fixture)
    worker.capture()
    def poll(timeout):
        client.get_watermark_offsets.side_effect=lambda *a,**kw:(1,1)
        return msg(events[0])
    client.poll.side_effect=poll
    with pytest.raises(ReplayError) as exc:
        worker.step()
    assert exc.value.code==ReplayFailure.EXPIRED
    assert store.replay_page(worker.session_id)==[]
    worker.close()

def test_active_state_reopens_without_false_completion(fixture):
    store,_=fixture
    worker,_=session(fixture)
    worker.capture()
    with IngestionStore(store.config.database_path) as reopened:
        assert reopened.replay_state(worker.session_id)["status"]=="ACTIVE"
        assert not all(p["complete"] for p in reopened.replay_state(worker.session_id)["partitions"])
    worker.close()

def test_replay_client_cannot_auto_commit_live_group():
    settings=ReplayConfig().client_settings("test-session")
    assert settings["group.id"]=="robottrust-replay-test-session"
    assert settings["enable.auto.commit"] is settings["enable.auto.offset.store"] is False
    assert settings["auto.offset.reset"]=="error"

def test_v2_migration_preserves_live_provenance(fixture):
    store,_=fixture
    before=[asdict(store.live_checkpoint(TOPIC,p)) for p in range(3)]
    path=store.config.database_path
    store.close()
    with sqlite3.connect(path) as db:
        db.executescript("DROP TABLE replay_observations; DROP TABLE replay_partitions; DROP TABLE replay_sessions; PRAGMA user_version=2;")
    with IngestionStore(path) as reopened:
        assert reopened.accepted_count()==3
        assert [asdict(reopened.live_checkpoint(TOPIC,p)) for p in range(3)]==before

def test_failed_session_creation_does_not_mask_original_error(fixture):
    store,_=fixture
    worker,_=session(fixture)
    original=store.create_replay
    def fail(manifest):
        raise ValueError("session persistence unavailable")
    store.create_replay=fail
    with pytest.raises(ValueError,match="persistence unavailable"):
        worker.capture()
    assert not worker.persisted
    worker.close()
    store.create_replay=original
