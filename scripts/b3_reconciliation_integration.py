"""Bounded real-broker startup reconciliation; no replay or crash simulation."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import time
import uuid
from confluent_kafka import Consumer, TopicPartition
from robottrust.generator import generate_episodes
from robottrust.streaming.config import ConsumerConfig, ProducerConfig
from robottrust.streaming.consumer import EpisodeConsumer
from robottrust.streaming.events import create_event
from robottrust.streaming.producer import EpisodeProducer
from robottrust.streaming.reconciliation import Action
from robottrust.streaming.store import IngestionStore, Disposition


def force_commit(config: ConsumerConfig, partition: int, offset: int) -> None:
    client = Consumer(config.client_settings())
    try:
        target = TopicPartition(config.topic, partition, offset)
        client.assign([target])
        result = client.commit(offsets=[target], asynchronous=False)
        assert len(result) == 1 and result[0].error is None and result[0].offset == offset
        assert client.committed([target], timeout=15)[0].offset == offset
    finally:
        client.close()


def run(ledger: Path, output: Path, bootstrap: str) -> dict:
    if ledger.exists():
        raise FileExistsError("a fresh integration ledger is required")
    ledger.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    run_id = "b3.3.1-" + uuid.uuid4().hex
    config = ConsumerConfig(group_id=run_id, bootstrap_servers=bootstrap, bootstrap_policy="explicit")
    producer = EpisodeProducer(ProducerConfig(bootstrap_servers=bootstrap))
    probe = Consumer(config.client_settings())
    try:
        starts = [TopicPartition(config.topic,p,probe.get_watermark_offsets(TopicPartition(config.topic,p),timeout=15)[1]) for p in range(3)]
    finally:
        probe.close()
    events = [create_event(ep,run_id,"robot",i) for i,ep in enumerate(generate_episodes(6,42))]
    deliveries = [producer.publish(event) for event in events]
    partition = deliveries[0].partition
    assert all(d.partition == partition for d in deliveries)
    expected = {e.event_id for e in events}
    with IngestionStore(ledger) as store:
        with EpisodeConsumer(config,store,subscribe=False) as worker:
            worker.reconcile_assignment(starts,explicit_starts={p.partition:p.offset for p in starts})
            worker.consume(3,60)
        durable_before = store.live_checkpoint(config.topic,partition).next_offset
        end = deliveries[-1].offset+1
        assert durable_before < end
        force_commit(config,partition,end)
        before = store.counts_by_disposition()
        # Restart through the normal subscribed/on_assign path.
        resume = config.model_copy(update={"bootstrap_policy":None})
        with EpisodeConsumer(resume,store) as worker:
            worker.consume(3,60)
        actions = store.startup_actions()
        ahead = [json.loads(a["result_json"]) for a in actions if a["status"]=="READY" and json.loads(a["result_json"])["partition"]==partition][-1]
        assert ahead["D"]==durable_before and ahead["K"]==end
        assert ahead["decision"]==Action.RESUME_AT_D and ahead["seek_offset"]==durable_before
        after = store.counts_by_disposition()
        actual = {e.event_id for e in store.accepted_events(run_id)}
        assert actual==expected
        ahead.update(repair_action="NONE",accepted_delta=after[Disposition.ACCEPTED]-before[Disposition.ACCEPTED],
                     duplicate_delta=after[Disposition.DUPLICATE]-before[Disposition.DUPLICATE],
                     missing_ids=sorted(expected-actual),unexpected_ids=sorted(actual-expected))
        assert ahead["accepted_delta"]==3 and ahead["duplicate_delta"]==0
        force_commit(config,partition,durable_before)
        before = store.counts_by_disposition()
        action_count = len(store.startup_actions())
        with EpisodeConsumer(resume,store) as worker:
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                result=worker.poll_once(0.25)
                assert result is None, "repair must not re-ingest durable history"
                actions=store.startup_actions()[action_count:]
                if any(a["status"]=="REPAIRED" for a in actions):
                    break
            else:
                raise TimeoutError("subscribed startup repair did not finish")
        repaired=[json.loads(a["result_json"]) for a in actions if a["status"]=="REPAIRED"]
        assert len(repaired)==1
        behind=repaired[0]
        assert behind["D"]==end and behind["K"]==durable_before
        assert behind["seek_offset"]==end and behind["decision"]==Action.REPAIR
        verifier=Consumer(resume.client_settings())
        try:
            committed=verifier.committed([TopicPartition(config.topic,partition)],timeout=15)[0]
            assert committed.error is None and committed.offset==end
        finally:
            verifier.close()
        after=store.counts_by_disposition()
        actual={e.event_id for e in store.accepted_events(run_id)}
        behind.update(repair_action="VERIFIED_COMMIT_TO_D",accepted_delta=after[Disposition.ACCEPTED]-before[Disposition.ACCEPTED],
                      duplicate_delta=after[Disposition.DUPLICATE]-before[Disposition.DUPLICATE],
                      missing_ids=sorted(expected-actual),unexpected_ids=sorted(actual-expected))
        assert behind["accepted_delta"]==behind["duplicate_delta"]==0 and actual==expected
        report=dict(run_id=run_id,seed=42,events_published=6,expected_ids=sorted(expected),
                    deliveries=[asdict(d) for d in deliveries],scenarios={"K_gt_D":ahead,"D_gt_K":behind},
                    checkpoints=[asdict(store.live_checkpoint(config.topic,p)) for p in range(3)],
                    startup_actions=store.startup_actions())
        (output/"report.json").write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8",newline="\n")
        checksums={"report.json":hashlib.sha256((output/"report.json").read_bytes()).hexdigest()}
        (output/"checksums.json").write_text(json.dumps(checksums,indent=2)+"\n",encoding="utf-8",newline="\n")
        return report


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger",type=Path,required=True)
    parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--bootstrap-servers",default="127.0.0.1:19092")
    args=parser.parse_args()
    print(json.dumps(run(args.ledger,args.output_dir,args.bootstrap_servers)["scenarios"],indent=2))
