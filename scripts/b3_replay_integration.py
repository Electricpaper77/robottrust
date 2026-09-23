"""Real Redpanda finite replay against a populated ledger and isolated test topic."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import time
import uuid
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from robottrust.generator import generate_episodes
from robottrust.streaming.config import ConsumerConfig, ProducerConfig, ReplayConfig
from robottrust.streaming.consumer import EpisodeConsumer
from robottrust.streaming.events import create_event, canonical_payload_bytes
from robottrust.streaming.producer import source_key
from robottrust.streaming.replay import ReplaySession, write_replay_evidence
from robottrust.streaming.store import IngestionStore


def publish(client: Producer, topic: str, partition: int, value: bytes | None, key: bytes) -> dict:
    outcomes=[]
    client.produce(topic,partition=partition,value=value,key=key,on_delivery=lambda error,msg:outcomes.append((error,msg)))
    assert client.flush(15)==0, "delivery timeout"
    assert len(outcomes)==1 and outcomes[0][0] is None, outcomes
    message=outcomes[0][1]
    return dict(topic=message.topic(),partition=message.partition(),offset=message.offset())


def commits(config: ConsumerConfig) -> dict[int,int]:
    client=Consumer(config.client_settings())
    try:
        results=client.committed([TopicPartition(config.topic,p) for p in range(3)],timeout=15)
        assert len(results)==3 and all(r.error is None for r in results)
        return {r.partition:r.offset for r in results}
    finally:
        client.close()


def run(ledger: Path, output: Path, bootstrap: str) -> dict:
    if ledger.exists():
        raise FileExistsError("integration requires a fresh test ledger, populated by live ingestion before replay")
    ledger.parent.mkdir(parents=True,exist_ok=True)
    suffix=uuid.uuid4().hex
    topic="robottrust.replay.b3_3_2."+suffix
    admin=AdminClient({"bootstrap.servers":bootstrap})
    admin.create_topics([NewTopic(topic,num_partitions=3,replication_factor=1)])[topic].result(20)
    info=admin.list_topics(topic,timeout=15).topics[topic]
    assert info.error is None and len(info.partitions)==3
    assert all(len(p.replicas)==1 for p in info.partitions.values())
    config=ConsumerConfig(bootstrap_servers=bootstrap,topic=topic,group_id="b332-live-"+suffix,bootstrap_policy="earliest")
    raw=Producer(ProducerConfig(bootstrap_servers=bootstrap,topic=topic).client_settings())
    # Bounded fixture: a stable source per explicitly selected partition.
    events=[create_event(ep,"b3.3.2-fixture","robot-"+str(i%3),i) for i,ep in enumerate(generate_episodes(15,42))]
    deliveries=[publish(raw,topic,i%3,canonical_payload_bytes(e),source_key(e)) for i,e in enumerate(events[:12])]
    deliveries.append(publish(raw,topic,0,canonical_payload_bytes(events[0]),source_key(events[0])))
    conflict=events[1].model_copy(update={"sequence":999})
    deliveries.append(publish(raw,topic,1,canonical_payload_bytes(conflict),source_key(conflict)))
    deliveries.append(publish(raw,topic,2,b"{",b"invalid"))
    deliveries.append(publish(raw,topic,0,None,b"tombstone"))
    deliveries.append(publish(raw,topic,1,b"",b"empty"))
    expected={e.event_id for e in events[:12]}
    with IngestionStore(ledger) as store:
        with EpisodeConsumer(config,store) as live:
            live.consume(len(deliveries),90)
        before=store.accepted_count()
        assert before==12
        accepted_before=store.accepted_page(limit=100)
        assert {row["event_id"] for row in accepted_before}==expected
        live_before={p:asdict(store.live_checkpoint(topic,p)) for p in range(3)}
        legacy_before={p:store.latest_checkpoint(topic,p) for p in range(3)}
        kafka_before=commits(config)
        with ReplaySession(ReplayConfig(bootstrap_servers=bootstrap,topic=topic,timeout_s=90),store) as replay:
            manifest=replay.capture()
            assert [b.start_offset for b in manifest.boundaries]==[0,0,0]
            deadline=time.monotonic()+20
            while not sum(store.replay_counts(replay.session_id).values()):
                replay.step()
                if time.monotonic()>deadline:
                    raise TimeoutError("replay did not begin before publication deadline")
            post=[publish(raw,topic,i%3,canonical_payload_bytes(events[i]),source_key(events[i])) for i in range(12,15)]
            ends={b.partition:b.end_offset for b in manifest.boundaries}
            assert all(d["offset"]>=ends[d["partition"]] for d in post)
            state=replay.run()
        counts=store.replay_counts(replay.session_id)
        assert counts=={"ACCEPTED":0,"DUPLICATE":13,"CONFLICT":1,"REJECTED":3},counts
        after=store.accepted_count()
        assert after==before
        accepted_after=store.accepted_page(limit=100)
        assert accepted_after==accepted_before
        assert all(store.lookup_event(e.event_id) is None for e in events[12:])
        live_after={p:asdict(store.live_checkpoint(topic,p)) for p in range(3)}
        legacy_after={p:store.latest_checkpoint(topic,p) for p in range(3)}
        kafka_after=commits(config)
        assert live_after==live_before and legacy_after==legacy_before and kafka_after==kafka_before
        # Fixture is exactly 17 records; production evidence writing remains paginated.
        cursor=0
        observations=[]
        while True:
            page=store.replay_page(replay.session_id,after_receipt=cursor,limit=4)
            if not page:
                break
            observations.extend(page)
            assert len(observations)<=17
            cursor=page[-1]["receipt_id"]
        observed={(r["topic"],r["partition"],r["offset"]) for r in observations}
        assert observed=={(d["topic"],d["partition"],d["offset"]) for d in deliveries}
        assert not observed.intersection((d["topic"],d["partition"],d["offset"]) for d in post)
        covered={r["event_id"] for r in observations if r["disposition"]=="DUPLICATE"}
        missing,unexpected=sorted(expected-covered),sorted(covered-expected)
        assert not missing and not unexpected
        assert state["status"]=="COMPLETE" and all(p["complete"] and p["next_offset"]==p["end_offset"] for p in state["partitions"])
        summary=dict(session_id=replay.session_id,topic=topic,seed=42,broker_id=manifest.broker_id,topic_id=manifest.topic_id,
            partitions=[b.partition for b in manifest.boundaries],start_vector={b.partition:b.start_offset for b in manifest.boundaries},
            end_vector=ends,completion_vector={p["partition"]:p["next_offset"] for p in state["partitions"]},
            accepted_before=before,accepted_after=after,accepted_delta=after-before,counts=counts,
            accepted_ids_before=accepted_before,accepted_ids_after=accepted_after,missing_ids=missing,unexpected_ids=unexpected,
            post_boundary_excluded=len(post),post_boundary_deliveries=post,observations=len(observations),
            live_checkpoints_before=live_before,live_checkpoints_after=live_after,
            live_legacy_checkpoints_before=legacy_before,live_legacy_checkpoints_after=legacy_after,
            live_broker_commits_before=kafka_before,live_broker_commits_after=kafka_after)
        write_replay_evidence(store,replay.session_id,output,page_size=4)
        (output/"integration.json").write_text(json.dumps(summary,indent=2)+"\n",encoding="utf-8",newline="\n")
        checks=json.loads((output/"checksums.json").read_text(encoding="utf-8"))
        with (output/"integration.json").open("rb") as stream:
            checks["integration.json"]=hashlib.file_digest(stream,"sha256").hexdigest()
        (output/"checksums.json").write_text(json.dumps(checks,indent=2)+"\n",encoding="utf-8",newline="\n")
        return summary


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger",type=Path,required=True)
    parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--bootstrap-servers",default="127.0.0.1:19092")
    args=parser.parse_args()
    result=run(args.ledger,args.output_dir,args.bootstrap_servers)
    print(json.dumps({k:result[k] for k in ["session_id","start_vector","end_vector","completion_vector","accepted_delta","counts","missing_ids","unexpected_ids","post_boundary_excluded"]},indent=2))
