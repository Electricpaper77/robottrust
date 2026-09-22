"""Real broker acceptance check. Never skipped; requires running Compose broker."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import hashlib
from importlib.metadata import version as package_version
import json
from pathlib import Path
import uuid
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from robottrust.generator import generate_episodes
from robottrust.streaming.config import ConsumerConfig, ProducerConfig
from robottrust.streaming.events import create_event, canonical_payload_bytes
from robottrust.streaming.producer import EpisodeProducer, source_key
from robottrust.streaming.consumer import EpisodeConsumer
from robottrust.streaming.store import IngestionStore


def run(ledger: Path, output: Path, bootstrap: str) -> dict:
    if ledger.exists():
        raise FileExistsError("integration requires a fresh ledger")
    ledger.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    topic = "robottrust.episodes.v1"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=15)
    if topic not in metadata.topics:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(15)
    metadata = admin.list_topics(topic, timeout=15).topics[topic]
    assert metadata.error is None
    assert len(metadata.partitions) == 3
    assert all(len(part.replicas) == 1 for part in metadata.partitions.values())
    run_id = "b3.2-" + uuid.uuid4().hex
    config = ConsumerConfig(bootstrap_servers=bootstrap, group_id=run_id)
    client = Consumer(config.client_settings())
    try:
        # Isolate this test window without deleting existing topic history.
        starts = [TopicPartition(topic, p, client.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1]) for p in range(3)]
        client.assign(starts)
        events = [create_event(ep, run_id, "robot-" + str(i % 3), i) for i, ep in enumerate(generate_episodes(12, 42))]
        producer = EpisodeProducer(ProducerConfig(bootstrap_servers=bootstrap))
        deliveries = [asdict(producer.publish(event)) for event in events]
        deliveries.append(asdict(producer.publish(events[0])))
        deliveries.append(asdict(producer.publish(events[0].model_copy(update={"sequence": 999}))))
        raw = Producer(ProducerConfig(bootstrap_servers=bootstrap).client_settings())
        invalid_version = events[0].model_dump(mode="json")
        invalid_version["schema_version"] = 2
        invalid_episode = events[0].model_dump(mode="json")
        invalid_episode["episode"]["collision_count"] = -1
        invalid = [(b"\xff", b"bad"), (b"{", b"bad"),
                   (json.dumps(invalid_version).encode(), source_key(events[0])),
                   (json.dumps(invalid_episode).encode(), source_key(events[0])),
                   (None, b"tombstone"), (canonical_payload_bytes(events[0]), b"wrong")]
        for index, (value, key) in enumerate(invalid):
            outcomes = []
            raw.produce(topic, partition=index % 3, value=value, key=key,
                        on_delivery=lambda error, msg: outcomes.append((error, msg)))
            assert raw.flush(15) == 0, "raw fault-input delivery timeout"
            assert len(outcomes) == 1 and outcomes[0][0] is None, outcomes
            msg = outcomes[0][1]
            deliveries.append(dict(topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), event_id=None))
        with IngestionStore(ledger) as store:
            worker = EpisodeConsumer(config, store, client=client, subscribe=False)
            results = worker.consume(len(deliveries), timeout_s=90)
            accepted = store.accepted_events(run_id)
            expected = {event.event_id for event in events}
            actual = {event.event_id for event in accepted}
            assert actual == expected, (expected - actual, actual - expected)
            counts = {key.value: value for key, value in store.counts_by_disposition().items()}
            assert counts == {"ACCEPTED": 12, "DUPLICATE": 1, "CONFLICT": 1, "REJECTED": 6}, counts
            positions = {(d["topic"], d["partition"], d["offset"]) for d in deliveries}
            assert positions == {(r.position.topic, r.position.partition, r.position.offset) for r in results}
            assert all(store.lookup_position(r.position) == r.ingestion for r in results)
            for partition in range(3):
                offsets = [r.position.offset for r in results if r.position.partition == partition]
                assert offsets == sorted(offsets), "partition order violated"
                expected_next = max(offsets) + 1
                assert store.latest_checkpoint(topic, partition) == expected_next
                assert client.committed([TopicPartition(topic, partition)], timeout=15)[0].offset == expected_next
            report = dict(run_id=run_id, seed=42, client_version=package_version("confluent-kafka"), topic=topic,
                          partitions=3, replication_factor=1, published=len(deliveries), counts=counts,
                          missing=len(expected-actual), unexpected=len(actual-expected),
                          transport_positions=len(positions), deliveries=deliveries,
                          results=[dict(position=r.position.model_dump(), ingestion=asdict(r.ingestion),
                                        committed_next_offset=r.committed_next_offset) for r in results],
                          contract="at-least-once delivery plus idempotent durable ingestion")
            (output / "accepted-events.jsonl").write_text("".join(canonical_payload_bytes(e).decode()+"\n" for e in accepted), encoding="utf-8", newline="\n")
            (output / "report.json").write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8", newline="\n")
            checksums = {name: hashlib.sha256((output/name).read_bytes()).hexdigest() for name in ["accepted-events.jsonl", "report.json"]}
            (output / "checksums.json").write_text(json.dumps(checksums, indent=2)+"\n", encoding="utf-8", newline="\n")
            return report
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    args = parser.parse_args()
    result = run(args.ledger, args.output_dir, args.bootstrap_servers)
    print(json.dumps({k: v for k, v in result.items() if k not in ("deliveries", "results")}, indent=2))
