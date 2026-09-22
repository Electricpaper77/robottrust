# B3.2 local broker transport

B3.2 uses **at-least-once delivery plus idempotent durable ingestion**. It does not provide end-to-end exactly-once processing. B1/B2 evaluation and B3.1 event identity remain unchanged. Replay/reconstruction is reserved for B3.3.

The pinned client is confluent-kafka 2.15.1. The local broker is Redpanda v26.2.3, with one node, three partitions in `robottrust.episodes.v1`, and replication factor 1. This is a development topology without production HA. Compose exposes only loopback port 19092 and keeps data in a named volume.

## Contract

Producer keys are compact JSON `[run_id, source_id]` bytes. A stable source routes to one partition while partition count remains fixed. There is no global ordering across partitions. Producers use idempotence, `acks=all`, a 10-second delivery timeout and bounded 15-second flush. Each publish checks its callback and returns actual coordinates. Uncertain delivery raises an error; retain the same envelope when retrying. Producer idempotence alone does not remove duplicates across producer restarts.

The consumer requires an explicit group ID and disables both automatic commits and automatic offset storage. It processes one polled record at a time: decode and validate, commit the SQLite WAL/FULL transaction with receipt, position and checkpoint, then synchronously commit the broker next offset. A ledger failure prevents broker commit. A broker commit failure stops that consumer instance; a new instance can receive the record again, producing a duplicate receipt instead of a second accepted event. Kafka and SQLite do not share a transaction.

Malformed UTF-8/JSON, unsupported versions, invalid Episodes, tombstones and key mismatches produce durable REJECTED receipts before offset commit. Conflicting identities produce CONFLICT; exact retries produce DUPLICATE. The ledger's generic `reject` entry point records transport-level validation failures through the same transaction; it contains no Kafka logic. Tombstones have empty raw bytes and an explicit null-payload rejection reason. Positions retain their original binding under the B3.1 contract. On older redelivery SQLite checkpoints do not move backward.

Polling and publishing have bounded waits; processing is synchronous with no application work queue. Librdkafka still has its own finite buffers. Sustained overload, rebalance stress, broker restart recovery, and disk exhaustion under load are not established by this small B3.2 acceptance run.

## Local validation (PowerShell)

```powershell
docker compose config --quiet
docker compose up -d --wait --wait-timeout 180
.\.venv\Scripts\python.exe scripts/b3_transport_integration.py --ledger work/b3-fresh.sqlite3 --output-dir work/b3-evidence
docker compose stop
```

Use a new ledger path on each invocation; an existing file is rejected. The script creates or verifies exactly three partitions and replication factor one. It captures starting high offsets to isolate its test window from retained topic data. This is not a replay tool. A unique run namespace prevents interference; event IDs are deterministic for that namespace and seeded Episode data. Twelve valid events, one duplicate, one conflict, and six invalid records are published through real acknowledged broker calls. Assertions compare expected IDs, coordinates, partition order, durable counts and actual committed offsets. Failure exits nonzero; there is no broker-test skip.

The checked-in `evidence/b3_2` report and accepted-envelope JSONL come from the local real broker run. `checksums.json` covers both. SQLite remains in ignored `work/`; Compose stop preserves the named volume. CI runs the ordinary regression matrix and a separate real broker job, preserving logs and integration evidence before stopping its broker.

## CLI

```powershell
.\.venv\Scripts\python.exe -m robottrust.streaming.producer --run-id demo-1 --episodes 10 --seed 42
.\.venv\Scripts\python.exe -m robottrust.streaming.consumer --group-id demo-consumer --ledger work/demo.sqlite3 --messages 10 --timeout 60
```

Create the topic first using the integration script or `docker compose exec redpanda rpk topic create robottrust.episodes.v1 --partitions 3 --replicas 1`. A fresh consumer group starts at earliest retained offsets. CLI consumers must have exclusive ownership of their local ledger's topic/partition checkpoint namespace; sharing one ledger across independent groups is outside this milestone.
