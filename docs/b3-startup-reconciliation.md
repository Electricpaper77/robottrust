# B3.3.1 startup reconciliation

Live startup reconciles durable SQLite next offset D with broker committed offset K before permitting any record processing. `auto.offset.reset=error` prevents fallback to earliest/latest when required history is missing. Identity uses the broker-reported cluster ID and topic ID, plus topic name, partition number and configured partition count (three by default). Startup fails if the broker cannot expose a usable incarnation ID.

Schema version 2 deterministically adds `live_partitions` and `startup_actions` to a v1 ledger, preserving all existing tables and data. Legacy checkpoints remain readable but cannot drive live consumption: their start boundary and provenance cannot be inferred safely. Startup returns FAIL_BOOTSTRAP_REQUIRED; no automatic historical adoption or data deletion is provided.

Each live partition records LIVE mode, start_offset, next_offset, identity and UTC creation/update timestamps. A record transaction updates acceptance, receipt, unique transport position, legacy checkpoint and live progress atomically under WAL/FULL. Coverage applies only to [start_offset,next_offset). In this milestone, coverage verification conservatively requires every numeric offset in that interval to have a durable position. Topics with transaction/control/compaction offset gaps fail closed; gap-aware traversal is not implemented.

| State | Action |
| --- | --- |
| D=K | Seek D |
| D>K | Verify coverage/history, seek D, synchronously repair commit to D and verify result |
| K>D, D retained | Seek D; recover local work; do not skip to K |
| No K, valid D | Seek D |
| K but no durable provenance | Fail bootstrap required |
| Neither | Require explicit new-stream start policy |
| D below retained start | Fail expired history |
| D beyond end, invalid coverage/interval | Fail inconsistent history |
| Broker/topic incarnation mismatch | Fail identity mismatch |
| Partition count mismatch | Fail topology changed |

Use the same local ledger for subsequent starts. A new local stream/group requires deliberate `--bootstrap-policy earliest` or `--bootstrap-policy latest`; latest explicitly excludes previous history from coverage. Manual integrations may select an explicit offset per partition. Broker absence of a commit cannot prove a group has never existed: the policy is the operator's explicit authorization of a new stream. Legacy local positions/checkpoints cannot be promoted by this option.

```powershell
.\.venv\Scripts\python.exe -m robottrust.streaming.consumer --group-id new-b331-group --ledger work/new-b331.sqlite3 --messages 10 --bootstrap-policy earliest
# Subsequent starts omit --bootstrap-policy.
```

Startup receives the assignment, inspects every partition, records decisions, assigns explicit offsets, then seeks and performs any repair before allowing processing. This ordering avoids seeking an uninitialized Kafka assignment. Revoke/lost callbacks remove permission to process those partitions. Processing remains synchronous; this is not distributed ownership fencing or a backpressure implementation. One consumer process owns a live ledger.

Old redelivery inside verified coverage may append a DUPLICATE receipt but commits D rather than the old record's offset+1. When the observed broker commit is ahead, local processing still starts at D; commits are withheld until local durability catches up to avoid backward broker commits. Recovery outcomes include topic, partition, D, K, earliest, end, reason and seek offset. Startup intentions, completed repairs and failures are recorded durably when SQLite remains writable.

## Real broker validation

The test uses a new ledger and unique group, publishes six seed-42 events on one stable source partition, and initially accepts three. It forces K ahead, restarts via normal subscription, and verifies three missing local records are accepted. It then forces K behind, restarts again and verifies repair without re-ingestion. Both scenarios verify exact accepted IDs and actual committed offsets. Other partitions are also reconciled.

```powershell
docker compose up -d --wait --wait-timeout 180
.\.venv\Scripts\python.exe scripts/b3_reconciliation_integration.py --ledger work/new-reconciliation.sqlite3 --output-dir work/reconciliation-evidence
docker compose stop
```

Create the three-partition topic first using the existing transport integration script if absent. The checked-in evidence/b3_3_1 report records the actual bounded local run and SHA-256 checksum; B3.2 evidence is unchanged. CI runs both transport and reconciliation scenarios in its separate real-broker job.

Replay/re-ingestion, reconstruction, backpressure, subprocess crash tests and destructive retention tests are outside B3.3.1. Unit tests cover retention expiration and incompatible history. No exactly-once or high-availability guarantee is claimed.
