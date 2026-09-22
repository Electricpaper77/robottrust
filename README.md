# RobotTrust
## Robotics Evaluation & Reliability Platform

Current milestone: **B2 — Distributed Evaluation Workers**

B1 remains the canonical schema, metrics, release policy, evidence and API layer.

Python 3.11+ foundation for deterministic synthetic robotics policy evaluation. B1 supports warehouse_navigation, obstacle_avoidance, object_delivery, sensor_dropout, and emergency_stop scenarios, typed validation, calculated metrics, configurable release gates, a FastAPI service, and replayable evidence with SHA-256 checksums.

### Architecture

Synthetic Robot Episode → Episode/Event Schema → Validation → Evaluation Engine → Metrics → PASS / BLOCK / ESCALATE → Replayable JSONL Evidence.

See [the evaluation contract](docs/architecture.md) for semantics and limitations. B2 adds local distributed/parallel evaluation using Ray. No multi-node cluster, ROS 2, Isaac Sim, Kafka, Kubernetes, cloud deployment, real robot testing, or production distributed system is claimed.

### Example episode

```json
{"episode_id":"example-1","timestamp":"2026-01-01T00:00:00Z","policy_version":"synthetic-v1","scenario":"warehouse_navigation","task":"navigate_to_goal","success":true,"collision_count":0,"duration_ms":12000,"inference_latency_ms":100,"sensor_dropout_rate":0.01,"safety_violation":false,"termination_reason":"completed"}
```

### Metrics and release policy

Metrics come from supplied episodes: episode count, task success rate, collision rate (fraction of episodes with any collision), safety violation rate, mean inference latency, p95 inference latency (nearest rank), mean sensor dropout rate, and failed episode count. Rates use [0, 1]; latency uses milliseconds. Empty batches and duplicate IDs are rejected.

* BLOCK: safety violation rate > 1% OR collision rate > 5%.
* ESCALATE, unless blocked: success rate < 90% OR p95 latency > 500 ms.
* PASS otherwise. Equality at each boundary is allowed.

Configure `Thresholds` in Python, pass a JSON file to the evaluator with `--thresholds`, or provide a `thresholds` object to POST /evaluate. Keys: `max_safety_violation_rate`, `max_collision_rate`, `min_task_success_rate`, `max_p95_inference_latency_ms`.

### Local setup (PowerShell)

```powershell
cd C:\Projects\robottrust
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
python -m pytest -q
python -m robottrust.generator --episodes 100 --seed 42
python -m robottrust.generator --episodes 100 --seed 42 --output evidence/episodes.jsonl
python -m robottrust.evaluator --input evidence/episodes.jsonl --output-dir evidence
python -c "from robottrust.evidence import verify_checksums; assert verify_checksums('evidence')"
python -m uvicorn robottrust.api:app --host 127.0.0.1 --port 8000
```

On Unix activate with `source .venv/bin/activate`. The generator prints JSONL to stdout by default; `--output` writes UTF-8 directly, avoiding shell redirection encoding differences.

### API

GET /health returns status. POST /evaluate accepts `{"episodes": [episode, ...], "thresholds": {}}` and returns calculated `metrics`, `decision`, `reasons`, and effective `thresholds`. GET /metrics and GET /decision return the latest process-local result, or 404 before any evaluation. Interactive documentation: http://127.0.0.1:8000/docs.

```powershell
$episodes = @(Get-Content evidence/episodes.jsonl | ForEach-Object { $_ | ConvertFrom-Json })
$body = @{ episodes = $episodes } | ConvertTo-Json -Depth 10
Invoke-RestMethod -Uri http://127.0.0.1:8000/evaluate -Method Post -ContentType 'application/json' -Body $body
```

### Docker

```powershell
docker build -t robottrust:b2 .
docker run --rm --name robottrust-b2 -p 8002:8000 robottrust:b2
```

Docker requires a running Linux-container daemon. The image runs the API as a non-root user. This mapping exposes it at http://127.0.0.1:8002, leaving existing workloads on ports 8000 and 8001 available.

### Evidence and CI

* `evidence/episodes.jsonl`: validated replayable input episodes.
* `evidence/evaluation.json`: calculated metrics and release result.
* `evidence/decision.json`: decision, reasons and effective thresholds.
* `evidence/checksums.json`: SHA-256 hashes of the three artifacts above.

The CLI creates evidence from actual evaluated inputs. API calls do not persist evidence. Use separate output directories for concurrent runs. GitHub Actions installs dependencies and runs pytest on Python 3.11–3.13; failed tests fail CI. Synthetic latency inputs are not service benchmarks; `benchmarks/` documents this distinction.

### B2 Ray workers

Episode batch → deterministic balanced partitions → local Ray coordinator → one actor process per configured worker → validated partition results → canonical B1 reduction → release decision + measured benchmark evidence.

Each episode is assigned once to a worker; each worker calls the existing B1 evaluator. The coordinator rejects duplicate, missing, unknown or changed records, partition loss, malformed results and invalid partial evaluations. It verifies partial results and reduces returned records in original order using B1, so all metrics and decisions match serial evaluation exactly, without floating-point tolerance. This serial verification and reduction remains a scaling bottleneck and is included in benchmark timing. No partition means or percentiles are averaged.

The context-managed RayEvaluator owns and shuts down a local runtime. Worker count is configurable; 1/2/4/8 are measured here. Sixteen workers are allowed only on hosts with sufficient logical CPUs and are not claimed on this eight-CPU host. Actor restarts and task retries are disabled; failures surface with attempt count, status and reason. No partial success is returned.

```powershell
$env:RAY_USAGE_STATS_ENABLED = "0"
python -m robottrust.benchmark --episodes 10000 --seed 42 --workers 4
python -m robottrust.benchmark --episodes 1000 10000 100000 --seed 42 --workers 1 2 4 8 --repetitions 3 --ray-temp-dir C:/Projects/robottrust/work/ray --output-dir benchmarks
```

A one-worker baseline is always measured. Durations are median warm end-to-end wall seconds from three repetitions, including serialization, dispatch, schema validation, integrity checking and reduction; generation/startup/warmup/shutdown are excluded. Startup duration and every raw sample are retained. Throughput is episodes/second; speedup is one-worker duration/current duration; efficiency is speedup/workers. Worker partition latency is recorded separately from synthetic inference latency. The extra 100K dataset increases actual input size because B1 evaluation is lightweight; no artificial sleeps or inflated compute loops are used.

See [methodology and limitations](docs/b2_methodology.md), [raw JSON](benchmarks/b2_scaling.json), [CSV](benchmarks/b2_scaling.csv), and [generated summary](benchmarks/b2_summary.md). Benchmarks record source fingerprints, checkout SHA, Python/Ray versions, CPU count, seed and input hashes. CI runs all B1 and B2 correctness tests, including small real Ray executions; it does not run the scaling suite.

### Actual benchmark results

| Episodes | Workers | Seconds | Episodes/s | Speedup | Efficiency |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1 | 0.049349 | 20263.8 | 1.000x | 1.000 |
| 1,000 | 2 | 0.045180 | 22133.5 | 1.092x | 0.546 |
| 1,000 | 4 | 0.037369 | 26760.0 | 1.321x | 0.330 |
| 1,000 | 8 | 0.038772 | 25792.1 | 1.273x | 0.159 |
| 10,000 | 1 | 0.456945 | 21884.5 | 1.000x | 1.000 |
| 10,000 | 2 | 0.395379 | 25292.2 | 1.156x | 0.578 |
| 10,000 | 4 | 0.369556 | 27059.5 | 1.236x | 0.309 |
| 10,000 | 8 | 0.351333 | 28463.0 | 1.301x | 0.163 |
| 100,000 | 1 | 6.425811 | 15562.2 | 1.000x | 1.000 |
| 100,000 | 2 | 4.921223 | 20320.2 | 1.306x | 0.653 |
| 100,000 | 4 | 4.420621 | 22621.3 | 1.454x | 0.363 |
| 100,000 | 8 | 4.479566 | 22323.6 | 1.434x | 0.179 |

Speedup uses the matching Ray one-worker baseline, not serial B1. Serial B1 timings, startup, worker partition timings, exact results, integrity and every raw sample are in JSON.

These lightweight evaluations include serial integrity verification and canonical reduction; negative scaling is valid evidence. No sleeps or artificial workload are used.

The measurements show modest gains over one Ray worker, not linear scaling. Eight workers were slower than four for 1K and 100K inputs. Serial B1 remains substantially faster than the Ray execution layer for these inexpensive metrics; B2 establishes worker correctness and measured overhead rather than a production throughput claim.

## B3.2 local streaming transport

Pinned Redpanda and confluent-kafka connect acknowledged publishing to the B3.1 durable ledger with manual offset commits. The contract is at-least-once delivery plus idempotent durable ingestion. See [transport setup, guarantees and validation](docs/b3-transport.md). B3.3 replay/reconstruction is not implemented.
