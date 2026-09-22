# RobotTrust
## Robotics Evaluation & Reliability Platform

Current milestone: **B1 — Canonical Evaluation Foundation**

Python 3.11+ foundation for deterministic synthetic robotics policy evaluation. B1 supports warehouse_navigation, obstacle_avoidance, object_delivery, sensor_dropout, and emergency_stop scenarios, typed validation, calculated metrics, configurable release gates, a FastAPI service, and replayable evidence with SHA-256 checksums.

### Architecture

Synthetic Robot Episode → Episode/Event Schema → Validation → Evaluation Engine → Metrics → PASS / BLOCK / ESCALATE → Replayable JSONL Evidence.

See [the evaluation contract](docs/architecture.md) for semantics and limitations. There is no ROS 2, Isaac Sim, Kafka, Ray, Kubernetes, cloud integration, distributed execution, real robot testing, or production deployment in B1.

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
docker build -t robottrust:b1 .
docker run --rm --name robottrust-b1 -p 8001:8000 robottrust:b1
```

Docker requires a running Linux-container daemon. The image runs the API as a non-root user. This mapping exposes it at http://127.0.0.1:8001 and avoids a host-port-8000 collision.

### Evidence and CI

* `evidence/episodes.jsonl`: validated replayable input episodes.
* `evidence/evaluation.json`: calculated metrics and release result.
* `evidence/decision.json`: decision, reasons and effective thresholds.
* `evidence/checksums.json`: SHA-256 hashes of the three artifacts above.

The CLI creates evidence from actual evaluated inputs. API calls do not persist evidence. Use separate output directories for concurrent runs. GitHub Actions installs dependencies and runs pytest on Python 3.11–3.13; failed tests fail CI. Synthetic latency inputs are not service benchmarks; `benchmarks/` documents this distinction.
