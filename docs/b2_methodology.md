# B2 local Ray evaluation contract

B2 is local distributed/parallel evaluation using Ray 2.54.0, not a multi-node system. One Ray actor process reserves one logical CPU. Initialization explicitly starts a local runtime, waits for every actor to report its PID, and shuts down the owned runtime on exit. Existing external runtimes are rejected rather than taken over. Worker count cannot exceed the machine's logical CPU count. This host has eight logical CPUs, so no 16-worker measurements are claimed.

## Correctness and integrity

Input-order contiguous partitions differ in size by at most one. Each episode is assigned to exactly one worker task; task retry and actor restart are disabled. Duplicate input IDs fail before dispatch. Returned episode IDs and records must match the assigned partition exactly; duplicates, omissions, unknown IDs, lost partitions, unexpected partition IDs, record mutation, malformed outputs and invalid worker evaluations fail closed. No partial result is returned.

Workers validate their payload with the existing Episode schema and call the existing B1 evaluator. The coordinator verifies each partition evaluation using B1, restores the original record order, and calls B1 for final reduction. This preserves exact floating-point means and the global nearest-rank p95 rather than averaging partition percentiles. All canonical fields, reasons and thresholds must compare exactly; no numeric tolerance is used.

This is a correctness-first execution foundation. It intentionally performs serial verification and final reduction on the coordinator, using returned observations, and therefore is not a scalable replacement for the B1 arithmetic. Each input has one worker assignment, while coordinator verification re-evaluates the returned data. The cost of that work is included in reported timings and limits scaling.

## Failure handling

There are no retries: one attempt per worker task, with max_restarts=0 and max_task_retries=0. A remote exception, dead actor or task timeout produces WorkerFailure with attempt_count=1, final_status=failed and failure_reason; the runtime closes to cancel outstanding work. Integrity errors raise IntegrityError with counters whenever output IDs can be recovered. Benchmark CLI failures write b2_failure.json and exit nonzero. Previously completed evidence is never interpreted as success for a failed invocation; inspect timestamp and source fingerprint. Startup handshakes and task collection have a 180-second timeout; Ray's own initialization is governed by Ray.

## Benchmark protocol

Generate fixed seed-42 inputs with the unmodified B1 generator. For each dataset size and worker count, initialize a fresh Ray runtime, record startup time through all-worker readiness, warm the same code path on 32 seed-43 records, then retain three measured repetitions of the requested dataset. Each repetition is a separate evaluation invocation. Timing uses perf_counter. No sleeps, inflated per-episode loops or simulated latency are added.

Wall duration includes partitioning, encoding, transport, worker validation and evaluation, result validation, integrity checks and canonical reduction. Dataset generation, serial-reference calculation, runtime startup, warmup, report writing and shutdown are outside the measured wall interval. The median of the three wall durations is reported; every sample remains in JSON. Worker configurations run in ascending order, so system drift/cache effects can influence comparisons. Runs are descriptive local measurements, not statistical claims of universal speedup.

Throughput = episodes / median duration in seconds. Speedup = matching dataset's one-Ray-worker median / current median. Efficiency = speedup / configured workers. CLI automatically runs the one-worker baseline if omitted. Serial B1 duration is recorded separately and is not the scaling denominator.

Mean and nearest-rank p95 partition evaluation latency measure only the B1 evaluator inside workers, in milliseconds. They are not end-to-end episode latency and not the synthetic inference_latency_ms field. Partition sizes vary with worker count, so these timing distributions should not be compared as equal-sized operations. Success/failure counters describe worker tasks; robot success and failure remain in canonical metrics. Per-row integrity counters describe each repetition; task/attempt counts in CSV total all measured repetitions. Warmup is excluded.

The mandatory 1,000 and 10,000 episode datasets are supplemented by 100,000 episodes because canonical evaluation is lightweight. This increases the actual input size without changing evaluation semantics. All positive or negative scaling results are retained. Benchmarks never run in CI; CI runs deterministic correctness and actual small Ray integration tests.

## Provenance and reproduction

Each row records timestamp, checked-out commit, dirty status, exact source SHA-256, Python/Ray/platform versions, logical CPU count, dataset hash, seed and worker count. The source fingerprint hashes sorted src/robottrust/*.py followed by pyproject.toml, including relative path + NUL + file bytes. It disambiguates benchmark execution before the final feature commit. Dataset hashes cover canonical UTF-8 JSONL with LF newlines. Generated JSON stores all raw trials, full evaluation outputs, PID identities and integrity counters; CSV is a flattened summary, and the Markdown table is generated from the same rows.

```powershell
python -m pip install -e ".[test]"
$env:RAY_USAGE_STATS_ENABLED = "0"
python -m robottrust.benchmark --episodes 1000 10000 100000 --seed 42 --workers 1 2 4 8 --repetitions 3 --ray-temp-dir C:/Projects/robottrust/work/ray --output-dir benchmarks
python -m robottrust.benchmark --episodes 10000 --seed 42 --workers 4
python -m pytest -q
```

Use a short absolute Ray temporary directory when specifying one on Linux (Unix-domain socket paths have limits); omit --ray-temp-dir for Ray defaults. Windows Ray support is beta, as described by [Ray installation documentation](https://docs.ray.io/en/latest/ray-overview/installation.html). Lifecycle and resource configuration follow [Ray initialization](https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html); retries follow [actor fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html).

No Kafka, ROS 2, Isaac Sim, Kubernetes, cloud deployment or multi-node cluster is implemented. API behavior and B1 evidence remain unchanged.
