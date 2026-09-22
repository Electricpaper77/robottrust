"""Measured local Ray scaling; all samples and provenance are persisted."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
from statistics import fmean, median
import subprocess
import sys
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field
import ray

from robottrust.distributed import Integrity, RayEvaluator
from robottrust.evaluator import Evaluation, evaluate
from robottrust.generator import generate_episodes


class Trial(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    duration_s: float = Field(gt=0)
    partition_evaluation_durations_s: list[float]
    worker_pids: list[int]
    integrity: Integrity
    evaluation: Evaluation
    successful_tasks: int = Field(gt=0)
    failed_tasks: Literal[0] = 0
    attempt_count: int = Field(gt=0)
    final_status: Literal["success"] = "success"
    failure_reason: None = None


class BenchmarkRow(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    timestamp: str
    commit_sha: str
    source_sha256: str
    source_dirty: bool
    python_version: str
    ray_version: str
    platform: str
    cpu_count: int = Field(gt=0)
    episode_count: int = Field(gt=0)
    dataset_sha256: str
    seed: int
    worker_count: int = Field(gt=0)
    startup_duration_s: float = Field(ge=0)
    serial_duration_s: float = Field(gt=0)
    baseline_duration_s: float = Field(gt=0)
    samples: list[Trial] = Field(min_length=1)

    @computed_field
    @property
    def duration_s(self) -> float:
        return median(sample.duration_s for sample in self.samples)

    @computed_field
    @property
    def episodes_per_second(self) -> float:
        return self.episode_count / self.duration_s

    @computed_field
    @property
    def speedup(self) -> float:
        return self.baseline_duration_s / self.duration_s

    @computed_field
    @property
    def scaling_efficiency(self) -> float:
        return self.speedup / self.worker_count

    @computed_field
    @property
    def mean_partition_evaluation_latency_ms(self) -> float:
        return 1000 * fmean(t for sample in self.samples for t in sample.partition_evaluation_durations_s)

    @computed_field
    @property
    def p95_partition_evaluation_latency_ms(self) -> float:
        values = sorted(t for sample in self.samples for t in sample.partition_evaluation_durations_s)
        return 1000 * values[math.ceil(.95 * len(values))-1]


def source_provenance() -> dict[str, object]:
    root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
    paths = sorted((root / "src/robottrust").glob("*.py")) + [root / "pyproject.toml"]
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes())
    return {"commit_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "source_sha256": digest.hexdigest(),
            "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True)),
            "python_version": platform.python_version(), "ray_version": ray.__version__,
            "platform": platform.platform(), "cpu_count": os.cpu_count() or 1}


def run_benchmarks(sizes: list[int], workers: list[int], seed: int = 42, repetitions: int = 3,
                   temp_dir: str | None = None) -> list[BenchmarkRow]:
    if repetitions < 1 or not sizes or any(n < 1 for n in sizes) or not workers:
        raise ValueError("positive dataset sizes, workers and repetitions required")
    # Always measure a real same-dataset one-worker baseline, even for --workers 4.
    worker_counts = sorted(set([1, *workers]))
    if min(worker_counts) < 1 or max(worker_counts) > min(16, os.cpu_count() or 1):
        raise ValueError("requested worker count exceeds supported local CPU capacity")
    provenance = source_provenance()
    rows = []
    for size in sorted(set(sizes)):
        episodes = list(generate_episodes(size, seed))
        payload = "".join(e.model_dump_json() + "\n" for e in episodes).encode()
        dataset_sha = hashlib.sha256(payload).hexdigest()
        serial_started = time.perf_counter()
        canonical = evaluate(episodes)
        serial_duration = time.perf_counter() - serial_started
        baseline = None
        for worker_count in worker_counts:
            timestamp = datetime.now(timezone.utc).isoformat()
            with RayEvaluator(worker_count, temp_dir=temp_dir) as engine:
                # Warm the same code path, using distinct warmup observations.
                warmup = list(generate_episodes(max(32, worker_count), seed + 1))
                if engine.evaluate(warmup).evaluation != evaluate(warmup):
                    raise RuntimeError("warmup differs from serial evaluation")
                samples = []
                for _ in range(repetitions):
                    result = engine.evaluate(episodes)
                    if result.evaluation != canonical:
                        raise RuntimeError("distributed evaluation differs from canonical B1")
                    samples.append(Trial(duration_s=result.wall_duration_s,
                        partition_evaluation_durations_s=[p.evaluation_duration_s for p in result.partitions],
                        worker_pids=[p.worker_pid for p in result.partitions], integrity=result.integrity,
                        evaluation=result.evaluation, successful_tasks=len(result.partitions),
                        attempt_count=len(result.partitions)))
                if worker_count == 1:
                    baseline = median(sample.duration_s for sample in samples)
                row = BenchmarkRow(**provenance, timestamp=timestamp, episode_count=size,
                    dataset_sha256=dataset_sha, seed=seed, worker_count=worker_count,
                    startup_duration_s=engine.startup_duration_s, serial_duration_s=serial_duration,
                    baseline_duration_s=baseline, samples=samples)
                rows.append(row)
                print(f"{size} episodes / {worker_count} workers: {row.duration_s:.6f}s, "
                      f"{row.episodes_per_second:.1f} episodes/s, {row.speedup:.3f}x", file=sys.stderr, flush=True)
    return rows


def write_benchmarks(rows: list[BenchmarkRow], output_dir: str | Path) -> None:
    if not rows:
        raise ValueError("no benchmark results")
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "methodology": {
        "duration": "median measured warm wall-clock seconds; includes partitioning, serialization, worker dispatch, validation and canonical reduction; excludes generation, Ray startup and warmup",
        "startup": "Ray startup plus ready handshake with one distinct actor process per worker",
        "latency": "worker canonical evaluator time per partition, milliseconds; not per-episode inference latency",
        "speedup": "same-dataset median Ray 1-worker duration / median current duration",
        "efficiency": "speedup / configured worker count",
        "reduction": "B1 reevaluation of validated worker observations in input order plus partition verification; serial overhead is included",
        "equivalence": "exact equality, no tolerance", "retry_policy": "none; one attempt, fail closed",
        "provenance": "commit identifies checkout at run; source_sha256 identifies exact Python source and pyproject bytes when dirty",
        "samples": "all repetitions retained; successful_tasks/failed_tasks refer to worker tasks, not robot task success",
        "scope": "single-machine local Ray actors; no multi-node claims"},
        "results": [row.model_dump(mode="json") for row in rows]}
    (target / "b2_scaling.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8", newline="\n")
    flat = []
    for row in rows:
        item = row.model_dump(mode="json", exclude={"samples"})
        item.update(row.samples[0].integrity.model_dump())
        item.update(successful_tasks=sum(s.successful_tasks for s in row.samples),
                    failed_tasks=sum(s.failed_tasks for s in row.samples),
                    attempt_count=sum(s.attempt_count for s in row.samples),
                    final_status="success", failure_reason="", repetitions=len(row.samples),
                    decision=row.samples[0].evaluation.decision.value)
        flat.append(item)
    with (target / "b2_scaling.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(flat[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(flat)
    lines = ["# B2 measured local Ray scaling", "", "Generated from real benchmark samples; medians of measured repetitions.", "",
             "| Episodes | Workers | Seconds | Episodes/s | Speedup | Efficiency |",
             "|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append(f"| {row.episode_count:,} | {row.worker_count} | {row.duration_s:.6f} | {row.episodes_per_second:.1f} | {row.speedup:.3f}x | {row.scaling_efficiency:.3f} |")
    lines += ["", "Speedup uses the matching Ray one-worker baseline, not serial B1. Serial B1 timings, startup, worker partition timings, exact results, integrity and every raw sample are in JSON.",
              "", "These lightweight evaluations include serial integrity verification and canonical reduction; negative scaling is valid evidence. No sleeps or artificial workload are used."]
    (target / "b2_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--episodes", nargs="+", type=int, default=[1000, 10000])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output-dir", default="benchmarks")
    parser.add_argument("--ray-temp-dir", default=None)
    args = parser.parse_args()
    try:
        rows = run_benchmarks(args.episodes, args.workers, args.seed, args.repetitions, args.ray_temp_dir)
        write_benchmarks(rows, args.output_dir)
    except Exception as exc:
        target = Path(args.output_dir)
        target.mkdir(parents=True, exist_ok=True)
        failure = {"timestamp": datetime.now(timezone.utc).isoformat(), "final_status": "failed",
                   "failure_reason": f"{type(exc).__name__}: {exc}", "requested_episodes": args.episodes,
                   "requested_workers": args.workers, "seed": args.seed}
        (target / "b2_failure.json").write_text(json.dumps(failure, indent=2) + "\n", encoding="utf-8", newline="\n")
        raise


if __name__ == "__main__":
    main()
