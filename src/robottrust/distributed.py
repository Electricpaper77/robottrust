"""Local Ray workers with fail-closed integrity and canonical B1 reduction."""
from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from contextlib import AbstractContextManager
import os
from pathlib import Path
import time
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError
import ray

from robottrust.evaluator import Evaluation, evaluate
from robottrust.models import Episode
from robottrust.policy import Thresholds


class WorkerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    workers: int = Field(default=1, ge=1, le=16, strict=True)
    timeout_s: float = Field(default=180, gt=0)


class Integrity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    expected_episodes: int
    processed_episodes: int
    unique_episodes: int
    duplicates: int
    missing: int
    unknown: int


class IntegrityError(ValueError):
    def __init__(self, message: str, counters: Integrity | None = None):
        super().__init__(message)
        self.counters = counters


class WorkerFailure(RuntimeError):
    """One attempt, no automatic retry; a partial result is never published."""
    attempt_count = 1
    final_status = "failed"

    def __init__(self, reason: str):
        super().__init__(reason)
        self.failure_reason = reason


class PartitionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    partition_id: int = Field(ge=0, strict=True)
    episodes: list[Episode] = Field(min_length=1)
    evaluation: Evaluation
    evaluation_duration_s: float = Field(ge=0)
    worker_pid: int = Field(gt=0, strict=True)
    attempt_count: Literal[1] = 1
    final_status: Literal["success"] = "success"


class DistributedResult(BaseModel):
    evaluation: Evaluation
    integrity: Integrity
    partitions: list[PartitionResult]
    wall_duration_s: float
    configured_workers: int
    attempt_count: Literal[1] = 1
    final_status: Literal["success"] = "success"
    failure_reason: None = None


def partition_episodes(episodes: Sequence[Episode], workers: int) -> list[list[Episode]]:
    WorkerConfig(workers=workers)
    if not episodes:
        raise ValueError("at least one episode is required")
    counts = Counter(e.episode_id for e in episodes)
    if len(counts) != len(episodes):
        raise IntegrityError("duplicate input episode IDs")
    # Balanced contiguous slices preserve original order, with no empty task.
    size, remainder = divmod(len(episodes), min(workers, len(episodes)))
    partitions = []
    start = 0
    for index in range(min(workers, len(episodes))):
        stop = start + size + int(index < remainder)
        partitions.append(list(episodes[start:stop]))
        start = stop
    return partitions


def evaluate_partition(partition_id: int, records: list[dict[str, Any]], thresholds: dict[str, Any]) -> dict[str, Any]:
    """Worker boundary validates transport payload and calls B1 exactly once."""
    episodes = [Episode.model_validate(record) for record in records]
    limits = Thresholds.model_validate(thresholds)
    started = time.perf_counter()
    result = evaluate(episodes, limits)
    elapsed = time.perf_counter() - started
    return PartitionResult(partition_id=partition_id, episodes=episodes, evaluation=result,
                           evaluation_duration_s=elapsed, worker_pid=os.getpid()).model_dump(mode="json")


@ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
class EvaluationWorker:
    def ready(self) -> int:
        return os.getpid()

    def evaluate(self, partition_id: int, records: list[dict[str, Any]], thresholds: dict[str, Any]) -> dict[str, Any]:
        return evaluate_partition(partition_id, records, thresholds)


def aggregate_results(episodes: Sequence[Episode], workers: int, outputs: Sequence[object],
                      thresholds: Thresholds | None = None) -> tuple[Evaluation, Integrity, list[PartitionResult]]:
    """Validate all results, restore input order, then invoke the canonical reducer.

    Do not average p95s or rounded partition means. The B1 evaluator reduces the
    returned observations in input order, preserving exact floating-point behavior.
    Per-partition B1 evaluations are also verified against returned observations.
    This deliberately retains serial reduction/verification cost in B2.
    """
    partitions = partition_episodes(episodes, workers)
    limits = thresholds or Thresholds()
    try:
        results = [PartitionResult.model_validate(output) for output in outputs]
    except (ValidationError, TypeError) as exc:
        raise IntegrityError(f"malformed worker result: {exc}") from exc
    expected = {e.episode_id: e for e in episodes}
    returned = [e for result in results for e in result.episodes]
    counts = Counter(e.episode_id for e in returned)
    counters = Integrity(expected_episodes=len(episodes), processed_episodes=len(returned),
                         unique_episodes=len(counts), duplicates=sum(n-1 for n in counts.values()),
                         missing=len(expected.keys()-counts.keys()), unknown=len(counts.keys()-expected.keys()))
    if counters.duplicates or counters.missing or counters.unknown:
        raise IntegrityError("duplicate, missing or unknown output episodes", counters)
    partition_ids = [r.partition_id for r in results]
    if sorted(partition_ids) != list(range(len(partitions))):
        raise IntegrityError("partition loss, duplicate or unknown partition", counters)
    results.sort(key=lambda r: r.partition_id)
    for result, partition in zip(results, partitions, strict=True):
        if result.episodes != partition:
            raise IntegrityError("worker changed episode data, order or partition assignment", counters)
        if result.evaluation != evaluate(result.episodes, limits):
            raise IntegrityError("invalid worker evaluation", counters)
    ordered = [e for result in results for e in result.episodes]
    return evaluate(ordered, limits), counters, results


class RayEvaluator(AbstractContextManager["RayEvaluator"]):
    """Own one local Ray runtime; never attach to or shut down another runtime."""
    def __init__(self, workers: int = 1, timeout_s: float = 180, temp_dir: str | None = None):
        self.config = WorkerConfig(workers=workers, timeout_s=timeout_s)
        if workers > (os.cpu_count() or 1):
            raise ValueError("workers exceeds available logical CPUs; oversubscription is not benchmarked")
        self.temp_dir = temp_dir
        self.actors: list[Any] = []
        self.worker_pids: list[int] = []
        self.startup_duration_s = 0.0
        self._owns_runtime = False

    def __enter__(self) -> RayEvaluator:
        if ray.is_initialized():
            raise RuntimeError("Ray already initialized; use the existing RayEvaluator or shut it down first")
        started = time.perf_counter()
        options: dict[str, Any] = {}
        if self.temp_dir:
            options["_temp_dir"] = str(Path(self.temp_dir).resolve())
        self._owns_runtime = True
        try:
            ray.init(address="local", num_cpus=self.config.workers, include_dashboard=False,
                     log_to_driver=False, **options)
            self.actors = [EvaluationWorker.remote() for _ in range(self.config.workers)]
            self.worker_pids = ray.get([actor.ready.remote() for actor in self.actors], timeout=self.config.timeout_s)
            if len(set(self.worker_pids)) != self.config.workers:
                raise RuntimeError("worker processes are not distinct")
        except Exception:
            self.close()
            raise
        self.startup_duration_s = time.perf_counter() - started
        return self

    def evaluate(self, episodes: Sequence[Episode], thresholds: Thresholds | None = None) -> DistributedResult:
        if not self._owns_runtime or not self.actors:
            raise RuntimeError("RayEvaluator must be entered before evaluation")
        started = time.perf_counter()
        partitions = partition_episodes(episodes, self.config.workers)
        limits = thresholds or Thresholds()
        refs = [self.actors[index].evaluate.remote(index, [e.model_dump(mode="json") for e in partition],
                                                 limits.model_dump()) for index, partition in enumerate(partitions)]
        try:
            outputs = ray.get(refs, timeout=self.config.timeout_s)
        except Exception as exc:
            # Shutdown cancels outstanding work; no partial success is returned.
            self.close()
            raise WorkerFailure(f"Ray evaluation failed without retry: {type(exc).__name__}: {exc}") from exc
        evaluation, integrity, results = aggregate_results(episodes, self.config.workers, outputs, limits)
        return DistributedResult(evaluation=evaluation, integrity=integrity, partitions=results,
                                 wall_duration_s=time.perf_counter()-started, configured_workers=self.config.workers)

    def close(self) -> None:
        if self._owns_runtime:
            ray.shutdown()
            self._owns_runtime = False
            self.actors = []

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()
