"""B2 contract tests: pure integrity checks plus real local Ray execution."""
import csv
from contextlib import nullcontext
import json
import os
import time
from tempfile import TemporaryDirectory

import pytest
from pydantic import ValidationError
import ray
from ray.exceptions import RayActorError, RayTaskError

from robottrust.benchmark import BenchmarkRow, Trial, run_benchmarks, write_benchmarks
from robottrust.distributed import (IntegrityError, RayEvaluator, WorkerConfig, WorkerFailure,
                                   aggregate_results, evaluate_partition, partition_episodes)
from robottrust.evaluator import evaluate
from robottrust.generator import generate_episodes
from robottrust.models import Episode
from robottrust.policy import Thresholds


@pytest.fixture
def episodes():
    return list(generate_episodes(20, 42))


def outputs_for(episodes, workers=2):
    return [evaluate_partition(i, [e.model_dump(mode="json") for e in partition], {})
            for i, partition in enumerate(partition_episodes(episodes, workers))]


def test_partitioning_deterministic(episodes):
    assert partition_episodes(episodes, 4) == partition_episodes(episodes, 4)


@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_every_episode_assigned_once(episodes, workers):
    partitions = partition_episodes(episodes, workers)
    assert [e for p in partitions for e in p] == episodes
    assert max(map(len, partitions)) - min(map(len, partitions)) <= 1
    assert len(partitions) == workers


def test_more_workers_than_episodes(episodes):
    assert len(partition_episodes(episodes[:2], 8)) == 2


@pytest.mark.parametrize("workers", [0, -1, 17, 1.5, True])
def test_invalid_worker_count(workers):
    with pytest.raises(ValidationError):
        WorkerConfig(workers=workers)


def test_empty_input():
    with pytest.raises(ValueError):
        partition_episodes([], 1)


def test_duplicate_input(episodes):
    with pytest.raises(IntegrityError, match="duplicate input"):
        partition_episodes([*episodes, episodes[0]], 2)


def test_aggregation_order_independent(episodes):
    result, integrity, _ = aggregate_results(episodes, 2, list(reversed(outputs_for(episodes))))
    assert result == evaluate(episodes)
    assert integrity.expected_episodes == integrity.processed_episodes == integrity.unique_episodes == 20
    assert integrity.duplicates == integrity.missing == integrity.unknown == 0


def test_duplicate_output(episodes):
    outputs = outputs_for(episodes)
    outputs[0]["episodes"].append(outputs[0]["episodes"][0])
    with pytest.raises(IntegrityError) as error:
        aggregate_results(episodes, 2, outputs)
    assert error.value.counters.duplicates == 1


def test_missing_output(episodes):
    outputs = outputs_for(episodes)
    outputs[0]["episodes"].pop()
    with pytest.raises(IntegrityError) as error:
        aggregate_results(episodes, 2, outputs)
    assert error.value.counters.missing == 1


def test_unknown_episode(episodes):
    outputs = outputs_for(episodes)
    outputs[0]["episodes"][0]["episode_id"] = "unknown"
    with pytest.raises(IntegrityError) as error:
        aggregate_results(episodes, 2, outputs)
    assert error.value.counters.unknown == error.value.counters.missing == 1


def test_partition_loss(episodes):
    with pytest.raises(IntegrityError):
        aggregate_results(episodes, 2, outputs_for(episodes)[:1])


def test_partition_id_corruption(episodes):
    outputs = outputs_for(episodes)
    outputs[1]["partition_id"] = 0
    with pytest.raises(IntegrityError, match="partition"):
        aggregate_results(episodes, 2, outputs)


@pytest.mark.parametrize("bad", [None, {}, {"partition_id": "invalid"}])
def test_malformed_result(episodes, bad):
    with pytest.raises(IntegrityError, match="malformed"):
        aggregate_results(episodes, 2, [bad])


def test_mutated_episode(episodes):
    outputs = outputs_for(episodes)
    outputs[0]["episodes"][0]["duration_ms"] += 1
    with pytest.raises(IntegrityError, match="changed episode"):
        aggregate_results(episodes, 2, outputs)


def test_invalid_worker_metrics(episodes):
    outputs = outputs_for(episodes)
    outputs[0]["evaluation"]["metrics"]["episode_count"] += 1
    with pytest.raises(IntegrityError, match="invalid worker evaluation"):
        aggregate_results(episodes, 2, outputs)


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    os.environ["RAY_USAGE_STATS_ENABLED"] = "0"
    # Windows Ray keeps a driver log open until process exit; let pytest retain
    # its temp directory. Linux needs short Unix-domain socket paths.
    directory = nullcontext(str(tmp_path_factory.mktemp("ray"))) if os.name == "nt" else TemporaryDirectory(prefix="rt-")
    with directory as temp_dir:
        with RayEvaluator(2, temp_dir=temp_dir) as runtime:
            yield runtime


def test_real_ray_metric_equivalence(engine, episodes):
    assert engine.evaluate(episodes).evaluation.metrics == evaluate(episodes).metrics
    assert len(set(engine.worker_pids)) == 2


@pytest.mark.parametrize("overrides,decision", [({}, "PASS"), ({"safety_violation": True}, "BLOCK"), ({"success": False}, "ESCALATE")])
def test_real_ray_decision_equivalence(engine, overrides, decision):
    records = list(generate_episodes(10, 42))
    records = [Episode.model_validate({**e.model_dump(), "success": True, "collision_count": 0,
              "safety_violation": False, **overrides}) for e in records]
    result = engine.evaluate(records)
    assert result.evaluation == evaluate(records)
    assert result.evaluation.decision.value == decision


def test_real_ray_custom_thresholds(engine, episodes):
    thresholds = Thresholds(min_task_success_rate=1, max_p95_inference_latency_ms=1)
    assert engine.evaluate(episodes, thresholds).evaluation == evaluate(episodes, thresholds)


def test_worker_exception_propagates(engine):
    # Executes malformed work in a real actor and observes Ray's remote exception.
    ref = engine.actors[0].evaluate.remote(0, [{}], {})
    with pytest.raises(RayTaskError):
        ray.get(ref, timeout=30)


def test_existing_runtime_not_taken_over(engine):
    with pytest.raises(RuntimeError, match="already initialized"):
        with RayEvaluator(1):
            pass
    assert ray.is_initialized()


def test_failed_actor_closes_runtime(engine, episodes):
    # Last engine test: simulate loss of one actual worker, with restart disabled.
    ray.kill(engine.actors[0], no_restart=True)
    # kill is asynchronous: wait for an observed actor death, not a fixed sleep.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            ray.get(engine.actors[0].ready.remote(), timeout=5)
        except RayActorError:
            break
    else:
        pytest.fail("killed actor did not report death")
    with pytest.raises(WorkerFailure) as error:
        engine.evaluate(episodes)
    assert error.value.attempt_count == 1
    assert error.value.final_status == "failed"
    assert error.value.failure_reason
    assert not ray.is_initialized()


def test_unentered_runtime_rejected(episodes):
    with pytest.raises(RuntimeError, match="entered"):
        RayEvaluator(1).evaluate(episodes)


def test_oversubscription_rejected(monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 2)
    with pytest.raises(ValueError, match="CPUs"):
        RayEvaluator(4)


def example_row(episodes):
    evaluation, integrity, _ = aggregate_results(episodes, 2, outputs_for(episodes))
    sample = Trial(duration_s=2, partition_evaluation_durations_s=[.1, .2], worker_pids=[1, 2],
                   integrity=integrity, evaluation=evaluation, successful_tasks=2, attempt_count=2)
    return BenchmarkRow(timestamp="2026-01-01T00:00:00Z", commit_sha="a"*40, source_sha256="b"*64,
        source_dirty=True, python_version="3.12", ray_version=ray.__version__, platform="test",
        cpu_count=2, episode_count=20, dataset_sha256="c"*64, seed=42, worker_count=2,
        startup_duration_s=1, serial_duration_s=.01, baseline_duration_s=3, samples=[sample])


def test_benchmark_schema(episodes):
    row = example_row(episodes)
    assert BenchmarkRow.model_validate(row.model_dump(exclude_computed_fields=True)) == row
    with pytest.raises(ValidationError):
        Trial(duration_s=0)


def test_throughput(episodes):
    assert example_row(episodes).episodes_per_second == 10


def test_speedup(episodes):
    assert example_row(episodes).speedup == 1.5


def test_scaling_efficiency(episodes):
    assert example_row(episodes).scaling_efficiency == .75


def test_partition_latency_units(episodes):
    assert example_row(episodes).mean_partition_evaluation_latency_ms == pytest.approx(150)
    assert example_row(episodes).p95_partition_evaluation_latency_ms == 200


def test_benchmark_artifacts(tmp_path, episodes):
    row = example_row(episodes)
    write_benchmarks([row], tmp_path)
    report = json.loads((tmp_path / "b2_scaling.json").read_text())
    assert report["results"][0] == row.model_dump(mode="json")
    with (tmp_path / "b2_scaling.csv").open(newline="") as stream:
        item = next(csv.DictReader(stream))
    assert float(item["episodes_per_second"]) == row.episodes_per_second
    assert int(item["duplicates"]) == int(item["missing"]) == 0


def test_benchmark_bad_arguments():
    with pytest.raises(ValueError):
        run_benchmarks([0], [1])
