import hashlib
import json
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from robottrust.api import create_app
from robottrust.evaluator import evaluate
from robottrust.evidence import read_jsonl, sha256_file, verify_checksums, write_evidence, write_jsonl
from robottrust.generator import generate_episodes
from robottrust.metrics import calculate_metrics
from robottrust.models import Episode, Scenario
from robottrust.policy import Decision, Thresholds


def episode(index=0, **overrides):
    values = dict(episode_id=f"test-{index}", timestamp="2026-01-01T00:00:00Z",
                  policy_version="v1", scenario="warehouse_navigation", task="navigate",
                  success=True, collision_count=0, duration_ms=1000,
                  inference_latency_ms=100, sensor_dropout_rate=0,
                  safety_violation=False, termination_reason="completed")
    values.update(overrides)
    return Episode.model_validate(values)


def test_valid_schema():
    assert Episode.model_validate_json(episode().model_dump_json()) == episode()


@pytest.mark.parametrize("field,value", [
    ("episode_id", ""), ("task", " "), ("collision_count", -1),
    ("collision_count", 1.5), ("collision_count", True), ("success", "true"),
    ("sensor_dropout_rate", 1.1), ("sensor_dropout_rate", -0.1),
    ("inference_latency_ms", float("nan")), ("duration_ms", -1),
    ("timestamp", "2026-01-01T00:00:00"), ("scenario", "unknown"),
    ("unexpected", "value"),
])
def test_invalid_schema(field, value):
    with pytest.raises(ValidationError):
        episode(**{field: value})


def test_missing_field():
    record = episode().model_dump()
    del record["termination_reason"]
    with pytest.raises(ValidationError):
        Episode.model_validate(record)


def test_deterministic_generation():
    a = list(generate_episodes(100, 42))
    assert a == list(generate_episodes(100, 42))
    assert a != list(generate_episodes(100, 43))
    assert {e.scenario for e in a} == set(Scenario)
    assert len({e.episode_id for e in a}) == 100
    assert all(Episode.model_validate_json(e.model_dump_json()) == e for e in a)


def test_generator_rejects_empty():
    with pytest.raises(ValueError):
        list(generate_episodes(0, 42))


def test_generator_cli():
    result = subprocess.run([sys.executable, "-m", "robottrust.generator", "--episodes", "5", "--seed", "42"], check=True, capture_output=True, text=True)
    assert [Episode.model_validate_json(line) for line in result.stdout.splitlines()] == list(generate_episodes(5, 42))


def test_success_rate_and_failures():
    metrics = calculate_metrics([episode(0), episode(1, success=False), episode(2), episode(3)])
    assert metrics.task_success_rate == 0.75
    assert metrics.failed_episode_count == 1
    assert metrics.episode_count == 4


def test_collision_rate_counts_episodes():
    assert calculate_metrics([episode(0, collision_count=3), episode(1)]).collision_rate == 0.5


def test_p95_nearest_rank():
    records = [episode(i, inference_latency_ms=i + 1) for i in range(20)]
    metrics = calculate_metrics(list(reversed(records)))
    assert metrics.p95_inference_latency_ms == 19
    assert metrics.mean_inference_latency_ms == 10.5


def test_p95_single_episode():
    assert calculate_metrics([episode(inference_latency_ms=123)]).p95_inference_latency_ms == 123


def test_safety_violation_rate():
    assert calculate_metrics([episode(0, safety_violation=True), episode(1)]).safety_violation_rate == 0.5


def test_mean_dropout():
    assert calculate_metrics([episode(0, sensor_dropout_rate=0.2), episode(1, sensor_dropout_rate=0.4)]).mean_sensor_dropout_rate == pytest.approx(0.3)


def test_pass():
    assert evaluate([episode()]).decision == Decision.PASS


@pytest.mark.parametrize("overrides", [{"safety_violation": True}, {"collision_count": 1}])
def test_block(overrides):
    assert evaluate([episode(**overrides)]).decision == Decision.BLOCK


@pytest.mark.parametrize("overrides", [{"success": False}, {"inference_latency_ms": 501}])
def test_escalate(overrides):
    assert evaluate([episode(**overrides)]).decision == Decision.ESCALATE


def test_block_precedence():
    result = evaluate([episode(success=False, safety_violation=True, inference_latency_ms=900)])
    assert result.decision == Decision.BLOCK
    assert len(result.reasons) == 3


def test_exact_boundaries_pass():
    records = [episode(i, success=i >= 10, collision_count=int(i < 5), safety_violation=i == 0, inference_latency_ms=500) for i in range(100)]
    assert evaluate(records).decision == Decision.PASS


def test_configurable_thresholds():
    result = evaluate([episode(success=False, collision_count=1, safety_violation=True, inference_latency_ms=600)], Thresholds(max_collision_rate=1, max_safety_violation_rate=1, min_task_success_rate=0, max_p95_inference_latency_ms=600))
    assert result.decision == Decision.PASS


def test_invalid_thresholds():
    with pytest.raises(ValidationError):
        Thresholds(max_collision_rate=2)


def test_empty_evaluation():
    with pytest.raises(ValueError, match="at least one"):
        evaluate([])


def test_duplicate_ids():
    with pytest.raises(ValueError, match="duplicate"):
        evaluate([episode(), episode()])


def test_jsonl_replay(tmp_path):
    records = list(generate_episodes(100, 42))
    target = tmp_path / "episodes.jsonl"
    write_jsonl(target, records)
    assert read_jsonl(target) == records
    assert evaluate(read_jsonl(target)) == evaluate(records)


def test_replay_line_error(tmp_path):
    target = tmp_path / "bad.jsonl"
    target.write_text(episode().model_dump_json() + "\n{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="line 2"):
        read_jsonl(target)


def test_checksum_known_value(tmp_path):
    target = tmp_path / "known"
    target.write_bytes(b"abc")
    assert sha256_file(target) == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"


def test_evidence_artifacts(tmp_path):
    records = list(generate_episodes(100, 42))
    result = write_evidence(tmp_path, records)
    assert read_jsonl(tmp_path / "episodes.jsonl") == records
    assert json.loads((tmp_path / "evaluation.json").read_text()) == result.model_dump(mode="json")
    decision = json.loads((tmp_path / "decision.json").read_text())
    assert decision == result.model_dump(mode="json", exclude={"metrics"})
    assert verify_checksums(tmp_path)
    checksums = json.loads((tmp_path / "checksums.json").read_text())
    for name, digest in checksums.items():
        assert b"\r\n" not in (tmp_path / name).read_bytes()
        assert hashlib.sha256((tmp_path / name).read_bytes()).hexdigest() == digest


def test_checksum_tampering(tmp_path):
    write_evidence(tmp_path, [episode()])
    (tmp_path / "decision.json").write_text("{}", encoding="utf-8")
    assert not verify_checksums(tmp_path)


def test_evaluator_cli(tmp_path):
    source = tmp_path / "input.jsonl"
    write_jsonl(source, [episode()])
    target = tmp_path / "evidence"
    result = subprocess.run([sys.executable, "-m", "robottrust.evaluator", "--input", str(source), "--output-dir", str(target)], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout)["decision"] == "PASS"
    assert verify_checksums(target)


def test_health():
    with TestClient(create_app()) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_api_evaluate_and_latest():
    records = [episode(0), episode(1, success=False)]
    with TestClient(create_app()) as client:
        response = client.post("/evaluate", json={"episodes": [e.model_dump(mode="json") for e in records]})
        assert response.status_code == 200
        assert response.json() == evaluate(records).model_dump(mode="json")
        assert client.get("/metrics").json() == response.json()["metrics"]
        assert client.get("/decision").json()["decision"] == "ESCALATE"


def test_api_no_evaluation():
    with TestClient(create_app()) as client:
        assert client.get("/metrics").status_code == 404
        assert client.get("/decision").status_code == 404


@pytest.mark.parametrize("records", [[], [{}], [episode().model_dump(mode="json")] * 2])
def test_api_invalid_records(records):
    with TestClient(create_app()) as client:
        assert client.post("/evaluate", json={"episodes": records}).status_code == 422
        assert client.get("/metrics").status_code == 404


def test_api_threshold_override():
    with TestClient(create_app()) as client:
        response = client.post("/evaluate", json={"episodes": [episode(success=False).model_dump(mode="json")], "thresholds": {"min_task_success_rate": 0}})
        assert response.status_code == 200
        assert response.json()["decision"] == "PASS"
