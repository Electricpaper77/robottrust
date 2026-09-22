"""Deterministic synthetic episodes; stdout contains JSONL only."""
import argparse
import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from robottrust.models import Episode, Scenario


def generate_episodes(episodes: int, seed: int) -> Iterator[Episode]:
    if episodes < 1:
        raise ValueError("episodes must be positive")
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    scenarios = list(Scenario)
    for index in range(episodes):
        scenario = scenarios[index % len(scenarios)]
        collisions = int(rng.random() < 0.025)
        violation = rng.random() < 0.005
        dropout = rng.uniform(0.15, 0.5) if scenario == Scenario.SENSOR_DROPOUT else rng.uniform(0, 0.03)
        success = rng.random() >= 0.04 and not collisions and not violation
        reason = "completed" if success else "task_failed"
        if scenario == Scenario.EMERGENCY_STOP:
            reason = "safe_stop" if success else "stop_failed"
        yield Episode(
            episode_id=f"synthetic-{seed}-{index:06d}",
            timestamp=start + timedelta(seconds=index), policy_version="synthetic-v1",
            scenario=scenario, task=f"execute_{scenario.value}", success=success,
            collision_count=collisions, duration_ms=round(rng.uniform(1000, 30000), 3),
            inference_latency_ms=round(rng.uniform(20, 450), 3),
            sensor_dropout_rate=round(dropout, 6), safety_violation=violation,
            termination_reason=reason,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--episodes", type=int, default=100)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--output", help="Optional UTF-8 JSONL file; defaults to stdout")
    args = parser.parse_args()
    if args.episodes < 1:
        parser.error("--episodes must be positive")
    records = generate_episodes(args.episodes, args.seed)
    if args.output:
        from robottrust.evidence import write_jsonl
        write_jsonl(args.output, records)
    else:
        for episode in records:
            print(episode.model_dump_json())


if __name__ == "__main__":
    main()
