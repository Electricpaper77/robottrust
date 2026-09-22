"""Evaluate validated episodes and create replayable evidence from the CLI."""
import argparse
from collections.abc import Sequence
from pathlib import Path
from pydantic import BaseModel
from robottrust.models import Episode
from robottrust.metrics import Metrics, calculate_metrics
from robottrust.policy import Decision, Thresholds, apply_policy


class Evaluation(BaseModel):
    metrics: Metrics
    decision: Decision
    reasons: list[str]
    thresholds: Thresholds


def evaluate(episodes: Sequence[Episode], thresholds: Thresholds | None = None) -> Evaluation:
    metrics = calculate_metrics(episodes)
    policy = apply_policy(metrics, thresholds)
    return Evaluation(metrics=metrics, **policy.model_dump())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", default="evidence")
    parser.add_argument("--thresholds", help="JSON file with threshold overrides")
    args = parser.parse_args()
    from robottrust.evidence import read_jsonl, write_evidence
    limits = Thresholds.model_validate_json(Path(args.thresholds).read_text(encoding="utf-8")) if args.thresholds else None
    result = write_evidence(args.output_dir, read_jsonl(args.input), limits)
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
