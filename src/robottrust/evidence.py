"""UTF-8 JSONL replay and SHA-256 evidence manifests."""
import hashlib
import json
from collections.abc import Iterable, Sequence
from pathlib import Path
from robottrust.models import Episode
from robottrust.policy import Thresholds
from robottrust.evaluator import Evaluation, evaluate

ARTIFACTS = ("episodes.jsonl", "evaluation.json", "decision.json")


def write_jsonl(path: str | Path, episodes: Iterable[Episode]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="\n") as stream:
        for episode in episodes:
            stream.write(episode.model_dump_json() + "\n")


def read_jsonl(path: str | Path) -> list[Episode]:
    episodes = []
    with Path(path).open(encoding="utf-8-sig") as stream:
        for number, line in enumerate(stream, 1):
            try:
                episodes.append(Episode.model_validate_json(line))
            except ValueError as exc:
                raise ValueError(f"Invalid episode at line {number}: {exc}") from exc
    return episodes


def sha256_file(path: str | Path) -> str:
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_evidence(directory: str | Path, episodes: Sequence[Episode], thresholds: Thresholds | None = None) -> Evaluation:
    result = evaluate(episodes, thresholds)
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)
    write_jsonl(target / "episodes.jsonl", episodes)
    (target / "evaluation.json").write_text(result.model_dump_json(indent=2) + "\n", encoding="utf-8", newline="\n")
    decision = result.model_dump(mode="json", exclude={"metrics"})
    (target / "decision.json").write_text(json.dumps(decision, indent=2) + "\n", encoding="utf-8", newline="\n")
    checksums = {name: sha256_file(target / name) for name in ARTIFACTS}
    (target / "checksums.json").write_text(json.dumps(checksums, indent=2) + "\n", encoding="utf-8", newline="\n")
    return result


def verify_checksums(directory: str | Path) -> bool:
    target = Path(directory)
    checksums = json.loads((target / "checksums.json").read_text(encoding="utf-8"))
    return set(checksums) == set(ARTIFACTS) and all(sha256_file(target / name) == checksums[name] for name in ARTIFACTS)
