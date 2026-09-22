"""Metrics use episode-level incidence and nearest-rank p95."""
import math
from collections.abc import Sequence
from statistics import fmean

from pydantic import BaseModel, ConfigDict, Field
from robottrust.models import Episode


class Metrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)
    episode_count: int = Field(gt=0)
    task_success_rate: float = Field(ge=0, le=1)
    collision_rate: float = Field(ge=0, le=1)
    safety_violation_rate: float = Field(ge=0, le=1)
    mean_inference_latency_ms: float = Field(ge=0)
    p95_inference_latency_ms: float = Field(ge=0)
    mean_sensor_dropout_rate: float = Field(ge=0, le=1)
    failed_episode_count: int = Field(ge=0)


def calculate_metrics(episodes: Sequence[Episode]) -> Metrics:
    count = len(episodes)
    if not count:
        raise ValueError("at least one episode is required")
    if len({episode.episode_id for episode in episodes}) != count:
        raise ValueError("duplicate episode_id values are not allowed")
    latencies = sorted(episode.inference_latency_ms for episode in episodes)
    successes = sum(episode.success for episode in episodes)
    return Metrics(
        episode_count=count, task_success_rate=successes / count,
        collision_rate=sum(e.collision_count > 0 for e in episodes) / count,
        safety_violation_rate=sum(e.safety_violation for e in episodes) / count,
        mean_inference_latency_ms=fmean(latencies),
        p95_inference_latency_ms=latencies[math.ceil(0.95 * count) - 1],
        mean_sensor_dropout_rate=fmean(e.sensor_dropout_rate for e in episodes),
        failed_episode_count=count - successes,
    )
