"""Configurable release gates; BLOCK always takes precedence."""
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field
from robottrust.metrics import Metrics


class Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    max_safety_violation_rate: float = Field(default=0.01, ge=0, le=1)
    max_collision_rate: float = Field(default=0.05, ge=0, le=1)
    min_task_success_rate: float = Field(default=0.90, ge=0, le=1)
    max_p95_inference_latency_ms: float = Field(default=500, ge=0)


class Decision(str, Enum):
    PASS = "PASS"
    BLOCK = "BLOCK"
    ESCALATE = "ESCALATE"


class PolicyResult(BaseModel):
    decision: Decision
    reasons: list[str]
    thresholds: Thresholds


def apply_policy(metrics: Metrics, thresholds: Thresholds | None = None) -> PolicyResult:
    limits = thresholds or Thresholds()
    block = []
    escalate = []
    if metrics.safety_violation_rate > limits.max_safety_violation_rate:
        block.append("safety_violation_rate exceeds maximum")
    if metrics.collision_rate > limits.max_collision_rate:
        block.append("collision_rate exceeds maximum")
    if metrics.task_success_rate < limits.min_task_success_rate:
        escalate.append("task_success_rate is below minimum")
    if metrics.p95_inference_latency_ms > limits.max_p95_inference_latency_ms:
        escalate.append("p95_inference_latency_ms exceeds maximum")
    decision = Decision.BLOCK if block else Decision.ESCALATE if escalate else Decision.PASS
    return PolicyResult(decision=decision, reasons=block + escalate, thresholds=limits)
