"""Strict episode records shared by generation, replay and HTTP evaluation."""
from enum import Enum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Scenario(str, Enum):
    WAREHOUSE_NAVIGATION = "warehouse_navigation"
    OBSTACLE_AVOIDANCE = "obstacle_avoidance"
    OBJECT_DELIVERY = "object_delivery"
    SENSOR_DROPOUT = "sensor_dropout"
    EMERGENCY_STOP = "emergency_stop"


class Episode(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    episode_id: str = Field(min_length=1, pattern=r"\S")
    timestamp: AwareDatetime
    policy_version: str = Field(min_length=1, pattern=r"\S")
    scenario: Scenario
    task: str = Field(min_length=1, pattern=r"\S")
    success: bool = Field(strict=True)
    collision_count: int = Field(ge=0, strict=True)
    duration_ms: float = Field(ge=0, strict=True)
    inference_latency_ms: float = Field(ge=0, strict=True)
    sensor_dropout_rate: float = Field(ge=0, le=1, strict=True)
    safety_violation: bool = Field(strict=True)
    termination_reason: str = Field(min_length=1, pattern=r"\S")
