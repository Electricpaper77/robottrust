"""Synchronous in-process evaluation API; latest result is per service process."""
from threading import Lock
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from robottrust.evaluator import Evaluation, evaluate
from robottrust.metrics import Metrics
from robottrust.models import Episode
from robottrust.policy import PolicyResult, Thresholds


class EvaluateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    episodes: list[Episode] = Field(min_length=1, max_length=100000)
    thresholds: Thresholds = Field(default_factory=Thresholds)


def create_app() -> FastAPI:
    app = FastAPI(title="RobotTrust", version="0.1.0")
    lock = Lock()
    latest: Evaluation | None = None

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/evaluate", response_model=Evaluation)
    def evaluate_records(request: EvaluateRequest) -> Evaluation:
        nonlocal latest
        try:
            result = evaluate(request.episodes, request.thresholds)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        with lock:
            latest = result
        return result

    def get_latest() -> Evaluation:
        with lock:
            result = latest
        if result is None:
            raise HTTPException(status_code=404, detail="No evaluation available")
        return result

    @app.get("/metrics", response_model=Metrics)
    def metrics() -> Metrics:
        return get_latest().metrics

    @app.get("/decision", response_model=PolicyResult)
    def decision() -> PolicyResult:
        return PolicyResult(**get_latest().model_dump(exclude={"metrics"}))

    return app


app = create_app()
