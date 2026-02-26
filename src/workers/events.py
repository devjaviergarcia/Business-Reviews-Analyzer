from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.workers.contracts import JobProgressEvent, JobProgressState


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_job_event(
    *,
    stage: str,
    message: str,
    data: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    model = JobProgressEvent(
        stage=stage,
        message=message,
        data=data or {},
        created_at=created_at or utc_now(),
    )
    return model.model_dump(mode="python")


def build_job_progress(
    *,
    stage: str,
    message: str,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    model = JobProgressState(
        stage=stage,
        message=message,
        updated_at=updated_at or utc_now(),
    )
    return model.model_dump(mode="python")


def build_job_event_and_progress(
    *,
    stage: str,
    message: str,
    data: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[datetime, dict[str, Any], dict[str, Any]]:
    now_value = now or utc_now()
    event = build_job_event(stage=stage, message=message, data=data, created_at=now_value)
    progress = build_job_progress(stage=stage, message=message, updated_at=now_value)
    return now_value, event, progress
