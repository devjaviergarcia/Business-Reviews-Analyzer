from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.workers.contracts import AnalysisJobStatus, JobProgressEvent, JobProgressState

_STAGE_TO_STATUS: dict[str, AnalysisJobStatus] = {
    "queued": AnalysisJobStatus.QUEUED,
    "worker_started": AnalysisJobStatus.RUNNING,
    "running": AnalysisJobStatus.RUNNING,
    "done": AnalysisJobStatus.DONE,
    "failed": AnalysisJobStatus.FAILED,
    "retrying": AnalysisJobStatus.RETRYING,
    "partial": AnalysisJobStatus.PARTIAL,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_job_status(
    *,
    stage: str,
    explicit_status: AnalysisJobStatus | str | None = None,
) -> AnalysisJobStatus:
    if explicit_status is not None:
        if isinstance(explicit_status, AnalysisJobStatus):
            return explicit_status
        return AnalysisJobStatus(str(explicit_status).strip().lower())
    normalized_stage = str(stage or "").strip().lower()
    return _STAGE_TO_STATUS.get(normalized_stage, AnalysisJobStatus.RUNNING)


def build_job_event(
    *,
    stage: str,
    message: str,
    data: dict[str, Any] | None = None,
    status: AnalysisJobStatus | str | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_status = normalize_job_status(stage=stage, explicit_status=status)
    model = JobProgressEvent(
        status=normalized_status,
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
    status: AnalysisJobStatus | str | None = None,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    normalized_status = normalize_job_status(stage=stage, explicit_status=status)
    model = JobProgressState(
        status=normalized_status,
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
    status: AnalysisJobStatus | str | None = None,
    now: datetime | None = None,
) -> tuple[datetime, dict[str, Any], dict[str, Any]]:
    now_value = now or utc_now()
    event = build_job_event(stage=stage, message=message, data=data, status=status, created_at=now_value)
    progress = build_job_progress(stage=stage, message=message, status=status, updated_at=now_value)
    return now_value, event, progress
