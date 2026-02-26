from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


AnalysisJobStatus = Literal["queued", "running", "done", "failed", "retrying", "partial"]
JobQueueName = Literal["scrape", "analysis", "report"]


class JobProgressEvent(BaseModel):
    stage: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class JobProgressState(BaseModel):
    stage: str
    message: str
    updated_at: datetime

    model_config = ConfigDict(extra="forbid")


class AnalyzeBusinessTaskPayload(BaseModel):
    name: str
    force: bool = False
    strategy: str | None = None

    model_config = ConfigDict(extra="forbid")


class AnalysisJobQueueDocument(BaseModel):
    name: str
    name_normalized: str
    force: bool
    strategy: str
    queue_name: str = "scrape"
    job_type: str = "business_analyze"
    status: AnalysisJobStatus
    progress: JobProgressState
    events: list[JobProgressEvent] = Field(default_factory=list)
    attempts: int = 0
    error: str | None = None
    result: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")
