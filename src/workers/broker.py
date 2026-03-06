from __future__ import annotations

from typing import Any, Protocol

from src.workers.contracts import AnalysisJobStatus, JobQueueName, JobType, WorkerTaskPayload


class WorkerJobBroker(Protocol):
    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        """Atomically claim the next queued job for a logical queue."""

    async def is_cancel_requested(self, *, job_id: Any) -> bool:
        """Return whether the running job should be interrupted/cancelled."""

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
        status: AnalysisJobStatus | str | None = None,
    ) -> None:
        """Append a progress event and update current progress state."""

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        """Mark a job as completed with a result payload."""

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        """Mark a job as failed with an error message."""

    async def handoff_job(
        self,
        *,
        job_id: Any,
        queue_name: JobQueueName,
        job_type: JobType,
        task_payload: WorkerTaskPayload,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Requeue the same job for the next worker stage with a typed payload."""
