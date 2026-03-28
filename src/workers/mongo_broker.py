from __future__ import annotations

from typing import Any

from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import AnalysisJobStatus, JobQueueName, JobType, WorkerTaskPayload
from src.workers.broker import WorkerJobBroker


class MongoJobBroker(WorkerJobBroker):
    """Worker-side broker backed by Mongo job documents via AnalysisJobService."""

    def __init__(self, job_service: AnalysisJobService | None = None) -> None:
        self._job_service = job_service or AnalysisJobService()

    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        return await self._job_service.pick_next_queued_job(queue_name=queue_name)

    async def is_cancel_requested(self, *, job_id: Any) -> bool:
        return await self._job_service.is_job_cancel_requested(job_id=job_id)

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
        status: AnalysisJobStatus | str | None = None,
    ) -> None:
        await self._job_service.append_event(
            job_id=job_id,
            stage=stage,
            message=message,
            data=data,
            status=status,
        )

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        await self._job_service.mark_done(job_id=job_id, result=result)

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        await self._job_service.mark_failed(job_id=job_id, error=error)

    async def mark_needs_human(
        self,
        *,
        job_id: Any,
        reason: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        await self._job_service.mark_needs_human(
            job_id=job_id,
            reason=reason,
            data=data,
        )

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
        await self._job_service.handoff_job(
            job_id=job_id,
            queue_name=queue_name,
            job_type=job_type,
            task_payload=task_payload,
            stage=stage,
            message=message,
            data=data,
        )
