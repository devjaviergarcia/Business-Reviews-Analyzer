from __future__ import annotations

from typing import Any

from src.services.analysis_job_service import AnalysisJobService
from src.workers.broker import WorkerJobBroker


class MongoJobBroker(WorkerJobBroker):
    """Worker-side broker backed by Mongo job documents via AnalysisJobService."""

    def __init__(self, job_service: AnalysisJobService | None = None) -> None:
        self._job_service = job_service or AnalysisJobService()

    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        return await self._job_service.pick_next_queued_job(queue_name=queue_name)

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        await self._job_service.append_event(
            job_id=job_id,
            stage=stage,
            message=message,
            data=data,
        )

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        await self._job_service.mark_done(job_id=job_id, result=result)

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        await self._job_service.mark_failed(job_id=job_id, error=error)
