from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.leads.lead_job_enqueue_runtime import LeadJobEnqueueRuntime


class EnqueueCRMLeadPipelineJobUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        lead_job_enqueue_runtime: LeadJobEnqueueRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._lead_job_enqueue_runtime = lead_job_enqueue_runtime

    async def execute(
        self,
        *,
        lead_id: str,
        force: bool = False,
        sources: list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._lead_job_enqueue_runtime.enqueue_lead_pipeline_job(
            lead_id=lead_id,
            force=force,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )
