from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.leads.lead_job_enqueue_runtime import LeadJobEnqueueRuntime


class EnqueueCRMLeadDiscoveryJobUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        lead_job_enqueue_runtime: LeadJobEnqueueRuntime,
        use_discovery_v2: Callable[[], bool],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._lead_job_enqueue_runtime = lead_job_enqueue_runtime
        self._use_discovery_v2 = use_discovery_v2

    async def execute(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
    ) -> dict[str, Any]:
        if self._use_discovery_v2():
            await self._ensure_indexes()
        return await self._lead_job_enqueue_runtime.enqueue_lead_discovery_job(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=source,
        )
