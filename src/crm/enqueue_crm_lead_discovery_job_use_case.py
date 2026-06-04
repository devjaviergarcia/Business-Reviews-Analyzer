from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


class EnqueueCRMLeadDiscoveryJobUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        ensure_indexes: Callable[[], Awaitable[None]],
        create_discovery_run: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        append_discovery_run_step: Callable[..., Awaitable[None]],
        use_discovery_v2: bool,
        live_google_discovery_sources: tuple[str, ...],
        live_google_discovery_aliases: tuple[str, ...],
    ) -> None:
        self._job_service = job_service
        self._ensure_indexes = ensure_indexes
        self._create_discovery_run = create_discovery_run
        self._append_discovery_run_step = append_discovery_run_step
        self._use_discovery_v2 = use_discovery_v2
        self._live_google_discovery_sources = live_google_discovery_sources
        self._live_google_discovery_aliases = live_google_discovery_aliases

    async def execute(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
    ) -> dict[str, Any]:
        normalized_source = str(source or "").strip().lower()
        if normalized_source in self._live_google_discovery_aliases:
            normalized_source = "auto_live_google_maps"
        if not normalized_source:
            normalized_source = "auto_live_google_maps"
        queue_name = "scrape_google_maps" if normalized_source in self._live_google_discovery_sources else "crm"
        discovery_run_id: str | None = None
        if self._use_discovery_v2:
            await self._ensure_indexes()
            run_doc = await self._create_discovery_run(
                {
                    "job_id": None,
                    "query": query,
                    "city": city,
                    "category": category,
                    "limit": limit,
                    "source": normalized_source,
                }
            )
            discovery_run_id = str(run_doc.get("discovery_run_id") or "").strip() or None

        payload = CRMLeadDiscoveryTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=normalized_source,
            discovery_run_id=discovery_run_id,
        )
        queued = await self._job_service.enqueue_job(
            task_payload=payload,
            queue_name=queue_name,
            job_type="crm_lead_discovery",
            source="google_maps" if queue_name == "scrape_google_maps" else None,
            runtime_target=(
                DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET if queue_name == "scrape_google_maps" else "server_worker"
            ),
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            requested_by="crm_discovery_api",
            fallback_policy=(
                DEFAULT_BROWSER_FALLBACK_POLICY if queue_name == "scrape_google_maps" else "none"
            ),
            source_display_name="Google Maps" if queue_name == "scrape_google_maps" else None,
        )
        if discovery_run_id:
            await self._append_discovery_run_step(
                run_id=discovery_run_id,
                step="job_enqueued",
                ok=True,
                duration_ms=0,
                data={
                    "job_id": str(queued.get("job_id") or "").strip() or None,
                    "queue_name": str(queued.get("queue_name") or "").strip() or None,
                },
            )
            queued["discovery_run_id"] = discovery_run_id
        return queued
