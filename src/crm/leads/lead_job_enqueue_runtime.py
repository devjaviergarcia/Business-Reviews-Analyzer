from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
from src.models.crm import CRMLeadStatus
from src.workers.contracts import CRMLeadDiscoveryTaskPayload, CRMLeadPipelineTaskPayload


CreateDiscoveryRunFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
AppendDiscoveryRunStepFn = Callable[..., Awaitable[None]]
EnqueueJobFn = Callable[..., Awaitable[dict[str, Any]]]
ParseObjectIdFn = Callable[..., Any]
NowUtcFn = Callable[[], datetime]
RecordEventFn = Callable[..., Awaitable[None]]
SanitizePayloadFn = Callable[[Any], Any]


class LeadJobEnqueueRuntime:
    def __init__(
        self,
        *,
        enqueue_job: EnqueueJobFn,
        create_discovery_run: CreateDiscoveryRunFn,
        append_discovery_run_step: AppendDiscoveryRunStepFn,
        parse_object_id: ParseObjectIdFn,
        now_utc: NowUtcFn,
        record_event: RecordEventFn,
        sanitize_payload: SanitizePayloadFn,
        use_discovery_v2: Callable[[], bool],
        live_google_discovery_sources: tuple[str, ...],
        live_google_discovery_aliases: tuple[str, ...],
        leads_collection_name: str,
    ) -> None:
        self._enqueue_job = enqueue_job
        self._create_discovery_run = create_discovery_run
        self._append_discovery_run_step = append_discovery_run_step
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._use_discovery_v2 = use_discovery_v2
        self._live_google_discovery_sources = live_google_discovery_sources
        self._live_google_discovery_aliases = live_google_discovery_aliases
        self._leads_collection_name = leads_collection_name

    async def enqueue_lead_discovery_job(
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
        if self._use_discovery_v2():
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
        queued = await self._enqueue_job(
            task_payload=payload,
            queue_name=queue_name,
            job_type="crm_lead_discovery",
            source="google_maps" if queue_name == "scrape_google_maps" else None,
            runtime_target=(DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET if queue_name == "scrape_google_maps" else "server_worker"),
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            requested_by="crm_discovery_api",
            fallback_policy=(DEFAULT_BROWSER_FALLBACK_POLICY if queue_name == "scrape_google_maps" else "none"),
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

    async def enqueue_lead_pipeline_job(
        self,
        *,
        lead_id: str,
        force: bool = False,
        sources: list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict[str, Any]:
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._leads_collection_name]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        payload = CRMLeadPipelineTaskPayload(
            lead_id=lead_id,
            force=force,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )
        queued_job = await self._enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="crm_lead_pipeline",
        )
        now = self._now_utc()
        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_QUEUED.value,
                    "updated_at": now,
                }
            },
        )
        await self._record_event(
            event_type="lead_pipeline_job_queued",
            lead_id=lead_id,
            data={
                "crm_job_id": queued_job.get("job_id"),
                "sources": payload.sources,
                "force": payload.force,
            },
        )
        return self._sanitize_payload(queued_job)
