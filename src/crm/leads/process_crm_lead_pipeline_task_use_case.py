from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMLeadStatus
from src.workers.contracts import CRMLeadPipelineTaskPayload


class ProcessCRMLeadPipelineTaskUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        parse_object_id: Callable[..., Any],
        now_utc: Callable[[], datetime],
        sanitize_payload: Callable[[Any], Any],
        record_event: Callable[..., Awaitable[None]],
        enqueue_browser_scrape_jobs: Callable[..., Awaitable[dict[str, Any]]],
        leads_collection_name: str,
        allowed_sources: tuple[str, ...],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._sanitize_payload = sanitize_payload
        self._record_event = record_event
        self._enqueue_browser_scrape_jobs = enqueue_browser_scrape_jobs
        self._leads_collection_name = leads_collection_name
        self._allowed_sources = allowed_sources

    async def execute(
        self,
        *,
        task_payload: CRMLeadPipelineTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        parsed_lead_id = self._parse_object_id(task_payload.lead_id, field_name="lead_id")
        leads = get_database()[self._leads_collection_name]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{task_payload.lead_id}' not found.")

        business_name = str(lead_doc.get("business_name") or "").strip()
        if not business_name:
            raise ValueError("Lead has no business_name to run pipeline.")

        requested_sources = tuple(task_payload.sources)
        used_sources = requested_sources
        fallback_reason: str | None = None

        try:
            queue_result = await self._enqueue_browser_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=requested_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
                execution_mode="automatic",
                requested_by="crm_lead_pipeline_worker",
            )
        except RuntimeError as exc:
            error_text = str(exc)
            can_fallback_to_google = (
                "tripadvisor" in requested_sources
                and "google_maps" in requested_sources
                and (
                    "Tripadvisor local worker bridge is unreachable" in error_text
                    or "TRIPADVISOR_LOCAL_WORKER_BRIDGE_ENABLED" in error_text
                )
            )
            if not can_fallback_to_google:
                raise
            used_sources = ("google_maps",)
            fallback_reason = error_text
            queue_result = await self._enqueue_browser_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=used_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
                execution_mode="automatic",
                requested_by="crm_lead_pipeline_worker_fallback",
            )

        jobs_by_source = (
            queue_result.get("jobs_by_source")
            if isinstance(queue_result.get("jobs_by_source"), dict)
            else {}
        )
        source_job_ids: list[str] = []
        for source_name in self._allowed_sources:
            source_job = jobs_by_source.get(source_name) if isinstance(jobs_by_source, dict) else None
            if isinstance(source_job, dict):
                source_job_id = str(source_job.get("job_id") or "").strip()
                if source_job_id:
                    source_job_ids.append(source_job_id)

        root_business_id = str(queue_result.get("business_id") or "").strip() or None
        now = self._now_utc()

        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_RUNNING.value,
                    "pipeline.business_id": root_business_id,
                    "pipeline.source_job_ids": source_job_ids,
                    "updated_at": now,
                }
            },
        )

        await self._record_event(
            event_type="lead_pipeline_started",
            lead_id=task_payload.lead_id,
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            },
        )

        return self._sanitize_payload(
            {
                "lead_id": task_payload.lead_id,
                "business_name": business_name,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            }
        )
