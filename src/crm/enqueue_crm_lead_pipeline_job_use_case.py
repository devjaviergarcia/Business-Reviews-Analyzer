from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMLeadStatus
from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import CRMLeadPipelineTaskPayload


class EnqueueCRMLeadPipelineJobUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        ensure_indexes: Callable[[], Awaitable[None]],
        parse_object_id: Callable[..., Any],
        now_utc: Callable[[], datetime],
        sanitize_payload: Callable[[Any], Any],
        record_event: Callable[..., Awaitable[None]],
        leads_collection_name: str,
    ) -> None:
        self._job_service = job_service
        self._ensure_indexes = ensure_indexes
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._sanitize_payload = sanitize_payload
        self._record_event = record_event
        self._leads_collection_name = leads_collection_name

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
        queued_job = await self._job_service.enqueue_job(
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
