from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from pymongo import ReturnDocument

from src.database import get_database
from src.models.crm import CRMLeadStatus


class SyncCRMLeadPipelineRefsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        parse_object_id: Callable[..., Any],
        now_utc: Callable[[], datetime],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
        jobs_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name
        self._jobs_collection_name = jobs_collection_name

    async def execute(self, *, lead_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        database = get_database()
        leads = database[self._leads_collection_name]
        jobs = database[self._jobs_collection_name]

        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        source_job_ids = pipeline.get("source_job_ids") if isinstance(pipeline.get("source_job_ids"), list) else []
        source_job_ids = [str(item).strip() for item in source_job_ids if str(item).strip()]
        if not source_job_ids:
            return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

        analysis_job_doc = (
            await jobs.find(
                {
                    "queue_name": "analysis",
                    "job_type": "analysis_generate",
                    "payload.source_job_id": {"$in": source_job_ids},
                }
            )
            .sort([("updated_at", -1), ("_id", -1)])
            .limit(1)
            .to_list(length=1)
        )
        latest_analysis_job = analysis_job_doc[0] if analysis_job_doc else None

        report_job_doc: dict[str, Any] | None = None
        if latest_analysis_job is not None:
            analysis_job_id = str(latest_analysis_job.get("_id"))
            report_docs = (
                await jobs.find(
                    {
                        "queue_name": "report",
                        "job_type": "report_generate",
                        "payload.source_job_id": analysis_job_id,
                    }
                )
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(1)
                .to_list(length=1)
            )
            report_job_doc = report_docs[0] if report_docs else None

        update_fields: dict[str, Any] = {}
        if latest_analysis_job is not None:
            update_fields["pipeline.analysis_job_id"] = str(latest_analysis_job.get("_id"))
            update_fields["status"] = CRMLeadStatus.PIPELINE_DONE.value

        if report_job_doc is not None:
            update_fields["pipeline.report_job_id"] = str(report_job_doc.get("_id"))
            report_result = report_job_doc.get("result") if isinstance(report_job_doc.get("result"), dict) else {}
            artifacts = report_result.get("artifacts") if isinstance(report_result.get("artifacts"), dict) else {}
            update_fields["pipeline.latest_report_artifacts"] = artifacts

        if update_fields:
            update_fields["updated_at"] = self._now_utc()
            updated = await leads.find_one_and_update(
                {"_id": parsed_lead_id},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                lead_doc = updated

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))
