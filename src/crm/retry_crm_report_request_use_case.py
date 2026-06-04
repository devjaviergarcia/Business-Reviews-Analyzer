from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database


class RetryCRMReportRequestUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        parse_object_id: Callable[..., Any],
        now_utc: Callable[[], datetime],
        record_event: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        enqueue_benchmark_study_job: Callable[..., Awaitable[dict[str, Any]]],
        report_requests_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._enqueue_benchmark_study_job = enqueue_benchmark_study_job
        self._report_requests_collection_name = report_requests_collection_name

    async def execute(self, *, report_request_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        parsed_id = self._parse_object_id(report_request_id, field_name="report_request_id")
        collection = get_database()[self._report_requests_collection_name]
        doc = await collection.find_one({"_id": parsed_id})
        if doc is None:
            raise LookupError(f"Report request '{report_request_id}' not found.")
        update_fields = await self._build_queue_update_fields(doc)
        await collection.update_one({"_id": parsed_id}, {"$set": update_fields})
        doc.update(update_fields)
        await self._record_event(
            event_type="report_request_retried",
            data={
                "report_request_id": report_request_id,
                "job_id": doc.get("job_id"),
                "benchmark_run_id": doc.get("benchmark_run_id"),
            },
        )
        serialized = self._serialize_mongo_doc(doc, id_key="report_request_id")
        return self._sanitize_payload(serialized)

    async def _build_queue_update_fields(self, doc: dict[str, Any]) -> dict[str, Any]:
        business_name = str(doc.get("business_name") or "").strip()
        query = str(doc.get("query") or "").strip()
        if not query:
            query = " ".join(item for item in (business_name, str(doc.get("city") or "").strip()) if item)
        if not query:
            raise ValueError("Report request has no query to enqueue.")
        queued = await self._enqueue_benchmark_study_job(
            query=query,
            city=str(doc.get("city") or "").strip() or None,
            category=str(doc.get("category") or "").strip() or None,
            limit=30,
            source="auto_live_google_maps",
            title=f"Solicitud informe: {business_name or query}",
        )
        return {
            "status": "queued",
            "job_id": str(queued.get("job_id") or "").strip() or None,
            "benchmark_run_id": str(queued.get("benchmark_run_id") or "").strip() or None,
            "failure_reason": None,
            "updated_at": self._now_utc(),
        }
