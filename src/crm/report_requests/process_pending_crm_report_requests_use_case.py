from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database


class ProcessPendingCRMReportRequestsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        enqueue_benchmark_study_job: Callable[..., Awaitable[dict[str, Any]]],
        report_requests_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._enqueue_benchmark_study_job = enqueue_benchmark_study_job
        self._report_requests_collection_name = report_requests_collection_name

    async def execute(self, *, limit: int = 50) -> dict[str, Any]:
        await self._ensure_indexes()
        limit_value = max(1, min(int(limit or 50), 200))
        collection = get_database()[self._report_requests_collection_name]
        query = {
            "$or": [
                {"status": {"$in": ["pending", "failed_to_queue"]}},
                {"job_id": None},
            ]
        }
        docs = (
            await collection.find(query)
            .sort([("created_at", 1), ("_id", 1)])
            .limit(limit_value)
            .to_list(length=limit_value)
        )
        processed = 0
        retried = 0
        failed = 0
        errors: list[dict[str, Any]] = []
        for doc in docs:
            processed += 1
            report_request_id = str(doc.get("_id"))
            try:
                update_fields = await self._build_queue_update_fields(doc)
                await collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})
                retried += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                errors.append({"report_request_id": report_request_id, "error": str(exc)})
                await collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "status": "failed_to_queue",
                            "failure_reason": str(exc),
                            "updated_at": self._now_utc(),
                        }
                    },
                )

        if processed > 0:
            await self._record_event(
                event_type="report_requests_pending_processed",
                data={"processed": processed, "retried": retried, "failed": failed, "limit": limit_value},
            )
        return self._sanitize_payload(
            {
                "processed": processed,
                "retried": retried,
                "failed": failed,
                "errors": errors,
            }
        )

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
