from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.workers.contracts import AnalysisJobQueueDocument
from src.workers.events import build_job_event_and_progress, build_job_progress


class AnalysisJobService:
    _JOBS_COLLECTION = "analysis_jobs"

    async def enqueue_job(
        self,
        *,
        name: str,
        name_normalized: str,
        force: bool,
        strategy: str,
        queue_name: str = "scrape",
        job_type: str = "business_analyze",
    ) -> dict[str, Any]:
        now, initial_event, initial_progress = build_job_event_and_progress(
            stage="queued",
            message="Job queued.",
            data={"strategy": strategy, "force": bool(force), "queue_name": queue_name, "job_type": job_type},
        )

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        doc = AnalysisJobQueueDocument(
            name=name,
            name_normalized=name_normalized,
            force=bool(force),
            strategy=strategy,
            queue_name=str(queue_name or "scrape"),
            job_type=str(job_type or "business_analyze"),
            status="queued",
            progress=initial_progress,
            events=[initial_event],
            created_at=now,
            updated_at=now,
        ).model_dump(mode="python")

        inserted = await jobs.insert_one(doc)
        payload = {
            "job_id": str(inserted.inserted_id),
            "name": name,
            "status": "queued",
            "force": bool(force),
            "strategy": strategy,
            "queue_name": str(queue_name or "scrape"),
            "job_type": str(job_type or "business_analyze"),
            "created_at": now,
        }
        return self._sanitize_response_payload(payload)

    async def get_job(self, *, job_id: str) -> dict[str, Any]:
        parsed_id = self._parse_object_id(job_id, field_name="job_id")
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        job_doc = await jobs.find_one({"_id": parsed_id})
        if job_doc is None:
            raise LookupError(f"Job '{job_id}' not found.")

        return self._sanitize_response_payload(self._serialize_analysis_job_doc(job_doc))

    async def list_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        total = await jobs.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await jobs.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        payload = build_pagination_payload(
            items=[self._serialize_analysis_job_doc(doc) for doc in docs],
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        if normalized_status:
            payload["status"] = normalized_status
        return self._sanitize_response_payload(payload)

    async def pick_next_queued_job(self, *, queue_name: str = "scrape") -> dict[str, Any] | None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        normalized_queue = str(queue_name or "scrape").strip().lower() or "scrape"
        pick_query: dict[str, Any]
        if normalized_queue == "scrape":
            # Backward-compatible with jobs queued before `queue_name` existed.
            pick_query = {
                "status": "queued",
                "$or": [
                    {"queue_name": "scrape"},
                    {"queue_name": {"$exists": False}},
                ],
            }
        else:
            pick_query = {"status": "queued", "queue_name": normalized_queue}
        now, start_event, start_progress = build_job_event_and_progress(
            stage="worker_started",
            message="Worker started processing job.",
            data={"queue_name": normalized_queue},
        )
        return await jobs.find_one_and_update(
            pick_query,
            {
                "$set": {
                    "status": "running",
                    "started_at": now,
                    "updated_at": now,
                    "progress": start_progress,
                },
                "$push": {"events": start_event},
                "$inc": {"attempts": 1},
            },
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        now, event, progress = build_job_event_and_progress(
            stage=stage,
            message=message,
            data=data,
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "updated_at": now,
                    "progress": progress,
                },
                "$push": {"events": event},
            },
        )

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        finished_at, done_event, done_progress = build_job_event_and_progress(
            stage="done",
            message="Job completed successfully.",
            data={"strategy": result.get("strategy"), "review_count": result.get("review_count")},
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "done",
                    "result": result,
                    "error": None,
                    "finished_at": finished_at,
                    "updated_at": finished_at,
                    "progress": done_progress,
                },
                "$push": {"events": done_event},
            },
        )

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        finished_at, failed_event, _ = build_job_event_and_progress(
            stage="failed",
            message="Job failed.",
            data={"error": str(error)},
        )
        failed_progress = build_job_progress(
            stage="failed",
            message=str(error),
            updated_at=finished_at,
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "error": str(error),
                    "finished_at": finished_at,
                    "updated_at": finished_at,
                    "progress": failed_progress,
                },
                "$push": {"events": failed_event},
            },
        )

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _serialize_analysis_job_doc(self, job_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job_doc)
        payload["job_id"] = str(payload.pop("_id"))
        return payload

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
