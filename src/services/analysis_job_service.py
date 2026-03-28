from __future__ import annotations

import asyncio
import time
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.workers.contracts import (
    AnalysisJobStatus,
    AnalysisJobQueueDocument,
    AnalysisGenerateTaskPayload,
    AnalyzeBusinessTaskPayload,
    JobType,
    JobQueueName,
    ReportGenerateTaskPayload,
    WorkerTaskPayload,
    build_worker_job_envelope,
)
from src.workers.events import build_job_event_and_progress, build_job_progress, normalize_job_status


class AnalysisJobService:
    _JOBS_COLLECTION = "analysis_jobs"
    _ACTIVE_STATUSES = {
        AnalysisJobStatus.RUNNING.value,
        AnalysisJobStatus.RETRYING.value,
        AnalysisJobStatus.PARTIAL.value,
    }

    async def enqueue_analyze_business_job(
        self,
        *,
        task_payload: AnalyzeBusinessTaskPayload,
        name_normalized: str | None = None,
    ) -> dict[str, Any]:
        return await self.enqueue_job(
            task_payload=task_payload,
            name_normalized=name_normalized,
            queue_name="scrape",
            job_type="business_analyze",
        )

    async def enqueue_analysis_generate_job(
        self,
        *,
        task_payload: AnalysisGenerateTaskPayload,
    ) -> dict[str, Any]:
        return await self.enqueue_job(
            task_payload=task_payload,
            queue_name="analysis",
            job_type="analysis_generate",
        )

    async def enqueue_report_generate_job(
        self,
        *,
        task_payload: ReportGenerateTaskPayload,
    ) -> dict[str, Any]:
        return await self.enqueue_job(
            task_payload=task_payload,
            queue_name="report",
            job_type="report_generate",
        )

    async def enqueue_job(
        self,
        *,
        task_payload: WorkerTaskPayload,
        name_normalized: str | None = None,
        queue_name: JobQueueName = "scrape",
        job_type: JobType = "business_analyze",
    ) -> dict[str, Any]:
        envelope = build_worker_job_envelope(
            queue_name=queue_name,
            job_type=job_type,
            task_payload=task_payload,
        )
        payload_data = envelope.payload.model_dump(mode="python")
        now, initial_event, initial_progress = build_job_event_and_progress(
            stage="queued",
            message="Job queued.",
            status=AnalysisJobStatus.QUEUED,
            data={
                "queue_name": envelope.queue_name,
                "job_type": envelope.job_type,
                "payload": payload_data,
            },
        )

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        payload_name = payload_data.get("name") if isinstance(payload_data, dict) else None
        payload_canonical_name = payload_data.get("canonical_name") if isinstance(payload_data, dict) else None
        payload_canonical_name_normalized = (
            payload_data.get("canonical_name_normalized") if isinstance(payload_data, dict) else None
        )
        payload_source_name = payload_data.get("source_name") if isinstance(payload_data, dict) else None
        payload_source_name_normalized = (
            payload_data.get("source_name_normalized") if isinstance(payload_data, dict) else None
        )
        payload_root_business_id = payload_data.get("root_business_id") if isinstance(payload_data, dict) else None
        payload_force = payload_data.get("force") if isinstance(payload_data, dict) else None
        payload_strategy = payload_data.get("strategy") if isinstance(payload_data, dict) else None
        payload_force_mode = payload_data.get("force_mode") if isinstance(payload_data, dict) else None
        payload_interactive_max_rounds = (
            payload_data.get("interactive_max_rounds") if isinstance(payload_data, dict) else None
        )
        payload_html_scroll_max_rounds = (
            payload_data.get("html_scroll_max_rounds") if isinstance(payload_data, dict) else None
        )
        payload_html_stable_rounds = (
            payload_data.get("html_stable_rounds") if isinstance(payload_data, dict) else None
        )
        payload_tripadvisor_max_pages = (
            payload_data.get("tripadvisor_max_pages") if isinstance(payload_data, dict) else None
        )
        payload_tripadvisor_pages_percent = (
            payload_data.get("tripadvisor_pages_percent") if isinstance(payload_data, dict) else None
        )

        doc = AnalysisJobQueueDocument(
            queue_name=envelope.queue_name,
            job_type=envelope.job_type,
            payload=payload_data,
            name=str(payload_name).strip() if isinstance(payload_name, str) else None,
            name_normalized=str(name_normalized or "").strip() or None,
            canonical_name=(
                str(payload_canonical_name).strip() if isinstance(payload_canonical_name, str) else None
            ),
            canonical_name_normalized=(
                str(payload_canonical_name_normalized).strip()
                if isinstance(payload_canonical_name_normalized, str)
                else None
            ),
            source_name=str(payload_source_name).strip() if isinstance(payload_source_name, str) else None,
            source_name_normalized=(
                str(payload_source_name_normalized).strip()
                if isinstance(payload_source_name_normalized, str)
                else None
            ),
            root_business_id=(
                str(payload_root_business_id).strip() if isinstance(payload_root_business_id, str) else None
            ),
            force=bool(payload_force) if isinstance(payload_force, bool) else None,
            strategy=str(payload_strategy).strip() if isinstance(payload_strategy, str) else None,
            force_mode=str(payload_force_mode).strip() if isinstance(payload_force_mode, str) else None,
            interactive_max_rounds=(
                int(payload_interactive_max_rounds)
                if isinstance(payload_interactive_max_rounds, int) and not isinstance(payload_interactive_max_rounds, bool)
                else None
            ),
            html_scroll_max_rounds=(
                int(payload_html_scroll_max_rounds)
                if isinstance(payload_html_scroll_max_rounds, int) and not isinstance(payload_html_scroll_max_rounds, bool)
                else None
            ),
            html_stable_rounds=(
                int(payload_html_stable_rounds)
                if isinstance(payload_html_stable_rounds, int) and not isinstance(payload_html_stable_rounds, bool)
                else None
            ),
            tripadvisor_max_pages=(
                int(payload_tripadvisor_max_pages)
                if isinstance(payload_tripadvisor_max_pages, int) and not isinstance(payload_tripadvisor_max_pages, bool)
                else None
            ),
            tripadvisor_pages_percent=(
                float(payload_tripadvisor_pages_percent)
                if isinstance(payload_tripadvisor_pages_percent, (int, float))
                and not isinstance(payload_tripadvisor_pages_percent, bool)
                else None
            ),
            status=AnalysisJobStatus.QUEUED,
            progress=initial_progress,
            events=[initial_event],
            created_at=now,
            updated_at=now,
        ).model_dump(mode="python")

        inserted = await jobs.insert_one(doc)
        payload = {
            "job_id": str(inserted.inserted_id),
            "status": AnalysisJobStatus.QUEUED.value,
            "queue_name": envelope.queue_name,
            "job_type": envelope.job_type,
            "payload": payload_data,
            "created_at": now,
        }
        if isinstance(payload_name, str):
            payload["name"] = payload_name
        if isinstance(payload_canonical_name, str):
            payload["canonical_name"] = payload_canonical_name
        if isinstance(payload_canonical_name_normalized, str):
            payload["canonical_name_normalized"] = payload_canonical_name_normalized
        if isinstance(payload_source_name, str):
            payload["source_name"] = payload_source_name
        if isinstance(payload_source_name_normalized, str):
            payload["source_name_normalized"] = payload_source_name_normalized
        if isinstance(payload_root_business_id, str):
            payload["root_business_id"] = payload_root_business_id
        if isinstance(payload_force, bool):
            payload["force"] = payload_force
        if isinstance(payload_strategy, str):
            payload["strategy"] = payload_strategy
        if isinstance(payload_force_mode, str):
            payload["force_mode"] = payload_force_mode
        if isinstance(payload_interactive_max_rounds, int) and not isinstance(payload_interactive_max_rounds, bool):
            payload["interactive_max_rounds"] = payload_interactive_max_rounds
        if isinstance(payload_html_scroll_max_rounds, int) and not isinstance(payload_html_scroll_max_rounds, bool):
            payload["html_scroll_max_rounds"] = payload_html_scroll_max_rounds
        if isinstance(payload_html_stable_rounds, int) and not isinstance(payload_html_stable_rounds, bool):
            payload["html_stable_rounds"] = payload_html_stable_rounds
        if isinstance(payload_tripadvisor_max_pages, int) and not isinstance(payload_tripadvisor_max_pages, bool):
            payload["tripadvisor_max_pages"] = payload_tripadvisor_max_pages
        if isinstance(payload_tripadvisor_pages_percent, (int, float)) and not isinstance(payload_tripadvisor_pages_percent, bool):
            payload["tripadvisor_pages_percent"] = float(payload_tripadvisor_pages_percent)
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
        queue_names: list[str] | tuple[str, ...] | None = None,
        job_type_filter: str | None = None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            try:
                query["status"] = AnalysisJobStatus(normalized_status).value
            except ValueError as exc:
                allowed_values = ", ".join(status.value for status in AnalysisJobStatus)
                raise ValueError(f"Invalid status filter '{status_filter}'. Allowed: {allowed_values}.") from exc

        if queue_names is not None:
            if not isinstance(queue_names, (list, tuple)):
                raise ValueError("queue_names must be a list of queue names.")
            allowed_queues = {"scrape", "scrape_google_maps", "scrape_tripadvisor", "analysis", "report"}
            normalized_queues: list[str] = []
            for raw in queue_names:
                normalized_queue = str(raw or "").strip().lower()
                if not normalized_queue:
                    continue
                if normalized_queue not in allowed_queues:
                    allowed_values = ", ".join(sorted(allowed_queues))
                    raise ValueError(
                        f"Invalid queue name '{raw}'. Allowed: {allowed_values}."
                    )
                if normalized_queue not in normalized_queues:
                    normalized_queues.append(normalized_queue)
            if not normalized_queues:
                raise ValueError("queue_names cannot be empty.")
            if len(normalized_queues) == 1:
                query["queue_name"] = normalized_queues[0]
            else:
                query["queue_name"] = {"$in": normalized_queues}

        normalized_job_type = str(job_type_filter or "").strip().lower()
        if normalized_job_type:
            allowed_job_types = {
                "business_analyze",
                "business_reanalyze",
                "analysis_generate",
                "report_generate",
            }
            if normalized_job_type not in allowed_job_types:
                allowed_values = ", ".join(sorted(allowed_job_types))
                raise ValueError(
                    f"Invalid job_type filter '{job_type_filter}'. Allowed: {allowed_values}."
                )
            query["job_type"] = normalized_job_type

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
        if queue_names is not None:
            payload["queue_names"] = list(query.get("queue_name", {}).get("$in", [])) if isinstance(query.get("queue_name"), dict) else [query.get("queue_name")]
        if normalized_job_type:
            payload["job_type"] = normalized_job_type
        return self._sanitize_response_payload(payload)

    async def delete_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict[str, Any]:
        parsed_id = self._parse_object_id(job_id, field_name="job_id")
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        existing_doc = await jobs.find_one({"_id": parsed_id})
        if existing_doc is None:
            raise LookupError(f"Job '{job_id}' not found.")

        status_before = self._normalize_status_value(existing_doc.get("status"))
        was_active = self._is_active_status(status_before)
        cancel_requested = False
        timed_out_waiting_for_stop = False
        forced_delete = False

        if was_active:
            await self.request_job_cancellation(
                job_id=job_id,
                reason="Deletion requested via API.",
            )
            cancel_requested = True
            try:
                await self._wait_until_job_not_active(
                    parsed_id=parsed_id,
                    timeout_seconds=wait_active_stop_seconds,
                    poll_seconds=poll_seconds,
                )
            except TimeoutError:
                timed_out_waiting_for_stop = True
                if not bool(force_delete_on_timeout):
                    raise
                forced_delete = True

        deleted_doc = await jobs.find_one_and_delete({"_id": parsed_id})
        if deleted_doc is None:
            # Deleted by another client while we were waiting.
            return self._sanitize_response_payload(
                {
                    "job_id": job_id,
                    "deleted": True,
                    "status_before": status_before,
                    "status_at_delete": None,
                    "was_active": was_active,
                    "cancel_requested": cancel_requested,
                    "timed_out_waiting_for_stop": timed_out_waiting_for_stop,
                    "forced_delete": forced_delete,
                    "already_deleted": True,
                }
            )

        status_at_delete = self._normalize_status_value(deleted_doc.get("status"))
        return self._sanitize_response_payload(
            {
                "job_id": str(deleted_doc.get("_id")),
                "deleted": True,
                "status_before": status_before,
                "status_at_delete": status_at_delete,
                "was_active": was_active,
                "cancel_requested": cancel_requested,
                "timed_out_waiting_for_stop": timed_out_waiting_for_stop,
                "forced_delete": forced_delete,
            }
        )

    async def request_job_cancellation(
        self,
        *,
        job_id: str,
        reason: str | None = None,
    ) -> dict[str, Any]:
        parsed_id = self._parse_object_id(job_id, field_name="job_id")
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        existing_doc = await jobs.find_one({"_id": parsed_id})
        if existing_doc is None:
            raise LookupError(f"Job '{job_id}' not found.")

        normalized_status = self._normalize_status_value(existing_doc.get("status"))
        if not self._is_active_status(normalized_status):
            serialized = self._serialize_analysis_job_doc(existing_doc)
            serialized["cancel_requested"] = bool(serialized.get("cancel_requested"))
            return self._sanitize_response_payload(serialized)

        now, cancel_event, cancel_progress = build_job_event_and_progress(
            stage="cancel_requested",
            message=str(reason or "Cancellation requested."),
            status=AnalysisJobStatus.RUNNING,
            data={},
        )
        updated_doc = await jobs.find_one_and_update(
            {"_id": parsed_id},
            {
                "$set": {
                    "cancel_requested": True,
                    "cancel_requested_at": now,
                    "cancel_reason": str(reason or "").strip() or None,
                    "updated_at": now,
                    "progress": cancel_progress,
                },
                "$push": {"events": cancel_event},
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise LookupError(f"Job '{job_id}' not found.")

        serialized = self._serialize_analysis_job_doc(updated_doc)
        serialized["cancel_requested"] = True
        return self._sanitize_response_payload(serialized)

    async def is_job_cancel_requested(self, *, job_id: Any) -> bool:
        parsed_id: ObjectId
        if isinstance(job_id, ObjectId):
            parsed_id = job_id
        else:
            parsed_id = self._parse_object_id(str(job_id), field_name="job_id")

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        doc = await jobs.find_one(
            {"_id": parsed_id},
            projection={"status": 1, "cancel_requested": 1},
        )
        if doc is None:
            # Deleted/missing job should be treated as cancelled for in-flight workers.
            return True

        status_value = self._normalize_status_value(doc.get("status"))
        if not self._is_active_status(status_value):
            return True

        return bool(doc.get("cancel_requested"))

    async def pick_next_queued_job(self, *, queue_name: JobQueueName = "scrape") -> dict[str, Any] | None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        normalized_queue = str(queue_name or "scrape").strip().lower() or "scrape"
        pick_query: dict[str, Any]
        if normalized_queue == "scrape":
            # Backward-compatible with jobs queued before `queue_name` existed.
            pick_query = {
                "status": AnalysisJobStatus.QUEUED.value,
                "$or": [
                    {"queue_name": "scrape"},
                    {"queue_name": {"$exists": False}},
                ],
            }
        else:
            pick_query = {"status": AnalysisJobStatus.QUEUED.value, "queue_name": normalized_queue}
        now, start_event, start_progress = build_job_event_and_progress(
            stage="worker_started",
            message="Worker started processing job.",
            status=AnalysisJobStatus.RUNNING,
            data={"queue_name": normalized_queue},
        )
        return await jobs.find_one_and_update(
            pick_query,
            {
                "$set": {
                    "status": AnalysisJobStatus.RUNNING.value,
                    "cancel_requested": False,
                    "cancel_requested_at": None,
                    "cancel_reason": None,
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
        status: AnalysisJobStatus | str | None = None,
    ) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        now, event, progress = build_job_event_and_progress(
            stage=stage,
            message=message,
            data=data,
            status=status,
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

    async def handoff_job(
        self,
        *,
        job_id: Any,
        queue_name: JobQueueName,
        job_type: JobType,
        task_payload: WorkerTaskPayload,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        envelope = build_worker_job_envelope(
            queue_name=queue_name,
            job_type=job_type,
            task_payload=task_payload,
        )
        payload_data = envelope.payload.model_dump(mode="python")
        event_data = {
            "queue_name": envelope.queue_name,
            "job_type": envelope.job_type,
            "payload": payload_data,
        }
        if isinstance(data, dict):
            event_data.update(data)

        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        now, handoff_event, handoff_progress = build_job_event_and_progress(
            stage=stage,
            message=message,
            status=AnalysisJobStatus.QUEUED,
            data=event_data,
        )
        legacy_fields = self._extract_legacy_scrape_fields(payload_data)
        set_fields: dict[str, Any] = {
            "status": AnalysisJobStatus.QUEUED.value,
            "queue_name": envelope.queue_name,
            "job_type": envelope.job_type,
            "payload": payload_data,
            "error": None,
            "result": None,
            "finished_at": None,
            "updated_at": now,
            "progress": handoff_progress,
        }
        set_fields.update(legacy_fields)
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": set_fields,
                "$push": {"events": handoff_event},
            },
        )

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        finished_at, done_event, done_progress = build_job_event_and_progress(
            stage="done",
            message="Job completed successfully.",
            status=AnalysisJobStatus.DONE,
            data={"strategy": result.get("strategy"), "review_count": result.get("review_count")},
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": AnalysisJobStatus.DONE.value,
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
            status=AnalysisJobStatus.FAILED,
            data={"error": str(error)},
        )
        failed_progress = build_job_progress(
            stage="failed",
            message=str(error),
            status=AnalysisJobStatus.FAILED,
            updated_at=finished_at,
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": AnalysisJobStatus.FAILED.value,
                    "error": str(error),
                    "finished_at": finished_at,
                    "updated_at": finished_at,
                    "progress": failed_progress,
                },
                "$push": {"events": failed_event},
            },
        )

    async def mark_needs_human(
        self,
        *,
        job_id: Any,
        reason: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        now, event, _ = build_job_event_and_progress(
            stage="needs_human",
            message="Job requires human intervention.",
            status=AnalysisJobStatus.NEEDS_HUMAN,
            data={
                "reason": str(reason or "").strip(),
                **(data or {}),
            },
        )
        needs_human_progress = build_job_progress(
            stage="needs_human",
            message=str(reason or "Human intervention required."),
            status=AnalysisJobStatus.NEEDS_HUMAN,
            updated_at=now,
        )
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": AnalysisJobStatus.NEEDS_HUMAN.value,
                    "error": str(reason),
                    "finished_at": now,
                    "updated_at": now,
                    "progress": needs_human_progress,
                },
                "$push": {"events": event},
            },
        )

    async def relaunch_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        parsed_id = self._parse_object_id(job_id, field_name="job_id")
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]

        existing = await jobs.find_one({"_id": parsed_id})
        if existing is None:
            raise LookupError(f"Job '{job_id}' not found.")

        status_before = self._normalize_status_value(existing.get("status"))
        payload_before = existing.get("payload")
        payload_for_relaunch = self._build_relaunch_payload(
            payload_before if isinstance(payload_before, dict) else {},
            queue_name=existing.get("queue_name"),
            job_type=existing.get("job_type"),
            restart_from_zero=bool(restart_from_zero),
        )
        legacy_fields = self._extract_legacy_scrape_fields(payload_for_relaunch)
        if self._is_active_status(status_before) and not bool(force):
            raise ValueError("Active jobs cannot be relaunched.")

        if self._is_active_status(status_before) and bool(force):
            now, event, progress = build_job_event_and_progress(
                stage="queued",
                message=str(reason or "Job force relaunched while active; queued as a new job."),
                status=AnalysisJobStatus.QUEUED,
                data={
                    "relaunch": True,
                    "force": True,
                    "restart_from_zero": bool(restart_from_zero),
                    "status_before": status_before,
                    "origin_job_id": str(job_id),
                },
            )
            cloned_doc = dict(existing)
            cloned_doc.pop("_id", None)
            cloned_doc.update(
                {
                    "payload": payload_for_relaunch,
                    "status": AnalysisJobStatus.QUEUED.value,
                    "error": None,
                    "result": None,
                    "finished_at": None,
                    "started_at": None,
                    "updated_at": now,
                    "created_at": now,
                    "attempts": 0,
                    "progress": progress,
                    "events": [event],
                    "cancel_requested": False,
                    "cancel_requested_at": None,
                    "cancel_reason": None,
                }
            )
            cloned_doc.update(legacy_fields)
            insert_result = await jobs.insert_one(cloned_doc)
            updated_doc = await jobs.find_one({"_id": insert_result.inserted_id})
            if updated_doc is None:
                raise LookupError(f"Job '{job_id}' was force relaunched but could not be loaded.")
            serialized = self._serialize_analysis_job_doc(updated_doc)
            serialized["force_relaunch"] = True
            serialized["origin_job_id"] = str(job_id)
            serialized["restart_from_zero"] = bool(restart_from_zero)
            return self._sanitize_response_payload(serialized)

        now, event, progress = build_job_event_and_progress(
            stage="queued",
            message=str(reason or "Job requeued manually."),
            status=AnalysisJobStatus.QUEUED,
            data={
                "relaunch": True,
                "status_before": status_before,
                "force": bool(force),
                "restart_from_zero": bool(restart_from_zero),
            },
        )
        updated_doc = await jobs.find_one_and_update(
            {"_id": parsed_id},
            {
                "$set": {
                    "payload": payload_for_relaunch,
                    "status": AnalysisJobStatus.QUEUED.value,
                    "error": None,
                    "result": None,
                    "finished_at": None,
                    "started_at": None,
                    "updated_at": now,
                    "progress": progress,
                    "cancel_requested": False,
                    "cancel_requested_at": None,
                    "cancel_reason": None,
                    **legacy_fields,
                },
                "$push": {"events": event},
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise LookupError(f"Job '{job_id}' not found.")
        return self._sanitize_response_payload(self._serialize_analysis_job_doc(updated_doc))

    def _build_relaunch_payload(
        self,
        payload: dict[str, Any],
        *,
        queue_name: Any,
        job_type: Any,
        restart_from_zero: bool,
    ) -> dict[str, Any]:
        rebuilt = dict(payload)
        if not restart_from_zero:
            return rebuilt

        normalized_job_type = str(job_type or "").strip().lower()
        normalized_queue = str(queue_name or "").strip().lower()
        is_scrape_job = (
            normalized_job_type == "business_analyze"
            and normalized_queue in {"scrape", "scrape_google_maps", "scrape_tripadvisor"}
        )
        if not is_scrape_job:
            return rebuilt

        rebuilt["force"] = True
        rebuilt["force_mode"] = "strict_rescrape"
        return rebuilt

    async def relaunch_jobs_waiting_human(
        self,
        *,
        queue_name: JobQueueName = "scrape_tripadvisor",
        limit: int = 100,
        reason: str | None = None,
    ) -> dict[str, Any]:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        normalized_queue = str(queue_name or "").strip().lower() or "scrape_tripadvisor"
        safe_limit = max(1, min(int(limit), 500))
        docs = (
            await jobs.find(
                {
                    "queue_name": normalized_queue,
                    "status": AnalysisJobStatus.NEEDS_HUMAN.value,
                }
            )
            .sort([("updated_at", 1), ("_id", 1)])
            .limit(safe_limit)
            .to_list(length=safe_limit)
        )

        relaunched: list[str] = []
        errors: list[dict[str, str]] = []
        for doc in docs:
            current_job_id = str(doc.get("_id"))
            try:
                await self.relaunch_job(
                    job_id=current_job_id,
                    reason=reason or "Relaunched after TripAdvisor manual intervention.",
                )
                relaunched.append(current_job_id)
            except Exception as exc:  # noqa: BLE001
                errors.append({"job_id": current_job_id, "error": str(exc)})

        return self._sanitize_response_payload(
            {
                "queue_name": normalized_queue,
                "requested_limit": safe_limit,
                "matched_jobs": len(docs),
                "relaunched_jobs": relaunched,
                "errors": errors,
            }
        )

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _serialize_analysis_job_doc(self, job_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job_doc)
        payload["job_id"] = str(payload.pop("_id"))
        payload_status = str(payload.get("status") or "").strip().lower()
        try:
            payload["status"] = AnalysisJobStatus(payload_status).value
        except ValueError:
            payload["status"] = AnalysisJobStatus.RUNNING.value
        if not isinstance(payload.get("queue_name"), str):
            payload["queue_name"] = "scrape"
        if not isinstance(payload.get("job_type"), str):
            payload["job_type"] = "business_analyze"

        if not isinstance(payload.get("payload"), dict):
            payload["payload"] = self._legacy_payload_from_doc(payload)
        payload["progress"] = self._normalize_progress_payload(payload.get("progress"))
        payload["events"] = self._normalize_events_payload(payload.get("events"))
        return payload

    def _legacy_payload_from_doc(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_type = str(payload.get("job_type") or "business_analyze").strip().lower()
        try:
            if job_type == "business_analyze":
                task = AnalyzeBusinessTaskPayload.model_validate(
                    {
                        "name": str(payload.get("name", "")).strip(),
                        "canonical_name": str(payload.get("canonical_name") or "").strip() or None,
                        "canonical_name_normalized": (
                            str(payload.get("canonical_name_normalized") or "").strip() or None
                        ),
                        "source_name": str(payload.get("source_name") or "").strip() or None,
                        "source_name_normalized": (
                            str(payload.get("source_name_normalized") or "").strip() or None
                        ),
                        "root_business_id": str(payload.get("root_business_id") or "").strip() or None,
                        "force": bool(payload.get("force", False)),
                        "strategy": str(payload.get("strategy") or "").strip() or None,
                        "force_mode": str(payload.get("force_mode") or "").strip() or None,
                        "interactive_max_rounds": payload.get("interactive_max_rounds"),
                        "html_scroll_max_rounds": payload.get("html_scroll_max_rounds"),
                        "html_stable_rounds": payload.get("html_stable_rounds"),
                        "tripadvisor_max_pages": payload.get("tripadvisor_max_pages"),
                        "tripadvisor_pages_percent": payload.get("tripadvisor_pages_percent"),
                    }
                )
                return task.model_dump(mode="python")

            if job_type == "analysis_generate":
                task = AnalysisGenerateTaskPayload.model_validate(
                    {
                        "business_id": str(payload.get("business_id", "")).strip(),
                        "dataset_id": str(payload.get("dataset_id") or "").strip() or None,
                        "source_profile_id": str(payload.get("source_profile_id") or "").strip() or None,
                        "scrape_run_id": str(payload.get("scrape_run_id") or "").strip() or None,
                        "batchers": payload.get("batchers"),
                        "batch_size": payload.get("batch_size"),
                        "max_reviews_pool": payload.get("max_reviews_pool"),
                        "source_job_id": str(payload.get("source_job_id") or "").strip() or None,
                    }
                )
                return task.model_dump(mode="python")

            if job_type == "report_generate":
                task = ReportGenerateTaskPayload.model_validate(
                    {
                        "business_id": str(payload.get("business_id", "")).strip(),
                        "analysis_id": str(payload.get("analysis_id", "")).strip(),
                        "output_format": str(payload.get("output_format") or payload.get("format") or "pdf"),
                        "locale": str(payload.get("locale") or "").strip() or None,
                        "template_id": str(payload.get("template_id") or "").strip() or None,
                        "source_job_id": str(payload.get("source_job_id") or "").strip() or None,
                    }
                )
                return task.model_dump(mode="python")
        except Exception:  # noqa: BLE001
            return {}
        return {}

    def _extract_legacy_scrape_fields(self, payload_data: dict[str, Any]) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        payload_name = payload_data.get("name")
        payload_canonical_name = payload_data.get("canonical_name")
        payload_canonical_name_normalized = payload_data.get("canonical_name_normalized")
        payload_source_name = payload_data.get("source_name")
        payload_source_name_normalized = payload_data.get("source_name_normalized")
        payload_root_business_id = payload_data.get("root_business_id")
        payload_force = payload_data.get("force")
        payload_strategy = payload_data.get("strategy")
        payload_force_mode = payload_data.get("force_mode")
        payload_interactive_max_rounds = payload_data.get("interactive_max_rounds")
        payload_html_scroll_max_rounds = payload_data.get("html_scroll_max_rounds")
        payload_html_stable_rounds = payload_data.get("html_stable_rounds")
        payload_tripadvisor_max_pages = payload_data.get("tripadvisor_max_pages")
        payload_tripadvisor_pages_percent = payload_data.get("tripadvisor_pages_percent")

        if isinstance(payload_name, str):
            fields["name"] = payload_name
        if isinstance(payload_canonical_name, str):
            fields["canonical_name"] = payload_canonical_name
        if isinstance(payload_canonical_name_normalized, str):
            fields["canonical_name_normalized"] = payload_canonical_name_normalized
        if isinstance(payload_source_name, str):
            fields["source_name"] = payload_source_name
        if isinstance(payload_source_name_normalized, str):
            fields["source_name_normalized"] = payload_source_name_normalized
        if isinstance(payload_root_business_id, str):
            fields["root_business_id"] = payload_root_business_id
        if isinstance(payload_force, bool):
            fields["force"] = payload_force
        if isinstance(payload_strategy, str):
            fields["strategy"] = payload_strategy.strip()
        if isinstance(payload_force_mode, str):
            fields["force_mode"] = payload_force_mode.strip()
        if isinstance(payload_interactive_max_rounds, int) and not isinstance(payload_interactive_max_rounds, bool):
            fields["interactive_max_rounds"] = payload_interactive_max_rounds
        if isinstance(payload_html_scroll_max_rounds, int) and not isinstance(payload_html_scroll_max_rounds, bool):
            fields["html_scroll_max_rounds"] = payload_html_scroll_max_rounds
        if isinstance(payload_html_stable_rounds, int) and not isinstance(payload_html_stable_rounds, bool):
            fields["html_stable_rounds"] = payload_html_stable_rounds
        if isinstance(payload_tripadvisor_max_pages, int) and not isinstance(payload_tripadvisor_max_pages, bool):
            fields["tripadvisor_max_pages"] = payload_tripadvisor_max_pages
        if isinstance(payload_tripadvisor_pages_percent, (int, float)) and not isinstance(payload_tripadvisor_pages_percent, bool):
            fields["tripadvisor_pages_percent"] = float(payload_tripadvisor_pages_percent)
        return fields

    def _normalize_progress_payload(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        payload = dict(value)
        stage = str(payload.get("stage") or "running")
        status = payload.get("status")
        try:
            payload["status"] = normalize_job_status(stage=stage, explicit_status=status).value
        except Exception:
            payload["status"] = AnalysisJobStatus.RUNNING.value
        return payload

    def _normalize_events_payload(self, value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                normalized.append({"status": AnalysisJobStatus.RUNNING.value, "message": str(item)})
                continue
            payload = dict(item)
            stage = str(payload.get("stage") or "running")
            status = payload.get("status")
            try:
                payload["status"] = normalize_job_status(stage=stage, explicit_status=status).value
            except Exception:
                payload["status"] = AnalysisJobStatus.RUNNING.value
            normalized.append(payload)
        return normalized

    async def _wait_until_job_not_active(
        self,
        *,
        parsed_id: ObjectId,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        safe_timeout = max(0.5, float(timeout_seconds))
        safe_poll = max(0.1, float(poll_seconds))
        started_at = time.monotonic()

        while True:
            doc = await jobs.find_one(
                {"_id": parsed_id},
                projection={"status": 1},
            )
            if doc is None:
                return
            status_value = self._normalize_status_value(doc.get("status"))
            if not self._is_active_status(status_value):
                return
            if (time.monotonic() - started_at) >= safe_timeout:
                raise TimeoutError(
                    f"Job '{parsed_id}' is still active after {safe_timeout:.1f}s."
                )
            await asyncio.sleep(safe_poll)

    def _normalize_status_value(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        try:
            return AnalysisJobStatus(raw).value
        except ValueError:
            return AnalysisJobStatus.RUNNING.value

    def _is_active_status(self, status_value: str) -> bool:
        return str(status_value or "").strip().lower() in self._ACTIVE_STATUSES

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
