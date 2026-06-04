from __future__ import annotations

from typing import Any, Iterable

from pymongo import ReturnDocument

from src.database import get_database
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
    LOCAL_BROWSER_JOB_TYPES,
    LOCAL_BROWSER_QUEUE_NAMES,
    normalize_browser_source,
)
from src.workers.contracts import AnalysisJobStatus
from src.workers.events import build_job_event_and_progress


class LocalBrowserJobCoordinator:
    _JOBS_COLLECTION = "analysis_jobs"

    def __init__(self, *, jobs_collection_name: str | None = None) -> None:
        self._jobs_collection_name = str(jobs_collection_name or self._JOBS_COLLECTION).strip() or self._JOBS_COLLECTION

    async def claim_next_job(
        self,
        *,
        worker_id: str,
        host_name: str,
        supported_sources: Iterable[str] | None = None,
    ) -> dict[str, Any] | None:
        normalized_sources = {
            source
            for source in (
                normalize_browser_source(raw_source) for raw_source in (supported_sources or ())
            )
            if source is not None
        }

        live_job = await self._claim_with_execution_mode(
            worker_id=worker_id,
            host_name=host_name,
            execution_mode="live",
            supported_sources=normalized_sources,
        )
        if live_job is not None:
            return live_job

        return await self._claim_with_execution_mode(
            worker_id=worker_id,
            host_name=host_name,
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            supported_sources=normalized_sources,
        )

    async def _claim_with_execution_mode(
        self,
        *,
        worker_id: str,
        host_name: str,
        execution_mode: str,
        supported_sources: set[str],
    ) -> dict[str, Any] | None:
        database = get_database()
        jobs = database[self._jobs_collection_name]
        pick_query: dict[str, Any] = {
            "status": AnalysisJobStatus.QUEUED.value,
            "queue_name": {"$in": sorted(LOCAL_BROWSER_QUEUE_NAMES)},
            "job_type": {"$in": sorted(LOCAL_BROWSER_JOB_TYPES)},
            "$and": [
                {
                    "$or": [
                        {"runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET},
                        {"runtime_target": {"$exists": False}},
                        {"runtime_target": None},
                    ]
                }
            ],
        }
        normalized_execution_mode = str(execution_mode).strip().lower()
        if normalized_execution_mode == "live":
            pick_query["execution_mode"] = "live"
        else:
            pick_query["$and"].append(
                {
                    "$or": [
                        {"execution_mode": normalized_execution_mode},
                        {"execution_mode": {"$exists": False}},
                        {"execution_mode": None},
                    ]
                }
            )
        if supported_sources and supported_sources != {"google_maps", "tripadvisor"}:
            pick_query["source"] = {"$in": sorted(supported_sources)}

        now, start_event, start_progress = build_job_event_and_progress(
            stage="local_browser_worker_started",
            message="Local browser runtime worker started processing job.",
            status=AnalysisJobStatus.RUNNING,
            data={
                "worker_id": str(worker_id),
                "host_name": str(host_name),
                "execution_mode": str(execution_mode),
                "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
            },
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
                    "claimed_by_worker_id": str(worker_id),
                    "claimed_by_host": str(host_name),
                    "worker_runtime": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                },
                "$push": {"events": start_event},
                "$inc": {"attempts": 1},
            },
            sort=[("created_at", 1), ("_id", 1)],
            return_document=ReturnDocument.AFTER,
        )
