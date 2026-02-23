from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from pymongo import ReturnDocument

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo, get_database
from src.services.business_service import BusinessService

LOGGER = logging.getLogger("scraper_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class ScraperWorker:
    _JOBS_COLLECTION = "analysis_jobs"

    def __init__(self) -> None:
        self._service = BusinessService()
        self._poll_seconds = max(1, int(settings.worker_poll_seconds))

    async def run_forever(self) -> None:
        await connect_to_mongo()
        try:
            LOGGER.info("Scraper worker started. Poll interval: %ss", self._poll_seconds)
            while True:
                job = await self._pick_next_job()
                if not job:
                    await asyncio.sleep(self._poll_seconds)
                    continue
                await self._process_job(job)
        finally:
            await close_mongo_connection()

    async def _pick_next_job(self) -> dict | None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        now = datetime.now(timezone.utc)
        start_event = {
            "stage": "worker_started",
            "message": "Worker started processing job.",
            "data": {},
            "created_at": now,
        }
        return await jobs.find_one_and_update(
            {"status": "queued"},
            {
                "$set": {
                    "status": "running",
                    "started_at": now,
                    "updated_at": now,
                    "progress": {
                        "stage": "worker_started",
                        "message": "Worker started processing job.",
                        "updated_at": now,
                    },
                },
                "$push": {"events": start_event},
                "$inc": {"attempts": 1},
            },
            sort=[("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    async def _process_job(self, job: dict) -> None:
        database = get_database()
        jobs = database[self._JOBS_COLLECTION]
        job_id = job.get("_id")
        job_name = str(job.get("name", "")).strip()
        force = bool(job.get("force", False))
        strategy = str(job.get("strategy") or "").strip() or None
        LOGGER.info("Processing job=%s name=%r force=%s strategy=%s", job_id, job_name, force, strategy)

        async def on_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "running")
            message = str(event.get("message", "") or "In progress.")
            data = event.get("data", {})
            await self._emit_job_event(
                jobs=jobs,
                job_id=job_id,
                stage=stage,
                message=message,
                data=data if isinstance(data, dict) else {},
            )

        try:
            result = await self._service.analyze_business(
                name=job_name,
                force=force,
                strategy=strategy,
                progress_callback=on_progress,
            )
            finished_at = datetime.now(timezone.utc)
            done_event = {
                "stage": "done",
                "message": "Job completed successfully.",
                "data": {"strategy": result.get("strategy"), "review_count": result.get("review_count")},
                "created_at": finished_at,
            }
            await jobs.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "done",
                        "result": result,
                        "error": None,
                        "finished_at": finished_at,
                        "updated_at": finished_at,
                        "progress": {
                            "stage": "done",
                            "message": "Job completed successfully.",
                            "updated_at": finished_at,
                        },
                    }
                    ,
                    "$push": {"events": done_event},
                },
            )
            LOGGER.info("Job done=%s", job_id)
        except Exception as exc:  # noqa: BLE001
            finished_at = datetime.now(timezone.utc)
            failed_event = {
                "stage": "failed",
                "message": "Job failed.",
                "data": {"error": str(exc)},
                "created_at": finished_at,
            }
            await jobs.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "finished_at": finished_at,
                        "updated_at": finished_at,
                        "progress": {
                            "stage": "failed",
                            "message": str(exc),
                            "updated_at": finished_at,
                        },
                    }
                    ,
                    "$push": {"events": failed_event},
                },
            )
            LOGGER.exception("Job failed=%s", job_id)

    async def _emit_job_event(
        self,
        *,
        jobs,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        event = {
            "stage": stage,
            "message": message,
            "data": data or {},
            "created_at": now,
        }
        await jobs.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "updated_at": now,
                    "progress": {
                        "stage": stage,
                        "message": message,
                        "updated_at": now,
                    },
                },
                "$push": {"events": event},
            },
        )


async def _main() -> None:
    worker = ScraperWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
