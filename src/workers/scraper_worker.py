from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.dependencies import create_business_service, create_worker_job_broker
from src.services.business_service import BusinessService
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalyzeBusinessTaskPayload

LOGGER = logging.getLogger("scraper_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class ScraperWorker(QueuedJobWorkerBase):
    queue_name = "scrape"
    logger_name = "scraper_worker"

    def __init__(
        self,
        service: BusinessService | None = None,
        job_broker: WorkerJobBroker | None = None,
    ) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())
        self._service = service or create_business_service()

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        task_payload = AnalyzeBusinessTaskPayload.model_validate(
            {
                "name": str(job.get("name", "")).strip(),
                "force": bool(job.get("force", False)),
                "strategy": str(job.get("strategy") or "").strip() or None,
            }
        )
        job_name = task_payload.name
        force = bool(task_payload.force)
        strategy = task_payload.strategy
        LOGGER.info("Processing job=%s name=%r force=%s strategy=%s", job_id, job_name, force, strategy)

        async def on_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "running")
            message = str(event.get("message", "") or "In progress.")
            data = event.get("data", {})
            await self._job_broker.append_event(
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
            await self._job_broker.mark_done(job_id=job_id, result=result)
            LOGGER.info("Job done=%s", job_id)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception("Job failed=%s", job_id)


async def _main() -> None:
    worker = ScraperWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
