from __future__ import annotations

import asyncio
import logging

from src.config import settings
from src.dependencies import create_business_service, create_worker_job_broker
from src.services.business_service import BusinessService
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisJobStatus, parse_analysis_generate_payload

LOGGER = logging.getLogger("analysis_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class AnalysisWorker(QueuedJobWorkerBase):
    queue_name = "analysis"
    logger_name = "analysis_worker"

    def __init__(
        self,
        service: BusinessService | None = None,
        job_broker: WorkerJobBroker | None = None,
    ) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())
        self._service = service or create_business_service()

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip() or "unknown"
        try:
            task_payload = parse_analysis_generate_payload(job)
            LOGGER.info(
                "Processing analysis job id=%s business_id=%s dataset_id=%s batchers=%s batch_size=%s max_reviews_pool=%s source_job_id=%s",
                job_id,
                task_payload.business_id,
                task_payload.dataset_id,
                task_payload.batchers,
                task_payload.batch_size,
                task_payload.max_reviews_pool,
                task_payload.source_job_id,
            )
            await self._job_broker.append_event(
                job_id=job_id,
                stage="analysis_worker_started",
                message="Analysis worker started.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                    "payload": task_payload.model_dump(mode="python"),
                },
            )

            result = await self._service.reanalyze_business_from_stored_reviews(
                business_id=task_payload.business_id,
                dataset_id=task_payload.dataset_id,
                batchers=task_payload.batchers,
                batch_size=task_payload.batch_size,
                max_reviews_pool=task_payload.max_reviews_pool,
            )
            LOGGER.info(
                "Analysis result job=%s business_id=%s dataset_id=%s review_count=%s processed_review_count=%s batchers_used=%s",
                job_id,
                result.get("business_id"),
                result.get("dataset_id"),
                result.get("review_count"),
                result.get("processed_review_count"),
                result.get("batchers_used"),
            )
            LOGGER.debug(
                "Analysis meta job=%s meta=%s",
                job_id,
                (result.get("analysis") or {}).get("meta"),
            )
            await self._job_broker.append_event(
                job_id=job_id,
                stage="analysis_worker_summary",
                message="Analysis stage completed; finalizing job.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "business_id": result.get("business_id"),
                    "dataset_id": result.get("dataset_id"),
                    "review_count": result.get("review_count"),
                    "processed_review_count": result.get("processed_review_count"),
                    "batchers_used": result.get("batchers_used"),
                    "analysis_meta": (result.get("analysis") or {}).get("meta"),
                },
            )
            result["pipeline"] = {
                "source_job_id": task_payload.source_job_id,
                "worker": "analysis",
            }
            await self._job_broker.mark_done(job_id=job_id, result=result)
            LOGGER.info("Analysis job done=%s business_id=%s", job_id, task_payload.business_id)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception(
                "Analysis job failed id=%s job_type=%s error=%s",
                job_id,
                job_type,
                exc,
            )


async def _main() -> None:
    worker = AnalysisWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
