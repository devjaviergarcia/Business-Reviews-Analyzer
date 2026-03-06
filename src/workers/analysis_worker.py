from __future__ import annotations

import asyncio
import contextlib
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

_CANCELLED_BY_USER_ERROR = "Cancelled by user."


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
        cancellation_watch_task: asyncio.Task[None] | None = None
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

            async def cancellation_watch_loop() -> None:
                while True:
                    should_cancel = await self._job_broker.is_cancel_requested(job_id=job_id)
                    if should_cancel:
                        LOGGER.warning("Cancellation requested for analysis job=%s", job_id)
                        return
                    await asyncio.sleep(1.0)

            cancellation_watch_task = asyncio.create_task(cancellation_watch_loop())
            analysis_task = asyncio.create_task(
                self._service.reanalyze_business_from_stored_reviews(
                    business_id=task_payload.business_id,
                    dataset_id=task_payload.dataset_id,
                    batchers=task_payload.batchers,
                    batch_size=task_payload.batch_size,
                    max_reviews_pool=task_payload.max_reviews_pool,
                )
            )
            done, _ = await asyncio.wait(
                {analysis_task, cancellation_watch_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_watch_task in done:
                analysis_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await analysis_task
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

            result = await analysis_task
            cancellation_watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_watch_task

            if await self._job_broker.is_cancel_requested(job_id=job_id):
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

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
        except RuntimeError as exc:
            if str(exc).strip() != _CANCELLED_BY_USER_ERROR:
                raise
            await self._job_broker.mark_failed(job_id=job_id, error=_CANCELLED_BY_USER_ERROR)
            LOGGER.warning(
                "Analysis job cancelled id=%s job_type=%s business_id=%s",
                job_id,
                job_type,
                (task_payload.business_id if "task_payload" in locals() else None),
            )
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception(
                "Analysis job failed id=%s job_type=%s error=%s",
                job_id,
                job_type,
                exc,
            )
        finally:
            if cancellation_watch_task is not None:
                cancellation_watch_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await cancellation_watch_task


async def _main() -> None:
    worker = AnalysisWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
