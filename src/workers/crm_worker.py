from __future__ import annotations

import asyncio
import logging

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.dependencies import create_crm_service, create_worker_job_broker
from src.services.crm_service import CRMService
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import (
    AnalysisJobStatus,
    parse_crm_campaign_dispatch_payload,
    parse_crm_lead_discovery_payload,
    parse_crm_lead_pipeline_payload,
)

LOGGER = logging.getLogger("crm_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class CRMWorker:
    queue_name = "crm"

    def __init__(
        self,
        *,
        service: CRMService | None = None,
        job_broker: WorkerJobBroker | None = None,
    ) -> None:
        self._service = service or create_crm_service()
        self._job_broker = job_broker or create_worker_job_broker()
        self._poll_seconds = max(1, int(settings.worker_poll_seconds))
        self._idle_log_seconds = max(5, int(settings.worker_idle_log_seconds))
        self._idle_log_every_ticks = max(1, self._idle_log_seconds // self._poll_seconds)
        self._scheduler_batch_size = max(1, int(settings.crm_scheduler_batch_size))

    async def run_forever(self) -> None:
        await connect_to_mongo()
        await self._service.ensure_indexes()
        try:
            LOGGER.info(
                "CRMWorker started. queue=%s poll_interval=%ss idle_log_every=%ss scheduler_batch=%s",
                self.queue_name,
                self._poll_seconds,
                self._idle_log_seconds,
                self._scheduler_batch_size,
            )
            idle_ticks = 0
            while True:
                try:
                    queued_due = await self._service.enqueue_due_campaign_dispatch_jobs(limit=self._scheduler_batch_size)
                    if queued_due > 0:
                        LOGGER.info("CRM scheduler enqueued due dispatch jobs=%s", queued_due)
                except Exception:  # noqa: BLE001
                    LOGGER.exception("CRM scheduler failed while enqueueing due dispatch jobs")

                job = await self._job_broker.claim_next_job(queue_name=self.queue_name)
                if not job:
                    idle_ticks += 1
                    if idle_ticks % self._idle_log_every_ticks == 0:
                        LOGGER.info(
                            "CRMWorker idle. queue=%s no_jobs_for=%ss",
                            self.queue_name,
                            idle_ticks * self._poll_seconds,
                        )
                    await asyncio.sleep(self._poll_seconds)
                    continue

                idle_ticks = 0
                LOGGER.info(
                    "CRMWorker claimed job queue=%s job_id=%s job_type=%s attempts=%s status=%s",
                    self.queue_name,
                    job.get("_id"),
                    job.get("job_type"),
                    job.get("attempts"),
                    job.get("status"),
                )
                await self._process_job(job)
        finally:
            await close_mongo_connection()

    async def _process_job(self, job: dict[str, object]) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip().lower() or "unknown"
        try:
            await self._job_broker.append_event(
                job_id=job_id,
                stage="crm_worker_started",
                message="CRM worker started.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                },
            )

            if job_type == "crm_lead_discovery":
                payload = parse_crm_lead_discovery_payload(job)
                result = await self._service.process_discovery_task(task_payload=payload, job_id=job_id)
            elif job_type == "crm_lead_pipeline":
                payload = parse_crm_lead_pipeline_payload(job)
                result = await self._service.process_lead_pipeline_task(task_payload=payload, job_id=job_id)
            elif job_type == "crm_campaign_dispatch":
                payload = parse_crm_campaign_dispatch_payload(job)
                result = await self._service.process_campaign_dispatch_task(task_payload=payload, job_id=job_id)
            else:
                raise ValueError(f"Unsupported CRM job type '{job_type}'.")

            await self._job_broker.append_event(
                job_id=job_id,
                stage="crm_worker_completed",
                message="CRM worker completed job.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                    "result": result,
                },
            )
            await self._job_broker.mark_done(job_id=job_id, result=result)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception("CRM job failed id=%s job_type=%s error=%s", job_id, job_type, exc)


async def _main() -> None:
    worker = CRMWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
