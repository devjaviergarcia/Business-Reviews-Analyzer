from __future__ import annotations

import asyncio
import logging

from src.dependencies import create_worker_job_broker
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker

LOGGER = logging.getLogger("analysis_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class AnalysisWorker(QueuedJobWorkerBase):
    queue_name = "analysis"
    logger_name = "analysis_worker"

    def __init__(self, job_broker: WorkerJobBroker | None = None) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip() or "unknown"
        message = f"Analysis worker handler not implemented for job_type='{job_type}'."
        await self._job_broker.append_event(
            job_id=job_id,
            stage="analysis_worker_unimplemented",
            message=message,
            data={"queue_name": self.queue_name, "job_type": job_type},
        )
        await self._job_broker.mark_failed(job_id=job_id, error=message)
        LOGGER.warning("Job failed as unimplemented in analysis worker: job=%s job_type=%s", job_id, job_type)


async def _main() -> None:
    worker = AnalysisWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
