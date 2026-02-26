from __future__ import annotations

import asyncio
import logging

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.dependencies import create_worker_job_broker
from src.workers.broker import WorkerJobBroker


class QueuedJobWorkerBase:
    queue_name = "scrape"
    logger_name = "queued_job_worker"

    def __init__(self, job_broker: WorkerJobBroker | None = None) -> None:
        self._job_broker = job_broker or create_worker_job_broker()
        self._poll_seconds = max(1, int(settings.worker_poll_seconds))
        self._logger = logging.getLogger(self.logger_name)

    async def run_forever(self) -> None:
        await connect_to_mongo()
        try:
            self._logger.info(
                "%s started. queue=%s poll_interval=%ss",
                type(self).__name__,
                self.queue_name,
                self._poll_seconds,
            )
            while True:
                job = await self._job_broker.claim_next_job(queue_name=self.queue_name)
                if not job:
                    await asyncio.sleep(self._poll_seconds)
                    continue
                await self._process_job(job)
        finally:
            await close_mongo_connection()

    async def _process_job(self, job: dict) -> None:  # pragma: no cover - abstract hook
        raise NotImplementedError
