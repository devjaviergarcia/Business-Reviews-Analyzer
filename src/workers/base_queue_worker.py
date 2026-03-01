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
        self._idle_log_seconds = max(5, int(settings.worker_idle_log_seconds))
        self._idle_log_every_ticks = max(1, self._idle_log_seconds // self._poll_seconds)
        self._logger = logging.getLogger(self.logger_name)
        self._logger.setLevel(getattr(logging, str(settings.log_level).upper(), logging.INFO))

    async def run_forever(self) -> None:
        await connect_to_mongo()
        try:
            self._logger.info(
                "%s started. queue=%s poll_interval=%ss idle_log_every=%ss",
                type(self).__name__,
                self.queue_name,
                self._poll_seconds,
                self._idle_log_seconds,
            )
            idle_ticks = 0
            while True:
                job = await self._job_broker.claim_next_job(queue_name=self.queue_name)
                if not job:
                    idle_ticks += 1
                    if idle_ticks % self._idle_log_every_ticks == 0:
                        self._logger.info(
                            "%s idle. queue=%s no_jobs_for=%ss",
                            type(self).__name__,
                            self.queue_name,
                            idle_ticks * self._poll_seconds,
                        )
                    await asyncio.sleep(self._poll_seconds)
                    continue
                idle_ticks = 0
                self._logger.info(
                    "%s claimed job queue=%s job_id=%s job_type=%s attempts=%s status=%s",
                    type(self).__name__,
                    self.queue_name,
                    job.get("_id"),
                    job.get("job_type"),
                    job.get("attempts"),
                    job.get("status"),
                )
                try:
                    await self._process_job(job)
                except Exception:  # noqa: BLE001
                    self._logger.exception(
                        "%s unexpected error while processing claimed job queue=%s job_id=%s",
                        type(self).__name__,
                        self.queue_name,
                        job.get("_id"),
                    )
        finally:
            await close_mongo_connection()

    async def _process_job(self, job: dict) -> None:  # pragma: no cover - abstract hook
        raise NotImplementedError
