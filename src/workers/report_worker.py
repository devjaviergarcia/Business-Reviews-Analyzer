from __future__ import annotations

import asyncio
import logging

from src.config import settings
from src.dependencies import create_worker_job_broker
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisJobStatus, parse_report_generate_payload

LOGGER = logging.getLogger("report_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class ReportWorker(QueuedJobWorkerBase):
    """Report stage worker stub.

    Phase status:
    - Contract is defined and validated (`report_generate` payload).
    - Queue integration is active (`queue_name='report'`).
    - Rendering pipeline (PDF/Typst/HTML) is pending implementation.
    """

    queue_name = "report"
    logger_name = "report_worker"

    def __init__(self, job_broker: WorkerJobBroker | None = None) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip() or "unknown"
        try:
            task_payload = parse_report_generate_payload(job)
            LOGGER.info(
                "Report stub job id=%s business_id=%s analysis_id=%s format=%s template_id=%s",
                job_id,
                task_payload.business_id,
                task_payload.analysis_id,
                task_payload.output_format,
                task_payload.template_id,
            )
            message = (
                "Report worker stub reached. "
                "Renderer phase is pending implementation for "
                f"job_type='{job_type}' analysis_id='{task_payload.analysis_id}'."
            )
            # TODO(phase-07): replace this stub with renderer orchestration:
            # 1. Load analysis/business data.
            # 2. Build structured report model.
            # 3. Render artifact (PDF/Typst/HTML) and persist metadata.
            await self._job_broker.append_event(
                job_id=job_id,
                stage="report_worker_stub",
                message=message,
                status=AnalysisJobStatus.FAILED,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                    "payload": task_payload.model_dump(mode="python"),
                    "phase": "fase_07_informe_estructurado_pending",
                },
            )
            await self._job_broker.mark_failed(job_id=job_id, error=message)
            LOGGER.warning("Report stub job failed intentionally: job=%s job_type=%s", job_id, job_type)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception(
                "Report job failed id=%s job_type=%s error=%s",
                job_id,
                job_type,
                exc,
            )


async def _main() -> None:
    worker = ReportWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
