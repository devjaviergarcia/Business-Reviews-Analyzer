from __future__ import annotations

from typing import Any

from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisJobStatus, JobQueueName, JobType, WorkerTaskPayload


class RabbitMQJobBroker(WorkerJobBroker):
    """Placeholder broker for future RabbitMQ migration."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        self._config = {"args": args, "kwargs": kwargs}

    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        raise NotImplementedError(
            f"RabbitMQJobBroker is not implemented yet (queue_name={queue_name!r})."
        )

    async def is_cancel_requested(self, *, job_id: Any) -> bool:
        raise NotImplementedError(
            f"RabbitMQJobBroker cancel-check is not implemented yet (job_id={job_id!r})."
        )

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
        status: AnalysisJobStatus | str | None = None,
    ) -> None:
        raise NotImplementedError("RabbitMQJobBroker event append is not implemented yet.")

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        raise NotImplementedError("RabbitMQJobBroker mark_done is not implemented yet.")

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        raise NotImplementedError("RabbitMQJobBroker mark_failed is not implemented yet.")

    async def mark_needs_human(
        self,
        *,
        job_id: Any,
        reason: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError("RabbitMQJobBroker mark_needs_human is not implemented yet.")

    async def handoff_job(
        self,
        *,
        job_id: Any,
        queue_name: JobQueueName,
        job_type: JobType,
        task_payload: WorkerTaskPayload,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        raise NotImplementedError("RabbitMQJobBroker handoff_job is not implemented yet.")
