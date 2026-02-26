from __future__ import annotations

from typing import Any, Protocol


class WorkerJobBroker(Protocol):
    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        """Atomically claim the next queued job for a logical queue."""

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Append a progress event and update current progress state."""

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        """Mark a job as completed with a result payload."""

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        """Mark a job as failed with an error message."""
