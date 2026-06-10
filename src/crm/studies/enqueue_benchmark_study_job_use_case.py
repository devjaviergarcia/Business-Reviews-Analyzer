from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.studies.study_job_enqueue_runtime import StudyJobEnqueueRuntime


class EnqueueBenchmarkStudyJobUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        study_job_enqueue_runtime: StudyJobEnqueueRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._study_job_enqueue_runtime = study_job_enqueue_runtime

    async def execute(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
        title: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._study_job_enqueue_runtime.enqueue_benchmark_study_job(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=source,
            title=title,
        )
