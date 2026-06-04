from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import BenchmarkLocalStudyTaskPayload


class EnqueueBenchmarkStudyJobUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        ensure_indexes: Callable[[], Awaitable[None]],
        create_benchmark_run: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        live_google_discovery_aliases: tuple[str, ...],
    ) -> None:
        self._job_service = job_service
        self._ensure_indexes = ensure_indexes
        self._create_benchmark_run = create_benchmark_run
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._live_google_discovery_aliases = live_google_discovery_aliases

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
        normalized_source = str(source or "").strip().lower()
        if normalized_source in self._live_google_discovery_aliases:
            normalized_source = "auto_live_google_maps"
        if not normalized_source:
            normalized_source = "auto_live_google_maps"

        base_payload = BenchmarkLocalStudyTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=normalized_source,
            title=title,
        )
        run_doc = await self._create_benchmark_run(
            {
                "title": base_payload.title,
                "query": base_payload.query,
                "city": base_payload.city,
                "category": base_payload.category,
                "limit": base_payload.limit,
                "source": base_payload.source,
            }
        )
        benchmark_run_id = str(run_doc.get("benchmark_run_id") or "").strip() or None
        payload = base_payload.model_copy(update={"benchmark_run_id": benchmark_run_id})
        queued = await self._job_service.enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="benchmark_local_study",
        )
        await self._record_event(
            event_type="benchmark_study_job_queued",
            data={
                "crm_job_id": queued.get("job_id"),
                "benchmark_run_id": benchmark_run_id,
                "query": payload.query,
                "city": payload.city,
                "category": payload.category,
                "limit": payload.limit,
                "source": payload.source,
            },
        )
        queued["benchmark_run_id"] = benchmark_run_id
        return self._sanitize_payload(queued)
