from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.benchmark.orchestrator import BenchmarkOrchestrator
from src.workers.contracts import BenchmarkLocalStudyTaskPayload


DiscoverCandidatesFn = Callable[..., Awaitable[list[dict[str, Any]]]]
RecordEventFn = Callable[..., Awaitable[None]]


class BenchmarkStudyProcessingRuntime:
    def __init__(
        self,
        *,
        benchmark_run_repository: Any,
        benchmark_business_repository: Any,
        competitor_set_repository: Any,
        discover_candidates: DiscoverCandidatesFn,
        record_event: RecordEventFn,
    ) -> None:
        self._benchmark_run_repository = benchmark_run_repository
        self._benchmark_business_repository = benchmark_business_repository
        self._competitor_set_repository = competitor_set_repository
        self._discover_candidates = discover_candidates
        self._record_event = record_event

    async def process_task(
        self,
        *,
        task_payload: BenchmarkLocalStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        orchestrator = BenchmarkOrchestrator(
            runs=self._benchmark_run_repository,
            businesses=self._benchmark_business_repository,
            discover_candidates=self._discover_candidates,
            competitor_sets=self._competitor_set_repository,
        )
        result = await orchestrator.run(
            task_payload=task_payload,
            job_id=str(job_id) if job_id is not None else None,
        )
        await self._record_event(
            event_type='benchmark_study_processed',
            data={
                'job_id': str(job_id) if job_id is not None else None,
                'benchmark_run_id': result.get('benchmark_run_id'),
                'query': task_payload.query,
                'city': task_payload.city,
                'category': task_payload.category,
                'limit': task_payload.limit,
                'source': task_payload.source,
                'status': result.get('status'),
                'candidates': result.get('candidates'),
                'inserted': result.get('inserted'),
                'updated': result.get('updated'),
                'skipped': result.get('skipped'),
                'failure_reason': result.get('failure_reason'),
            },
        )
        return result
