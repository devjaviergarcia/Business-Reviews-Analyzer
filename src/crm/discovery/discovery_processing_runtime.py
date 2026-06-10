from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.discovery.orchestrator import DiscoveryOrchestrator
from src.workers.contracts import CRMLeadDiscoveryTaskPayload, BenchmarkLocalStudyTaskPayload


NormalizeTextFn = Callable[[Any], str]
DiscoverLiveFn = Callable[..., Awaitable[list[dict[str, Any]]]]
DiscoverStoredFn = Callable[..., Awaitable[list[dict[str, Any]]]]
UpsertCandidateFn = Callable[[dict[str, Any]], Awaitable[str]]
RecordEventFn = Callable[..., Awaitable[None]]
UseDiscoveryV2Fn = Callable[[], bool]


class DiscoveryProcessingRuntime:
    def __init__(
        self,
        *,
        normalize_text: NormalizeTextFn,
        discover_candidates_live_google_maps: DiscoverLiveFn,
        discover_candidates_from_stored_sources: DiscoverStoredFn,
        upsert_candidate: UpsertCandidateFn,
        record_event: RecordEventFn,
        discovery_run_repository: Any,
        use_discovery_v2: bool | UseDiscoveryV2Fn,
        live_google_discovery_sources: tuple[str, ...],
        live_google_discovery_aliases: tuple[str, ...],
    ) -> None:
        self._normalize_text = normalize_text
        self._discover_candidates_live_google_maps = discover_candidates_live_google_maps
        self._discover_candidates_from_stored_sources = discover_candidates_from_stored_sources
        self._upsert_candidate = upsert_candidate
        self._record_event = record_event
        self._discovery_run_repository = discovery_run_repository
        self._use_discovery_v2 = use_discovery_v2
        self._live_google_discovery_sources = live_google_discovery_sources
        self._live_google_discovery_aliases = live_google_discovery_aliases

    async def process_discovery_task(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._is_discovery_v2_enabled():
            orchestrator = DiscoveryOrchestrator(
                runs=self._discovery_run_repository,
                discover_candidates=self.discover_candidates,
                upsert_candidate=self._upsert_candidate,
                record_event=self._record_event,
            )
            return await orchestrator.run(
                task_payload=task_payload,
                job_id=str(job_id) if job_id is not None else None,
                discovery_run_id=task_payload.discovery_run_id,
            )

        candidates = await self.discover_candidates(task_payload=task_payload)
        inserted = 0
        updated = 0
        skipped = 0
        for candidate in candidates:
            action = await self._upsert_candidate(candidate)
            if action == 'inserted':
                inserted += 1
            elif action == 'updated':
                updated += 1
            else:
                skipped += 1

        result = {
            'discovery_run_id': task_payload.discovery_run_id,
            'query': task_payload.query,
            'city': task_payload.city,
            'category': task_payload.category,
            'limit': task_payload.limit,
            'source': task_payload.source,
            'candidates': len(candidates),
            'inserted': inserted,
            'updated': updated,
            'skipped': skipped,
        }
        await self._record_event(
            event_type='lead_discovery_processed',
            data={
                'job_id': str(job_id) if job_id is not None else None,
                **result,
            },
        )
        return result

    def _is_discovery_v2_enabled(self) -> bool:
        if callable(self._use_discovery_v2):
            return bool(self._use_discovery_v2())
        return bool(self._use_discovery_v2)

    async def discover_candidates(self, *, task_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or '').strip()
        normalized_query = self._normalize_text(query_text)
        normalized_city = self._normalize_text(task_payload.city) if task_payload.city else None
        normalized_category = self._normalize_text(task_payload.category) if task_payload.category else None
        safe_limit = max(1, min(int(task_payload.limit), 5000))

        normalized_source = str(task_payload.source or '').strip().lower()
        if normalized_source in self._live_google_discovery_aliases:
            normalized_source = 'auto_live_google_maps'
        if normalized_source in self._live_google_discovery_sources:
            live_candidates = await self._discover_candidates_live_google_maps(
                task_payload=task_payload,
                normalized_query=normalized_query,
                safe_limit=safe_limit,
            )
            if normalized_source == 'live_google_maps':
                return live_candidates[:safe_limit]
            if len(live_candidates) >= safe_limit:
                return live_candidates[:safe_limit]
            fallback_candidates = await self._discover_candidates_from_stored_sources(
                task_payload=task_payload,
                normalized_query=normalized_query,
                normalized_city=normalized_city,
                normalized_category=normalized_category,
                safe_limit=safe_limit - len(live_candidates),
            )
            return (live_candidates + fallback_candidates)[:safe_limit]

        return await self._discover_candidates_from_stored_sources(
            task_payload=task_payload,
            normalized_query=normalized_query,
            normalized_city=normalized_city,
            normalized_category=normalized_category,
            safe_limit=safe_limit,
        )

    async def discover_benchmark_candidates(
        self,
        *,
        task_payload: BenchmarkLocalStudyTaskPayload,
    ) -> list[dict[str, Any]]:
        discovery_payload = CRMLeadDiscoveryTaskPayload(
            query=task_payload.query,
            city=task_payload.city,
            category=task_payload.category,
            limit=task_payload.limit,
            source=task_payload.source,
        )
        candidates = await self.discover_candidates(task_payload=discovery_payload)
        for candidate in candidates:
            candidate.setdefault('source_ref', {})
            if isinstance(candidate['source_ref'], dict):
                candidate['source_ref']['benchmark_query'] = task_payload.query
                candidate['source_ref']['benchmark_title'] = task_payload.title
        return candidates
