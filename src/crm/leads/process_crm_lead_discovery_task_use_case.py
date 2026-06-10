from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.discovery import DiscoveryOrchestrator
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


class ProcessCRMLeadDiscoveryTaskUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_discovery_v2: bool,
        discovery_run_repository: Any,
        discover_candidates_for_orchestrator: Callable[..., Awaitable[list[dict[str, Any]]]],
        upsert_lead_candidate: Callable[[dict[str, Any]], Awaitable[str]],
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        discover_candidates: Callable[..., Awaitable[list[dict[str, Any]]]],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_discovery_v2 = use_discovery_v2
        self._discovery_run_repository = discovery_run_repository
        self._discover_candidates_for_orchestrator = discover_candidates_for_orchestrator
        self._upsert_lead_candidate = upsert_lead_candidate
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._discover_candidates = discover_candidates

    async def execute(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        if self._use_discovery_v2:
            orchestrator = DiscoveryOrchestrator(
                runs=self._discovery_run_repository,
                discover_candidates=self._discover_candidates_for_orchestrator,
                upsert_candidate=self._upsert_lead_candidate,
                record_event=self._record_event,
            )
            result = await orchestrator.run(
                task_payload=task_payload,
                job_id=str(job_id) if job_id is not None else None,
                discovery_run_id=task_payload.discovery_run_id,
            )
            return self._sanitize_payload(result)

        candidates = await self._discover_candidates(task_payload=task_payload)

        inserted = 0
        updated = 0
        skipped = 0
        for candidate in candidates:
            action = await self._upsert_lead_candidate(candidate)
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1

        await self._record_event(
            event_type="lead_discovery_processed",
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "discovery_run_id": task_payload.discovery_run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            },
        )

        return self._sanitize_payload(
            {
                "discovery_run_id": task_payload.discovery_run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            }
        )
