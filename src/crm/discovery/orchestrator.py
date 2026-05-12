from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

from src.crm.repositories.interfaces import DiscoveryRunRepository
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


DiscoverCandidatesFn = Callable[[CRMLeadDiscoveryTaskPayload], Awaitable[list[dict[str, Any]]]]
UpsertCandidateFn = Callable[[dict[str, Any]], Awaitable[str]]
RecordEventFn = Callable[..., Awaitable[None]]


class DiscoveryOrchestrator:
    def __init__(
        self,
        *,
        runs: DiscoveryRunRepository,
        discover_candidates: DiscoverCandidatesFn,
        upsert_candidate: UpsertCandidateFn,
        record_event: RecordEventFn,
    ) -> None:
        self._runs = runs
        self._discover_candidates = discover_candidates
        self._upsert_candidate = upsert_candidate
        self._record_event = record_event

    async def run(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        job_id: str | None,
        discovery_run_id: str | None = None,
    ) -> dict[str, Any]:
        run_id = str(discovery_run_id or "").strip()
        if not run_id:
            run_doc = await self._runs.create_run(
                {
                    "job_id": job_id,
                    "query": task_payload.query,
                    "city": task_payload.city,
                    "category": task_payload.category,
                    "limit": task_payload.limit,
                    "source": task_payload.source,
                }
            )
            run_id = str(run_doc.get("discovery_run_id") or "").strip()

        await self._runs.mark_running(run_id=run_id)

        await self._append_step(
            run_id=run_id,
            step="build_query",
            ok=True,
            duration_ms=0,
            data={
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
            },
        )

        candidates: list[dict[str, Any]] = []
        discover_error: str | None = None
        discover_started = time.monotonic()
        try:
            candidates = await self._discover_candidates(task_payload)
            await self._append_step(
                run_id=run_id,
                step="discover_candidates",
                ok=True,
                duration_ms=int((time.monotonic() - discover_started) * 1000),
                data={"candidates": len(candidates)},
            )
        except Exception as exc:  # noqa: BLE001
            discover_error = str(exc)
            await self._append_step(
                run_id=run_id,
                step="discover_candidates",
                ok=False,
                duration_ms=int((time.monotonic() - discover_started) * 1000),
                error=discover_error,
            )

        if discover_error is not None:
            final_metrics = {
                "cards_seen": 0,
                "candidates_deduped": 0,
                "enriched_ok": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "failure_reason": discover_error,
            }
            await self._runs.finalize(
                run_id=run_id,
                status="failed",
                metrics=final_metrics,
                failure_reason=discover_error,
            )
            await self._record_event(
                event_type="lead_discovery_processed",
                data={
                    "job_id": job_id,
                    "discovery_run_id": run_id,
                    "query": task_payload.query,
                    "city": task_payload.city,
                    "category": task_payload.category,
                    "limit": task_payload.limit,
                    "source": task_payload.source,
                    "candidates": 0,
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "failure_reason": discover_error,
                },
            )
            return {
                "discovery_run_id": run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "failure_reason": discover_error,
                "status": "failed",
            }

        inserted = 0
        updated = 0
        skipped = 0
        persist_errors = 0
        persist_started = time.monotonic()
        for candidate in candidates:
            try:
                action = await self._upsert_candidate(candidate)
            except Exception:  # noqa: BLE001
                persist_errors += 1
                skipped += 1
                continue
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1

        await self._append_step(
            run_id=run_id,
            step="persist_candidates",
            ok=persist_errors == 0,
            duration_ms=int((time.monotonic() - persist_started) * 1000),
            data={
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "persist_errors": persist_errors,
            },
            error=f"persist_errors={persist_errors}" if persist_errors else None,
        )

        enriched_ok = 0
        for candidate in candidates:
            source_ref = candidate.get("source_ref") if isinstance(candidate.get("source_ref"), dict) else {}
            if bool(source_ref.get("listing_enriched")):
                enriched_ok += 1

        final_metrics = {
            "cards_seen": len(candidates),
            "candidates_deduped": len(candidates),
            "enriched_ok": enriched_ok,
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "failure_reason": None,
        }

        final_status = "completed"
        if persist_errors > 0 or (len(candidates) > 0 and enriched_ok == 0):
            final_status = "partial"

        await self._runs.finalize(
            run_id=run_id,
            status=final_status,
            metrics=final_metrics,
            failure_reason=None,
        )

        await self._record_event(
            event_type="lead_discovery_processed",
            data={
                "job_id": job_id,
                "discovery_run_id": run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "status": final_status,
            },
        )

        return {
            "discovery_run_id": run_id,
            "query": task_payload.query,
            "city": task_payload.city,
            "category": task_payload.category,
            "limit": task_payload.limit,
            "source": task_payload.source,
            "candidates": len(candidates),
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "status": final_status,
        }

    async def _append_step(
        self,
        *,
        run_id: str,
        step: str,
        ok: bool,
        duration_ms: int,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        await self._runs.append_step(
            run_id=run_id,
            step=step,
            ok=ok,
            duration_ms=duration_ms,
            data=data,
            error=error,
        )
