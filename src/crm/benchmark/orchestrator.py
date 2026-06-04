from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

from src.crm.benchmark.competitors import select_competitors_for_business
from src.crm.repositories.interfaces import (
    BenchmarkBusinessRepository,
    BenchmarkRunRepository,
    CompetitorSetRepository,
)
from src.workers.contracts import BenchmarkLocalStudyTaskPayload


DiscoverBenchmarkCandidatesFn = Callable[[BenchmarkLocalStudyTaskPayload], Awaitable[list[dict[str, Any]]]]


class BenchmarkOrchestrator:
    def __init__(
        self,
        *,
        runs: BenchmarkRunRepository,
        businesses: BenchmarkBusinessRepository,
        discover_candidates: DiscoverBenchmarkCandidatesFn,
        competitor_sets: CompetitorSetRepository | None = None,
    ) -> None:
        self._runs = runs
        self._businesses = businesses
        self._discover_candidates = discover_candidates
        self._competitor_sets = competitor_sets

    async def run(
        self,
        *,
        task_payload: BenchmarkLocalStudyTaskPayload,
        job_id: str | None,
    ) -> dict[str, Any]:
        benchmark_id = str(task_payload.benchmark_run_id or "").strip()
        if not benchmark_id:
            run_doc = await self._runs.create_run(
                {
                    "title": task_payload.title,
                    "query": task_payload.query,
                    "city": task_payload.city,
                    "category": task_payload.category,
                    "limit": task_payload.limit,
                    "source": task_payload.source,
                    "metrics": {"job_id": job_id},
                }
            )
            benchmark_id = str(run_doc.get("benchmark_run_id") or "").strip()

        await self._runs.mark_running(benchmark_run_id=benchmark_id)

        discover_started = time.monotonic()
        try:
            candidates = await self._discover_candidates(task_payload)
        except Exception as exc:  # noqa: BLE001
            failure_reason = str(exc)
            metrics = {
                "candidates": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "enriched_ok": 0,
                "duration_ms": int((time.monotonic() - discover_started) * 1000),
                "failure_reason": failure_reason,
            }
            await self._runs.finalize(
                benchmark_run_id=benchmark_id,
                status="failed",
                metrics=metrics,
                failure_reason=failure_reason,
            )
            return {
                "benchmark_run_id": benchmark_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": 0,
                "inserted": 0,
                "updated": 0,
                "skipped": 0,
                "status": "failed",
                "failure_reason": failure_reason,
            }

        inserted = 0
        updated = 0
        skipped = 0
        persist_errors = 0
        enriched_ok = 0
        persisted_businesses: list[dict[str, Any]] = []

        ranked_candidates: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates, start=1):
            ranked_candidate = dict(candidate)
            source_ref = ranked_candidate.get("source_ref") if isinstance(ranked_candidate.get("source_ref"), dict) else {}
            source_ref = dict(source_ref)
            source_ref.setdefault("discovery_rank", index)
            ranked_candidate["source_ref"] = source_ref
            ranked_candidate.setdefault("discovery_rank", index)
            ranked_candidates.append(ranked_candidate)

        for candidate in ranked_candidates:
            source_ref = candidate.get("source_ref") if isinstance(candidate.get("source_ref"), dict) else {}
            if bool(source_ref.get("listing_enriched") or candidate.get("listing_enriched")):
                enriched_ok += 1

            payload = dict(candidate)
            payload.setdefault("raw_snapshot", dict(candidate))
            payload["listing_enriched"] = bool(source_ref.get("listing_enriched") or candidate.get("listing_enriched"))
            try:
                result = await self._businesses.upsert_business(
                    benchmark_id=benchmark_id,
                    payload=payload,
                )
            except Exception:  # noqa: BLE001
                persist_errors += 1
                skipped += 1
                continue

            action = str(result.get("action") or "skipped")
            if action == "inserted":
                inserted += 1
                if isinstance(result.get("business"), dict):
                    persisted_businesses.append(result["business"])
            elif action == "updated":
                updated += 1
                if isinstance(result.get("business"), dict):
                    persisted_businesses.append(result["business"])
            else:
                skipped += 1

        competitor_sets_written = 0
        competitor_set_errors = 0
        if self._competitor_sets is not None and len(persisted_businesses) > 1:
            for business in persisted_businesses:
                benchmark_business_id = str(business.get("benchmark_business_id") or "").strip()
                benchmark_id_for_business = str(business.get("benchmark_id") or benchmark_id).strip()
                if not benchmark_business_id or not benchmark_id_for_business:
                    continue
                competitors = select_competitors_for_business(
                    business,
                    persisted_businesses,
                    max_competitors=5,
                )
                try:
                    await self._competitor_sets.upsert_set(
                        benchmark_id=benchmark_id_for_business,
                        target_business_id=benchmark_business_id,
                        competitors=competitors,
                        selection_version="v1",
                    )
                    competitor_sets_written += 1
                except Exception:  # noqa: BLE001
                    competitor_set_errors += 1

        final_status = "completed"
        if persist_errors > 0 or competitor_set_errors > 0 or (candidates and enriched_ok == 0):
            final_status = "partial"

        metrics = {
            "candidates": len(candidates),
            "ranked_candidates": len(ranked_candidates),
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
            "persist_errors": persist_errors,
            "enriched_ok": enriched_ok,
            "competitor_sets": competitor_sets_written,
            "competitor_set_errors": competitor_set_errors,
            "duration_ms": int((time.monotonic() - discover_started) * 1000),
            "failure_reason": None,
        }
        await self._runs.finalize(
            benchmark_run_id=benchmark_id,
            status=final_status,
            metrics=metrics,
            failure_reason=None,
        )

        return {
            "benchmark_run_id": benchmark_id,
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
