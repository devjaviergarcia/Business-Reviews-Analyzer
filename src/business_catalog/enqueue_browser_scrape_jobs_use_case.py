from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
    default_source_display_name,
)
from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import AnalyzeBusinessTaskPayload


class EnqueueBrowserScrapeJobsUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        validate_business_name: Callable[[str], str],
        normalize_text: Callable[[str], str],
        resolve_reviews_strategy: Callable[[str | None], str],
        resolve_force_mode: Callable[[str | None], str | None],
        resolve_scrape_sources: Callable[[tuple[str, ...] | list[str] | None], tuple[str, ...]],
        ensure_tripadvisor_worker_started_on_enqueue: Callable[[tuple[str, ...]], Awaitable[None]],
        ensure_root_business_on_enqueue: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        self._job_service = job_service
        self._validate_business_name = validate_business_name
        self._normalize_text = normalize_text
        self._resolve_reviews_strategy = resolve_reviews_strategy
        self._resolve_force_mode = resolve_force_mode
        self._resolve_scrape_sources = resolve_scrape_sources
        self._ensure_tripadvisor_worker_started_on_enqueue = ensure_tripadvisor_worker_started_on_enqueue
        self._ensure_root_business_on_enqueue = ensure_root_business_on_enqueue

    async def execute(
        self,
        *,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        sources: tuple[str, ...] | list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
        execution_mode: str | None = None,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        business_name = self._validate_business_name(name)
        canonical_name_normalized = self._normalize_text(business_name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        selected_sources = self._resolve_scrape_sources(sources)
        await self._ensure_tripadvisor_worker_started_on_enqueue(selected_sources=selected_sources)
        root_business_doc = await self._ensure_root_business_on_enqueue(
            canonical_name=business_name,
            canonical_name_normalized=canonical_name_normalized,
        )
        root_business_id = str(root_business_doc.get("_id") or "").strip() or None
        normalized_execution_mode = (
            str(execution_mode or DEFAULT_BROWSER_EXECUTION_MODE).strip().lower()
            or DEFAULT_BROWSER_EXECUTION_MODE
        )
        normalized_requested_by = (
            str(requested_by or "").strip().lower().replace(" ", "_")
            or "business_api"
        )

        source_names: dict[str, str] = {}
        for source in selected_sources:
            raw_source_name = (
                google_maps_name
                if source == "google_maps"
                else tripadvisor_name if source == "tripadvisor" else None
            )
            resolved_name = (
                self._validate_business_name(raw_source_name)
                if isinstance(raw_source_name, str) and raw_source_name.strip()
                else business_name
            )
            source_names[source] = resolved_name

        queue_by_source = {
            "google_maps": "scrape_google_maps",
            "tripadvisor": "scrape_tripadvisor",
        }
        jobs_by_source: dict[str, dict[str, Any]] = {}
        for source in selected_sources:
            source_business_name = source_names[source]
            source_name_normalized = self._normalize_text(source_business_name)
            task_payload = AnalyzeBusinessTaskPayload(
                name=source_business_name,
                source=source,
                canonical_name=business_name,
                canonical_name_normalized=canonical_name_normalized,
                source_name=source_business_name,
                source_name_normalized=source_name_normalized,
                root_business_id=root_business_id,
                execution_mode=normalized_execution_mode,
                runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                requested_by=normalized_requested_by,
                fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
                source_display_name=default_source_display_name(source),
                force=bool(force),
                strategy=selected_strategy,
                force_mode=selected_force_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                tripadvisor_max_pages=tripadvisor_max_pages,
                tripadvisor_pages_percent=tripadvisor_pages_percent,
            )
            queued_job = await self._job_service.enqueue_job(
                task_payload=task_payload,
                name_normalized=source_name_normalized,
                queue_name=queue_by_source[source],
                job_type="business_analyze",
                source=source,
                runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                execution_mode=normalized_execution_mode,
                requested_by=normalized_requested_by,
                fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
                source_display_name=default_source_display_name(source),
            )
            jobs_by_source[source] = queued_job

        primary_source = selected_sources[0]
        primary_job_id = str((jobs_by_source.get(primary_source) or {}).get("job_id", "")).strip()
        return {
            "job_id": primary_job_id,
            "primary_job_id": primary_job_id,
            "primary_source": primary_source,
            "status": "queued",
            "name": business_name,
            "canonical_name": business_name,
            "canonical_name_normalized": canonical_name_normalized,
            "business_id": root_business_id,
            "execution_mode": normalized_execution_mode,
            "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
            "sources_requested": list(selected_sources),
            "source_names": source_names,
            "jobs_by_source": jobs_by_source,
        }
