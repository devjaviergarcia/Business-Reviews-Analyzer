from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.job_runtime.browser_job_contracts import DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET
from src.services.analysis_job_service import AnalysisJobService


class RelaunchBrowserScrapeJobUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        ensure_job_is_scrape: Callable[[dict[str, Any]], None],
        ensure_tripadvisor_session_available_for_relaunch: Callable[..., Awaitable[None]],
        validate_business_name: Callable[[str], str],
        normalize_text: Callable[[str], str],
    ) -> None:
        self._job_service = job_service
        self._ensure_job_is_scrape = ensure_job_is_scrape
        self._ensure_tripadvisor_session_available_for_relaunch = (
            ensure_tripadvisor_session_available_for_relaunch
        )
        self._validate_business_name = validate_business_name
        self._normalize_text = normalize_text

    async def execute(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
        execution_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
    ) -> dict[str, Any]:
        existing = await self._job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing)
        queue_name = str(existing.get("queue_name") or "").strip().lower()
        normalized_execution_mode = str(execution_mode or "").strip().lower() or None
        if queue_name == "scrape_tripadvisor" and normalized_execution_mode != "live":
            await self._ensure_tripadvisor_session_available_for_relaunch(
                operation="relaunch_tripadvisor_job",
                job_id=job_id,
            )

        payload_override: dict[str, Any] = {}
        if normalized_execution_mode:
            payload_override["execution_mode"] = normalized_execution_mode
            payload_override["runtime_target"] = DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET
            payload_override["requested_by"] = (
                "manual_live_relaunch" if normalized_execution_mode == "live" else "manual_relaunch"
            )
        if interactive_max_rounds is not None:
            payload_override["interactive_max_rounds"] = int(interactive_max_rounds)
        if html_scroll_max_rounds is not None:
            payload_override["html_scroll_max_rounds"] = int(html_scroll_max_rounds)
        if html_stable_rounds is not None:
            payload_override["html_stable_rounds"] = int(html_stable_rounds)
        if tripadvisor_max_pages is not None:
            payload_override["tripadvisor_max_pages"] = int(tripadvisor_max_pages)
        if tripadvisor_pages_percent is not None:
            payload_override["tripadvisor_pages_percent"] = float(tripadvisor_pages_percent)

        override_source_name: str | None = None
        if queue_name == "scrape_google_maps":
            if isinstance(google_maps_name, str) and google_maps_name.strip():
                override_source_name = self._validate_business_name(google_maps_name)
        elif queue_name == "scrape_tripadvisor":
            if isinstance(tripadvisor_name, str) and tripadvisor_name.strip():
                override_source_name = self._validate_business_name(tripadvisor_name)
        if override_source_name:
            payload_override["name"] = override_source_name
            payload_override["source_name"] = override_source_name
            payload_override["source_name_normalized"] = self._normalize_text(override_source_name)

        return await self._job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force) or bool(restart_from_zero),
            restart_from_zero=bool(restart_from_zero),
            payload_override=payload_override or None,
        )
