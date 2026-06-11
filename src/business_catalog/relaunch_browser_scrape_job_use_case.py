from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_LIVE_DISPLAY_MODE,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
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
        launch_tripadvisor_live_session: Callable[..., Awaitable[dict[str, Any]]] | None = None,
    ) -> None:
        self._job_service = job_service
        self._ensure_job_is_scrape = ensure_job_is_scrape
        self._ensure_tripadvisor_session_available_for_relaunch = (
            ensure_tripadvisor_session_available_for_relaunch
        )
        self._validate_business_name = validate_business_name
        self._normalize_text = normalize_text
        self._launch_tripadvisor_live_session = launch_tripadvisor_live_session

    async def _maybe_launch_tripadvisor_live_session(
        self,
        *,
        job_id: str,
        reason: str,
        live_display_mode: str,
    ) -> dict[str, Any]:
        if self._launch_tripadvisor_live_session is None:
            return {
                "launched": False,
                "skipped": True,
                "reason": "tripadvisor_live_launcher_not_configured",
            }
        try:
            launch_result = await self._launch_tripadvisor_live_session(
                reason=reason,
                job_id=job_id,
                live_display_mode=live_display_mode,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "launched": False,
                "skipped": False,
                "error": str(exc),
            }
        return {
            "launched": True,
            "skipped": False,
            "result": launch_result,
        }

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
        live_display_mode: str | None = None,
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
        normalized_live_display_mode = (
            str(live_display_mode or DEFAULT_BROWSER_LIVE_DISPLAY_MODE).strip().lower()
            or DEFAULT_BROWSER_LIVE_DISPLAY_MODE
        )
        if queue_name == "scrape_tripadvisor":
            normalized_execution_mode = "live"

        payload_override: dict[str, Any] = {}
        if normalized_execution_mode:
            payload_override["execution_mode"] = normalized_execution_mode
            payload_override["live_display_mode"] = normalized_live_display_mode
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

        relaunched_job = await self._job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force) or bool(restart_from_zero),
            restart_from_zero=bool(restart_from_zero),
            payload_override=payload_override or None,
        )
        if queue_name != "scrape_tripadvisor":
            return relaunched_job

        relaunched_job_id = str(relaunched_job.get("job_id") or job_id).strip() or job_id
        launch_status = await self._maybe_launch_tripadvisor_live_session(
            job_id=relaunched_job_id,
            reason=f"relaunch_tripadvisor_live:{relaunched_job_id}",
            live_display_mode=normalized_live_display_mode,
        )
        return {
            **relaunched_job,
            "effective_execution_mode": "live",
            "live_display_mode": normalized_live_display_mode,
            "tripadvisor_live_session": launch_status,
        }
