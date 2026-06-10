from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from src.config import settings
from src.scraping_shared.browser_scrape_errors import (
    ScrapeBotDetectedError,
    ScrapeNeedsHumanInterventionError,
)
from src.scraping_tripadvisor.browser_scraper import TripadvisorScraper
from src.scraping_tripadvisor.tripadvisor_scrape_diagnostics import TripadvisorScrapeDiagnostics
from src.services.tripadvisor_session_service import TripadvisorSessionService

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None] | None


@dataclass
class TripadvisorScrapePipelineRuntime:
    business_name: str
    progress_callback: ProgressCallback
    stage_timeout_seconds: int
    reviews_time_limit_seconds: float
    start_delay_seconds: float
    stage_elapsed_seconds: dict[str, float]
    current_stage: str = "init"


class TripadvisorBusinessPageScraper:
    """Run the Tripadvisor scraping pipeline with explicit browser stages.

    Pipeline:
    1. Validate Tripadvisor session availability.
    2. Start browser runtime.
    3. Optionally wait the configured pre-search delay.
    4. Search and open the business page.
    5. Extract listing.
    6. Extract paginated reviews with a soft time limit.
    7. Persist diagnostics if any stage fails or hits anti-bot.
    """

    def __init__(
        self,
        *,
        scraper: TripadvisorScraper,
        emit_progress: Callable[..., Awaitable[None]],
        diagnostics: TripadvisorScrapeDiagnostics,
    ) -> None:
        self._scraper = scraper
        self._emit_progress = emit_progress
        self._diagnostics = diagnostics

    async def scrape_business_page(
        self,
        business_name: str,
        *,
        max_pages: int | None = None,
        pages_percent: float | None = None,
        progress_callback: ProgressCallback = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        scraper = self._scraper
        session_service = TripadvisorSessionService()
        await self._ensure_session_available(
            session_service=session_service,
            progress_callback=progress_callback,
        )
        runtime = self._build_pipeline_runtime(
            business_name=business_name,
            progress_callback=progress_callback,
        )
        reviews_progress_callback = self._build_reviews_progress_callback(
            progress_callback=progress_callback,
        )

        await self._emit_scraper_starting(runtime=runtime)
        try:
            return await self._run_scrape_pipeline(
                scraper=scraper,
                session_service=session_service,
                runtime=runtime,
                max_pages=max_pages,
                pages_percent=pages_percent,
                reviews_progress_callback=reviews_progress_callback,
            )
        except ScrapeBotDetectedError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self._handle_unexpected_failure(
                scraper=scraper,
                session_service=session_service,
                runtime=runtime,
                error=exc,
            )
            raise
        finally:
            await scraper.close()

    async def _ensure_session_available(
        self,
        *,
        session_service: TripadvisorSessionService,
        progress_callback: ProgressCallback,
    ) -> None:
        session_state = await session_service.ensure_available()
        if bool(session_state.get("availability_now")):
            return

        recovery_context = self._diagnostics.build_recovery_context(
            reason_code="tripadvisor_session_unavailable",
            session_state=session_state,
            user_reason="Tripadvisor session is not available.",
        )
        human_message = str(recovery_context.get("human_message") or "Tripadvisor session is not available.")
        await self._emit_progress(
            progress_callback,
            "scraper_needs_human",
            human_message,
            {
                "source": "tripadvisor",
                **recovery_context,
            },
        )
        raise ScrapeNeedsHumanInterventionError(
            human_message,
            context=recovery_context,
        )

    def _build_pipeline_runtime(
        self,
        *,
        business_name: str,
        progress_callback: ProgressCallback,
    ) -> TripadvisorScrapePipelineRuntime:
        stage_timeout_seconds = max(1, int(settings.scraper_tripadvisor_stage_timeout_seconds))
        configured_reviews_time_limit = settings.scraper_tripadvisor_reviews_time_limit_seconds
        if configured_reviews_time_limit is None:
            reviews_time_limit_seconds = float(stage_timeout_seconds)
        else:
            try:
                parsed_reviews_time_limit = float(configured_reviews_time_limit)
            except (TypeError, ValueError):
                parsed_reviews_time_limit = float(stage_timeout_seconds)
            reviews_time_limit_seconds = (
                parsed_reviews_time_limit
                if parsed_reviews_time_limit > 0
                else float(stage_timeout_seconds)
            )

        return TripadvisorScrapePipelineRuntime(
            business_name=business_name,
            progress_callback=progress_callback,
            stage_timeout_seconds=stage_timeout_seconds,
            reviews_time_limit_seconds=reviews_time_limit_seconds,
            start_delay_seconds=self._diagnostics.resolve_effective_start_delay_seconds(),
            stage_elapsed_seconds={},
        )

    def _build_reviews_progress_callback(
        self,
        *,
        progress_callback: ProgressCallback,
    ) -> Callable[[dict[str, Any]], Awaitable[None]]:
        async def _scraper_progress(event: dict[str, Any]) -> None:
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_progress",
                "Review pagination in progress.",
                event,
            )

        return _scraper_progress

    async def _emit_scraper_starting(self, *, runtime: TripadvisorScrapePipelineRuntime) -> None:
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_starting",
            "Starting browser and scraper.",
            {
                "source": "tripadvisor",
                "browser_profile_id": getattr(self._scraper, "_browser_profile_id", None),
                "stage_timeout_seconds": runtime.stage_timeout_seconds,
                "start_delay_seconds": runtime.start_delay_seconds,
                "start_delay_min_seconds": settings.scraper_tripadvisor_start_delay_min_seconds,
                "start_delay_max_seconds": settings.scraper_tripadvisor_start_delay_max_seconds,
            },
        )

    async def _run_scrape_pipeline(
        self,
        *,
        scraper: TripadvisorScraper,
        session_service: TripadvisorSessionService,
        runtime: TripadvisorScrapePipelineRuntime,
        max_pages: int | None,
        pages_percent: float | None,
        reviews_progress_callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        await self._run_stage(
            scraper=scraper,
            session_service=session_service,
            runtime=runtime,
            stage="start",
            operation=scraper.start,
        )
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_started",
            "Browser and scraper started.",
            {
                "source": "tripadvisor",
                "elapsed_seconds": runtime.stage_elapsed_seconds.get("start"),
            },
        )

        if runtime.start_delay_seconds > 0:
            await self._emit_progress(
                runtime.progress_callback,
                "scraper_start_delay_started",
                "Waiting before starting Tripadvisor search.",
                {
                    "source": "tripadvisor",
                    "start_delay_seconds": runtime.start_delay_seconds,
                    "start_delay_min_seconds": settings.scraper_tripadvisor_start_delay_min_seconds,
                    "start_delay_max_seconds": settings.scraper_tripadvisor_start_delay_max_seconds,
                },
            )
            await self._run_stage(
                scraper=scraper,
                session_service=session_service,
                runtime=runtime,
                stage="start_delay",
                operation=lambda: asyncio.sleep(runtime.start_delay_seconds),
            )

        await self._emit_progress(
            runtime.progress_callback,
            "scraper_search_started",
            "Searching business on TripAdvisor.",
            {
                "source": "tripadvisor",
                "query": runtime.business_name,
                "stage_timeout_seconds": runtime.stage_timeout_seconds,
            },
        )
        await self._run_stage(
            scraper=scraper,
            session_service=session_service,
            runtime=runtime,
            stage="search",
            operation=lambda: scraper.search_business(runtime.business_name),
        )
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_search_completed",
            "Business page opened.",
            {
                "source": "tripadvisor",
                "query": runtime.business_name,
                "elapsed_seconds": runtime.stage_elapsed_seconds.get("search"),
            },
        )

        listing = await self._run_stage(
            scraper=scraper,
            session_service=session_service,
            runtime=runtime,
            stage="listing",
            operation=scraper.extract_listing,
        )
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_listing_completed",
            "Listing extracted.",
            {
                "source": "tripadvisor",
                "business_name": listing.get("business_name"),
                "total_reviews": listing.get("total_reviews"),
                "elapsed_seconds": runtime.stage_elapsed_seconds.get("listing"),
            },
        )

        reviews = await self._run_reviews_stage(
            scraper=scraper,
            runtime=runtime,
            max_pages=max_pages,
            pages_percent=pages_percent,
            reviews_progress_callback=reviews_progress_callback,
        )
        return listing, reviews

    async def _run_reviews_stage(
        self,
        *,
        scraper: TripadvisorScraper,
        runtime: TripadvisorScrapePipelineRuntime,
        max_pages: int | None,
        pages_percent: float | None,
        reviews_progress_callback: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> list[dict[str, Any]]:
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_reviews_started",
            "Starting reviews extraction.",
            {
                "source": "tripadvisor",
                "tripadvisor_max_pages": max_pages,
                "tripadvisor_pages_percent": pages_percent,
                "stage_timeout_seconds": runtime.stage_timeout_seconds,
                "reviews_time_limit_seconds": runtime.reviews_time_limit_seconds,
            },
        )
        reviews = await self._run_stage_without_hard_timeout(
            runtime=runtime,
            stage="reviews",
            operation=lambda: scraper.extract_reviews(
                max_rounds=0,
                html_scroll_max_rounds=0,
                html_stable_rounds=6,
                html_min_interval_s=max(0.2, settings.scraper_html_scroll_min_interval_s),
                html_max_interval_s=max(
                    max(0.2, settings.scraper_html_scroll_min_interval_s),
                    settings.scraper_html_scroll_max_interval_s,
                ),
                max_pages=max_pages,
                max_pages_percent=pages_percent,
                max_duration_seconds=runtime.reviews_time_limit_seconds,
                progress_callback=reviews_progress_callback,
            ),
        )
        if runtime.stage_elapsed_seconds.get("reviews", 0.0) >= float(runtime.reviews_time_limit_seconds):
            await self._emit_progress(
                runtime.progress_callback,
                "scraper_reviews_time_limit_reached",
                "Tripadvisor reviews stopped by time limit; keeping collected data.",
                {
                    "source": "tripadvisor",
                    "query": runtime.business_name,
                    "stage_name": "reviews",
                    "elapsed_seconds": runtime.stage_elapsed_seconds.get("reviews"),
                    "stage_timeout_seconds": runtime.stage_timeout_seconds,
                    "reviews_time_limit_seconds": runtime.reviews_time_limit_seconds,
                    "scraped_review_count": len(reviews),
                },
            )
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_reviews_completed",
            "Reviews extracted.",
            {
                "source": "tripadvisor",
                "scraped_review_count": len(reviews),
                "elapsed_seconds": runtime.stage_elapsed_seconds.get("reviews"),
                "stage_elapsed_seconds": runtime.stage_elapsed_seconds,
            },
        )
        return reviews

    async def _run_stage(
        self,
        *,
        scraper: TripadvisorScraper,
        session_service: TripadvisorSessionService,
        runtime: TripadvisorScrapePipelineRuntime,
        stage: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        runtime.current_stage = stage
        started_at = time.monotonic()
        try:
            result = await asyncio.wait_for(operation(), timeout=float(runtime.stage_timeout_seconds))
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            runtime.stage_elapsed_seconds[stage] = elapsed_seconds
            await self._emit_progress(
                runtime.progress_callback,
                "scraper_stage_timing",
                "Tripadvisor stage completed.",
                {
                    "source": "tripadvisor",
                    "stage_name": stage,
                    "elapsed_seconds": elapsed_seconds,
                    "stage_timeout_seconds": runtime.stage_timeout_seconds,
                },
            )
            return result
        except asyncio.TimeoutError as exc:
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            runtime.stage_elapsed_seconds[stage] = elapsed_seconds
            diagnostic_payload = await self._diagnostics.record_stage_timeout_diagnostic(
                business_name=runtime.business_name,
                stage=stage,
                timeout_seconds=runtime.stage_timeout_seconds,
                elapsed_seconds=elapsed_seconds,
                scraper=scraper,
                error=f"Stage '{stage}' timed out after {runtime.stage_timeout_seconds}s.",
            )
            await self._emit_progress(
                runtime.progress_callback,
                "scraper_stage_timeout",
                "Tripadvisor stage timed out.",
                {
                    "source": "tripadvisor",
                    "query": runtime.business_name,
                    "stage_name": stage,
                    "elapsed_seconds": elapsed_seconds,
                    "stage_timeout_seconds": runtime.stage_timeout_seconds,
                    "diagnostic_id": diagnostic_payload.get("diagnostic_id"),
                    "diagnostic_persist_error": diagnostic_payload.get("persist_error"),
                    "bot_match_count": diagnostic_payload.get("bot_match_count", 0),
                    "anti_bot_detected": bool(diagnostic_payload.get("anti_bot_detected")),
                    "page_url": diagnostic_payload.get("page_url"),
                },
            )
            diagnostic_id = str(diagnostic_payload.get("diagnostic_id") or "").strip() or "n/a"
            if bool(diagnostic_payload.get("anti_bot_detected")):
                await session_service.mark_invalid(
                    reason=f"Anti-bot challenge detected during stage '{stage}'.",
                    increment_bot_detected=True,
                )
                updated_state = await session_service.get_state()
                recovery_context = self._diagnostics.build_recovery_context(
                    reason_code="tripadvisor_antibot_detected",
                    session_state=updated_state,
                    user_reason=(
                        f"Tripadvisor anti-bot challenge detected during stage '{stage}' "
                        f"(diagnostic_id={diagnostic_id})."
                    ),
                    stage=stage,
                    diagnostic_id=diagnostic_id,
                )
                raise ScrapeBotDetectedError(
                    str(recovery_context.get("human_message")),
                    context=recovery_context,
                ) from exc
            raise RuntimeError(
                f"Tripadvisor stage '{stage}' timed out after {runtime.stage_timeout_seconds}s "
                f"(diagnostic_id={diagnostic_id})."
            ) from exc

    async def _run_stage_without_hard_timeout(
        self,
        *,
        runtime: TripadvisorScrapePipelineRuntime,
        stage: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        runtime.current_stage = stage
        started_at = time.monotonic()
        result = await operation()
        elapsed_seconds = round(time.monotonic() - started_at, 3)
        runtime.stage_elapsed_seconds[stage] = elapsed_seconds
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_stage_timing",
            "Tripadvisor stage completed.",
            {
                "source": "tripadvisor",
                "stage_name": stage,
                "elapsed_seconds": elapsed_seconds,
                "stage_timeout_seconds": runtime.stage_timeout_seconds,
                "timeout_mode": "soft",
            },
        )
        return result

    async def _handle_unexpected_failure(
        self,
        *,
        scraper: TripadvisorScraper,
        session_service: TripadvisorSessionService,
        runtime: TripadvisorScrapePipelineRuntime,
        error: Exception,
    ) -> None:
        if "diagnostic_id=" in str(error):
            return

        diagnostic_payload = await self._diagnostics.record_failure_diagnostic(
            business_name=runtime.business_name,
            stage=runtime.current_stage,
            scraper=scraper,
            error=str(error),
        )
        diagnostic_id = str(diagnostic_payload.get("diagnostic_id") or "").strip()
        await self._emit_progress(
            runtime.progress_callback,
            "scraper_stage_error",
            "Tripadvisor stage failed.",
            {
                "source": "tripadvisor",
                "query": runtime.business_name,
                "stage_name": runtime.current_stage,
                "diagnostic_id": diagnostic_payload.get("diagnostic_id"),
                "diagnostic_persist_error": diagnostic_payload.get("persist_error"),
                "bot_match_count": diagnostic_payload.get("bot_match_count", 0),
                "anti_bot_detected": bool(diagnostic_payload.get("anti_bot_detected")),
                "page_url": diagnostic_payload.get("page_url"),
            },
        )
        if not diagnostic_id:
            return

        if bool(diagnostic_payload.get("anti_bot_detected")):
            await session_service.mark_invalid(
                reason=f"Anti-bot challenge detected during stage '{runtime.current_stage}'.",
                increment_bot_detected=True,
            )
            updated_state = await session_service.get_state()
            recovery_context = self._diagnostics.build_recovery_context(
                reason_code="tripadvisor_antibot_detected",
                session_state=updated_state,
                user_reason=(
                    f"Tripadvisor anti-bot challenge detected during stage '{runtime.current_stage}' "
                    f"(diagnostic_id={diagnostic_id})."
                ),
                stage=runtime.current_stage,
                diagnostic_id=diagnostic_id,
            )
            raise ScrapeBotDetectedError(
                str(recovery_context.get("human_message")),
                context=recovery_context,
            ) from error

        raise RuntimeError(
            f"Tripadvisor stage '{runtime.current_stage}' failed: {error} "
            f"(diagnostic_id={diagnostic_id})."
        ) from error
