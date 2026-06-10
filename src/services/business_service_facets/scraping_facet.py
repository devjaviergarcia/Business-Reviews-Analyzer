from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from src.scraping_tripadvisor import TripadvisorScraper


class BusinessServiceScrapingFacet:

    async def scrape_business_for_analysis_pipeline(
        self,
        name: str,
        *,
        canonical_name: str | None = None,
        source_name: str | None = None,
        root_business_id: str | None = None,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        sources: tuple[str, ...] | list[str] | None = None,
        preloaded_source_payloads: dict[str, dict[str, Any]] | None = None,
        source_job_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict[str, Any]:
        return await self._business_scrape_pipeline_runner.run(
            name=name,
            canonical_name=canonical_name,
            source_name=source_name,
            root_business_id=root_business_id,
            force=force,
            strategy=strategy,
            force_mode=force_mode,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            tripadvisor_max_pages=tripadvisor_max_pages,
            tripadvisor_pages_percent=tripadvisor_pages_percent,
            sources=sources,
            preloaded_source_payloads=preloaded_source_payloads,
            source_job_id=source_job_id,
            progress_callback=progress_callback,
        )

    async def _build_cached_response(
        self,
        *,
        businesses,
        reviews,
        analyses,
        name_normalized: str,
        strategy: str,
    ) -> dict | None:
        return await self._business_summary_runtime.build_cached_response(
            businesses=businesses,
            reviews=reviews,
            analyses=analyses,
            name_normalized=name_normalized,
            strategy=strategy,
        )

    async def _scrape_business_page(
        self,
        business_name: str,
        *,
        strategy: str,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        browser_profile_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> tuple[dict, list[dict]]:
        self.scraper.use_browser_profile(explicit_profile_id=browser_profile_id)
        return await self._google_maps_business_page_scraper.scrape_business_page(
            business_name,
            strategy=strategy,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            progress_callback=progress_callback,
        )

    def _build_source_progress_callback(
        self,
        *,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        source: str,
    ) -> Callable[[dict[str, Any]], Awaitable[None] | None] | None:
        if progress_callback is None:
            return None

        async def _source_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "scraper_source_progress")
            message = str(event.get("message", "") or "Scraper source progress.")
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            payload_data = {"source": source, **data}
            await self._emit_progress(
                progress_callback,
                stage,
                message,
                payload_data,
            )

        return _source_progress

    async def _scrape_tripadvisor_business_page(
        self,
        business_name: str,
        *,
        max_pages: int | None = None,
        pages_percent: float | None = None,
        browser_profile_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        self.tripadvisor_scraper.use_browser_profile(explicit_profile_id=browser_profile_id)
        return await self._tripadvisor_business_page_scraper.scrape_business_page(
            business_name,
            max_pages=max_pages,
            pages_percent=pages_percent,
            progress_callback=progress_callback,
        )

    def _resolve_effective_tripadvisor_start_delay_seconds(self) -> float:
        return self._tripadvisor_scrape_diagnostics.resolve_effective_start_delay_seconds()

    async def _ensure_tripadvisor_session_available_for_relaunch(
        self,
        *,
        operation: str,
        job_id: str | None = None,
    ) -> None:
        await self._tripadvisor_scrape_diagnostics.ensure_session_available_for_relaunch(
            operation=operation,
            job_id=job_id,
        )

    def _resolve_tripadvisor_profile_dir_hint(self) -> str:
        return self._tripadvisor_scrape_diagnostics.resolve_profile_dir_hint()

    def _build_tripadvisor_recovery_context(
        self,
        *,
        reason_code: str,
        session_state: dict[str, Any] | None,
        user_reason: str,
        stage: str | None = None,
        diagnostic_id: str | None = None,
    ) -> dict[str, Any]:
        return self._tripadvisor_scrape_diagnostics.build_recovery_context(
            reason_code=reason_code,
            session_state=session_state,
            user_reason=user_reason,
            stage=stage,
            diagnostic_id=diagnostic_id,
        )

    async def _record_tripadvisor_failure_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        scraper: TripadvisorScraper,
        error: str,
        diagnostic_type: str = "stage_error",
        timeout_seconds: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> dict[str, Any]:
        return await self._tripadvisor_scrape_diagnostics.record_failure_diagnostic(
            business_name=business_name,
            stage=stage,
            scraper=scraper,
            error=error,
            diagnostic_type=diagnostic_type,
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )

    def _detect_tripadvisor_antibot(
        self,
        *,
        html_text: str,
        keyword_matches: dict[str, list[str]],
    ) -> tuple[bool, str]:
        return self._tripadvisor_scrape_diagnostics.detect_antibot(
            html_text=html_text,
            keyword_matches=keyword_matches,
        )

    async def _record_tripadvisor_stage_timeout_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        timeout_seconds: int,
        elapsed_seconds: float,
        scraper: TripadvisorScraper,
        error: str,
    ) -> dict[str, Any]:
        return await self._tripadvisor_scrape_diagnostics.record_stage_timeout_diagnostic(
            business_name=business_name,
            stage=stage,
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
            scraper=scraper,
            error=error,
        )

    async def _capture_tripadvisor_snapshot(
        self,
        *,
        scraper: TripadvisorScraper,
    ) -> dict[str, Any]:
        return await self._tripadvisor_scrape_diagnostics.capture_snapshot(scraper=scraper)

    def _extract_anti_bot_keyword_matches(self, text: str) -> dict[str, list[str]]:
        return self._tripadvisor_scrape_diagnostics.extract_anti_bot_keyword_matches(text)

    def _extract_antibot_scan_text(self, html_text: str) -> str:
        return self._tripadvisor_scrape_diagnostics.extract_antibot_scan_text(html_text)

    def _extract_keyword_context_snippets(
        self,
        text: str,
        *,
        keyword: str,
        max_matches: int = 8,
        context_chars: int = 120,
    ) -> list[str]:
        return self._tripadvisor_scrape_diagnostics.extract_keyword_context_snippets(
            text,
            keyword=keyword,
            max_matches=max_matches,
            context_chars=context_chars,
        )

    def _resolve_reviews_strategy(self, strategy: str | None) -> str:
        return self._business_common_runtime.resolve_reviews_strategy(strategy)

    def _resolve_scrape_sources(self, sources: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        return self._business_common_runtime.resolve_scrape_sources(sources)

    def _resolve_force_mode(self, force_mode: str | None) -> str:
        return self._business_common_runtime.resolve_force_mode(force_mode)

    def _resolve_optional_int_override(
        self,
        *,
        value: int | None,
        fallback: int,
        min_value: int,
        field_name: str,
    ) -> int:
        return self._business_common_runtime.resolve_optional_int_override(
            value=value,
            fallback=fallback,
            min_value=min_value,
            field_name=field_name,
        )

    def _resolve_optional_float_override(
        self,
        *,
        value: float | None,
        min_value: float,
        max_value: float,
        field_name: str,
    ) -> float:
        return self._business_common_runtime.resolve_optional_float_override(
            value=value,
            min_value=min_value,
            max_value=max_value,
            field_name=field_name,
        )

    async def _emit_progress(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        if callback is None:
            return
        payload = {
            "stage": stage,
            "message": message,
            "data": data or {},
            "created_at": datetime.now(timezone.utc),
        }
        try:
            maybe_awaitable = callback(payload)
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        except Exception:
            # Progress callback errors must not affect core flow.
            return

    async def _upsert_reviews(
        self,
        *,
        reviews_collection,
        business_id: str,
        processed_reviews: list[dict],
        scraped_at: datetime,
        source_profile_id: str | None = None,
        dataset_id: str | None = None,
        scrape_run_id: str | None = None,
    ) -> None:
        await self._business_source_persistence.upsert_reviews(
            reviews_collection=reviews_collection,
            business_id=business_id,
            processed_reviews=processed_reviews,
            scraped_at=scraped_at,
            source_profile_id=source_profile_id,
            dataset_id=dataset_id,
            scrape_run_id=scrape_run_id,
        )

    async def _upsert_job_comments(
        self,
        *,
        comments_collection,
        business_id: str,
        business_name: str,
        name_normalized: str,
        source: str,
        source_job_id: str | None,
        processed_reviews: list[dict[str, Any]],
        scraped_at: datetime,
        source_profile_id: str | None = None,
        dataset_id: str | None = None,
        scrape_run_id: str | None = None,
    ) -> None:
        await self._business_source_persistence.upsert_job_comments(
            comments_collection=comments_collection,
            business_id=business_id,
            business_name=business_name,
            name_normalized=name_normalized,
            source=source,
            source_job_id=source_job_id,
            processed_reviews=processed_reviews,
            scraped_at=scraped_at,
            source_profile_id=source_profile_id,
            dataset_id=dataset_id,
            scrape_run_id=scrape_run_id,
        )

    async def _get_or_create_source_profile(
        self,
        *,
        source_profiles_collection,
        business_id: str,
        source: str,
        name_normalized: str,
        canonical_name_normalized: str,
        source_business_name: str,
        listing_payload: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        return await self._business_source_persistence.get_or_create_source_profile(
            source_profiles_collection=source_profiles_collection,
            business_id=business_id,
            source=source,
            name_normalized=name_normalized,
            canonical_name_normalized=canonical_name_normalized,
            source_business_name=source_business_name,
            listing_payload=listing_payload,
            now=now,
        )

    async def _package_legacy_reviews_into_dataset(
        self,
        *,
        reviews_collection,
        datasets_collection,
        source_profiles_collection,
        business_id: str,
        source_profile_id: str,
        source: str,
        now: datetime,
    ) -> dict[str, Any]:
        return await self._legacy_review_dataset_packager.package_reviews_into_dataset(
            reviews_collection=reviews_collection,
            datasets_collection=datasets_collection,
            source_profiles_collection=source_profiles_collection,
            business_id=business_id,
            source_profile_id=source_profile_id,
            source=source,
            now=now,
        )

    async def _create_scrape_run(
        self,
        *,
        scrape_runs_collection,
        business_id: str,
        source_profile_id: str,
        source: str,
        strategy: str,
        force: bool,
        force_mode: str,
        now: datetime,
    ) -> dict[str, Any]:
        return await self._business_scrape_run_store.create_scrape_run(
            scrape_runs_collection=scrape_runs_collection,
            business_id=business_id,
            source_profile_id=source_profile_id,
            source=source,
            strategy=strategy,
            force=force,
            force_mode=force_mode,
            now=now,
        )

    async def _create_dataset_snapshot(
        self,
        *,
        datasets_collection,
        business_id: str,
        source_profile_id: str,
        source: str,
        scrape_run_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        return await self._business_scrape_run_store.create_dataset_snapshot(
            datasets_collection=datasets_collection,
            business_id=business_id,
            source_profile_id=source_profile_id,
            source=source,
            scrape_run_id=scrape_run_id,
            now=now,
        )

    async def _finalize_scrape_run(
        self,
        *,
        scrape_runs_collection,
        scrape_run_id: str,
        now: datetime,
        status: str,
        metrics: dict[str, Any],
        dataset_id: str | None = None,
    ) -> None:
        await self._business_scrape_run_store.finalize_scrape_run(
            scrape_runs_collection=scrape_runs_collection,
            scrape_run_id=scrape_run_id,
            now=now,
            status=status,
            metrics=metrics,
            dataset_id=dataset_id,
        )
