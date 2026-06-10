from __future__ import annotations

from pathlib import Path
from typing import Any


class BusinessServiceJobsFacet:

    async def enqueue_business_scrape_jobs(
        self,
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
    ) -> dict:
        if self._enqueue_browser_scrape_jobs_use_case is not None:
            return await self._enqueue_browser_scrape_jobs_use_case.execute(
                name=name,
                force=force,
                strategy=strategy,
                force_mode=force_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                tripadvisor_max_pages=tripadvisor_max_pages,
                tripadvisor_pages_percent=tripadvisor_pages_percent,
                sources=sources,
                google_maps_name=google_maps_name,
                tripadvisor_name=tripadvisor_name,
                execution_mode=execution_mode,
                requested_by=requested_by,
            )
        return await self._browser_job_control_runtime.enqueue_business_scrape_jobs(
            name=name,
            force=force,
            strategy=strategy,
            force_mode=force_mode,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            tripadvisor_max_pages=tripadvisor_max_pages,
            tripadvisor_pages_percent=tripadvisor_pages_percent,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
            execution_mode=execution_mode,
            requested_by=requested_by,
        )

    async def _ensure_root_business_on_enqueue(
        self,
        *,
        canonical_name: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        return await self._browser_job_control_runtime.ensure_root_business_on_enqueue(
            canonical_name=canonical_name,
            canonical_name_normalized=canonical_name_normalized,
        )

    async def _ensure_tripadvisor_worker_started_on_enqueue(
        self,
        *,
        selected_sources: tuple[str, ...],
    ) -> None:
        await self._browser_job_control_runtime.ensure_tripadvisor_worker_started_on_enqueue(
            selected_sources=selected_sources,
        )

    async def enqueue_business_analysis_generate_job(
        self,
        *,
        business_id: str,
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
        source_job_id: str | None = None,
        source_mode: str | None = None,
        selected_source: str | None = None,
    ) -> dict[str, Any]:
        return await self._business_job_runtime.enqueue_business_analysis_generate_job(
            business_id=business_id,
            dataset_id=dataset_id,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
            source_job_id=source_job_id,
            source_mode=source_mode,
            selected_source=selected_source,
        )

    async def get_scrape_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(job_payload)
        return job_payload

    async def commit_tripadvisor_live_capture(
        self,
        *,
        job_id: str,
        listing: dict[str, Any],
        reviews: list[dict[str, Any]],
        commit_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._tripadvisor_live_capture_runtime.commit_live_capture(
            job_id=job_id,
            listing=listing,
            reviews=reviews,
            commit_reason=commit_reason,
            metadata=metadata,
        )

    async def get_analysis_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(job_payload)
        return job_payload

    async def get_report_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(job_payload)
        return job_payload

    async def list_scrape_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_filter: str | None = None,
    ) -> dict:
        return await self.job_service.list_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            queue_names=["scrape_google_maps", "scrape_tripadvisor", "scrape"],
            job_type_filter="business_analyze",
        )

    async def list_scrape_job_comments(
        self,
        *,
        job_id: str,
        source: str | None = None,
        scrape_type: str | None = None,
        page: int = 1,
        page_size: int = 50,
        rating_gte: float | None = None,
        rating_lte: float | None = None,
        order: str = "desc-date",
    ) -> dict[str, Any]:
        await self.get_scrape_job(job_id=job_id)
        return await self.query_service.list_job_comments(
            job_id=job_id,
            source=source,
            scrape_type=scrape_type,
            page=page,
            page_size=page_size,
            rating_gte=rating_gte,
            rating_lte=rating_lte,
            order=order,
        )

    async def list_tripadvisor_antibot_jobs(
        self,
        *,
        limit: int = 20,
        status_filter: str = "failed_or_needs_human",
    ) -> dict[str, Any]:
        return await self._tripadvisor_antibot_job_runtime.list_jobs(
            limit=limit,
            status_filter=status_filter,
        )

    async def relaunch_tripadvisor_antibot_jobs(
        self,
        *,
        limit: int = 20,
        reason: str | None = None,
        status_filter: str = "failed_or_needs_human",
    ) -> dict[str, Any]:
        return await self._tripadvisor_antibot_job_runtime.relaunch_jobs(
            limit=limit,
            reason=reason,
            status_filter=status_filter,
        )

    async def list_analysis_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_filter: str | None = None,
    ) -> dict:
        return await self.job_service.list_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            queue_names=["analysis"],
            job_type_filter="analysis_generate",
        )

    async def list_report_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_filter: str | None = None,
    ) -> dict:
        return await self.job_service.list_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            queue_names=["report"],
            job_type_filter="report_generate",
        )

    def resolve_report_artifact_path(self, *, path: str) -> Path:
        return self._business_artifact_runtime.resolve_report_artifact_path(path=path)

    async def delete_business(
        self,
        *,
        business_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
        delete_related_jobs: bool = True,
    ) -> dict[str, Any]:
        return await self._business_cleanup_runtime.delete_business(
            business_id=business_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
            delete_related_jobs=delete_related_jobs,
        )

    async def delete_scrape_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    def _build_related_business_jobs_query(
        self,
        *,
        business_id: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        return self._browser_job_control_runtime.build_related_business_jobs_query(
            business_id=business_id,
            canonical_name_normalized=canonical_name_normalized,
        )

    async def delete_analysis_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    async def delete_report_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    async def relaunch_scrape_job(
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
        if self._relaunch_browser_scrape_job_use_case is not None:
            return await self._relaunch_browser_scrape_job_use_case.execute(
                job_id=job_id,
                reason=reason,
                force=force,
                restart_from_zero=restart_from_zero,
                google_maps_name=google_maps_name,
                tripadvisor_name=tripadvisor_name,
                execution_mode=execution_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                tripadvisor_max_pages=tripadvisor_max_pages,
                tripadvisor_pages_percent=tripadvisor_pages_percent,
            )
        return await self._browser_job_control_runtime.relaunch_scrape_job(
            job_id=job_id,
            reason=reason,
            force=force,
            restart_from_zero=restart_from_zero,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
            execution_mode=execution_mode,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            tripadvisor_max_pages=tripadvisor_max_pages,
            tripadvisor_pages_percent=tripadvisor_pages_percent,
            ensure_tripadvisor_session_available_for_relaunch=self._ensure_tripadvisor_session_available_for_relaunch,
        )

    async def relaunch_analysis_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(existing)
        if restart_from_zero:
            raise ValueError("restart_from_zero is supported only for scrape jobs.")
        return await self.job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force),
            restart_from_zero=False,
        )

    async def relaunch_report_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(existing)
        if restart_from_zero:
            raise ValueError("restart_from_zero is not supported for report jobs.")
        return await self.job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force),
            restart_from_zero=False,
        )

    async def stop_business_scrape_job(
        self,
        *,
        job_id: str,
        continue_analysis_if_google: bool = True,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
    ) -> dict[str, Any]:
        return await self._browser_job_control_runtime.stop_business_scrape_job(
            job_id=job_id,
            continue_analysis_if_google=continue_analysis_if_google,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
        )

    def _ensure_job_is_scrape(self, job_payload: dict[str, Any]) -> None:
        self._business_job_runtime.ensure_job_is_scrape(job_payload)

    def _ensure_job_is_analysis(self, job_payload: dict[str, Any]) -> None:
        self._business_job_runtime.ensure_job_is_analysis(job_payload)

    def _ensure_job_is_report(self, job_payload: dict[str, Any]) -> None:
        self._business_job_runtime.ensure_job_is_report(job_payload)

    async def _resolve_business_id_for_scrape_job(self, job_payload: dict[str, Any]) -> str | None:
        return await self._browser_job_control_runtime.resolve_business_id_for_scrape_job(job_payload)
