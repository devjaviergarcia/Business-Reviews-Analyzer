from __future__ import annotations

from typing import Any

from src.scraping_google_maps import GoogleMapsScraper
from src.workers.contracts import BenchmarkLocalStudyTaskPayload, CRMLeadDiscoveryTaskPayload


class CRMServiceDiscoveryFacet:

    async def enqueue_lead_discovery_job(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
    ) -> dict[str, Any]:
        if self._enqueue_crm_lead_discovery_job_use_case is not None:
            return await self._enqueue_crm_lead_discovery_job_use_case.execute(
                query=query,
                city=city,
                category=category,
                limit=limit,
                source=source,
            )
        if self._use_discovery_v2:
            await self.ensure_indexes()
        return await self._lead_job_enqueue_runtime.enqueue_lead_discovery_job(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=source,
        )

    async def list_discovery_runs(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._list_crm_discovery_runs_use_case is not None:
            return await self._list_crm_discovery_runs_use_case.execute(page=page, page_size=page_size)
        await self.ensure_indexes()
        payload = await self._discovery_run_repository.list_runs(page=page, page_size=page_size)
        return self._sanitize_payload(payload)

    async def get_discovery_run(self, *, discovery_run_id: str) -> dict[str, Any]:
        if self._get_crm_discovery_run_use_case is not None:
            return await self._get_crm_discovery_run_use_case.execute(discovery_run_id=discovery_run_id)
        await self.ensure_indexes()
        run_doc = await self._discovery_run_repository.get_run(run_id=discovery_run_id)
        if run_doc is None:
            raise LookupError(f"Discovery run '{discovery_run_id}' not found.")
        return self._sanitize_payload(run_doc)

    async def process_discovery_task(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_crm_lead_discovery_task_use_case is not None:
            return await self._process_crm_lead_discovery_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        return self._sanitize_payload(
            await self._discovery_processing_runtime.process_discovery_task(
                task_payload=task_payload,
                job_id=job_id,
            )
        )

    async def _discover_candidates(self, *, task_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        return await self._discovery_processing_runtime.discover_candidates(task_payload=task_payload)

    async def _discover_candidates_for_orchestrator(
        self,
        task_payload: CRMLeadDiscoveryTaskPayload,
    ) -> list[dict[str, Any]]:
        return await self._discovery_processing_runtime.discover_candidates(task_payload=task_payload)

    async def _discover_benchmark_candidates_for_orchestrator(
        self,
        task_payload: BenchmarkLocalStudyTaskPayload,
    ) -> list[dict[str, Any]]:
        return await self._discovery_processing_runtime.discover_benchmark_candidates(
            task_payload=task_payload,
        )

    async def _discover_candidates_from_stored_sources(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        normalized_city: str | None,
        normalized_category: str | None,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        return await self._stored_lead_discovery_reader.discover_candidates_from_stored_sources(
            task_payload=task_payload,
            normalized_query=normalized_query,
            normalized_city=normalized_city,
            normalized_category=normalized_category,
            safe_limit=safe_limit,
        )

    async def _discover_candidates_live_google_maps(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        return await self._google_maps_live_discovery_runtime.discover_candidates_live_google_maps(
            task_payload=task_payload,
            normalized_query=normalized_query,
            safe_limit=safe_limit,
        )

    async def _wait_for_results_feed(self, *, scraper: GoogleMapsScraper, timeout_ms: int = 15_000) -> bool:
        return await self._google_maps_live_discovery_runtime.wait_for_results_feed(
            scraper=scraper,
            timeout_ms=timeout_ms,
        )

    async def _first_visible_from_patterns(
        self,
        *,
        scraper: GoogleMapsScraper,
        key: str,
        timeout_ms: int = 1_200,
    ) -> Any | None:
        return await self._google_maps_live_discovery_runtime.first_visible_from_patterns(
            scraper=scraper,
            key=key,
            timeout_ms=timeout_ms,
        )

    async def _search_google_maps_query(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        await self._google_maps_live_discovery_runtime.search_google_maps_query(
            scraper=scraper,
            query=query,
        )

    async def _search_google_maps_query_from_current_view(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        await self._google_maps_live_discovery_runtime.search_google_maps_query_from_current_view(
            scraper=scraper,
            query=query,
        )

    async def _discover_geo_grid_point_results(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
    ) -> list[dict[str, Any]]:
        return await self._google_maps_geo_grid_runtime.discover_geo_grid_point_results(
            scraper=scraper,
            keyword=keyword,
            point=point,
            top_n=top_n,
        )

    async def _discover_geo_grid_point_results_uule(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
        radius_m: int,
        throttle_ms: int,
    ) -> list[dict[str, Any]]:
        return await self._google_maps_geo_grid_runtime.discover_geo_grid_point_results_uule(
            scraper=scraper,
            keyword=keyword,
            point=point,
            top_n=top_n,
            radius_m=radius_m,
            throttle_ms=throttle_ms,
        )

    async def _extract_geo_grid_single_listing_result(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point_order: int,
        point_label: str,
        lat: float,
        lng: float,
    ) -> dict[str, Any] | None:
        return await self._google_maps_geo_grid_runtime.extract_geo_grid_single_listing_result(
            scraper=scraper,
            keyword=keyword,
            point_order=point_order,
            point_label=point_label,
            lat=lat,
            lng=lng,
        )

    async def _safe_listing_text(self, *, scraper: GoogleMapsScraper, key: str) -> str | None:
        return await self._google_maps_geo_grid_runtime.safe_listing_text(scraper=scraper, key=key)

    async def _read_results_feed_metrics(self, *, scraper: GoogleMapsScraper) -> dict[str, Any]:
        return await self._google_maps_live_discovery_runtime.read_results_feed_metrics(scraper=scraper)

    async def _wait_for_results_feed_growth(
        self,
        *,
        scraper: GoogleMapsScraper,
        min_wait_ms: int,
        max_wait_ms: int,
    ) -> bool:
        return await self._google_maps_live_discovery_runtime.wait_for_results_feed_growth(
            scraper=scraper,
            min_wait_ms=min_wait_ms,
            max_wait_ms=max_wait_ms,
        )

    async def _collect_visible_google_maps_results(self, *, scraper: GoogleMapsScraper) -> list[dict[str, Any]]:
        return await self._google_maps_live_discovery_runtime.collect_visible_google_maps_results(
            scraper=scraper,
        )

    async def _scroll_google_maps_results(self, *, scraper: GoogleMapsScraper) -> None:
        await self._google_maps_live_discovery_runtime.scroll_google_maps_results(scraper=scraper)

    async def _enrich_live_google_maps_candidates(
        self,
        *,
        scraper: GoogleMapsScraper,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return await self._google_maps_live_discovery_runtime.enrich_live_google_maps_candidates(
            scraper=scraper,
            candidates=candidates,
        )

    async def _enrich_live_google_maps_candidate(
        self,
        *,
        detail_scraper: GoogleMapsScraper,
        candidate: dict[str, Any],
        timeout_ms: int = 11_000,
    ) -> dict[str, Any]:
        return await self._google_maps_live_discovery_runtime.enrich_live_google_maps_candidate(
            detail_scraper=detail_scraper,
            candidate=candidate,
            timeout_ms=timeout_ms,
        )

    async def _extract_listing_fallback_from_dom(self, *, detail_scraper: GoogleMapsScraper) -> dict[str, Any]:
        return await self._google_maps_live_discovery_runtime.extract_listing_fallback_from_dom(
            detail_scraper=detail_scraper,
        )

    async def _upsert_lead_candidate(self, candidate: dict[str, Any]) -> str:
        return await self._legacy_lead_registry_runtime.upsert_lead_candidate(candidate)
