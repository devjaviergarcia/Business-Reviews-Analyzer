from __future__ import annotations

from typing import TYPE_CHECKING

from src.config import settings
from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.scraping_google_maps import GoogleMapsScraper
from src.scraping_tripadvisor import TripadvisorScraper
from src.services.analyze_business_use_case import AnalyzeBusinessUseCase
from src.services.reanalyze_use_case import ReanalyzeUseCase
from src.workers.contracts import AnalysisJobStatus, AnalyzeBusinessTaskPayload, parse_analyze_business_payload

if TYPE_CHECKING:
    from src.business_catalog.enqueue_browser_scrape_jobs_use_case import EnqueueBrowserScrapeJobsUseCase
    from src.business_catalog.relaunch_browser_scrape_job_use_case import RelaunchBrowserScrapeJobUseCase


class BusinessServiceConstructionFacet:

    def _reset_browser_scrape_job_use_cases(self) -> None:
        self._enqueue_browser_scrape_jobs_use_case: EnqueueBrowserScrapeJobsUseCase | None = None
        self._relaunch_browser_scrape_job_use_case: RelaunchBrowserScrapeJobUseCase | None = None

    @property

    def enqueue_browser_scrape_jobs_use_case(self) -> "EnqueueBrowserScrapeJobsUseCase | None":
        return self._enqueue_browser_scrape_jobs_use_case

    @property

    def relaunch_browser_scrape_job_use_case(self) -> "RelaunchBrowserScrapeJobUseCase | None":
        return self._relaunch_browser_scrape_job_use_case

    def attach_browser_scrape_job_use_cases(
        self,
        *,
        enqueue_browser_scrape_jobs_use_case: "EnqueueBrowserScrapeJobsUseCase",
        relaunch_browser_scrape_job_use_case: "RelaunchBrowserScrapeJobUseCase",
    ) -> "BusinessService":
        self._enqueue_browser_scrape_jobs_use_case = enqueue_browser_scrape_jobs_use_case
        self._relaunch_browser_scrape_job_use_case = relaunch_browser_scrape_job_use_case
        return self

    def _build_analyze_use_case(self) -> AnalyzeBusinessUseCase:
        return AnalyzeBusinessUseCase(
            preprocessor=self.preprocessor,
            llm_analyzer=self.llm_analyzer,
            validate_business_name=self._validate_business_name,
            resolve_reviews_strategy=self._resolve_reviews_strategy,
            normalize_text=self._normalize_text,
            emit_progress=self._emit_progress,
            build_cached_response=self._build_cached_response,
            scrape_business_page=self._scrape_business_page,
            normalize_scraped_review=self._normalize_scraped_review,
            upsert_reviews=self._upsert_reviews,
            sanitize_response_payload=self._sanitize_response_payload,
            build_advanced_report=self._build_advanced_report,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
        )

    def _build_reanalyze_use_case(self) -> ReanalyzeUseCase:
        return ReanalyzeUseCase(
            preprocessor=self.preprocessor,
            llm_analyzer=self.llm_analyzer,
            parse_object_id=self._parse_object_id,
            resolve_reanalysis_batchers=self._resolve_reanalysis_batchers,
            normalize_stored_review=self._normalize_stored_review,
            serialize_review_doc=self._serialize_review_doc,
            build_reanalysis_batches=self._build_reanalysis_batches,
            analysis_quality_score=self._analysis_quality_score,
            merge_reanalysis_runs=self._merge_reanalysis_runs,
            sanitize_response_payload=self._sanitize_response_payload,
            build_advanced_report=self._build_advanced_report,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
        )

    async def _build_advanced_report(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        reviews: list[dict[str, Any]],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        database = get_database()
        return await self.report_builder.build(
            business_id=str(business_id),
            business_name=str(business_name or "").strip(),
            listing=listing if isinstance(listing, dict) else {},
            stats=stats if isinstance(stats, dict) else {},
            reviews=reviews if isinstance(reviews, list) else [],
            analysis_payload=analysis_payload if isinstance(analysis_payload, dict) else {},
            businesses_collection=database[self._BUSINESSES_COLLECTION],
            analyses_collection=database[self._ANALYSES_COLLECTION],
        )

    @classmethod

    def build_default_scraper(cls) -> GoogleMapsScraper:
        default_strategy = "scroll_copy"
        return GoogleMapsScraper(
            headless=settings.scraper_headless,
            incognito=settings.scraper_incognito,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir=settings.scraper_user_data_dir,
            browser_channel=settings.scraper_browser_channel,
            maps_url=settings.scraper_maps_url,
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
            stealth_mode=settings.scraper_stealth_mode,
            harden_headless=settings.scraper_harden_headless,
            extra_chromium_args=settings.scraper_extra_chromium_args,
            reviews_strategy=default_strategy,
        )

    @classmethod

    def build_geo_grid_scraper(cls) -> GoogleMapsScraper:
        """GeoGrid scraper isolated from the shared persistent Chrome profile.

        GeoGrid does not need session persistence. Running it in incognito mode
        prevents profile lock collisions with regular Google Maps scraping or
        any visible browser session already using `playwright-data`.
        """
        return GoogleMapsScraper(
            headless=settings.scraper_headless,
            incognito=True,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir=settings.scraper_user_data_dir,
            browser_channel=settings.scraper_browser_channel,
            maps_url=settings.scraper_maps_url,
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
            stealth_mode=settings.scraper_stealth_mode,
            harden_headless=settings.scraper_harden_headless,
            extra_chromium_args=settings.scraper_extra_chromium_args,
            reviews_strategy="scroll_copy",
        )

    @classmethod

    def build_default_tripadvisor_scraper(cls) -> TripadvisorScraper:
        return TripadvisorScraper(
            headless=settings.scraper_headless,
            incognito=settings.scraper_incognito,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir=settings.scraper_tripadvisor_user_data_dir,
            browser_channel=settings.scraper_browser_channel,
            tripadvisor_url="https://www.tripadvisor.es",
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
            stealth_mode=settings.scraper_stealth_mode,
            harden_headless=settings.scraper_harden_headless,
            extra_chromium_args=settings.scraper_extra_chromium_args,
        )
