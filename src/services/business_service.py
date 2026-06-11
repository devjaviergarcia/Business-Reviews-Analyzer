from __future__ import annotations

from pathlib import Path

from src.browser_runtime.local_browser_worker_registry import LocalBrowserWorkerRegistry
from src.business_catalog import (
    BrowserJobControlRuntime,
    BrowserScrapeRoundRuntime,
    BusinessArtifactRuntime,
    BusinessCleanupRuntime,
    BusinessCommonRuntime,
    BusinessJobRuntime,
    BusinessSummaryRuntime,
    ReanalysisSupportRuntime,
    TripadvisorAntibotJobRuntime,
    TripadvisorLiveCaptureRuntime,
)
from src.business_catalog.business_scrape_pipeline_runner import BusinessScrapePipelineRunner
from src.business_catalog.business_scrape_run_store import BusinessScrapeRunStore
from src.business_catalog.business_source_persistence import BusinessSourcePersistence
from src.business_catalog.legacy_review_dataset_packager import LegacyReviewDatasetPackager
from src.config import settings
from src.database import get_database
from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.scraping_google_maps import GoogleMapsBusinessPageScraper, GoogleMapsScraper
from src.scraping_shared.browser_scrape_errors import ScrapeBotDetectedError
from src.scraping_tripadvisor import (
    TripadvisorBusinessPageScraper,
    TripadvisorScrapeDiagnostics,
    TripadvisorScraper,
)
from src.services.analysis_job_service import AnalysisJobService
from src.services.analyze_business_use_case import AnalyzeBusinessUseCase
from src.services.business_query_service import BusinessQueryService
from src.services.business_service_facets import (
    BusinessServiceAnalysisFacet,
    BusinessServiceConstructionFacet,
    BusinessServiceJobsFacet,
    BusinessServiceQueryFacet,
    BusinessServiceScrapingFacet,
    BusinessServiceSummaryFacet,
)
from src.services.tripadvisor_local_worker_control_service import TripadvisorLocalWorkerControlService
from src.services.reanalyze_use_case import ReanalyzeUseCase


class BusinessService(
    BusinessServiceConstructionFacet,
    BusinessServiceAnalysisFacet,
    BusinessServiceQueryFacet,
    BusinessServiceJobsFacet,
    BusinessServiceScrapingFacet,
    BusinessServiceSummaryFacet,
):
    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _COMMENTS_COLLECTION = "comments"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"
    _SOURCE_PROFILES_COLLECTION = "source_profiles"
    _DATASETS_COLLECTION = "datasets"
    _SCRAPE_RUNS_COLLECTION = "scrape_runs"
    _SCRAPE_DIAGNOSTICS_COLLECTION = "scrape_diagnostics"
    _ANTI_BOT_KEYWORDS = (
        "bot",
        "captcha",
        "robot",
        "verify you are human",
        "verifica que eres humano",
        "tráfico inusual",
        "unusual traffic",
        "security check",
        "automated access",
        "access denied",
        "forbidden",
        "blocked",
        "challenge",
        "not a robot",
        "no soy un robot",
    )
    _ANTI_BOT_STRONG_KEYWORDS = (
        "verify you are human",
        "verifica que eres humano",
        "tráfico inusual",
        "unusual traffic",
        "security check",
        "automated access",
        "access denied",
        "forbidden",
        "blocked",
        "not a robot",
        "no soy un robot",
    )
    _ANTI_BOT_CAPTCHA_COMPANION_KEYWORDS = (
        "challenge",
        "verify",
        "human",
        "security",
        "blocked",
        "denied",
        "tráfico inusual",
        "unusual traffic",
    )
    _ANTI_BOT_ROBOT_MARKERS = (
        "captcha__robot",
        "data-dd-captcha-robot",
        "no a un robot",
        "not a robot",
        "no soy un robot",
        "i am not a robot",
    )
    _ANTI_BOT_DATADOME_STRUCTURAL_MARKERS = (
        "ddv1-captcha-container",
        "captcha__frame",
        "captcha__human",
        "data-dd-captcha-human",
        "slidercontainer",
        "slidertext",
        "slidertarget",
        "slidermask",
        "captcha__puzzle",
        "captcha__audio",
    )
    _SUPPORTED_REANALYZE_BATCHERS = {
        "latest_text",
        "balanced_rating",
        "low_rating_focus",
        "high_rating_focus",
    }
    _SUPPORTED_REVIEW_STRATEGIES = {
        "interactive",
        "scroll_copy",
    }
    _SUPPORTED_FORCE_MODES = {
        "fallback_existing",
        "strict_rescrape",
    }
    _SCRAPE_SOURCES = ("google_maps", "tripadvisor")
    _PRIMARY_SOURCE = "google_maps"
    _ACTIVE_JOB_STATUSES = {"running", "retrying", "partial"}
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    _ARTIFACTS_ROOT = (_PROJECT_ROOT / "artifacts").resolve()

    def __init__(
        self,
        *,
        scraper: GoogleMapsScraper | None = None,
        tripadvisor_scraper: TripadvisorScraper | None = None,
        preprocessor: ReviewPreprocessor | None = None,
        llm_analyzer: ReviewLLMAnalyzer | None = None,
        report_builder: AdvancedBusinessReportBuilder | None = None,
        job_service: AnalysisJobService | None = None,
        query_service: BusinessQueryService | None = None,
        analyze_use_case: AnalyzeBusinessUseCase | None = None,
        reanalyze_use_case: ReanalyzeUseCase | None = None,
        local_browser_worker_registry: LocalBrowserWorkerRegistry | None = None,
        tripadvisor_live_session_launcher: TripadvisorLocalWorkerControlService | None = None,
    ) -> None:
        self._configure_business_foundation(
            scraper=scraper,
            tripadvisor_scraper=tripadvisor_scraper,
            preprocessor=preprocessor,
            llm_analyzer=llm_analyzer,
            report_builder=report_builder,
            job_service=job_service,
            query_service=query_service,
            local_browser_worker_registry=local_browser_worker_registry,
            tripadvisor_live_session_launcher=tripadvisor_live_session_launcher,
        )
        self._configure_business_analysis_context(
            analyze_use_case=analyze_use_case,
            reanalyze_use_case=reanalyze_use_case,
        )
        self._configure_business_pipeline_context()
        self._configure_business_operation_runtimes()
        self._reset_browser_scrape_job_use_cases()

    def _configure_business_foundation(
        self,
        *,
        scraper: GoogleMapsScraper | None,
        tripadvisor_scraper: TripadvisorScraper | None,
        preprocessor: ReviewPreprocessor | None,
        llm_analyzer: ReviewLLMAnalyzer | None,
        report_builder: AdvancedBusinessReportBuilder | None,
        job_service: AnalysisJobService | None,
        query_service: BusinessQueryService | None,
        local_browser_worker_registry: LocalBrowserWorkerRegistry | None,
        tripadvisor_live_session_launcher: TripadvisorLocalWorkerControlService | None,
    ) -> None:
        self.scraper = scraper or type(self).build_default_scraper()
        self.tripadvisor_scraper = tripadvisor_scraper or type(self).build_default_tripadvisor_scraper()
        self.preprocessor = preprocessor or ReviewPreprocessor()
        self.llm_analyzer = llm_analyzer or ReviewLLMAnalyzer()
        self.report_builder = report_builder or AdvancedBusinessReportBuilder()
        self.job_service = job_service or AnalysisJobService()
        self.query_service = query_service or BusinessQueryService()
        self.local_browser_worker_registry = local_browser_worker_registry or LocalBrowserWorkerRegistry()
        self.tripadvisor_live_session_launcher = tripadvisor_live_session_launcher

    def _configure_business_analysis_context(
        self,
        *,
        analyze_use_case: AnalyzeBusinessUseCase | None,
        reanalyze_use_case: ReanalyzeUseCase | None,
    ) -> None:
        self._business_common_runtime = BusinessCommonRuntime(
            supported_review_strategies=self._SUPPORTED_REVIEW_STRATEGIES,
            supported_force_modes=self._SUPPORTED_FORCE_MODES,
            scrape_sources=self._SCRAPE_SOURCES,
        )
        self._reanalysis_support_runtime = ReanalysisSupportRuntime(
            normalize_text=self._normalize_text,
            supported_batchers=self._SUPPORTED_REANALYZE_BATCHERS,
        )
        self.analyze_use_case = analyze_use_case or self._build_analyze_use_case()
        self.reanalyze_use_case = reanalyze_use_case or self._build_reanalyze_use_case()
        self._legacy_review_dataset_packager = LegacyReviewDatasetPackager(
            parse_object_id=self._parse_object_id,
        )
        self._business_source_persistence = BusinessSourcePersistence(
            build_review_fingerprint=self._review_fingerprint,
        )
        self._tripadvisor_scrape_diagnostics = TripadvisorScrapeDiagnostics(
            diagnostics_collection_name=self._SCRAPE_DIAGNOSTICS_COLLECTION,
            anti_bot_keywords=self._ANTI_BOT_KEYWORDS,
            anti_bot_strong_keywords=self._ANTI_BOT_STRONG_KEYWORDS,
            anti_bot_captcha_companion_keywords=self._ANTI_BOT_CAPTCHA_COMPANION_KEYWORDS,
            anti_bot_robot_markers=self._ANTI_BOT_ROBOT_MARKERS,
            anti_bot_datadome_structural_markers=self._ANTI_BOT_DATADOME_STRUCTURAL_MARKERS,
        )
        self._google_maps_business_page_scraper = GoogleMapsBusinessPageScraper(
            scraper=self.scraper,
            emit_progress=self._emit_progress,
            resolve_optional_int_override=self._resolve_optional_int_override,
        )
        self._tripadvisor_business_page_scraper = TripadvisorBusinessPageScraper(
            scraper=self.tripadvisor_scraper,
            emit_progress=self._emit_progress,
            diagnostics=self._tripadvisor_scrape_diagnostics,
        )

    def _configure_business_pipeline_context(self) -> None:
        self._business_scrape_run_store = BusinessScrapeRunStore(
            parse_object_id=self._parse_object_id,
        )
        self._business_scrape_pipeline_runner = BusinessScrapePipelineRunner(
            validate_business_name=self._validate_business_name,
            resolve_reviews_strategy=self._resolve_reviews_strategy,
            resolve_force_mode=self._resolve_force_mode,
            resolve_scrape_sources=self._resolve_scrape_sources,
            resolve_optional_int_override=self._resolve_optional_int_override,
            resolve_optional_float_override=self._resolve_optional_float_override,
            normalize_text=self._normalize_text,
            parse_object_id=self._parse_object_id,
            emit_progress=self._emit_progress,
            sanitize_response_payload=self._sanitize_response_payload,
            normalize_scraped_review=self._normalize_scraped_review,
            build_source_progress_callback=self._build_source_progress_callback,
            scrape_google_maps_business_page=self._scrape_business_page,
            scrape_tripadvisor_business_page=self._scrape_tripadvisor_business_page,
            get_or_create_source_profile=self._get_or_create_source_profile,
            package_legacy_reviews_into_dataset=self._package_legacy_reviews_into_dataset,
            create_scrape_run=self._create_scrape_run,
            create_dataset_snapshot=self._create_dataset_snapshot,
            upsert_reviews=self._upsert_reviews,
            upsert_job_comments=self._upsert_job_comments,
            finalize_scrape_run=self._finalize_scrape_run,
            preprocessor=self.preprocessor,
            primary_source=self._PRIMARY_SOURCE,
            scrape_sources=self._SCRAPE_SOURCES,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            comments_collection_name=self._COMMENTS_COLLECTION,
            source_profiles_collection_name=self._SOURCE_PROFILES_COLLECTION,
            datasets_collection_name=self._DATASETS_COLLECTION,
            scrape_runs_collection_name=self._SCRAPE_RUNS_COLLECTION,
            scrape_bot_detected_error_type=ScrapeBotDetectedError,
        )

    def _configure_business_operation_runtimes(self) -> None:
        self._browser_scrape_round_runtime = BrowserScrapeRoundRuntime(
            database_factory=lambda: get_database(),
            job_service=self.job_service,
        )
        self._browser_job_control_runtime = BrowserJobControlRuntime(
            database_factory=lambda: get_database(),
            job_service=self.job_service,
            open_browser_scrape_round=self._open_browser_scrape_round,
            register_browser_scrape_round_source_job=self._register_browser_scrape_round_source_job,
            local_browser_worker_registry=self.local_browser_worker_registry,
            validate_business_name=self._validate_business_name,
            normalize_text=self._normalize_text,
            resolve_reviews_strategy=self._resolve_reviews_strategy,
            resolve_force_mode=self._resolve_force_mode,
            resolve_scrape_sources=self._resolve_scrape_sources,
            parse_object_id=self._parse_object_id,
            sanitize_response_payload=self._sanitize_response_payload,
            ensure_job_is_scrape=self._ensure_job_is_scrape,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            active_job_statuses=self._ACTIVE_JOB_STATUSES,
            launch_tripadvisor_live_session=(
                self.tripadvisor_live_session_launcher.launch_live_session
                if self.tripadvisor_live_session_launcher is not None
                else None
            ),
        )
        self._business_cleanup_runtime = BusinessCleanupRuntime(
            database_factory=lambda: get_database(),
            job_service=self.job_service,
            parse_object_id=self._parse_object_id,
            sanitize_response_payload=self._sanitize_response_payload,
            build_related_business_jobs_query=self._build_related_business_jobs_query,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            comments_collection_name=self._COMMENTS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
            source_profiles_collection_name=self._SOURCE_PROFILES_COLLECTION,
            datasets_collection_name=self._DATASETS_COLLECTION,
            scrape_runs_collection_name=self._SCRAPE_RUNS_COLLECTION,
            jobs_collection_name=self._JOBS_COLLECTION,
        )
        self._tripadvisor_live_capture_runtime = TripadvisorLiveCaptureRuntime(
            job_service=self.job_service,
            parse_object_id=self._parse_object_id,
            validate_business_name=self._validate_business_name,
            sanitize_response_payload=self._sanitize_response_payload,
            ensure_job_is_scrape=self._ensure_job_is_scrape,
            scrape_business_for_analysis_pipeline=self.scrape_business_for_analysis_pipeline,
            handoff_completed_scrape_to_analysis=self.handoff_completed_scrape_to_analysis,
        )
        self._tripadvisor_antibot_job_runtime = TripadvisorAntibotJobRuntime(
            database_factory=lambda: get_database(),
            jobs_collection_name=self._JOBS_COLLECTION,
            active_job_statuses=self._ACTIVE_JOB_STATUSES,
            sanitize_response_payload=self._sanitize_response_payload,
            ensure_tripadvisor_session_available_for_relaunch=self._ensure_tripadvisor_session_available_for_relaunch,
            job_service=self.job_service,
        )
        self._business_summary_runtime = BusinessSummaryRuntime(
            sanitize_response_payload=self._sanitize_response_payload,
        )
        self._business_artifact_runtime = BusinessArtifactRuntime(
            project_root=self._PROJECT_ROOT,
            artifacts_root=self._ARTIFACTS_ROOT,
        )
        self._business_job_runtime = BusinessJobRuntime(
            database_factory=lambda: get_database(),
            parse_object_id=self._parse_object_id,
            job_service=self.job_service,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
        )
