from __future__ import annotations

import asyncio

from src.config import settings
from src.database import get_database
from src.crm.benchmark import build_geo_grid_points
from src.crm.campaigns import CampaignQueryRuntime, CampaignWorkflowRuntime, LegacyCampaignDispatchRuntime
from src.crm.discovery import (
    DiscoveryProcessingRuntime,
    GoogleMapsLiveDiscoveryRuntime,
    StoredLeadDiscoveryReader,
)
from src.crm.leads import LeadJobEnqueueRuntime, LeadPipelineSyncRuntime, LegacyLeadPipelineRuntime, LegacyLeadRegistryRuntime
from src.crm.repositories import (
    CRMRepositoryBootstrap,
    MongoBenchmarkBusinessRepository,
    MongoBenchmarkRunRepository,
    MongoCampaignRepository,
    MongoCompetitorSetRepository,
    MongoDiscoveryRunRepository,
    MongoEventRepository,
    MongoGeoCityRepository,
    MongoGeoGridResultRepository,
    MongoGeoGridRunRepository,
    MongoLeadRepository,
    MongoLeadReportRepository,
    MongoMessageRepository,
    MongoPaidReportRepository,
    MongoSuppressionRepository,
)
from src.crm.report_requests import LegacyReportRequestRuntime
from src.crm.shared import CRMCommonRuntime, CRMStudySupportRuntime, CampaignCadenceRuntime, CampaignDeliveryRuntime
from src.crm.studies import (
    BenchmarkReportRuntime,
    BenchmarkStudyProcessingRuntime,
    GeoGridStatsBuilder,
    GeoGridStudyRuntime,
    GoogleMapsGeoGridRuntime,
    StudyJobEnqueueRuntime,
)
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_service import BusinessService
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.services.crm_service_facets import (
    CRMServiceBindingsFacet,
    CRMServiceCampaignsFacet,
    CRMServiceDiscoveryFacet,
    CRMServiceLeadsFacet,
    CRMServiceReportRequestsFacet,
    CRMServiceSharedSupportFacet,
    CRMServiceStudiesFacet,
)


class CRMService(
    CRMServiceBindingsFacet,
    CRMServiceDiscoveryFacet,
    CRMServiceStudiesFacet,
    CRMServiceReportRequestsFacet,
    CRMServiceLeadsFacet,
    CRMServiceCampaignsFacet,
    CRMServiceSharedSupportFacet,
):
    _LEADS_COLLECTION = "crm_leads"
    _CAMPAIGNS_COLLECTION = "crm_campaigns"
    _CADENCE_COLLECTION = "crm_cadence_templates"
    _MESSAGES_COLLECTION = "crm_messages"
    _EVENTS_COLLECTION = "crm_events"
    _SUPPRESSIONS_COLLECTION = "crm_suppressions"
    _DISCOVERY_RUNS_COLLECTION = "crm_discovery_runs"
    _REPORT_REQUESTS_COLLECTION = "report_requests"
    _REPORT_FEEDBACK_COLLECTION = "report_feedback"
    _RESEARCH_LEADS_COLLECTION = "research_leads"
    _BUSINESSES_COLLECTION = "businesses"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"

    _DEFAULT_CADENCE_KEY = "default_optin_3touch"
    _ALLOWED_SOURCES = ("google_maps", "tripadvisor")
    _LIVE_GOOGLE_DISCOVERY_SOURCES = (
        "live_google_maps",
        "google_maps_live",
        "auto_live_google_maps",
        "live_auto_google_maps",
    )
    _LIVE_GOOGLE_DISCOVERY_ALIASES = ("auto", "all", "")
    _USE_CASE_BINDING_NAMES = (
        "enqueue_crm_lead_discovery_job_use_case",
        "enqueue_crm_lead_pipeline_job_use_case",
        "enqueue_benchmark_study_job_use_case",
        "create_crm_report_request_use_case",
        "create_crm_report_feedback_use_case",
        "generate_crm_lead_report_use_case",
        "generate_crm_paid_report_use_case",
        "generate_crm_public_study_use_case",
        "create_crm_lead_use_case",
        "update_crm_lead_use_case",
        "bulk_delete_crm_leads_use_case",
        "create_crm_campaign_use_case",
        "launch_crm_campaign_use_case",
        "handle_resend_webhook_use_case",
        "list_crm_report_requests_use_case",
        "list_crm_leads_use_case",
        "get_crm_lead_use_case",
        "list_crm_campaigns_use_case",
        "list_crm_messages_use_case",
        "list_crm_events_use_case",
        "list_crm_discovery_runs_use_case",
        "get_crm_discovery_run_use_case",
        "list_crm_geo_cities_use_case",
        "list_crm_geo_grid_runs_use_case",
        "get_crm_geo_grid_run_use_case",
        "list_crm_geo_grid_results_use_case",
        "get_crm_geo_grid_stats_use_case",
        "sync_crm_lead_pipeline_refs_use_case",
        "enqueue_geo_grid_study_job_use_case",
        "process_crm_lead_discovery_task_use_case",
        "process_benchmark_study_task_use_case",
        "process_crm_lead_pipeline_task_use_case",
        "process_geo_grid_study_task_use_case",
        "enqueue_due_campaign_dispatch_jobs_use_case",
        "process_campaign_dispatch_task_use_case",
        "retry_crm_report_request_use_case",
        "process_pending_crm_report_requests_use_case",
    )

    def __init__(
        self,
        *,
        job_service: AnalysisJobService | None = None,
        business_service: BusinessService | None = None,
    ) -> None:
        self.job_service = job_service or AnalysisJobService()
        self.business_service = business_service or BusinessService(job_service=self.job_service)
        self._indexes_ensured = False
        self._indexes_lock = asyncio.Lock()
        self._use_repo_v2 = bool(settings.crm_repo_v2)
        self._use_discovery_v2 = bool(settings.crm_discovery_v2)
        self._configure_crm_repositories()
        self._configure_crm_discovery_context()
        self._configure_crm_study_context()
        self._configure_crm_lead_pipeline_context()
        self._configure_crm_campaign_context()
        self._reset_crm_queue_use_case_bindings()

    def _configure_crm_repositories(self) -> None:
        self._repository_bootstrap = CRMRepositoryBootstrap()
        self._common_runtime = CRMCommonRuntime()
        self._lead_repository = MongoLeadRepository()
        self._event_repository = MongoEventRepository()
        self._campaign_repository = MongoCampaignRepository()
        self._message_repository = MongoMessageRepository()
        self._suppression_repository = MongoSuppressionRepository()
        self._discovery_run_repository = MongoDiscoveryRunRepository()
        self._benchmark_run_repository = MongoBenchmarkRunRepository()
        self._benchmark_business_repository = MongoBenchmarkBusinessRepository()
        self._competitor_set_repository = MongoCompetitorSetRepository()
        self._lead_report_repository = MongoLeadReportRepository()
        self._paid_report_repository = MongoPaidReportRepository()
        self._geo_city_repository = MongoGeoCityRepository()
        self._geo_grid_run_repository = MongoGeoGridRunRepository()
        self._geo_grid_result_repository = MongoGeoGridResultRepository()

    def _configure_crm_discovery_context(self) -> None:
        self._stored_lead_discovery_reader = StoredLeadDiscoveryReader(
            database_factory=lambda: get_database(),
            research_leads_collection_name=self._RESEARCH_LEADS_COLLECTION,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            normalize_text=self._normalize_text,
            extract_city_from_address=self._extract_city_from_address,
        )
        self._google_maps_live_discovery_runtime = GoogleMapsLiveDiscoveryRuntime(
            scraper_factory=lambda: BusinessService.build_default_scraper(),
            normalize_text=self._normalize_text,
            canonicalize_maps_url=self._canonicalize_maps_url,
            parse_rating_text=self._parse_rating_text,
            parse_reviews_count_text=self._parse_reviews_count_text,
            sanitize_listing_categories=self._sanitize_listing_categories,
            extract_city_from_address=self._extract_city_from_address,
            merge_listing_payloads=lambda **kwargs: self._merge_listing_payloads(**kwargs),
            scroll_interval_seconds=lambda: settings.scraper_html_scroll_min_interval_s,
            resolve_first_visible_pattern=lambda **kwargs: self._first_visible_from_patterns(**kwargs),
            search_google_maps_query=lambda **kwargs: self._search_google_maps_query(**kwargs),
            wait_for_results_feed=lambda **kwargs: self._wait_for_results_feed(**kwargs),
            wait_for_results_feed_growth=lambda **kwargs: self._wait_for_results_feed_growth(**kwargs),
            collect_visible_results=lambda **kwargs: self._collect_visible_google_maps_results(**kwargs),
            scroll_results=lambda **kwargs: self._scroll_google_maps_results(**kwargs),
            enrich_candidates=lambda **kwargs: self._enrich_live_google_maps_candidates(**kwargs),
        )
        self._discovery_processing_runtime = DiscoveryProcessingRuntime(
            normalize_text=self._normalize_text,
            discover_candidates_live_google_maps=lambda **kwargs: self._discover_candidates_live_google_maps(**kwargs),
            discover_candidates_from_stored_sources=lambda **kwargs: self._discover_candidates_from_stored_sources(**kwargs),
            upsert_candidate=lambda candidate: self._upsert_lead_candidate(candidate),
            record_event=lambda **kwargs: self._record_event(**kwargs),
            discovery_run_repository=self._discovery_run_repository,
            use_discovery_v2=lambda: self._use_discovery_v2,
            live_google_discovery_sources=self._LIVE_GOOGLE_DISCOVERY_SOURCES,
            live_google_discovery_aliases=self._LIVE_GOOGLE_DISCOVERY_ALIASES,
        )

    def _configure_crm_study_context(self) -> None:
        self._google_maps_geo_grid_runtime = GoogleMapsGeoGridRuntime(
            canonicalize_maps_url=self._canonicalize_maps_url,
            normalize_text=self._normalize_text,
            parse_rating_text=self._parse_rating_text,
            parse_reviews_count_text=self._parse_reviews_count_text,
            search_from_current_view=lambda **kwargs: self._search_google_maps_query_from_current_view(**kwargs),
            wait_for_results_feed=lambda **kwargs: self._wait_for_results_feed(**kwargs),
            collect_visible_results=lambda **kwargs: self._collect_visible_google_maps_results(**kwargs),
            wait_for_results_feed_growth=lambda **kwargs: self._wait_for_results_feed_growth(**kwargs),
            scroll_results=lambda **kwargs: self._scroll_google_maps_results(**kwargs),
            extract_single_listing_result=lambda **kwargs: self._extract_geo_grid_single_listing_result(**kwargs),
            timeout_ms=lambda: settings.scraper_timeout_ms,
            scroll_interval_seconds=lambda: settings.scraper_html_scroll_min_interval_s,
            geo_grid_gl=lambda: settings.geo_grid_uule_gl,
            geo_grid_hl=lambda: settings.geo_grid_uule_hl,
        )
        self._geo_grid_study_runtime = GeoGridStudyRuntime(
            get_geo_grid_run=self._geo_grid_run_repository.get_run,
            get_geo_city_by_slug=self._geo_city_repository.get_by_slug,
            set_geo_grid_run_job_id=self._geo_grid_run_repository.set_job_id,
            mark_geo_grid_run_running=self._geo_grid_run_repository.mark_running,
            update_geo_grid_run_progress=self._geo_grid_run_repository.update_progress,
            finalize_geo_grid_run=self._geo_grid_run_repository.finalize,
            replace_geo_grid_point_results=self._geo_grid_result_repository.replace_point_results,
            discover_geo_grid_point_results=lambda **kwargs: self._discover_geo_grid_point_results(**kwargs),
            discover_geo_grid_point_results_uule=lambda **kwargs: self._discover_geo_grid_point_results_uule(**kwargs),
            scraper_factory=BusinessService.build_geo_grid_scraper,
            record_event=self._record_event,
            sanitize_payload=self._sanitize_payload,
            default_provider_mode=lambda: settings.geo_grid_provider_mode,
            default_grid_size=lambda: settings.geo_grid_uule_grid_size,
            default_grid_spacing_km=lambda: settings.geo_grid_uule_spacing_km,
            default_uule_radius_m=lambda: settings.geo_grid_uule_radius_m,
            default_throttle_ms=lambda: settings.geo_grid_uule_throttle_ms,
            build_geo_grid_points=build_geo_grid_points,
        )
        self._geo_grid_stats_builder = GeoGridStatsBuilder()
        self._benchmark_report_runtime = BenchmarkReportRuntime(
            get_benchmark_business=self._benchmark_business_repository.get_business,
            get_competitor_set_for_business=self._competitor_set_repository.get_for_business,
            select_competitors_for_benchmark_business=self.select_competitors_for_benchmark_business,
            resolve_lead_report_cta=self._resolve_lead_report_cta,
            upsert_lead_report_for_business=self._lead_report_repository.upsert_for_business,
            upsert_paid_report_for_business_month=self._paid_report_repository.upsert_for_business_month,
            get_benchmark_run=self._benchmark_run_repository.get_run,
            list_benchmark_businesses=self._benchmark_business_repository.list_businesses,
            resolve_geo_grid_stats_for_public_study=self._resolve_geo_grid_stats_for_public_study,
            now_utc=self._now_utc,
            record_event=self._record_event,
            sanitize_payload=self._sanitize_payload,
        )
        self._study_job_enqueue_runtime = StudyJobEnqueueRuntime(
            enqueue_job=lambda **kwargs: self.job_service.enqueue_job(**kwargs),
            create_benchmark_run=self._benchmark_run_repository.create_run,
            create_geo_grid_run=self._geo_grid_run_repository.create_run,
            get_geo_city_by_slug=self._geo_city_repository.get_by_slug,
            set_geo_grid_run_job_id=self._geo_grid_run_repository.set_job_id,
            record_event=self._record_event,
            sanitize_payload=self._sanitize_payload,
            live_google_discovery_aliases=self._LIVE_GOOGLE_DISCOVERY_ALIASES,
            default_geo_grid_provider_mode=lambda: settings.geo_grid_provider_mode,
            default_geo_grid_size=lambda: settings.geo_grid_uule_grid_size,
            default_geo_grid_spacing_km=lambda: settings.geo_grid_uule_spacing_km,
            default_uule_radius_m=lambda: settings.geo_grid_uule_radius_m,
            default_throttle_ms=lambda: settings.geo_grid_uule_throttle_ms,
        )
        self._study_support_runtime = CRMStudySupportRuntime(
            list_geo_grid_runs=self._geo_grid_run_repository.list_runs,
            list_geo_grid_results=self._geo_grid_result_repository.list_results,
            build_geo_grid_stats=self._build_geo_grid_stats,
            normalize_text=self._normalize_text,
            get_benchmark_business=self._benchmark_business_repository.get_business,
            list_benchmark_businesses=self._benchmark_business_repository.list_businesses,
            upsert_competitor_set=self._competitor_set_repository.upsert_set,
            record_event=self._record_event,
            sanitize_payload=self._sanitize_payload,
        )
        self._benchmark_study_processing_runtime = BenchmarkStudyProcessingRuntime(
            benchmark_run_repository=self._benchmark_run_repository,
            benchmark_business_repository=self._benchmark_business_repository,
            competitor_set_repository=self._competitor_set_repository,
            discover_candidates=lambda **kwargs: self._discover_benchmark_candidates_for_orchestrator(**kwargs),
            record_event=lambda **kwargs: self._record_event(**kwargs),
        )

    def _configure_crm_lead_pipeline_context(self) -> None:
        self._legacy_report_request_runtime = LegacyReportRequestRuntime(
            database_factory=lambda: get_database(),
            report_requests_collection_name=self._REPORT_REQUESTS_COLLECTION,
            report_feedback_collection_name=self._REPORT_FEEDBACK_COLLECTION,
            leads_collection_name=self._LEADS_COLLECTION,
            now_utc=self._now_utc,
            normalize_text=self._normalize_text,
            normalize_email=self._normalize_email,
            normalize_utm=self._normalize_utm,
            parse_object_id=self._parse_object_id,
            enqueue_report_request_doc=self._enqueue_report_request_doc,
            record_event=self._record_event,
            serialize_mongo_doc=self._serialize_mongo_doc,
            sanitize_payload=self._sanitize_payload,
        )
        self._legacy_lead_pipeline_runtime = LegacyLeadPipelineRuntime(
            database_factory=lambda: get_database(),
            parse_object_id=self._parse_object_id,
            now_utc=self._now_utc,
            sanitize_payload=self._sanitize_payload,
            record_event=self._record_event,
            business_service=self.business_service,
            leads_collection_name=self._LEADS_COLLECTION,
            allowed_sources=self._ALLOWED_SOURCES,
        )
        self._lead_job_enqueue_runtime = LeadJobEnqueueRuntime(
            enqueue_job=lambda **kwargs: self.job_service.enqueue_job(**kwargs),
            create_discovery_run=self._discovery_run_repository.create_run,
            append_discovery_run_step=self._discovery_run_repository.append_step,
            parse_object_id=self._parse_object_id,
            now_utc=self._now_utc,
            record_event=self._record_event,
            sanitize_payload=self._sanitize_payload,
            use_discovery_v2=lambda: self._use_discovery_v2,
            live_google_discovery_sources=self._LIVE_GOOGLE_DISCOVERY_SOURCES,
            live_google_discovery_aliases=self._LIVE_GOOGLE_DISCOVERY_ALIASES,
            leads_collection_name=self._LEADS_COLLECTION,
        )
        self._legacy_lead_registry_runtime = LegacyLeadRegistryRuntime(
            database_factory=lambda: get_database(),
            lead_repository=self._lead_repository,
            leads_collection_name=self._LEADS_COLLECTION,
            now_utc=self._now_utc,
            normalize_text=self._normalize_text,
            normalize_email=self._normalize_email,
            domain_from_email_or_website=self._domain_from_email_or_website,
            parse_object_id=self._parse_object_id,
            serialize_mongo_doc=self._serialize_mongo_doc,
            sanitize_payload=self._sanitize_payload,
            record_event=self._record_event,
            sync_lead_pipeline_refs=self.sync_lead_pipeline_refs,
            upsert_suppression=self._upsert_suppression,
            parse_rating_text=self._parse_rating_text,
            parse_reviews_count_text=self._parse_reviews_count_text,
            build_lead_score=self._build_lead_score,
        )
        self._lead_pipeline_sync_runtime = LeadPipelineSyncRuntime(
            database_factory=lambda: get_database(),
            parse_object_id=self._parse_object_id,
            serialize_mongo_doc=self._serialize_mongo_doc,
            sanitize_payload=self._sanitize_payload,
            now_utc=self._now_utc,
            leads_collection_name=self._LEADS_COLLECTION,
            jobs_collection_name=self._JOBS_COLLECTION,
        )

    def _configure_crm_campaign_context(self) -> None:
        self._campaign_cadence_runtime = CampaignCadenceRuntime(
            database_factory=lambda: get_database(),
            cadence_collection_name=self._CADENCE_COLLECTION,
            default_cadence_key=self._DEFAULT_CADENCE_KEY,
            now_utc=self._now_utc,
        )
        self._campaign_delivery_runtime = CampaignDeliveryRuntime(
            database_factory=lambda: get_database(),
            now_utc=self._now_utc,
            normalize_email=self._normalize_email,
            parse_object_id=self._parse_object_id,
            record_event=self._record_event,
            leads_collection_name=self._LEADS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
            messages_collection_name=self._MESSAGES_COLLECTION,
            suppressions_collection_name=self._SUPPRESSIONS_COLLECTION,
            jobs_collection_name=self._JOBS_COLLECTION,
            events_collection_insert=self._event_repository.insert,
        )
        self._legacy_campaign_dispatch_runtime = LegacyCampaignDispatchRuntime(
            database_factory=lambda: get_database(),
            parse_object_id=self._parse_object_id,
            now_utc=self._now_utc,
            sanitize_payload=self._sanitize_payload,
            record_event=self._record_event,
            can_send_to_lead=self._can_send_to_lead,
            send_resend_email=self._send_resend_email,
            campaigns_collection_name=self._CAMPAIGNS_COLLECTION,
            messages_collection_name=self._MESSAGES_COLLECTION,
            leads_collection_name=self._LEADS_COLLECTION,
        )
        self._campaign_workflow_runtime = CampaignWorkflowRuntime(
            resolve_cadence_template=self._resolve_cadence_template,
            now_utc=self._now_utc,
            record_event=self._record_event,
            serialize_mongo_doc=self._serialize_mongo_doc,
            sanitize_payload=self._sanitize_payload,
            parse_object_id=self._parse_object_id,
            build_campaign_lead_query=self._build_campaign_lead_query,
            load_suppressed_emails=self._load_suppressed_emails,
            normalize_email=self._normalize_email,
            build_mini_report_for_lead=self._build_mini_report_for_lead,
            render_cadence_step=self._render_cadence_step,
            enqueue_job=lambda **kwargs: self.job_service.enqueue_job(**kwargs),
            campaigns_collection_name=self._CAMPAIGNS_COLLECTION,
            leads_collection_name=self._LEADS_COLLECTION,
            messages_collection_name=self._MESSAGES_COLLECTION,
        )
        self._campaign_query_runtime = CampaignQueryRuntime(
            database_factory=lambda: get_database(),
            coerce_pagination=coerce_pagination,
            build_pagination_payload=build_pagination_payload,
            sanitize_payload=self._sanitize_payload,
            event_repository=self._event_repository,
            use_repo_v2=self._use_repo_v2,
            campaigns_collection_name=self._CAMPAIGNS_COLLECTION,
            messages_collection_name=self._MESSAGES_COLLECTION,
            events_collection_name=self._EVENTS_COLLECTION,
        )

    async def ensure_indexes(self) -> None:
        if self._indexes_ensured:
            return
        async with self._indexes_lock:
            if self._indexes_ensured:
                return
            await self._repository_bootstrap.ensure_indexes()
            await self._geo_city_repository.seed_default_cities()
            self._indexes_ensured = True
