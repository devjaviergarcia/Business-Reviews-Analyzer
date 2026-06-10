from __future__ import annotations

from functools import cached_property

from src.browser_runtime.local_browser_runtime_worker import LocalBrowserRuntimeWorker
from src.business_catalog import (
    EnqueueBrowserScrapeJobsUseCase,
    RelaunchBrowserScrapeJobUseCase,
)
from src.browser_runtime.local_browser_worker_registry import LocalBrowserWorkerRegistry
from src.crm.leads.bulk_delete_crm_leads_use_case import BulkDeleteCRMLeadsUseCase
from src.crm.campaigns.create_crm_campaign_use_case import CreateCRMCampaignUseCase
from src.crm.report_requests.create_crm_report_feedback_use_case import CreateCRMReportFeedbackUseCase
from src.crm.leads.create_crm_lead_use_case import CreateCRMLeadUseCase
from src.crm.report_requests.create_crm_report_request_use_case import CreateCRMReportRequestUseCase
from src.crm.studies.generate_crm_lead_report_use_case import GenerateCRMLeadReportUseCase
from src.crm.studies.generate_crm_paid_report_use_case import GenerateCRMPaidReportUseCase
from src.crm.studies.generate_crm_public_study_use_case import GenerateCRMPublicStudyUseCase
from src.crm.campaigns.handle_resend_webhook_use_case import HandleResendWebhookUseCase
from src.crm.studies.get_crm_discovery_run_use_case import GetCRMDiscoveryRunUseCase
from src.crm.leads.get_crm_lead_use_case import GetCRMLeadUseCase
from src.crm.studies.get_crm_geo_grid_run_use_case import GetCRMGeoGridRunUseCase
from src.crm.studies.get_crm_geo_grid_stats_use_case import GetCRMGeoGridStatsUseCase
from src.crm.studies.enqueue_benchmark_study_job_use_case import EnqueueBenchmarkStudyJobUseCase
from src.crm.leads.enqueue_crm_lead_discovery_job_use_case import EnqueueCRMLeadDiscoveryJobUseCase
from src.crm.leads.enqueue_crm_lead_pipeline_job_use_case import EnqueueCRMLeadPipelineJobUseCase
from src.crm.campaigns.enqueue_due_campaign_dispatch_jobs_use_case import EnqueueDueCampaignDispatchJobsUseCase
from src.crm.studies.enqueue_geo_grid_study_job_use_case import EnqueueGeoGridStudyJobUseCase
from src.crm.campaigns.launch_crm_campaign_use_case import LaunchCRMCampaignUseCase
from src.crm.campaigns.list_crm_campaigns_use_case import ListCRMCampaignsUseCase
from src.crm.studies.list_crm_discovery_runs_use_case import ListCRMDiscoveryRunsUseCase
from src.crm.campaigns.list_crm_events_use_case import ListCRMEventsUseCase
from src.crm.studies.list_crm_geo_cities_use_case import ListCRMGeoCitiesUseCase
from src.crm.studies.list_crm_geo_grid_results_use_case import ListCRMGeoGridResultsUseCase
from src.crm.studies.list_crm_geo_grid_runs_use_case import ListCRMGeoGridRunsUseCase
from src.crm.leads.list_crm_leads_use_case import ListCRMLeadsUseCase
from src.crm.campaigns.list_crm_messages_use_case import ListCRMMessagesUseCase
from src.crm.report_requests.list_crm_report_requests_use_case import ListCRMReportRequestsUseCase
from src.crm.studies.process_benchmark_study_task_use_case import ProcessBenchmarkStudyTaskUseCase
from src.crm.campaigns.process_campaign_dispatch_task_use_case import ProcessCampaignDispatchTaskUseCase
from src.crm.leads.process_crm_lead_discovery_task_use_case import ProcessCRMLeadDiscoveryTaskUseCase
from src.crm.report_requests.process_pending_crm_report_requests_use_case import ProcessPendingCRMReportRequestsUseCase
from src.crm.leads.process_crm_lead_pipeline_task_use_case import ProcessCRMLeadPipelineTaskUseCase
from src.crm.studies.process_geo_grid_study_task_use_case import ProcessGeoGridStudyTaskUseCase
from src.crm.report_requests.retry_crm_report_request_use_case import RetryCRMReportRequestUseCase
from src.crm.leads.sync_crm_lead_pipeline_refs_use_case import SyncCRMLeadPipelineRefsUseCase
from src.crm.leads.update_crm_lead_use_case import UpdateCRMLeadUseCase
from src.job_runtime.local_browser_job_coordinator import LocalBrowserJobCoordinator
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.scraping_google_maps.google_maps_browser_adapter import GoogleMapsBrowserAdapter
from src.scraping_tripadvisor.tripadvisor_browser_adapter import TripadvisorBrowserAdapter
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.business_service import BusinessService
from src.services.crm_service import CRMService
from src.services.tripadvisor_local_worker_control_service import TripadvisorLocalWorkerControlService
from src.services.tripadvisor_session_service import TripadvisorSessionService
from src.workers.mongo_broker import MongoJobBroker


class ApplicationRoot:
    def _build_enqueue_browser_scrape_jobs_use_case(
        self,
        business: BusinessService,
    ) -> EnqueueBrowserScrapeJobsUseCase:
        return EnqueueBrowserScrapeJobsUseCase(
            job_service=self.analysis_jobs,
            validate_business_name=business._validate_business_name,
            normalize_text=business._normalize_text,
            resolve_reviews_strategy=business._resolve_reviews_strategy,
            resolve_force_mode=business._resolve_force_mode,
            resolve_scrape_sources=business._resolve_scrape_sources,
            inspect_local_browser_runtime_on_enqueue=business._inspect_local_browser_runtime_on_enqueue,
            ensure_root_business_on_enqueue=business._ensure_root_business_on_enqueue,
        )

    def _build_relaunch_browser_scrape_job_use_case(
        self,
        business: BusinessService,
    ) -> RelaunchBrowserScrapeJobUseCase:
        return RelaunchBrowserScrapeJobUseCase(
            job_service=self.analysis_jobs,
            ensure_job_is_scrape=business._ensure_job_is_scrape,
            ensure_tripadvisor_session_available_for_relaunch=business._ensure_tripadvisor_session_available_for_relaunch,
            validate_business_name=business._validate_business_name,
            normalize_text=business._normalize_text,
        )

    def _build_enqueue_crm_lead_discovery_job_use_case(
        self,
        crm: CRMService,
    ) -> EnqueueCRMLeadDiscoveryJobUseCase:
        return EnqueueCRMLeadDiscoveryJobUseCase(
            ensure_indexes=crm.ensure_indexes,
            lead_job_enqueue_runtime=crm._lead_job_enqueue_runtime,
            use_discovery_v2=lambda: bool(crm._use_discovery_v2),
        )

    def _build_enqueue_geo_grid_study_job_use_case(
        self,
        crm: CRMService,
    ) -> EnqueueGeoGridStudyJobUseCase:
        return EnqueueGeoGridStudyJobUseCase(
            ensure_indexes=crm.ensure_indexes,
            study_job_enqueue_runtime=crm._study_job_enqueue_runtime,
        )

    def _build_enqueue_crm_lead_pipeline_job_use_case(
        self,
        crm: CRMService,
    ) -> EnqueueCRMLeadPipelineJobUseCase:
        return EnqueueCRMLeadPipelineJobUseCase(
            ensure_indexes=crm.ensure_indexes,
            lead_job_enqueue_runtime=crm._lead_job_enqueue_runtime,
        )

    def _build_enqueue_benchmark_study_job_use_case(
        self,
        crm: CRMService,
    ) -> EnqueueBenchmarkStudyJobUseCase:
        return EnqueueBenchmarkStudyJobUseCase(
            ensure_indexes=crm.ensure_indexes,
            study_job_enqueue_runtime=crm._study_job_enqueue_runtime,
        )

    def _build_create_crm_report_request_use_case(
        self,
        crm: CRMService,
        enqueue_benchmark_study_job: EnqueueBenchmarkStudyJobUseCase,
    ) -> CreateCRMReportRequestUseCase:
        return CreateCRMReportRequestUseCase(
            ensure_indexes=crm.ensure_indexes,
            now_utc=crm._now_utc,
            normalize_email=crm._normalize_email,
            normalize_text=crm._normalize_text,
            normalize_utm=crm._normalize_utm,
            enqueue_benchmark_study_job=enqueue_benchmark_study_job.execute,
            record_event=crm._record_event,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            report_requests_collection_name=crm._REPORT_REQUESTS_COLLECTION,
        )

    def _build_create_crm_report_feedback_use_case(
        self,
        crm: CRMService,
    ) -> CreateCRMReportFeedbackUseCase:
        return CreateCRMReportFeedbackUseCase(
            ensure_indexes=crm.ensure_indexes,
            now_utc=crm._now_utc,
            parse_object_id=crm._parse_object_id,
            record_event=crm._record_event,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            report_feedback_collection_name=crm._REPORT_FEEDBACK_COLLECTION,
            report_requests_collection_name=crm._REPORT_REQUESTS_COLLECTION,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_generate_crm_lead_report_use_case(
        self,
        crm: CRMService,
    ) -> GenerateCRMLeadReportUseCase:
        return GenerateCRMLeadReportUseCase(
            ensure_indexes=crm.ensure_indexes,
            benchmark_report_runtime=crm._benchmark_report_runtime,
        )

    def _build_generate_crm_paid_report_use_case(
        self,
        crm: CRMService,
    ) -> GenerateCRMPaidReportUseCase:
        return GenerateCRMPaidReportUseCase(
            ensure_indexes=crm.ensure_indexes,
            benchmark_report_runtime=crm._benchmark_report_runtime,
        )

    def _build_generate_crm_public_study_use_case(
        self,
        crm: CRMService,
    ) -> GenerateCRMPublicStudyUseCase:
        return GenerateCRMPublicStudyUseCase(
            ensure_indexes=crm.ensure_indexes,
            benchmark_report_runtime=crm._benchmark_report_runtime,
        )

    def _build_create_crm_lead_use_case(
        self,
        crm: CRMService,
    ) -> CreateCRMLeadUseCase:
        return CreateCRMLeadUseCase(
            ensure_indexes=crm.ensure_indexes,
            now_utc=crm._now_utc,
            normalize_email=crm._normalize_email,
            normalize_text=crm._normalize_text,
            domain_from_email_or_website=crm._domain_from_email_or_website,
            record_event=crm._record_event,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_update_crm_lead_use_case(
        self,
        crm: CRMService,
    ) -> UpdateCRMLeadUseCase:
        return UpdateCRMLeadUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_repo_v2=bool(crm._use_repo_v2),
            update_lead_v2=crm._update_lead_v2,
            parse_object_id=crm._parse_object_id,
            now_utc=crm._now_utc,
            normalize_email=crm._normalize_email,
            normalize_text=crm._normalize_text,
            domain_from_email_or_website=crm._domain_from_email_or_website,
            get_lead=crm.get_lead,
            record_event=crm._record_event,
            upsert_suppression=crm._upsert_suppression,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_bulk_delete_crm_leads_use_case(
        self,
        crm: CRMService,
    ) -> BulkDeleteCRMLeadsUseCase:
        return BulkDeleteCRMLeadsUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_repo_v2=bool(crm._use_repo_v2),
            lead_repository=crm._lead_repository,
            parse_object_id=crm._parse_object_id,
            build_leads_query=crm._build_leads_query,
            record_event=crm._record_event,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_create_crm_campaign_use_case(
        self,
        crm: CRMService,
    ) -> CreateCRMCampaignUseCase:
        return CreateCRMCampaignUseCase(
            ensure_indexes=crm.ensure_indexes,
            campaign_workflow_runtime=crm._campaign_workflow_runtime,
        )

    def _build_launch_crm_campaign_use_case(
        self,
        crm: CRMService,
        enqueue_due_campaign_dispatch_jobs: EnqueueDueCampaignDispatchJobsUseCase,
    ) -> LaunchCRMCampaignUseCase:
        return LaunchCRMCampaignUseCase(
            ensure_indexes=crm.ensure_indexes,
            campaign_workflow_runtime=crm._campaign_workflow_runtime,
        )

    def _build_handle_resend_webhook_use_case(
        self,
        crm: CRMService,
    ) -> HandleResendWebhookUseCase:
        return HandleResendWebhookUseCase(
            ensure_indexes=crm.ensure_indexes,
            now_utc=crm._now_utc,
            parse_object_id=crm._parse_object_id,
            block_lead_contact=crm._block_lead_contact,
            upsert_suppression=crm._upsert_suppression,
            record_event=crm._record_event,
            sanitize_payload=crm._sanitize_payload,
            messages_collection_name=crm._MESSAGES_COLLECTION,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_list_crm_report_requests_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMReportRequestsUseCase:
        return ListCRMReportRequestsUseCase(
            ensure_indexes=crm.ensure_indexes,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            report_requests_collection_name=crm._REPORT_REQUESTS_COLLECTION,
        )

    def _build_list_crm_leads_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMLeadsUseCase:
        return ListCRMLeadsUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_repo_v2=bool(crm._use_repo_v2),
            lead_repository=crm._lead_repository,
            build_leads_query=crm._build_leads_query,
            resolve_leads_sort=crm._resolve_leads_sort,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_get_crm_lead_use_case(
        self,
        crm: CRMService,
        sync_crm_lead_pipeline_refs: SyncCRMLeadPipelineRefsUseCase,
    ) -> GetCRMLeadUseCase:
        return GetCRMLeadUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_repo_v2=bool(crm._use_repo_v2),
            lead_repository=crm._lead_repository,
            parse_object_id=crm._parse_object_id,
            sync_lead_pipeline_refs=sync_crm_lead_pipeline_refs.execute,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_list_crm_campaigns_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMCampaignsUseCase:
        return ListCRMCampaignsUseCase(
            ensure_indexes=crm.ensure_indexes,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            campaigns_collection_name=crm._CAMPAIGNS_COLLECTION,
        )

    def _build_list_crm_messages_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMMessagesUseCase:
        return ListCRMMessagesUseCase(
            ensure_indexes=crm.ensure_indexes,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            messages_collection_name=crm._MESSAGES_COLLECTION,
        )

    def _build_list_crm_events_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMEventsUseCase:
        return ListCRMEventsUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_repo_v2=bool(crm._use_repo_v2),
            event_repository=crm._event_repository,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            events_collection_name=crm._EVENTS_COLLECTION,
        )

    def _build_list_crm_discovery_runs_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMDiscoveryRunsUseCase:
        return ListCRMDiscoveryRunsUseCase(
            ensure_indexes=crm.ensure_indexes,
            discovery_run_repository=crm._discovery_run_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_get_crm_discovery_run_use_case(
        self,
        crm: CRMService,
    ) -> GetCRMDiscoveryRunUseCase:
        return GetCRMDiscoveryRunUseCase(
            ensure_indexes=crm.ensure_indexes,
            discovery_run_repository=crm._discovery_run_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_list_crm_geo_cities_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMGeoCitiesUseCase:
        return ListCRMGeoCitiesUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_city_repository=crm._geo_city_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_list_crm_geo_grid_runs_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMGeoGridRunsUseCase:
        return ListCRMGeoGridRunsUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_grid_run_repository=crm._geo_grid_run_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_get_crm_geo_grid_run_use_case(
        self,
        crm: CRMService,
    ) -> GetCRMGeoGridRunUseCase:
        return GetCRMGeoGridRunUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_grid_run_repository=crm._geo_grid_run_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_list_crm_geo_grid_results_use_case(
        self,
        crm: CRMService,
    ) -> ListCRMGeoGridResultsUseCase:
        return ListCRMGeoGridResultsUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_grid_run_repository=crm._geo_grid_run_repository,
            geo_grid_result_repository=crm._geo_grid_result_repository,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_get_crm_geo_grid_stats_use_case(
        self,
        crm: CRMService,
    ) -> GetCRMGeoGridStatsUseCase:
        return GetCRMGeoGridStatsUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_grid_run_repository=crm._geo_grid_run_repository,
            geo_grid_result_repository=crm._geo_grid_result_repository,
            build_geo_grid_stats=crm._build_geo_grid_stats,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_sync_crm_lead_pipeline_refs_use_case(
        self,
        crm: CRMService,
    ) -> SyncCRMLeadPipelineRefsUseCase:
        return SyncCRMLeadPipelineRefsUseCase(
            ensure_indexes=crm.ensure_indexes,
            parse_object_id=crm._parse_object_id,
            now_utc=crm._now_utc,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            leads_collection_name=crm._LEADS_COLLECTION,
            jobs_collection_name=crm._JOBS_COLLECTION,
        )

    def _build_process_crm_lead_pipeline_task_use_case(
        self,
        crm: CRMService,
    ) -> ProcessCRMLeadPipelineTaskUseCase:
        return ProcessCRMLeadPipelineTaskUseCase(
            ensure_indexes=crm.ensure_indexes,
            parse_object_id=crm._parse_object_id,
            now_utc=crm._now_utc,
            sanitize_payload=crm._sanitize_payload,
            record_event=crm._record_event,
            enqueue_browser_scrape_jobs=self.enqueue_browser_scrape_jobs.execute,
            leads_collection_name=crm._LEADS_COLLECTION,
            allowed_sources=crm._ALLOWED_SOURCES,
        )

    def _build_process_crm_lead_discovery_task_use_case(
        self,
        crm: CRMService,
    ) -> ProcessCRMLeadDiscoveryTaskUseCase:
        return ProcessCRMLeadDiscoveryTaskUseCase(
            ensure_indexes=crm.ensure_indexes,
            use_discovery_v2=lambda: bool(crm._use_discovery_v2),
            discovery_run_repository=crm._discovery_run_repository,
            discover_candidates_for_orchestrator=crm._discover_candidates_for_orchestrator,
            upsert_lead_candidate=crm._upsert_lead_candidate,
            record_event=crm._record_event,
            sanitize_payload=crm._sanitize_payload,
            discover_candidates=crm._discover_candidates,
        )

    def _build_process_benchmark_study_task_use_case(
        self,
        crm: CRMService,
    ) -> ProcessBenchmarkStudyTaskUseCase:
        return ProcessBenchmarkStudyTaskUseCase(
            ensure_indexes=crm.ensure_indexes,
            benchmark_run_repository=crm._benchmark_run_repository,
            benchmark_business_repository=crm._benchmark_business_repository,
            discover_benchmark_candidates_for_orchestrator=crm._discover_benchmark_candidates_for_orchestrator,
            competitor_set_repository=crm._competitor_set_repository,
            record_event=crm._record_event,
            sanitize_payload=crm._sanitize_payload,
        )

    def _build_process_geo_grid_study_task_use_case(
        self,
        crm: CRMService,
    ) -> ProcessGeoGridStudyTaskUseCase:
        return ProcessGeoGridStudyTaskUseCase(
            ensure_indexes=crm.ensure_indexes,
            geo_grid_study_runtime=crm._geo_grid_study_runtime,
        )

    def _build_enqueue_due_campaign_dispatch_jobs_use_case(
        self,
        crm: CRMService,
    ) -> EnqueueDueCampaignDispatchJobsUseCase:
        return EnqueueDueCampaignDispatchJobsUseCase(
            ensure_indexes=crm.ensure_indexes,
            campaign_workflow_runtime=crm._campaign_workflow_runtime,
        )

    def _build_process_campaign_dispatch_task_use_case(
        self,
        crm: CRMService,
    ) -> ProcessCampaignDispatchTaskUseCase:
        return ProcessCampaignDispatchTaskUseCase(
            ensure_indexes=crm.ensure_indexes,
            parse_object_id=crm._parse_object_id,
            now_utc=crm._now_utc,
            sanitize_payload=crm._sanitize_payload,
            record_event=crm._record_event,
            can_send_to_lead=crm._can_send_to_lead,
            send_resend_email=crm._send_resend_email,
            campaigns_collection_name=crm._CAMPAIGNS_COLLECTION,
            messages_collection_name=crm._MESSAGES_COLLECTION,
            leads_collection_name=crm._LEADS_COLLECTION,
        )

    def _build_retry_crm_report_request_use_case(
        self,
        crm: CRMService,
        enqueue_benchmark_study_job: EnqueueBenchmarkStudyJobUseCase,
    ) -> RetryCRMReportRequestUseCase:
        return RetryCRMReportRequestUseCase(
            ensure_indexes=crm.ensure_indexes,
            parse_object_id=crm._parse_object_id,
            now_utc=crm._now_utc,
            record_event=crm._record_event,
            serialize_mongo_doc=crm._serialize_mongo_doc,
            sanitize_payload=crm._sanitize_payload,
            enqueue_benchmark_study_job=enqueue_benchmark_study_job.execute,
            report_requests_collection_name=crm._REPORT_REQUESTS_COLLECTION,
        )

    def _build_process_pending_crm_report_requests_use_case(
        self,
        crm: CRMService,
        enqueue_benchmark_study_job: EnqueueBenchmarkStudyJobUseCase,
    ) -> ProcessPendingCRMReportRequestsUseCase:
        return ProcessPendingCRMReportRequestsUseCase(
            ensure_indexes=crm.ensure_indexes,
            now_utc=crm._now_utc,
            record_event=crm._record_event,
            sanitize_payload=crm._sanitize_payload,
            enqueue_benchmark_study_job=enqueue_benchmark_study_job.execute,
            report_requests_collection_name=crm._REPORT_REQUESTS_COLLECTION,
        )

    @cached_property
    def analysis_jobs(self) -> AnalysisJobService:
        return AnalysisJobService()

    @cached_property
    def worker_job_broker(self) -> MongoJobBroker:
        return MongoJobBroker(job_service=self.analysis_jobs)

    @cached_property
    def business_query(self) -> BusinessQueryService:
        return BusinessQueryService()

    @cached_property
    def review_preprocessor(self) -> ReviewPreprocessor:
        return ReviewPreprocessor()

    @cached_property
    def review_llm(self) -> ReviewLLMAnalyzer:
        return ReviewLLMAnalyzer()

    @cached_property
    def tripadvisor_session(self) -> TripadvisorSessionService:
        return TripadvisorSessionService()

    @cached_property
    def legacy_tripadvisor_bridge(self) -> TripadvisorLocalWorkerControlService:
        return TripadvisorLocalWorkerControlService()

    @cached_property
    def business_catalog(self) -> BusinessService:
        service = BusinessService(
            scraper=BusinessService.build_default_scraper(),
            tripadvisor_scraper=BusinessService.build_default_tripadvisor_scraper(),
            preprocessor=self.review_preprocessor,
            llm_analyzer=self.review_llm,
            job_service=self.analysis_jobs,
            query_service=self.business_query,
            local_browser_worker_registry=self.local_browser_registry,
        )
        return service.attach_browser_scrape_job_use_cases(
            enqueue_browser_scrape_jobs_use_case=self._build_enqueue_browser_scrape_jobs_use_case(service),
            relaunch_browser_scrape_job_use_case=self._build_relaunch_browser_scrape_job_use_case(service),
        )

    @cached_property
    def crm(self) -> CRMService:
        service = CRMService(
            job_service=self.analysis_jobs,
            business_service=self.business_catalog,
        )
        enqueue_benchmark_study_job = self._build_enqueue_benchmark_study_job_use_case(service)
        enqueue_due_campaign_dispatch_jobs = self._build_enqueue_due_campaign_dispatch_jobs_use_case(service)
        sync_crm_lead_pipeline_refs = self._build_sync_crm_lead_pipeline_refs_use_case(service)
        return service.attach_crm_queue_use_cases(
            enqueue_crm_lead_discovery_job_use_case=self._build_enqueue_crm_lead_discovery_job_use_case(service),
            enqueue_crm_lead_pipeline_job_use_case=self._build_enqueue_crm_lead_pipeline_job_use_case(service),
            enqueue_benchmark_study_job_use_case=enqueue_benchmark_study_job,
            create_crm_report_request_use_case=self._build_create_crm_report_request_use_case(
                service,
                enqueue_benchmark_study_job,
            ),
            create_crm_report_feedback_use_case=self._build_create_crm_report_feedback_use_case(service),
            generate_crm_lead_report_use_case=self._build_generate_crm_lead_report_use_case(service),
            generate_crm_paid_report_use_case=self._build_generate_crm_paid_report_use_case(service),
            generate_crm_public_study_use_case=self._build_generate_crm_public_study_use_case(service),
            create_crm_lead_use_case=self._build_create_crm_lead_use_case(service),
            update_crm_lead_use_case=self._build_update_crm_lead_use_case(service),
            bulk_delete_crm_leads_use_case=self._build_bulk_delete_crm_leads_use_case(service),
            create_crm_campaign_use_case=self._build_create_crm_campaign_use_case(service),
            launch_crm_campaign_use_case=self._build_launch_crm_campaign_use_case(
                service,
                enqueue_due_campaign_dispatch_jobs,
            ),
            handle_resend_webhook_use_case=self._build_handle_resend_webhook_use_case(service),
            list_crm_report_requests_use_case=self._build_list_crm_report_requests_use_case(service),
            list_crm_leads_use_case=self._build_list_crm_leads_use_case(service),
            get_crm_lead_use_case=self._build_get_crm_lead_use_case(
                service,
                sync_crm_lead_pipeline_refs,
            ),
            list_crm_campaigns_use_case=self._build_list_crm_campaigns_use_case(service),
            list_crm_messages_use_case=self._build_list_crm_messages_use_case(service),
            list_crm_events_use_case=self._build_list_crm_events_use_case(service),
            list_crm_discovery_runs_use_case=self._build_list_crm_discovery_runs_use_case(service),
            get_crm_discovery_run_use_case=self._build_get_crm_discovery_run_use_case(service),
            list_crm_geo_cities_use_case=self._build_list_crm_geo_cities_use_case(service),
            list_crm_geo_grid_runs_use_case=self._build_list_crm_geo_grid_runs_use_case(service),
            get_crm_geo_grid_run_use_case=self._build_get_crm_geo_grid_run_use_case(service),
            list_crm_geo_grid_results_use_case=self._build_list_crm_geo_grid_results_use_case(service),
            get_crm_geo_grid_stats_use_case=self._build_get_crm_geo_grid_stats_use_case(service),
            sync_crm_lead_pipeline_refs_use_case=sync_crm_lead_pipeline_refs,
            enqueue_geo_grid_study_job_use_case=self._build_enqueue_geo_grid_study_job_use_case(service),
            process_crm_lead_discovery_task_use_case=self._build_process_crm_lead_discovery_task_use_case(service),
            process_benchmark_study_task_use_case=self._build_process_benchmark_study_task_use_case(service),
            process_crm_lead_pipeline_task_use_case=self._build_process_crm_lead_pipeline_task_use_case(service),
            process_geo_grid_study_task_use_case=self._build_process_geo_grid_study_task_use_case(service),
            enqueue_due_campaign_dispatch_jobs_use_case=enqueue_due_campaign_dispatch_jobs,
            process_campaign_dispatch_task_use_case=self._build_process_campaign_dispatch_task_use_case(service),
            retry_crm_report_request_use_case=self._build_retry_crm_report_request_use_case(
                service,
                enqueue_benchmark_study_job,
            ),
            process_pending_crm_report_requests_use_case=self._build_process_pending_crm_report_requests_use_case(
                service,
                enqueue_benchmark_study_job,
            ),
        )

    @cached_property
    def enqueue_browser_scrape_jobs(self) -> EnqueueBrowserScrapeJobsUseCase:
        use_case = self.business_catalog.enqueue_browser_scrape_jobs_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def relaunch_browser_scrape_job(self) -> RelaunchBrowserScrapeJobUseCase:
        use_case = self.business_catalog.relaunch_browser_scrape_job_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def enqueue_crm_lead_discovery_job(self) -> EnqueueCRMLeadDiscoveryJobUseCase:
        use_case = self.crm.enqueue_crm_lead_discovery_job_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def enqueue_geo_grid_study_job(self) -> EnqueueGeoGridStudyJobUseCase:
        use_case = self.crm.enqueue_geo_grid_study_job_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def enqueue_crm_lead_pipeline_job(self) -> EnqueueCRMLeadPipelineJobUseCase:
        use_case = self.crm.enqueue_crm_lead_pipeline_job_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def enqueue_benchmark_study_job(self) -> EnqueueBenchmarkStudyJobUseCase:
        use_case = self.crm.enqueue_benchmark_study_job_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def enqueue_due_campaign_dispatch_jobs(self) -> EnqueueDueCampaignDispatchJobsUseCase:
        use_case = self.crm.enqueue_due_campaign_dispatch_jobs_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def create_crm_report_request(self) -> CreateCRMReportRequestUseCase:
        use_case = self.crm.create_crm_report_request_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def create_crm_report_feedback(self) -> CreateCRMReportFeedbackUseCase:
        use_case = self.crm.create_crm_report_feedback_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def generate_crm_lead_report(self) -> GenerateCRMLeadReportUseCase:
        use_case = self.crm.generate_crm_lead_report_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def generate_crm_paid_report(self) -> GenerateCRMPaidReportUseCase:
        use_case = self.crm.generate_crm_paid_report_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def generate_crm_public_study(self) -> GenerateCRMPublicStudyUseCase:
        use_case = self.crm.generate_crm_public_study_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def create_crm_lead(self) -> CreateCRMLeadUseCase:
        use_case = self.crm.create_crm_lead_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def update_crm_lead(self) -> UpdateCRMLeadUseCase:
        use_case = self.crm.update_crm_lead_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def bulk_delete_crm_leads(self) -> BulkDeleteCRMLeadsUseCase:
        use_case = self.crm.bulk_delete_crm_leads_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def create_crm_campaign(self) -> CreateCRMCampaignUseCase:
        use_case = self.crm.create_crm_campaign_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def launch_crm_campaign(self) -> LaunchCRMCampaignUseCase:
        use_case = self.crm.launch_crm_campaign_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def handle_resend_webhook(self) -> HandleResendWebhookUseCase:
        use_case = self.crm.handle_resend_webhook_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_report_requests(self) -> ListCRMReportRequestsUseCase:
        use_case = self.crm.list_crm_report_requests_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_leads(self) -> ListCRMLeadsUseCase:
        use_case = self.crm.list_crm_leads_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def get_crm_lead(self) -> GetCRMLeadUseCase:
        use_case = self.crm.get_crm_lead_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_campaigns(self) -> ListCRMCampaignsUseCase:
        use_case = self.crm.list_crm_campaigns_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_messages(self) -> ListCRMMessagesUseCase:
        use_case = self.crm.list_crm_messages_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_events(self) -> ListCRMEventsUseCase:
        use_case = self.crm.list_crm_events_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_discovery_runs(self) -> ListCRMDiscoveryRunsUseCase:
        use_case = self.crm.list_crm_discovery_runs_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def get_crm_discovery_run(self) -> GetCRMDiscoveryRunUseCase:
        use_case = self.crm.get_crm_discovery_run_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_geo_cities(self) -> ListCRMGeoCitiesUseCase:
        use_case = self.crm.list_crm_geo_cities_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_geo_grid_runs(self) -> ListCRMGeoGridRunsUseCase:
        use_case = self.crm.list_crm_geo_grid_runs_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def get_crm_geo_grid_run(self) -> GetCRMGeoGridRunUseCase:
        use_case = self.crm.get_crm_geo_grid_run_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def list_crm_geo_grid_results(self) -> ListCRMGeoGridResultsUseCase:
        use_case = self.crm.list_crm_geo_grid_results_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def get_crm_geo_grid_stats(self) -> GetCRMGeoGridStatsUseCase:
        use_case = self.crm.get_crm_geo_grid_stats_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def sync_crm_lead_pipeline_refs(self) -> SyncCRMLeadPipelineRefsUseCase:
        use_case = self.crm.sync_crm_lead_pipeline_refs_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def retry_crm_report_request(self) -> RetryCRMReportRequestUseCase:
        use_case = self.crm.retry_crm_report_request_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def process_pending_crm_report_requests(self) -> ProcessPendingCRMReportRequestsUseCase:
        use_case = self.crm.process_pending_crm_report_requests_use_case
        assert use_case is not None
        return use_case

    @cached_property
    def local_browser_jobs(self) -> LocalBrowserJobCoordinator:
        return LocalBrowserJobCoordinator()

    @cached_property
    def local_browser_registry(self) -> LocalBrowserWorkerRegistry:
        return LocalBrowserWorkerRegistry()

    @cached_property
    def google_maps_browser_adapter(self) -> GoogleMapsBrowserAdapter:
        return GoogleMapsBrowserAdapter(business_service=self.business_catalog)

    @cached_property
    def tripadvisor_browser_adapter(self) -> TripadvisorBrowserAdapter:
        return TripadvisorBrowserAdapter(business_service=self.business_catalog)

    def build_local_browser_runtime_worker(self) -> LocalBrowserRuntimeWorker:
        return LocalBrowserRuntimeWorker(
            job_service=self.analysis_jobs,
            business_service=self.business_catalog,
            crm_service=self.crm,
            local_browser_jobs=self.local_browser_jobs,
            local_browser_registry=self.local_browser_registry,
            google_maps_adapter=self.google_maps_browser_adapter,
            tripadvisor_adapter=self.tripadvisor_browser_adapter,
        )


_APPLICATION_ROOT = ApplicationRoot()


def get_application_root() -> ApplicationRoot:
    return _APPLICATION_ROOT
