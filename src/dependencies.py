from __future__ import annotations

from src.business_catalog import (
    EnqueueBrowserScrapeJobsUseCase,
    RelaunchBrowserScrapeJobUseCase,
)
from src.crm.bulk_delete_crm_leads_use_case import BulkDeleteCRMLeadsUseCase
from src.crm.create_crm_campaign_use_case import CreateCRMCampaignUseCase
from src.crm.create_crm_report_feedback_use_case import CreateCRMReportFeedbackUseCase
from src.crm.create_crm_lead_use_case import CreateCRMLeadUseCase
from src.crm.create_crm_report_request_use_case import CreateCRMReportRequestUseCase
from src.crm.handle_resend_webhook_use_case import HandleResendWebhookUseCase
from src.crm.get_crm_lead_use_case import GetCRMLeadUseCase
from src.crm.enqueue_benchmark_study_job_use_case import EnqueueBenchmarkStudyJobUseCase
from src.crm.enqueue_crm_lead_discovery_job_use_case import EnqueueCRMLeadDiscoveryJobUseCase
from src.crm.enqueue_crm_lead_pipeline_job_use_case import EnqueueCRMLeadPipelineJobUseCase
from src.crm.enqueue_due_campaign_dispatch_jobs_use_case import EnqueueDueCampaignDispatchJobsUseCase
from src.crm.enqueue_geo_grid_study_job_use_case import EnqueueGeoGridStudyJobUseCase
from src.crm.launch_crm_campaign_use_case import LaunchCRMCampaignUseCase
from src.crm.list_crm_campaigns_use_case import ListCRMCampaignsUseCase
from src.crm.list_crm_events_use_case import ListCRMEventsUseCase
from src.crm.list_crm_leads_use_case import ListCRMLeadsUseCase
from src.crm.list_crm_messages_use_case import ListCRMMessagesUseCase
from src.crm.list_crm_report_requests_use_case import ListCRMReportRequestsUseCase
from src.crm.process_pending_crm_report_requests_use_case import ProcessPendingCRMReportRequestsUseCase
from src.crm.retry_crm_report_request_use_case import RetryCRMReportRequestUseCase
from src.crm.update_crm_lead_use_case import UpdateCRMLeadUseCase
from src.platform.application_root import get_application_root
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.business_service import BusinessService
from src.services.crm_service import CRMService
from src.services.tripadvisor_local_worker_control_service import TripadvisorLocalWorkerControlService
from src.services.tripadvisor_session_service import TripadvisorSessionService
from src.workers.broker import WorkerJobBroker
from src.workers.mongo_broker import MongoJobBroker


def create_google_maps_scraper():
    return get_application_root().business_catalog.scraper


def create_tripadvisor_scraper():
    return get_application_root().business_catalog.tripadvisor_scraper


def create_review_preprocessor() -> ReviewPreprocessor:
    return get_application_root().review_preprocessor


def create_review_llm_analyzer() -> ReviewLLMAnalyzer:
    return get_application_root().review_llm


def create_analysis_job_service() -> AnalysisJobService:
    return get_application_root().analysis_jobs


def create_tripadvisor_session_service() -> TripadvisorSessionService:
    return get_application_root().tripadvisor_session


def create_tripadvisor_local_worker_control_service() -> TripadvisorLocalWorkerControlService:
    return get_application_root().legacy_tripadvisor_bridge


def create_worker_job_broker() -> WorkerJobBroker:
    return get_application_root().worker_job_broker


def create_business_query_service() -> BusinessQueryService:
    return get_application_root().business_query


def create_business_service() -> BusinessService:
    return get_application_root().business_catalog


def create_crm_service() -> CRMService:
    return get_application_root().crm


def create_enqueue_browser_scrape_jobs_use_case() -> EnqueueBrowserScrapeJobsUseCase:
    return get_application_root().enqueue_browser_scrape_jobs


def create_relaunch_browser_scrape_job_use_case() -> RelaunchBrowserScrapeJobUseCase:
    return get_application_root().relaunch_browser_scrape_job


def create_enqueue_crm_lead_discovery_job_use_case() -> EnqueueCRMLeadDiscoveryJobUseCase:
    return get_application_root().enqueue_crm_lead_discovery_job


def create_enqueue_geo_grid_study_job_use_case() -> EnqueueGeoGridStudyJobUseCase:
    return get_application_root().enqueue_geo_grid_study_job


def create_enqueue_crm_lead_pipeline_job_use_case() -> EnqueueCRMLeadPipelineJobUseCase:
    return get_application_root().enqueue_crm_lead_pipeline_job


def create_enqueue_benchmark_study_job_use_case() -> EnqueueBenchmarkStudyJobUseCase:
    return get_application_root().enqueue_benchmark_study_job


def create_enqueue_due_campaign_dispatch_jobs_use_case() -> EnqueueDueCampaignDispatchJobsUseCase:
    return get_application_root().enqueue_due_campaign_dispatch_jobs


def create_create_crm_report_request_use_case() -> CreateCRMReportRequestUseCase:
    return get_application_root().create_crm_report_request


def create_create_crm_report_feedback_use_case() -> CreateCRMReportFeedbackUseCase:
    return get_application_root().create_crm_report_feedback


def create_create_crm_lead_use_case() -> CreateCRMLeadUseCase:
    return get_application_root().create_crm_lead


def create_update_crm_lead_use_case() -> UpdateCRMLeadUseCase:
    return get_application_root().update_crm_lead


def create_bulk_delete_crm_leads_use_case() -> BulkDeleteCRMLeadsUseCase:
    return get_application_root().bulk_delete_crm_leads


def create_create_crm_campaign_use_case() -> CreateCRMCampaignUseCase:
    return get_application_root().create_crm_campaign


def create_launch_crm_campaign_use_case() -> LaunchCRMCampaignUseCase:
    return get_application_root().launch_crm_campaign


def create_handle_resend_webhook_use_case() -> HandleResendWebhookUseCase:
    return get_application_root().handle_resend_webhook


def create_list_crm_report_requests_use_case() -> ListCRMReportRequestsUseCase:
    return get_application_root().list_crm_report_requests


def create_list_crm_leads_use_case() -> ListCRMLeadsUseCase:
    return get_application_root().list_crm_leads


def create_get_crm_lead_use_case() -> GetCRMLeadUseCase:
    return get_application_root().get_crm_lead


def create_list_crm_campaigns_use_case() -> ListCRMCampaignsUseCase:
    return get_application_root().list_crm_campaigns


def create_list_crm_messages_use_case() -> ListCRMMessagesUseCase:
    return get_application_root().list_crm_messages


def create_list_crm_events_use_case() -> ListCRMEventsUseCase:
    return get_application_root().list_crm_events


def create_retry_crm_report_request_use_case() -> RetryCRMReportRequestUseCase:
    return get_application_root().retry_crm_report_request


def create_process_pending_crm_report_requests_use_case() -> ProcessPendingCRMReportRequestsUseCase:
    return get_application_root().process_pending_crm_report_requests
