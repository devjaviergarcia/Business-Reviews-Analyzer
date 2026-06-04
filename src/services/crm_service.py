from __future__ import annotations

import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import formataddr
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus, urlencode, urlparse
from urllib.request import Request, urlopen

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.config import settings
from src.database import get_database
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
from src.scraper.google_maps import GoogleMapsScraper
from src.scraper.selectors import SELECTOR_PATTERNS
from src.models.crm import (
    CRMCampaign,
    CRMCampaignStatus,
    CRMCadenceStep,
    CRMCadenceTemplate,
    CRMConsentProof,
    CRMConsentStatus,
    CRMEvent,
    CRMLead,
    CRMLeadLegalBlock,
    CRMLeadPipelineRefs,
    CRMLeadStatus,
    CRMMessage,
    CRMMessageStatus,
    CRMSuppression,
)
from src.crm.benchmark import (
    BenchmarkOrchestrator,
    build_deep_study_snapshot,
    build_geo_grid_points,
    generate_uule_v2,
    normalize_grid_size,
    select_competitors_for_business,
)
from src.crm.discovery import DiscoveryOrchestrator
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
from src.crm.reports import render_lead_report_html, render_paid_report_html, render_public_study_html
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_service import BusinessService
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.workers.contracts import (
    BenchmarkLocalStudyTaskPayload,
    CRMCampaignDispatchTaskPayload,
    CRMLeadDiscoveryTaskPayload,
    CRMLeadPipelineTaskPayload,
    GeoGridStudyTaskPayload,
)

if TYPE_CHECKING:
    from src.crm.bulk_delete_crm_leads_use_case import BulkDeleteCRMLeadsUseCase
    from src.crm.create_crm_campaign_use_case import CreateCRMCampaignUseCase
    from src.crm.create_crm_report_feedback_use_case import CreateCRMReportFeedbackUseCase
    from src.crm.create_crm_lead_use_case import CreateCRMLeadUseCase
    from src.crm.create_crm_report_request_use_case import CreateCRMReportRequestUseCase
    from src.crm.handle_resend_webhook_use_case import HandleResendWebhookUseCase
    from src.crm.get_crm_lead_use_case import GetCRMLeadUseCase
    from src.crm.enqueue_benchmark_study_job_use_case import EnqueueBenchmarkStudyJobUseCase
    from src.crm.enqueue_crm_lead_discovery_job_use_case import (
        EnqueueCRMLeadDiscoveryJobUseCase,
    )
    from src.crm.enqueue_crm_lead_pipeline_job_use_case import (
        EnqueueCRMLeadPipelineJobUseCase,
    )
    from src.crm.enqueue_due_campaign_dispatch_jobs_use_case import (
        EnqueueDueCampaignDispatchJobsUseCase,
    )
    from src.crm.enqueue_geo_grid_study_job_use_case import EnqueueGeoGridStudyJobUseCase
    from src.crm.launch_crm_campaign_use_case import LaunchCRMCampaignUseCase
    from src.crm.list_crm_campaigns_use_case import ListCRMCampaignsUseCase
    from src.crm.list_crm_events_use_case import ListCRMEventsUseCase
    from src.crm.list_crm_leads_use_case import ListCRMLeadsUseCase
    from src.crm.list_crm_messages_use_case import ListCRMMessagesUseCase
    from src.crm.list_crm_report_requests_use_case import ListCRMReportRequestsUseCase
    from src.crm.process_benchmark_study_task_use_case import ProcessBenchmarkStudyTaskUseCase
    from src.crm.process_campaign_dispatch_task_use_case import ProcessCampaignDispatchTaskUseCase
    from src.crm.process_crm_lead_discovery_task_use_case import ProcessCRMLeadDiscoveryTaskUseCase
    from src.crm.process_pending_crm_report_requests_use_case import ProcessPendingCRMReportRequestsUseCase
    from src.crm.process_crm_lead_pipeline_task_use_case import (
        ProcessCRMLeadPipelineTaskUseCase,
    )
    from src.crm.process_geo_grid_study_task_use_case import ProcessGeoGridStudyTaskUseCase
    from src.crm.retry_crm_report_request_use_case import RetryCRMReportRequestUseCase
    from src.crm.update_crm_lead_use_case import UpdateCRMLeadUseCase


class CRMService:
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
        self._repository_bootstrap = CRMRepositoryBootstrap()
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
        self._enqueue_crm_lead_discovery_job_use_case: EnqueueCRMLeadDiscoveryJobUseCase | None = None
        self._enqueue_crm_lead_pipeline_job_use_case: EnqueueCRMLeadPipelineJobUseCase | None = None
        self._enqueue_benchmark_study_job_use_case: EnqueueBenchmarkStudyJobUseCase | None = None
        self._create_crm_report_request_use_case: CreateCRMReportRequestUseCase | None = None
        self._create_crm_report_feedback_use_case: CreateCRMReportFeedbackUseCase | None = None
        self._create_crm_lead_use_case: CreateCRMLeadUseCase | None = None
        self._update_crm_lead_use_case: UpdateCRMLeadUseCase | None = None
        self._bulk_delete_crm_leads_use_case: BulkDeleteCRMLeadsUseCase | None = None
        self._create_crm_campaign_use_case: CreateCRMCampaignUseCase | None = None
        self._launch_crm_campaign_use_case: LaunchCRMCampaignUseCase | None = None
        self._handle_resend_webhook_use_case: HandleResendWebhookUseCase | None = None
        self._list_crm_report_requests_use_case: ListCRMReportRequestsUseCase | None = None
        self._list_crm_leads_use_case: ListCRMLeadsUseCase | None = None
        self._get_crm_lead_use_case: GetCRMLeadUseCase | None = None
        self._list_crm_campaigns_use_case: ListCRMCampaignsUseCase | None = None
        self._list_crm_messages_use_case: ListCRMMessagesUseCase | None = None
        self._list_crm_events_use_case: ListCRMEventsUseCase | None = None
        self._enqueue_geo_grid_study_job_use_case: EnqueueGeoGridStudyJobUseCase | None = None
        self._process_crm_lead_discovery_task_use_case: ProcessCRMLeadDiscoveryTaskUseCase | None = None
        self._process_benchmark_study_task_use_case: ProcessBenchmarkStudyTaskUseCase | None = None
        self._process_crm_lead_pipeline_task_use_case: ProcessCRMLeadPipelineTaskUseCase | None = None
        self._process_geo_grid_study_task_use_case: ProcessGeoGridStudyTaskUseCase | None = None
        self._enqueue_due_campaign_dispatch_jobs_use_case: EnqueueDueCampaignDispatchJobsUseCase | None = None
        self._process_campaign_dispatch_task_use_case: ProcessCampaignDispatchTaskUseCase | None = None
        self._retry_crm_report_request_use_case: RetryCRMReportRequestUseCase | None = None
        self._process_pending_crm_report_requests_use_case: ProcessPendingCRMReportRequestsUseCase | None = None

    @property
    def enqueue_crm_lead_discovery_job_use_case(self) -> "EnqueueCRMLeadDiscoveryJobUseCase | None":
        return self._enqueue_crm_lead_discovery_job_use_case

    @property
    def enqueue_crm_lead_pipeline_job_use_case(self) -> "EnqueueCRMLeadPipelineJobUseCase | None":
        return self._enqueue_crm_lead_pipeline_job_use_case

    @property
    def enqueue_benchmark_study_job_use_case(self) -> "EnqueueBenchmarkStudyJobUseCase | None":
        return self._enqueue_benchmark_study_job_use_case

    @property
    def create_crm_report_request_use_case(self) -> "CreateCRMReportRequestUseCase | None":
        return self._create_crm_report_request_use_case

    @property
    def create_crm_report_feedback_use_case(self) -> "CreateCRMReportFeedbackUseCase | None":
        return self._create_crm_report_feedback_use_case

    @property
    def create_crm_lead_use_case(self) -> "CreateCRMLeadUseCase | None":
        return self._create_crm_lead_use_case

    @property
    def update_crm_lead_use_case(self) -> "UpdateCRMLeadUseCase | None":
        return self._update_crm_lead_use_case

    @property
    def bulk_delete_crm_leads_use_case(self) -> "BulkDeleteCRMLeadsUseCase | None":
        return self._bulk_delete_crm_leads_use_case

    @property
    def create_crm_campaign_use_case(self) -> "CreateCRMCampaignUseCase | None":
        return self._create_crm_campaign_use_case

    @property
    def launch_crm_campaign_use_case(self) -> "LaunchCRMCampaignUseCase | None":
        return self._launch_crm_campaign_use_case

    @property
    def handle_resend_webhook_use_case(self) -> "HandleResendWebhookUseCase | None":
        return self._handle_resend_webhook_use_case

    @property
    def list_crm_report_requests_use_case(self) -> "ListCRMReportRequestsUseCase | None":
        return self._list_crm_report_requests_use_case

    @property
    def list_crm_leads_use_case(self) -> "ListCRMLeadsUseCase | None":
        return self._list_crm_leads_use_case

    @property
    def get_crm_lead_use_case(self) -> "GetCRMLeadUseCase | None":
        return self._get_crm_lead_use_case

    @property
    def list_crm_campaigns_use_case(self) -> "ListCRMCampaignsUseCase | None":
        return self._list_crm_campaigns_use_case

    @property
    def list_crm_messages_use_case(self) -> "ListCRMMessagesUseCase | None":
        return self._list_crm_messages_use_case

    @property
    def list_crm_events_use_case(self) -> "ListCRMEventsUseCase | None":
        return self._list_crm_events_use_case

    @property
    def enqueue_geo_grid_study_job_use_case(self) -> "EnqueueGeoGridStudyJobUseCase | None":
        return self._enqueue_geo_grid_study_job_use_case

    @property
    def process_crm_lead_discovery_task_use_case(self) -> "ProcessCRMLeadDiscoveryTaskUseCase | None":
        return self._process_crm_lead_discovery_task_use_case

    @property
    def process_benchmark_study_task_use_case(self) -> "ProcessBenchmarkStudyTaskUseCase | None":
        return self._process_benchmark_study_task_use_case

    @property
    def process_crm_lead_pipeline_task_use_case(self) -> "ProcessCRMLeadPipelineTaskUseCase | None":
        return self._process_crm_lead_pipeline_task_use_case

    @property
    def process_geo_grid_study_task_use_case(self) -> "ProcessGeoGridStudyTaskUseCase | None":
        return self._process_geo_grid_study_task_use_case

    @property
    def enqueue_due_campaign_dispatch_jobs_use_case(self) -> "EnqueueDueCampaignDispatchJobsUseCase | None":
        return self._enqueue_due_campaign_dispatch_jobs_use_case

    @property
    def process_campaign_dispatch_task_use_case(self) -> "ProcessCampaignDispatchTaskUseCase | None":
        return self._process_campaign_dispatch_task_use_case

    @property
    def retry_crm_report_request_use_case(self) -> "RetryCRMReportRequestUseCase | None":
        return self._retry_crm_report_request_use_case

    @property
    def process_pending_crm_report_requests_use_case(self) -> "ProcessPendingCRMReportRequestsUseCase | None":
        return self._process_pending_crm_report_requests_use_case

    def attach_crm_queue_use_cases(
        self,
        *,
        enqueue_crm_lead_discovery_job_use_case: "EnqueueCRMLeadDiscoveryJobUseCase",
        enqueue_crm_lead_pipeline_job_use_case: "EnqueueCRMLeadPipelineJobUseCase",
        enqueue_benchmark_study_job_use_case: "EnqueueBenchmarkStudyJobUseCase",
        create_crm_report_request_use_case: "CreateCRMReportRequestUseCase",
        create_crm_report_feedback_use_case: "CreateCRMReportFeedbackUseCase",
        create_crm_lead_use_case: "CreateCRMLeadUseCase",
        update_crm_lead_use_case: "UpdateCRMLeadUseCase",
        bulk_delete_crm_leads_use_case: "BulkDeleteCRMLeadsUseCase",
        create_crm_campaign_use_case: "CreateCRMCampaignUseCase",
        launch_crm_campaign_use_case: "LaunchCRMCampaignUseCase",
        handle_resend_webhook_use_case: "HandleResendWebhookUseCase",
        list_crm_report_requests_use_case: "ListCRMReportRequestsUseCase",
        list_crm_leads_use_case: "ListCRMLeadsUseCase",
        get_crm_lead_use_case: "GetCRMLeadUseCase",
        list_crm_campaigns_use_case: "ListCRMCampaignsUseCase",
        list_crm_messages_use_case: "ListCRMMessagesUseCase",
        list_crm_events_use_case: "ListCRMEventsUseCase",
        enqueue_geo_grid_study_job_use_case: "EnqueueGeoGridStudyJobUseCase",
        process_crm_lead_discovery_task_use_case: "ProcessCRMLeadDiscoveryTaskUseCase",
        process_benchmark_study_task_use_case: "ProcessBenchmarkStudyTaskUseCase",
        process_crm_lead_pipeline_task_use_case: "ProcessCRMLeadPipelineTaskUseCase",
        process_geo_grid_study_task_use_case: "ProcessGeoGridStudyTaskUseCase",
        enqueue_due_campaign_dispatch_jobs_use_case: "EnqueueDueCampaignDispatchJobsUseCase",
        process_campaign_dispatch_task_use_case: "ProcessCampaignDispatchTaskUseCase",
        retry_crm_report_request_use_case: "RetryCRMReportRequestUseCase",
        process_pending_crm_report_requests_use_case: "ProcessPendingCRMReportRequestsUseCase",
    ) -> "CRMService":
        self._enqueue_crm_lead_discovery_job_use_case = enqueue_crm_lead_discovery_job_use_case
        self._enqueue_crm_lead_pipeline_job_use_case = enqueue_crm_lead_pipeline_job_use_case
        self._enqueue_benchmark_study_job_use_case = enqueue_benchmark_study_job_use_case
        self._create_crm_report_request_use_case = create_crm_report_request_use_case
        self._create_crm_report_feedback_use_case = create_crm_report_feedback_use_case
        self._create_crm_lead_use_case = create_crm_lead_use_case
        self._update_crm_lead_use_case = update_crm_lead_use_case
        self._bulk_delete_crm_leads_use_case = bulk_delete_crm_leads_use_case
        self._create_crm_campaign_use_case = create_crm_campaign_use_case
        self._launch_crm_campaign_use_case = launch_crm_campaign_use_case
        self._handle_resend_webhook_use_case = handle_resend_webhook_use_case
        self._list_crm_report_requests_use_case = list_crm_report_requests_use_case
        self._list_crm_leads_use_case = list_crm_leads_use_case
        self._get_crm_lead_use_case = get_crm_lead_use_case
        self._list_crm_campaigns_use_case = list_crm_campaigns_use_case
        self._list_crm_messages_use_case = list_crm_messages_use_case
        self._list_crm_events_use_case = list_crm_events_use_case
        self._enqueue_geo_grid_study_job_use_case = enqueue_geo_grid_study_job_use_case
        self._process_crm_lead_discovery_task_use_case = process_crm_lead_discovery_task_use_case
        self._process_benchmark_study_task_use_case = process_benchmark_study_task_use_case
        self._process_crm_lead_pipeline_task_use_case = process_crm_lead_pipeline_task_use_case
        self._process_geo_grid_study_task_use_case = process_geo_grid_study_task_use_case
        self._enqueue_due_campaign_dispatch_jobs_use_case = enqueue_due_campaign_dispatch_jobs_use_case
        self._process_campaign_dispatch_task_use_case = process_campaign_dispatch_task_use_case
        self._retry_crm_report_request_use_case = retry_crm_report_request_use_case
        self._process_pending_crm_report_requests_use_case = process_pending_crm_report_requests_use_case
        return self

    async def ensure_indexes(self) -> None:
        if self._indexes_ensured:
            return
        async with self._indexes_lock:
            if self._indexes_ensured:
                return
            await self._repository_bootstrap.ensure_indexes()
            await self._geo_city_repository.seed_default_cities()
            self._indexes_ensured = True

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
        normalized_source = str(source or "").strip().lower()
        if normalized_source in self._LIVE_GOOGLE_DISCOVERY_ALIASES:
            normalized_source = "auto_live_google_maps"
        if not normalized_source:
            normalized_source = "auto_live_google_maps"
        queue_name = "scrape_google_maps" if normalized_source in self._LIVE_GOOGLE_DISCOVERY_SOURCES else "crm"
        discovery_run_id: str | None = None
        if self._use_discovery_v2:
            await self.ensure_indexes()
            run_doc = await self._discovery_run_repository.create_run(
                {
                    "job_id": None,
                    "query": query,
                    "city": city,
                    "category": category,
                    "limit": limit,
                    "source": normalized_source,
                }
            )
            discovery_run_id = str(run_doc.get("discovery_run_id") or "").strip() or None

        payload = CRMLeadDiscoveryTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=normalized_source,
            discovery_run_id=discovery_run_id,
        )
        queued = await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name=queue_name,
            job_type="crm_lead_discovery",
            source="google_maps" if queue_name == "scrape_google_maps" else None,
            runtime_target=(
                DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET if queue_name == "scrape_google_maps" else "server_worker"
            ),
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            requested_by="crm_discovery_api",
            fallback_policy=(
                DEFAULT_BROWSER_FALLBACK_POLICY if queue_name == "scrape_google_maps" else "none"
            ),
            source_display_name="Google Maps" if queue_name == "scrape_google_maps" else None,
        )
        if discovery_run_id:
            await self._discovery_run_repository.append_step(
                run_id=discovery_run_id,
                step="job_enqueued",
                ok=True,
                duration_ms=0,
                data={
                    "job_id": str(queued.get("job_id") or "").strip() or None,
                    "queue_name": str(queued.get("queue_name") or "").strip() or None,
                },
            )
            queued["discovery_run_id"] = discovery_run_id
        return queued

    async def enqueue_benchmark_study_job(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
        title: str | None = None,
    ) -> dict[str, Any]:
        if self._enqueue_benchmark_study_job_use_case is not None:
            return await self._enqueue_benchmark_study_job_use_case.execute(
                query=query,
                city=city,
                category=category,
                limit=limit,
                source=source,
                title=title,
            )
        await self.ensure_indexes()
        normalized_source = str(source or "").strip().lower()
        if normalized_source in self._LIVE_GOOGLE_DISCOVERY_ALIASES:
            normalized_source = "auto_live_google_maps"
        if not normalized_source:
            normalized_source = "auto_live_google_maps"

        base_payload = BenchmarkLocalStudyTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=normalized_source,
            title=title,
        )
        run_doc = await self._benchmark_run_repository.create_run(
            {
                "title": base_payload.title,
                "query": base_payload.query,
                "city": base_payload.city,
                "category": base_payload.category,
                "limit": base_payload.limit,
                "source": base_payload.source,
            }
        )
        benchmark_run_id = str(run_doc.get("benchmark_run_id") or "").strip() or None
        payload = base_payload.model_copy(update={"benchmark_run_id": benchmark_run_id})
        queued = await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="benchmark_local_study",
        )
        await self._record_event(
            event_type="benchmark_study_job_queued",
            data={
                "crm_job_id": queued.get("job_id"),
                "benchmark_run_id": benchmark_run_id,
                "query": payload.query,
                "city": payload.city,
                "category": payload.category,
                "limit": payload.limit,
                "source": payload.source,
            },
        )
        queued["benchmark_run_id"] = benchmark_run_id
        return self._sanitize_payload(queued)

    async def list_geo_cities(self) -> list[dict[str, Any]]:
        await self.ensure_indexes()
        return self._sanitize_payload(await self._geo_city_repository.list_enabled())

    async def enqueue_geo_grid_study_job(
        self,
        *,
        keyword: str,
        city_slug: str,
        top_n: int = 10,
        provider_mode: str | None = None,
        grid_size: int | None = None,
        grid_spacing_km: float | None = None,
        uule_radius_m: int | None = None,
        throttle_ms: int | None = None,
    ) -> dict[str, Any]:
        if self._enqueue_geo_grid_study_job_use_case is not None:
            return await self._enqueue_geo_grid_study_job_use_case.execute(
                keyword=keyword,
                city_slug=city_slug,
                top_n=top_n,
                provider_mode=provider_mode,
                grid_size=grid_size,
                grid_spacing_km=grid_spacing_km,
                uule_radius_m=uule_radius_m,
                throttle_ms=throttle_ms,
            )
        await self.ensure_indexes()
        normalized_keyword = str(keyword or "").strip()
        if not normalized_keyword:
            raise ValueError("keyword is required.")
        safe_top_n = max(1, min(100, int(top_n or 10)))
        requested_provider_mode = str(provider_mode or settings.geo_grid_provider_mode or "maps_live").strip().lower()
        safe_provider_mode = requested_provider_mode if requested_provider_mode in {"maps_live", "uule"} else "maps_live"
        safe_grid_size = None
        if grid_size is not None:
            safe_grid_size = normalize_grid_size(int(grid_size))
        elif safe_provider_mode == "uule" and int(settings.geo_grid_uule_grid_size or 0) >= 3:
            safe_grid_size = normalize_grid_size(int(settings.geo_grid_uule_grid_size))
        safe_grid_spacing_km = (
            float(grid_spacing_km)
            if grid_spacing_km is not None
            else float(settings.geo_grid_uule_spacing_km or 0.4)
        )
        safe_uule_radius_m = (
            max(100, int(uule_radius_m))
            if uule_radius_m is not None
            else max(100, int(settings.geo_grid_uule_radius_m or 1000))
        )
        safe_throttle_ms = (
            max(100, int(throttle_ms))
            if throttle_ms is not None
            else max(100, int(settings.geo_grid_uule_throttle_ms or 1200))
        )
        city = await self._geo_city_repository.get_by_slug(city_slug=city_slug)
        if city is None:
            raise LookupError(f"Geo city '{city_slug}' not found.")
        city_points = city.get("points") if isinstance(city.get("points"), list) else []
        expected_point_count = int(city.get("point_count") or len(city_points))
        if safe_provider_mode == "uule" and safe_grid_size is not None and safe_grid_size >= 3:
            expected_point_count = int(safe_grid_size) * int(safe_grid_size)

        run = await self._geo_grid_run_repository.create_run(
            {
                "keyword": normalized_keyword,
                "city": city.get("city"),
                "city_slug": city.get("city_slug"),
                "center": city.get("center"),
                "points": city_points,
                "point_count": expected_point_count,
                "top_n": safe_top_n,
                "provider_mode": safe_provider_mode,
                "grid_size": safe_grid_size,
                "grid_spacing_km": safe_grid_spacing_km,
                "uule_radius_m": safe_uule_radius_m,
                "throttle_ms": safe_throttle_ms,
            }
        )
        geo_grid_run_id = str(run.get("geo_grid_run_id") or "").strip()
        payload = GeoGridStudyTaskPayload(geo_grid_run_id=geo_grid_run_id)
        queued = await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name="scrape_google_maps",
            job_type="geo_grid_study",
            source="google_maps",
            runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            requested_by="crm_geo_grid_api",
            fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
            source_display_name="Google Maps",
        )
        await self._geo_grid_run_repository.set_job_id(
            geo_grid_run_id=geo_grid_run_id,
            job_id=str(queued.get("job_id") or "").strip() or None,
        )
        await self._record_event(
            event_type="geo_grid_study_job_queued",
            data={
                "job_id": queued.get("job_id"),
                "geo_grid_run_id": geo_grid_run_id,
                "keyword": normalized_keyword,
                "city_slug": city.get("city_slug"),
                "top_n": safe_top_n,
                "provider_mode": safe_provider_mode,
                "grid_size": safe_grid_size,
                "grid_spacing_km": safe_grid_spacing_km,
                "uule_radius_m": safe_uule_radius_m,
                "throttle_ms": safe_throttle_ms,
            },
        )
        queued["geo_grid_run_id"] = geo_grid_run_id
        return self._sanitize_payload(queued)

    async def list_geo_grid_runs(
        self,
        *,
        page: int,
        page_size: int,
        city_slug: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        return self._sanitize_payload(
            await self._geo_grid_run_repository.list_runs(
                page=page,
                page_size=page_size,
                city_slug=city_slug,
                status_filter=status_filter,
            )
        )

    async def get_geo_grid_run(self, *, geo_grid_run_id: str) -> dict[str, Any]:
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        return self._sanitize_payload(run)

    async def list_geo_grid_results(self, *, geo_grid_run_id: str) -> list[dict[str, Any]]:
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        return self._sanitize_payload(await self._geo_grid_result_repository.list_results(geo_grid_run_id=geo_grid_run_id))

    async def get_geo_grid_stats(self, *, geo_grid_run_id: str) -> dict[str, Any]:
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        results = await self._geo_grid_result_repository.list_results(geo_grid_run_id=geo_grid_run_id)
        return self._sanitize_payload(self._build_geo_grid_stats(run=run, results=results))

    async def create_report_request(
        self,
        *,
        business_name: str,
        city: str | None,
        category: str | None = None,
        contact_name: str | None = None,
        email: str,
        phone: str | None = None,
        website: str | None = None,
        message: str | None = None,
        consent_report: bool,
        consent_marketing: bool = False,
        utm: dict[str, Any] | None = None,
        source_page: str | None = None,
    ) -> dict[str, Any]:
        if self._create_crm_report_request_use_case is not None:
            return await self._create_crm_report_request_use_case.execute(
                business_name=business_name,
                city=city,
                category=category,
                contact_name=contact_name,
                email=email,
                phone=phone,
                website=website,
                message=message,
                consent_report=consent_report,
                consent_marketing=consent_marketing,
                utm=utm,
                source_page=source_page,
            )
        await self.ensure_indexes()
        normalized_business_name = str(business_name or "").strip()
        normalized_email = str(email or "").strip()
        if not normalized_business_name:
            raise ValueError("business_name is required.")
        normalized_email_address = self._normalize_email(normalized_email)
        if not normalized_email_address:
            raise ValueError("valid email is required.")
        if not consent_report:
            raise ValueError("consent_report is required to send the requested report.")

        now = self._now_utc()
        query = " ".join(item for item in (normalized_business_name, str(city or "").strip()) if item)
        doc: dict[str, Any] = {
            "business_name": normalized_business_name,
            "business_name_normalized": self._normalize_text(normalized_business_name),
            "city": str(city or "").strip() or None,
            "category": str(category or "").strip() or None,
            "contact_name": str(contact_name or "").strip() or None,
            "email": normalized_email,
            "email_normalized": normalized_email_address,
            "phone": str(phone or "").strip() or None,
            "website": str(website or "").strip() or None,
            "message": str(message or "").strip() or None,
            "query": query,
            "status": "queued",
            "source": "landing_report_request",
            "source_page": str(source_page or "").strip() or None,
            "utm": self._normalize_utm(utm or {}),
            "consents": {
                "report_delivery": {
                    "granted": True,
                    "granted_at": now,
                    "text": "Acepto recibir por email el informe solicitado y comunicaciones necesarias para entregarlo.",
                },
                "marketing": {
                    "granted": bool(consent_marketing),
                    "granted_at": now if consent_marketing else None,
                    "text": "Acepto recibir contenido comercial y seguimiento opcional.",
                },
            },
            "created_at": now,
            "updated_at": now,
            "benchmark_run_id": None,
            "job_id": None,
            "failure_reason": None,
        }
        inserted = await get_database()[self._REPORT_REQUESTS_COLLECTION].insert_one(doc)
        doc["_id"] = inserted.inserted_id
        report_request_id = str(inserted.inserted_id)

        try:
            update_fields = await self._enqueue_report_request_doc(doc)
            await get_database()[self._REPORT_REQUESTS_COLLECTION].update_one(
                {"_id": inserted.inserted_id},
                {"$set": update_fields},
            )
            doc.update(update_fields)
        except Exception as exc:
            update_fields = {
                "status": "failed_to_queue",
                "failure_reason": str(exc),
                "updated_at": self._now_utc(),
            }
            await get_database()[self._REPORT_REQUESTS_COLLECTION].update_one(
                {"_id": inserted.inserted_id},
                {"$set": update_fields},
            )
            doc.update(update_fields)

        await self._record_event(
            event_type="report_request_created",
            data={
                "report_request_id": report_request_id,
                "business_name": normalized_business_name,
                "city": doc.get("city"),
                "email": normalized_email_address,
                "consent_marketing": bool(consent_marketing),
                "status": doc.get("status"),
                "benchmark_run_id": doc.get("benchmark_run_id"),
                "job_id": doc.get("job_id"),
                "failure_reason": doc.get("failure_reason"),
                "utm": doc.get("utm"),
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_request_id"))

    async def list_report_requests(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        if self._list_crm_report_requests_use_case is not None:
            return await self._list_crm_report_requests_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                q=q,
            )
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query: dict[str, Any] = {}
        if str(status_filter or "").strip():
            query["status"] = str(status_filter).strip()
        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]

        collection = get_database()[self._REPORT_REQUESTS_COLLECTION]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key="report_request_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def retry_report_request(self, *, report_request_id: str) -> dict[str, Any]:
        if self._retry_crm_report_request_use_case is not None:
            return await self._retry_crm_report_request_use_case.execute(
                report_request_id=report_request_id,
            )
        await self.ensure_indexes()
        parsed_id = self._parse_object_id(report_request_id, field_name="report_request_id")
        collection = get_database()[self._REPORT_REQUESTS_COLLECTION]
        doc = await collection.find_one({"_id": parsed_id})
        if doc is None:
            raise LookupError(f"Report request '{report_request_id}' not found.")
        update_fields = await self._enqueue_report_request_doc(doc)
        await collection.update_one({"_id": parsed_id}, {"$set": update_fields})
        doc.update(update_fields)
        await self._record_event(
            event_type="report_request_retried",
            data={
                "report_request_id": report_request_id,
                "job_id": doc.get("job_id"),
                "benchmark_run_id": doc.get("benchmark_run_id"),
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_request_id"))

    async def process_pending_report_requests(self, *, limit: int = 50) -> dict[str, Any]:
        if self._process_pending_crm_report_requests_use_case is not None:
            return await self._process_pending_crm_report_requests_use_case.execute(limit=limit)
        await self.ensure_indexes()
        limit_value = max(1, min(int(limit or 50), 200))
        collection = get_database()[self._REPORT_REQUESTS_COLLECTION]
        query = {
            "$or": [
                {"status": {"$in": ["pending", "failed_to_queue"]}},
                {"job_id": None},
            ]
        }
        docs = (
            await collection.find(query)
            .sort([("created_at", 1), ("_id", 1)])
            .limit(limit_value)
            .to_list(length=limit_value)
        )
        processed = 0
        retried = 0
        failed = 0
        errors: list[dict[str, Any]] = []
        for doc in docs:
            processed += 1
            report_request_id = str(doc.get("_id"))
            try:
                update_fields = await self._enqueue_report_request_doc(doc)
                await collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})
                retried += 1
            except Exception as exc:
                failed += 1
                errors.append({"report_request_id": report_request_id, "error": str(exc)})
                await collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "status": "failed_to_queue",
                            "failure_reason": str(exc),
                            "updated_at": self._now_utc(),
                        }
                    },
                )

        if processed > 0:
            await self._record_event(
                event_type="report_requests_pending_processed",
                data={"processed": processed, "retried": retried, "failed": failed, "limit": limit_value},
            )
        return self._sanitize_payload(
            {
                "processed": processed,
                "retried": retried,
                "failed": failed,
                "errors": errors,
            }
        )

    async def create_report_feedback(
        self,
        *,
        branch: str,
        answers: dict[str, Any] | None = None,
        lead_id: str | None = None,
        report_request_id: str | None = None,
        lead_report_id: str | None = None,
        benchmark_business_id: str | None = None,
        report_kind: str | None = None,
        source_page: str | None = None,
        referrer: str | None = None,
        user_agent: str | None = None,
        ip_hash: str | None = None,
    ) -> dict[str, Any]:
        if self._create_crm_report_feedback_use_case is not None:
            return await self._create_crm_report_feedback_use_case.execute(
                branch=branch,
                answers=answers,
                lead_id=lead_id,
                report_request_id=report_request_id,
                lead_report_id=lead_report_id,
                benchmark_business_id=benchmark_business_id,
                report_kind=report_kind,
                source_page=source_page,
                referrer=referrer,
                user_agent=user_agent,
                ip_hash=ip_hash,
            )
        await self.ensure_indexes()
        normalized_branch = str(branch or "").strip().upper()
        if normalized_branch not in {"A", "B", "C"}:
            raise ValueError("branch must be one of A, B or C.")

        normalized_lead_id = str(lead_id or "").strip() or None
        normalized_report_request_id = str(report_request_id or "").strip() or None
        normalized_lead_report_id = str(lead_report_id or "").strip() or None
        normalized_benchmark_business_id = str(benchmark_business_id or "").strip() or None
        normalized_report_kind = str(report_kind or "").strip().lower() or "lead"
        normalized_source_page = str(source_page or "").strip() or None
        normalized_referrer = str(referrer or "").strip() or None
        normalized_user_agent = str(user_agent or "").strip() or None
        normalized_ip_hash = str(ip_hash or "").strip() or None
        payload_answers = dict(answers or {})

        if not any(
            (
                normalized_lead_id,
                normalized_report_request_id,
                normalized_lead_report_id,
                normalized_benchmark_business_id,
            )
        ):
            raise ValueError(
                "At least one identifier is required (lead_id, report_request_id, lead_report_id or benchmark_business_id)."
            )

        label = "warm_lead"
        if normalized_branch == "A":
            label = "hot_lead"
        elif normalized_branch == "C":
            reasons = payload_answers.get("c1_reasons")
            reason_values = (
                [str(item).strip().lower() for item in reasons]
                if isinstance(reasons, list)
                else [str(reasons or "").strip().lower()]
            )
            label = "recoverable" if "ia_gratis" in reason_values else "cold_lead"

        now = self._now_utc()
        doc: dict[str, Any] = {
            "branch": normalized_branch,
            "label": label,
            "report_kind": normalized_report_kind,
            "lead_id": normalized_lead_id,
            "report_request_id": normalized_report_request_id,
            "lead_report_id": normalized_lead_report_id,
            "benchmark_business_id": normalized_benchmark_business_id,
            "answers": payload_answers,
            "source_page": normalized_source_page,
            "referrer": normalized_referrer,
            "user_agent": normalized_user_agent,
            "ip_hash": normalized_ip_hash,
            "created_at": now,
            "updated_at": now,
        }
        inserted = await get_database()[self._REPORT_FEEDBACK_COLLECTION].insert_one(doc)
        doc["_id"] = inserted.inserted_id
        feedback_id = str(inserted.inserted_id)

        if normalized_report_request_id:
            parsed_request_id = self._parse_object_id(normalized_report_request_id, field_name="report_request_id")
            await get_database()[self._REPORT_REQUESTS_COLLECTION].update_one(
                {"_id": parsed_request_id},
                {
                    "$set": {
                        "feedback.latest_feedback_id": feedback_id,
                        "feedback.branch": normalized_branch,
                        "feedback.label": label,
                        "feedback.answers": payload_answers,
                        "feedback.updated_at": now,
                        "updated_at": now,
                    }
                },
            )

        if normalized_lead_report_id:
            parsed_lead_report_id = self._parse_object_id(normalized_lead_report_id, field_name="lead_report_id")
            await get_database()["lead_reports"].update_one(
                {"_id": parsed_lead_report_id},
                {
                    "$set": {
                        "feedback.latest_feedback_id": feedback_id,
                        "feedback.branch": normalized_branch,
                        "feedback.label": label,
                        "feedback.answers": payload_answers,
                        "feedback.updated_at": now,
                        "updated_at": now,
                    }
                },
            )

        if normalized_lead_id:
            parsed_lead_id = self._parse_object_id(normalized_lead_id, field_name="lead_id")
            await get_database()[self._LEADS_COLLECTION].update_one(
                {"_id": parsed_lead_id},
                {
                    "$set": {
                        "status": "form_2_done",
                        "updated_at": now,
                        "source_ref.last_feedback_id": feedback_id,
                    },
                    "$addToSet": {
                        "tags": label,
                        "notes": f"Feedback formulario final rama {normalized_branch} ({label}) · {now.isoformat()}",
                    },
                },
            )

        await self._record_event(
            event_type="report_feedback_submitted",
            lead_id=normalized_lead_id,
            data={
                "report_feedback_id": feedback_id,
                "branch": normalized_branch,
                "label": label,
                "lead_id": normalized_lead_id,
                "report_request_id": normalized_report_request_id,
                "lead_report_id": normalized_lead_report_id,
                "benchmark_business_id": normalized_benchmark_business_id,
                "report_kind": normalized_report_kind,
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_feedback_id"))

    async def enqueue_lead_pipeline_job(
        self,
        *,
        lead_id: str,
        force: bool = False,
        sources: list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict[str, Any]:
        if self._enqueue_crm_lead_pipeline_job_use_case is not None:
            return await self._enqueue_crm_lead_pipeline_job_use_case.execute(
                lead_id=lead_id,
                force=force,
                sources=sources,
                google_maps_name=google_maps_name,
                tripadvisor_name=tripadvisor_name,
            )
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        payload = CRMLeadPipelineTaskPayload(
            lead_id=lead_id,
            force=force,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )
        queued_job = await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="crm_lead_pipeline",
        )
        now = self._now_utc()
        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_QUEUED.value,
                    "updated_at": now,
                }
            },
        )
        await self._record_event(
            event_type="lead_pipeline_job_queued",
            lead_id=lead_id,
            data={
                "crm_job_id": queued_job.get("job_id"),
                "sources": payload.sources,
                "force": payload.force,
            },
        )
        return self._sanitize_payload(queued_job)

    async def list_leads(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        consent_filter: str | None = None,
        source_filter: str | None = None,
        q: str | None = None,
        sort_by: str = "updated_at",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        if self._list_crm_leads_use_case is not None:
            return await self._list_crm_leads_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
        await self.ensure_indexes()
        if self._use_repo_v2:
            payload = await self._lead_repository.list(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            return self._sanitize_payload(payload)

        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        leads = get_database()[self._LEADS_COLLECTION]
        query = self._build_leads_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )
        sort_spec = self._resolve_leads_sort(sort_by=sort_by, sort_dir=sort_dir)

        total = await leads.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await leads.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="lead_id") for doc in docs]
        payload = build_pagination_payload(
            items=items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        return self._sanitize_payload(payload)

    async def create_lead(
        self,
        *,
        business_name: str,
        contact_name: str | None = None,
        email: str | None = None,
        phone: str | None = None,
        website: str | None = None,
        category: str | None = None,
        city: str | None = None,
        address: str | None = None,
        source: str | None = None,
        status: str | None = None,
        notes: list[str] | None = None,
        tags: list[str] | None = None,
        do_not_contact: bool | None = None,
        consent_status: str | None = None,
        suppressed_reason: str | None = None,
        unsubscribed: bool | None = None,
        consent_proof: dict[str, Any] | None = None,
        source_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._create_crm_lead_use_case is not None:
            return await self._create_crm_lead_use_case.execute(
                business_name=business_name,
                contact_name=contact_name,
                email=email,
                phone=phone,
                website=website,
                category=category,
                city=city,
                address=address,
                source=source,
                status=status,
                notes=notes,
                tags=tags,
                do_not_contact=do_not_contact,
                consent_status=consent_status,
                suppressed_reason=suppressed_reason,
                unsubscribed=unsubscribed,
                consent_proof=consent_proof,
                source_ref=source_ref,
            )
        await self.ensure_indexes()

        normalized_business_name = str(business_name or "").strip()
        if not normalized_business_name:
            raise ValueError("business_name is required.")

        normalized_status = str(status or CRMLeadStatus.NEW.value).strip().lower() or CRMLeadStatus.NEW.value
        normalized_consent = str(consent_status or CRMConsentStatus.MISSING.value).strip().lower() or CRMConsentStatus.MISSING.value
        allowed_consents = {
            CRMConsentStatus.MISSING.value,
            CRMConsentStatus.GRANTED.value,
            CRMConsentStatus.REVOKED.value,
            CRMConsentStatus.DENIED.value,
        }
        if normalized_consent not in allowed_consents:
            raise ValueError("Invalid consent_status.")

        normalized_email = str(email or "").strip() or None
        normalized_phone = str(phone or "").strip() or None
        normalized_website = str(website or "").strip() or None
        normalized_category = str(category or "").strip() or None
        normalized_city = str(city or "").strip() or None
        normalized_address = str(address or "").strip() or None
        normalized_source = str(source or "manual").strip().lower() or "manual"
        normalized_notes = [str(item or "").strip() for item in list(notes or []) if str(item or "").strip()]
        normalized_tags = [str(item or "").strip().lower() for item in list(tags or []) if str(item or "").strip()]
        normalized_contact_name = str(contact_name or "").strip() or None

        email_normalized = self._normalize_email(normalized_email)
        domain_normalized = self._domain_from_email_or_website(email=normalized_email, website=normalized_website)

        now = self._now_utc()
        doc: dict[str, Any] = {
            "business_name": normalized_business_name,
            "business_name_normalized": self._normalize_text(normalized_business_name),
            "email": normalized_email,
            "email_normalized": email_normalized,
            "domain_normalized": domain_normalized,
            "phone": normalized_phone,
            "website": normalized_website,
            "category": normalized_category,
            "city": normalized_city,
            "address": normalized_address,
            "source": normalized_source,
            "source_ref": source_ref or {},
            "rating": None,
            "review_count": None,
            "status": normalized_status,
            "score": 0.0,
            "legal": {
                "consent_status": normalized_consent,
                "consent_proof": consent_proof,
                "do_not_contact": bool(do_not_contact),
                "unsubscribed_at": now if bool(unsubscribed) else None,
                "suppressed_reason": str(suppressed_reason or "").strip() or None,
            },
            "pipeline": {
                "business_id": None,
                "source_job_ids": [],
                "analysis_job_id": None,
                "report_job_id": None,
                "latest_report_artifacts": {},
            },
            "notes": normalized_notes,
            "tags": normalized_tags,
            "created_at": now,
            "updated_at": now,
        }
        if normalized_contact_name:
            doc["source_ref"]["contact_name"] = normalized_contact_name

        leads = get_database()[self._LEADS_COLLECTION]
        inserted = await leads.insert_one(doc)
        doc["_id"] = inserted.inserted_id

        await self._record_event(
            event_type="lead_created_manual",
            lead_id=str(inserted.inserted_id),
            data={
                "source": normalized_source,
                "status": normalized_status,
                "consent_status": normalized_consent,
                "contact_name": normalized_contact_name,
            },
        )

        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="lead_id"))

    async def bulk_delete_leads(
        self,
        *,
        lead_ids: list[str] | None = None,
        delete_all_matching: bool = False,
        exclude_lead_ids: list[str] | None = None,
        status_filter: str | None = None,
        consent_filter: str | None = None,
        source_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        if self._bulk_delete_crm_leads_use_case is not None:
            return await self._bulk_delete_crm_leads_use_case.execute(
                lead_ids=lead_ids,
                delete_all_matching=delete_all_matching,
                exclude_lead_ids=exclude_lead_ids,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
        await self.ensure_indexes()
        if self._use_repo_v2:
            result = await self._lead_repository.bulk_delete(
                lead_ids=lead_ids,
                delete_all_matching=delete_all_matching,
                exclude_lead_ids=exclude_lead_ids,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
            await self._record_event(
                event_type="leads_bulk_deleted",
                data=result,
            )
            return self._sanitize_payload(
                {
                    "deleted_count": int(result.get("deleted_count") or 0),
                    "matched_count": int(result.get("matched_count") or 0),
                    "delete_all_matching": bool(result.get("delete_all_matching")),
                    "requested_ids": int(result.get("requested_ids") or 0),
                    "excluded_ids": int(result.get("excluded_ids") or 0),
                }
            )

        leads = get_database()[self._LEADS_COLLECTION]

        normalized_ids: list[ObjectId] = []
        raw_ids = list(lead_ids or [])
        seen_ids: set[str] = set()
        for raw_id in raw_ids:
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(self._parse_object_id(normalized, field_name="lead_id"))

        excluded_ids: list[ObjectId] = []
        raw_excluded_ids = list(exclude_lead_ids or [])
        seen_excluded_ids: set[str] = set()
        for raw_id in raw_excluded_ids:
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_excluded_ids:
                continue
            seen_excluded_ids.add(normalized)
            excluded_ids.append(self._parse_object_id(normalized, field_name="exclude_lead_id"))

        if not normalized_ids and not bool(delete_all_matching):
            raise ValueError("Specify lead_ids or set delete_all_matching=true.")

        if normalized_ids:
            query: dict[str, Any] = {"_id": {"$in": normalized_ids}}
        else:
            query = self._build_leads_query(
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
            if excluded_ids:
                query["_id"] = {"$nin": excluded_ids}

        matched_count = await leads.count_documents(query)
        deleted_result = await leads.delete_many(query)
        deleted_count = int(deleted_result.deleted_count)
        await self._record_event(
            event_type="leads_bulk_deleted",
            data={
                "delete_all_matching": bool(delete_all_matching),
                "requested_ids": len(normalized_ids),
                "excluded_ids": len(excluded_ids),
                "matched_count": int(matched_count),
                "deleted_count": deleted_count,
                "filters": {
                    "status": str(status_filter or "").strip() or None,
                    "consent_status": str(consent_filter or "").strip() or None,
                    "source": str(source_filter or "").strip() or None,
                    "q": str(q or "").strip() or None,
                },
            },
        )
        return self._sanitize_payload(
            {
                "deleted_count": deleted_count,
                "matched_count": int(matched_count),
                "delete_all_matching": bool(delete_all_matching),
                "requested_ids": len(normalized_ids),
                "excluded_ids": len(excluded_ids),
            }
        )

    async def get_lead(self, *, lead_id: str, sync_pipeline_refs: bool = True) -> dict[str, Any]:
        if self._get_crm_lead_use_case is not None:
            return await self._get_crm_lead_use_case.execute(
                lead_id=lead_id,
                sync_pipeline_refs=sync_pipeline_refs,
            )
        await self.ensure_indexes()
        if self._use_repo_v2:
            lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
        else:
            parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
            leads = get_database()[self._LEADS_COLLECTION]
            lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        if sync_pipeline_refs:
            await self.sync_lead_pipeline_refs(lead_id=lead_id)
            if self._use_repo_v2:
                lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            else:
                lead_doc = await leads.find_one({"_id": parsed_lead_id})
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

    def _build_leads_query(
        self,
        *,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        normalized_consent = str(consent_filter or "").strip().lower()
        if normalized_consent:
            query["legal.consent_status"] = normalized_consent

        normalized_source = str(source_filter or "").strip().lower()
        if normalized_source:
            query["source"] = normalized_source

        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"website": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]
        return query

    def _resolve_leads_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        normalized_sort_by = str(sort_by or "updated_at").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("Invalid sort_dir. Use 'asc' or 'desc'.")

        field_map = {
            "updated_at": "updated_at",
            "business_name": "business_name_normalized",
            "score": "score",
            "status": "status",
            "consent_status": "legal.consent_status",
            "source": "source",
        }
        field_name = field_map.get(normalized_sort_by)
        if field_name is None:
            raise ValueError(
                "Invalid sort_by. Use 'updated_at', 'business_name', 'score', 'status', 'consent_status' or 'source'."
            )

        direction = -1 if normalized_sort_dir == "desc" else 1
        return [(field_name, direction), ("_id", direction)]

    async def update_lead(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        if self._update_crm_lead_use_case is not None:
            return await self._update_crm_lead_use_case.execute(lead_id=lead_id, updates=updates)
        await self.ensure_indexes()
        if self._use_repo_v2:
            return await self._update_lead_v2(lead_id=lead_id, updates=updates)

        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]

        set_fields: dict[str, Any] = {}
        now = self._now_utc()

        if "status" in updates and updates.get("status") is not None:
            set_fields["status"] = str(updates.get("status")).strip().lower()

        for text_field in ("business_name", "email", "phone", "website", "category", "city", "address"):
            if text_field in updates:
                value = updates.get(text_field)
                if value is None:
                    continue
                cleaned = str(value).strip()
                set_fields[text_field] = cleaned or None

        if "email" in set_fields:
            email_norm = self._normalize_email(set_fields.get("email"))
            set_fields["email_normalized"] = email_norm
            set_fields["domain_normalized"] = self._domain_from_email_or_website(
                email=set_fields.get("email"), website=set_fields.get("website")
            )

        if "business_name" in set_fields and set_fields.get("business_name"):
            set_fields["business_name_normalized"] = self._normalize_text(str(set_fields["business_name"]))

        if "do_not_contact" in updates:
            set_fields["legal.do_not_contact"] = bool(updates.get("do_not_contact"))

        if "consent_status" in updates and updates.get("consent_status") is not None:
            set_fields["legal.consent_status"] = str(updates.get("consent_status")).strip().lower()

        if "suppressed_reason" in updates:
            reason = str(updates.get("suppressed_reason") or "").strip()
            set_fields["legal.suppressed_reason"] = reason or None

        consent_proof_payload = updates.get("consent_proof")
        if isinstance(consent_proof_payload, dict):
            proof = CRMConsentProof.model_validate(consent_proof_payload)
            set_fields["legal.consent_proof"] = proof.model_dump(mode="python")
            set_fields["legal.consent_status"] = CRMConsentStatus.GRANTED.value

        if "unsubscribed" in updates:
            unsubscribed = bool(updates.get("unsubscribed"))
            set_fields["legal.unsubscribed_at"] = now if unsubscribed else None
            if unsubscribed:
                set_fields["legal.do_not_contact"] = True
                set_fields["legal.suppressed_reason"] = "unsubscribed"

        if not set_fields:
            return await self.get_lead(lead_id=lead_id, sync_pipeline_refs=False)

        set_fields["updated_at"] = now
        updated = await leads.find_one_and_update(
            {"_id": parsed_lead_id},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        await self._record_event(
            event_type="lead_updated",
            lead_id=lead_id,
            data={"fields": sorted(set_fields.keys())},
        )

        if set_fields.get("legal.suppressed_reason") or set_fields.get("legal.do_not_contact"):
            email_norm = self._normalize_email(updated.get("email"))
            email_value = str(updated.get("email") or "").strip()
            if email_norm and email_value:
                await self._upsert_suppression(
                    email=email_value,
                    reason=str(set_fields.get("legal.suppressed_reason") or "manual"),
                    source="manual",
                )

        return self._sanitize_payload(self._serialize_mongo_doc(updated, id_key="lead_id"))

    async def _update_lead_v2(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")

        set_fields: dict[str, Any] = {}
        now = self._now_utc()

        if "status" in updates and updates.get("status") is not None:
            set_fields["status"] = str(updates.get("status")).strip().lower()

        for text_field in ("business_name", "email", "phone", "website", "category", "city", "address"):
            if text_field in updates:
                value = updates.get(text_field)
                if value is None:
                    continue
                cleaned = str(value).strip()
                set_fields[text_field] = cleaned or None

        if "email" in set_fields:
            email_norm = self._normalize_email(set_fields.get("email"))
            set_fields["email_normalized"] = email_norm
            set_fields["domain_normalized"] = self._domain_from_email_or_website(
                email=set_fields.get("email"), website=set_fields.get("website")
            )

        if "business_name" in set_fields and set_fields.get("business_name"):
            set_fields["business_name_normalized"] = self._normalize_text(str(set_fields["business_name"]))

        if "do_not_contact" in updates:
            set_fields["legal.do_not_contact"] = bool(updates.get("do_not_contact"))

        if "consent_status" in updates and updates.get("consent_status") is not None:
            set_fields["legal.consent_status"] = str(updates.get("consent_status")).strip().lower()

        if "suppressed_reason" in updates:
            reason = str(updates.get("suppressed_reason") or "").strip()
            set_fields["legal.suppressed_reason"] = reason or None

        consent_proof_payload = updates.get("consent_proof")
        if isinstance(consent_proof_payload, dict):
            proof = CRMConsentProof.model_validate(consent_proof_payload)
            set_fields["legal.consent_proof"] = proof.model_dump(mode="python")
            set_fields["legal.consent_status"] = CRMConsentStatus.GRANTED.value

        if "unsubscribed" in updates:
            unsubscribed = bool(updates.get("unsubscribed"))
            set_fields["legal.unsubscribed_at"] = now if unsubscribed else None
            if unsubscribed:
                set_fields["legal.do_not_contact"] = True
                set_fields["legal.suppressed_reason"] = "unsubscribed"

        if not set_fields:
            lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")
            return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

        set_fields["updated_at"] = now
        updated = await self._lead_repository.find_one_and_update(
            {"_id": parsed_lead_id},
            {"$set": set_fields},
        )
        if updated is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        await self._record_event(
            event_type="lead_updated",
            lead_id=lead_id,
            data={"fields": sorted(set_fields.keys())},
        )

        if set_fields.get("legal.suppressed_reason") or set_fields.get("legal.do_not_contact"):
            email_norm = self._normalize_email(updated.get("email"))
            email_value = str(updated.get("email") or "").strip()
            if email_norm and email_value:
                await self._upsert_suppression(
                    email=email_value,
                    reason=str(set_fields.get("legal.suppressed_reason") or "manual"),
                    source="manual",
                )

        return self._sanitize_payload(self._serialize_mongo_doc(updated, id_key="lead_id"))

    async def list_campaigns(
        self,
        *,
        page: int = 1,
        page_size: int = 30,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        if self._list_crm_campaigns_use_case is not None:
            return await self._list_crm_campaigns_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
            )
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        campaigns = get_database()[self._CAMPAIGNS_COLLECTION]

        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        total = await campaigns.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await campaigns.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="campaign_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def create_campaign(
        self,
        *,
        name: str,
        description: str | None = None,
        audience_filter: dict[str, Any] | None = None,
        source_mode: str = "auto",
        selected_source: str | None = None,
        cadence_template_id: str | None = None,
    ) -> dict[str, Any]:
        if self._create_crm_campaign_use_case is not None:
            return await self._create_crm_campaign_use_case.execute(
                name=name,
                description=description,
                audience_filter=audience_filter,
                source_mode=source_mode,
                selected_source=selected_source,
                cadence_template_id=cadence_template_id,
            )
        await self.ensure_indexes()
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("Campaign name cannot be empty.")
        cadence_doc = await self._resolve_cadence_template(cadence_template_id)
        now = self._now_utc()

        campaign = CRMCampaign(
            name=clean_name,
            description=str(description or "").strip() or None,
            status=CRMCampaignStatus.DRAFT,
            source_mode=str(source_mode or "auto").strip().lower() or "auto",
            selected_source=(str(selected_source).strip().lower() if selected_source else None),
            cadence_template_id=str(cadence_doc.get("_id")),
            audience_filter=dict(audience_filter or {}),
            metrics={
                "targeted_leads": 0,
                "messages_created": 0,
                "messages_sent": 0,
                "messages_delivered": 0,
                "messages_opened": 0,
                "messages_clicked": 0,
                "messages_replied": 0,
                "messages_bounced": 0,
                "messages_unsubscribed": 0,
                "messages_failed": 0,
            },
            created_at=now,
            updated_at=now,
        )
        campaigns = get_database()[self._CAMPAIGNS_COLLECTION]
        inserted = await campaigns.insert_one(campaign.model_dump(mode="python"))
        created_doc = await campaigns.find_one({"_id": inserted.inserted_id})
        if created_doc is None:
            raise RuntimeError("Campaign could not be loaded after insert.")

        campaign_id = str(inserted.inserted_id)
        await self._record_event(
            event_type="campaign_created",
            campaign_id=campaign_id,
            data={"name": campaign.name, "cadence_template_id": campaign.cadence_template_id},
        )
        return self._sanitize_payload(self._serialize_mongo_doc(created_doc, id_key="campaign_id"))

    async def launch_campaign(self, *, campaign_id: str) -> dict[str, Any]:
        if self._launch_crm_campaign_use_case is not None:
            return await self._launch_crm_campaign_use_case.execute(campaign_id=campaign_id)
        await self.ensure_indexes()
        parsed_campaign_id = self._parse_object_id(campaign_id, field_name="campaign_id")
        database = get_database()
        campaigns = database[self._CAMPAIGNS_COLLECTION]
        leads_collection = database[self._LEADS_COLLECTION]
        messages_collection = database[self._MESSAGES_COLLECTION]

        campaign = await campaigns.find_one({"_id": parsed_campaign_id})
        if campaign is None:
            raise LookupError(f"Campaign '{campaign_id}' not found.")

        status_value = str(campaign.get("status") or "").strip().lower()
        if status_value not in {CRMCampaignStatus.DRAFT.value, CRMCampaignStatus.PAUSED.value}:
            raise ValueError("Only draft or paused campaigns can be launched.")

        cadence_doc = await self._resolve_cadence_template(str(campaign.get("cadence_template_id") or ""))
        steps_raw = cadence_doc.get("steps") if isinstance(cadence_doc.get("steps"), list) else []
        cadence_steps = [CRMCadenceStep.model_validate(item) for item in steps_raw if isinstance(item, dict)]
        if not cadence_steps:
            raise ValueError("Campaign cadence has no valid steps.")

        lead_query = self._build_campaign_lead_query(campaign.get("audience_filter"))
        leads = await leads_collection.find(lead_query).sort([("updated_at", -1), ("_id", -1)]).to_list(length=2000)
        suppressed_emails = await self._load_suppressed_emails()

        created_messages = 0
        targeted_leads = 0
        now = self._now_utc()

        message_docs: list[dict[str, Any]] = []
        for lead in leads:
            email = str(lead.get("email") or "").strip()
            email_normalized = self._normalize_email(email)
            if not email or not email_normalized:
                continue
            if email_normalized in suppressed_emails:
                continue

            lead_id = str(lead.get("_id"))
            mini_report = await self._build_mini_report_for_lead(lead_doc=lead)
            targeted_leads += 1
            for step in cadence_steps:
                scheduled_at = now + timedelta(days=int(step.delay_days))
                rendered_subject, rendered_body = self._render_cadence_step(
                    step=step,
                    lead_doc=lead,
                    mini_report=mini_report,
                )
                message = CRMMessage(
                    campaign_id=campaign_id,
                    lead_id=lead_id,
                    step_order=int(step.step_order),
                    step_key=str(step.step_key),
                    scheduled_at=scheduled_at,
                    status=CRMMessageStatus.QUEUED,
                    to_email=email,
                    subject=rendered_subject,
                    body=rendered_body,
                    provider="resend",
                    created_at=now,
                    updated_at=now,
                )
                message_docs.append(message.model_dump(mode="python"))
                created_messages += 1

        if message_docs:
            await messages_collection.insert_many(message_docs)

        await campaigns.update_one(
            {"_id": parsed_campaign_id},
            {
                "$set": {
                    "status": CRMCampaignStatus.ACTIVE.value,
                    "launched_at": now,
                    "updated_at": now,
                    "metrics.targeted_leads": targeted_leads,
                    "metrics.messages_created": created_messages,
                }
            },
        )

        await self._record_event(
            event_type="campaign_launched",
            campaign_id=campaign_id,
            data={
                "targeted_leads": targeted_leads,
                "messages_created": created_messages,
                "cadence_template_id": str(cadence_doc.get("_id")),
            },
        )

        queued_dispatch_jobs = await self.enqueue_due_campaign_dispatch_jobs(campaign_id=campaign_id, limit=500)
        return self._sanitize_payload(
            {
                "campaign_id": campaign_id,
                "status": CRMCampaignStatus.ACTIVE.value,
                "targeted_leads": targeted_leads,
                "messages_created": created_messages,
                "dispatch_jobs_queued": queued_dispatch_jobs,
            }
        )

    async def enqueue_due_campaign_dispatch_jobs(self, *, campaign_id: str | None = None, limit: int = 200) -> int:
        if self._enqueue_due_campaign_dispatch_jobs_use_case is not None:
            return await self._enqueue_due_campaign_dispatch_jobs_use_case.execute(
                campaign_id=campaign_id,
                limit=limit,
            )
        await self.ensure_indexes()
        database = get_database()
        messages = database[self._MESSAGES_COLLECTION]
        now = self._now_utc()
        safe_limit = max(1, min(int(limit), 2000))

        query: dict[str, Any] = {
            "status": CRMMessageStatus.QUEUED.value,
            "scheduled_at": {"$lte": now},
            "dispatch_job_id": None,
        }
        if campaign_id:
            query["campaign_id"] = str(campaign_id)

        docs = (
            await messages.find(query)
            .sort([("scheduled_at", 1), ("_id", 1)])
            .limit(safe_limit)
            .to_list(length=safe_limit)
        )
        queued_jobs = 0
        for doc in docs:
            message_id = str(doc.get("_id"))
            current_campaign_id = str(doc.get("campaign_id") or "").strip()
            if not current_campaign_id:
                continue
            payload = CRMCampaignDispatchTaskPayload(
                campaign_id=current_campaign_id,
                message_id=message_id,
            )
            enqueue_result = await self.job_service.enqueue_job(
                task_payload=payload,
                queue_name="crm",
                job_type="crm_campaign_dispatch",
            )
            dispatch_job_id = str(enqueue_result.get("job_id") or "").strip() or None
            await messages.update_one(
                {"_id": doc.get("_id")},
                {
                    "$set": {
                        "dispatch_job_id": dispatch_job_id,
                        "updated_at": now,
                    }
                },
            )
            queued_jobs += 1
        return queued_jobs

    async def list_messages(
        self,
        *,
        campaign_id: str | None = None,
        lead_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._list_crm_messages_use_case is not None:
            return await self._list_crm_messages_use_case.execute(
                campaign_id=campaign_id,
                lead_id=lead_id,
                page=page,
                page_size=page_size,
            )
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        messages = get_database()[self._MESSAGES_COLLECTION]
        query: dict[str, Any] = {}
        if campaign_id:
            query["campaign_id"] = str(campaign_id)
        if lead_id:
            query["lead_id"] = str(lead_id)

        total = await messages.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await messages.find(query)
            .sort([("scheduled_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="message_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def list_events(
        self,
        *,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._list_crm_events_use_case is not None:
            return await self._list_crm_events_use_case.execute(
                lead_id=lead_id,
                campaign_id=campaign_id,
                page=page,
                page_size=page_size,
            )
        await self.ensure_indexes()
        if self._use_repo_v2:
            payload = await self._event_repository.list(
                page=page,
                page_size=page_size,
                lead_id=lead_id,
                campaign_id=campaign_id,
            )
            return self._sanitize_payload(payload)

        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        events = get_database()[self._EVENTS_COLLECTION]
        query: dict[str, Any] = {}
        if lead_id:
            query["lead_id"] = str(lead_id)
        if campaign_id:
            query["campaign_id"] = str(campaign_id)

        total = await events.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await events.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="event_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def list_discovery_runs(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        payload = await self._discovery_run_repository.list_runs(page=page, page_size=page_size)
        return self._sanitize_payload(payload)

    async def get_discovery_run(self, *, discovery_run_id: str) -> dict[str, Any]:
        await self.ensure_indexes()
        run_doc = await self._discovery_run_repository.get_run(run_id=discovery_run_id)
        if run_doc is None:
            raise LookupError(f"Discovery run '{discovery_run_id}' not found.")
        return self._sanitize_payload(run_doc)

    async def handle_resend_webhook(self, *, payload: dict[str, Any]) -> dict[str, Any]:
        if self._handle_resend_webhook_use_case is not None:
            return await self._handle_resend_webhook_use_case.execute(payload=payload)
        await self.ensure_indexes()
        event_type = str(payload.get("type") or "").strip().lower()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        provider_message_id = str(
            data.get("email_id")
            or data.get("id")
            or payload.get("email_id")
            or ""
        ).strip()
        if not provider_message_id:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": "missing_provider_message_id"})

        messages = get_database()[self._MESSAGES_COLLECTION]
        message_doc = await messages.find_one({"provider_message_id": provider_message_id})
        if message_doc is None:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": "message_not_found"})

        message_id = str(message_doc.get("_id"))
        lead_id = str(message_doc.get("lead_id") or "").strip() or None
        campaign_id = str(message_doc.get("campaign_id") or "").strip() or None
        now = self._now_utc()

        status_map: dict[str, tuple[str, str | None]] = {
            "email.sent": (CRMMessageStatus.SENT.value, "sent_at"),
            "email.delivered": (CRMMessageStatus.DELIVERED.value, "delivered_at"),
            "email.opened": (CRMMessageStatus.OPEN.value, "opened_at"),
            "email.clicked": (CRMMessageStatus.CLICK.value, "clicked_at"),
            "email.bounced": (CRMMessageStatus.BOUNCED.value, "bounced_at"),
            "email.complained": (CRMMessageStatus.BOUNCED.value, "bounced_at"),
        }

        set_fields: dict[str, Any] = {"updated_at": now}
        mapped = status_map.get(event_type)
        if mapped:
            set_fields["status"] = mapped[0]
            if mapped[1]:
                set_fields[mapped[1]] = now
        elif event_type in {"email.unsubscribed", "email.suppressed"}:
            set_fields["status"] = CRMMessageStatus.UNSUBSCRIBED.value
            set_fields["unsubscribed_at"] = now
        elif event_type in {"email.replied", "email.reply"}:
            set_fields["status"] = CRMMessageStatus.REPLIED.value
            set_fields["replied_at"] = now
        else:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": f"unsupported_event_type:{event_type}"})

        await messages.update_one({"_id": message_doc.get("_id")}, {"$set": set_fields})

        if lead_id and event_type in {"email.unsubscribed", "email.suppressed", "email.bounced", "email.complained", "email.replied", "email.reply"}:
            reason = (
                "unsubscribed"
                if event_type in {"email.unsubscribed", "email.suppressed"}
                else "bounced"
                if event_type in {"email.bounced", "email.complained"}
                else "replied"
            )
            await self._block_lead_contact(lead_id=lead_id, reason=reason)
            if reason in {"unsubscribed", "bounced"}:
                lead_doc = await get_database()[self._LEADS_COLLECTION].find_one({"_id": self._parse_object_id(lead_id, field_name="lead_id")})
                if isinstance(lead_doc, dict):
                    email = str(lead_doc.get("email") or "").strip()
                    if email:
                        await self._upsert_suppression(email=email, reason=reason, source="resend_webhook")

        await self._record_event(
            event_type="email_webhook_processed",
            lead_id=lead_id,
            campaign_id=campaign_id,
            message_id=message_id,
            data={"provider_message_id": provider_message_id, "event_type": event_type},
        )
        return self._sanitize_payload({"ok": True, "message_id": message_id, "event_type": event_type})

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
        if self._use_discovery_v2:
            orchestrator = DiscoveryOrchestrator(
                runs=self._discovery_run_repository,
                discover_candidates=self._discover_candidates_for_orchestrator,
                upsert_candidate=self._upsert_lead_candidate,
                record_event=self._record_event,
            )
            result = await orchestrator.run(
                task_payload=task_payload,
                job_id=str(job_id) if job_id is not None else None,
                discovery_run_id=task_payload.discovery_run_id,
            )
            return self._sanitize_payload(result)

        candidates = await self._discover_candidates(task_payload=task_payload)

        inserted = 0
        updated = 0
        skipped = 0
        for candidate in candidates:
            action = await self._upsert_lead_candidate(candidate)
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1

        await self._record_event(
            event_type="lead_discovery_processed",
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "discovery_run_id": task_payload.discovery_run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            },
        )

        return self._sanitize_payload(
            {
                "discovery_run_id": task_payload.discovery_run_id,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            }
        )

    async def process_benchmark_study_task(
        self,
        *,
        task_payload: BenchmarkLocalStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_benchmark_study_task_use_case is not None:
            return await self._process_benchmark_study_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        orchestrator = BenchmarkOrchestrator(
            runs=self._benchmark_run_repository,
            businesses=self._benchmark_business_repository,
            discover_candidates=self._discover_benchmark_candidates_for_orchestrator,
            competitor_sets=self._competitor_set_repository,
        )
        result = await orchestrator.run(
            task_payload=task_payload,
            job_id=str(job_id) if job_id is not None else None,
        )
        await self._record_event(
            event_type="benchmark_study_processed",
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "benchmark_run_id": result.get("benchmark_run_id"),
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "status": result.get("status"),
                "candidates": result.get("candidates"),
                "inserted": result.get("inserted"),
                "updated": result.get("updated"),
                "skipped": result.get("skipped"),
                "failure_reason": result.get("failure_reason"),
            },
        )
        return self._sanitize_payload(result)

    async def process_geo_grid_study_task(
        self,
        *,
        task_payload: GeoGridStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_geo_grid_study_task_use_case is not None:
            return await self._process_geo_grid_study_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=task_payload.geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{task_payload.geo_grid_run_id}' not found.")
        city = await self._geo_city_repository.get_by_slug(city_slug=str(run.get("city_slug") or ""))
        if city is None:
            raise LookupError(f"Geo city '{run.get('city_slug')}' not found.")

        geo_grid_run_id = str(run.get("geo_grid_run_id") or task_payload.geo_grid_run_id)
        await self._geo_grid_run_repository.set_job_id(
            geo_grid_run_id=geo_grid_run_id,
            job_id=str(job_id) if job_id is not None else str(run.get("job_id") or "").strip() or None,
        )
        await self._geo_grid_run_repository.mark_running(geo_grid_run_id=geo_grid_run_id)

        keyword = str(run.get("keyword") or "").strip()
        top_n = max(1, min(100, int(run.get("top_n") or 10)))
        provider_mode = str(run.get("provider_mode") or settings.geo_grid_provider_mode or "maps_live").strip().lower()
        if provider_mode not in {"maps_live", "uule"}:
            provider_mode = "maps_live"
        configured_grid_size = int(run.get("grid_size") or settings.geo_grid_uule_grid_size or 0)
        configured_grid_spacing_km = float(run.get("grid_spacing_km") or settings.geo_grid_uule_spacing_km or 0.4)
        configured_uule_radius_m = max(100, int(run.get("uule_radius_m") or settings.geo_grid_uule_radius_m or 1000))
        configured_throttle_ms = max(100, int(run.get("throttle_ms") or settings.geo_grid_uule_throttle_ms or 1200))
        center = run.get("center") if isinstance(run.get("center"), dict) else city.get("center")

        if provider_mode == "uule" and configured_grid_size >= 3 and isinstance(center, dict):
            points = build_geo_grid_points(
                center_lat=float(center.get("lat", 0.0)),
                center_lng=float(center.get("lng", 0.0)),
                size=configured_grid_size,
                spacing_km=configured_grid_spacing_km,
                label_prefix="Grid",
            )
        else:
            points = [dict(point) for point in city.get("points") or [] if isinstance(point, dict)]

        total_points = len(points)
        total_units = total_points * top_n
        metrics: dict[str, Any] = {
            "provider_mode": provider_mode,
            "point_count": total_points,
            "top_n": top_n,
            "total_units": total_units,
            "points_completed": 0,
            "points_failed": 0,
            "results_found": 0,
            "points_with_results": 0,
            "points_top3": 0,
            "points_top10": 0,
            "points_not_found": 0,
            "grid_size": configured_grid_size if configured_grid_size >= 3 else None,
            "grid_spacing_km": configured_grid_spacing_km,
            "uule_radius_m": configured_uule_radius_m,
            "throttle_ms": configured_throttle_ms,
        }
        failures: list[dict[str, Any]] = []

        scraper = BusinessService.build_default_scraper()
        try:
            await scraper.start()
            for index, point in enumerate(points, start=1):
                point_results: list[dict[str, Any]] = []
                try:
                    if provider_mode == "uule":
                        point_results = await self._discover_geo_grid_point_results_uule(
                            scraper=scraper,
                            keyword=keyword,
                            point=point,
                            top_n=top_n,
                            radius_m=configured_uule_radius_m,
                            throttle_ms=configured_throttle_ms,
                        )
                    else:
                        point_results = await self._discover_geo_grid_point_results(
                            scraper=scraper,
                            keyword=keyword,
                            point=point,
                            top_n=top_n,
                        )
                    inserted = await self._geo_grid_result_repository.replace_point_results(
                        geo_grid_run_id=geo_grid_run_id,
                        city_slug=str(run.get("city_slug") or ""),
                        keyword=keyword,
                        point=point,
                        results=point_results,
                    )
                    metrics["results_found"] = int(metrics.get("results_found") or 0) + int(inserted)
                    if inserted > 0:
                        metrics["points_with_results"] = int(metrics.get("points_with_results") or 0) + 1
                    ranks: list[int] = []
                    for item in point_results:
                        try:
                            rank_value = int(item.get("rank") or 0)
                        except (TypeError, ValueError):
                            continue
                        if rank_value > 0:
                            ranks.append(rank_value)
                    best_rank = min(ranks) if ranks else None
                    if best_rank is None:
                        metrics["points_not_found"] = int(metrics.get("points_not_found") or 0) + 1
                    else:
                        if best_rank <= 3:
                            metrics["points_top3"] = int(metrics.get("points_top3") or 0) + 1
                        if best_rank <= 10:
                            metrics["points_top10"] = int(metrics.get("points_top10") or 0) + 1
                except Exception as exc:  # noqa: BLE001
                    metrics["points_failed"] = int(metrics.get("points_failed") or 0) + 1
                    metrics["points_not_found"] = int(metrics.get("points_not_found") or 0) + 1
                    failures.append(
                        {
                            "point_order": point.get("order"),
                            "point_label": point.get("label"),
                            "error": str(exc),
                        }
                    )

                metrics["points_completed"] = index
                completed_units = min(total_units, index * top_n)
                points_completed = max(1, int(metrics.get("points_completed") or 0))
                metrics["share_top3"] = round(float(metrics.get("points_top3") or 0) / points_completed, 4)
                metrics["share_top10"] = round(float(metrics.get("points_top10") or 0) / points_completed, 4)
                metrics["share_not_found"] = round(float(metrics.get("points_not_found") or 0) / points_completed, 4)
                await self._geo_grid_run_repository.update_progress(
                    geo_grid_run_id=geo_grid_run_id,
                    completed_points=index,
                    completed_units=completed_units,
                    metrics=metrics,
                )

            points_total = max(1, total_points)
            top3 = int(metrics.get("points_top3") or 0)
            top10 = int(metrics.get("points_top10") or 0)
            with_results = int(metrics.get("points_with_results") or 0)
            top4_10 = max(0, top10 - top3)
            top11_plus = max(0, with_results - top10)
            visibility_ratio = (top3 + (top4_10 * 0.6) + (top11_plus * 0.3)) / points_total
            metrics["visibility_score"] = round(visibility_ratio * 100.0, 2)
            metrics["share_top3"] = round(top3 / points_total, 4)
            metrics["share_top10"] = round(top10 / points_total, 4)
            metrics["share_not_found"] = round(float(metrics.get("points_not_found") or 0) / points_total, 4)

            final_status = "partial" if failures else "completed"
            final_run = await self._geo_grid_run_repository.finalize(
                geo_grid_run_id=geo_grid_run_id,
                status=final_status,
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=f"{len(failures)} puntos fallaron." if failures else None,
            )
            result = {
                "geo_grid_run_id": geo_grid_run_id,
                "job_id": str(job_id) if job_id is not None else None,
                "keyword": keyword,
                "city": run.get("city"),
                "city_slug": run.get("city_slug"),
                "top_n": top_n,
                "point_count": total_points,
                "status": final_status,
                "metrics": metrics,
                "run": final_run,
            }
            await self._record_event(event_type="geo_grid_study_processed", data=result)
            return self._sanitize_payload(result)
        except Exception as exc:
            await self._geo_grid_run_repository.finalize(
                geo_grid_run_id=geo_grid_run_id,
                status="failed",
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=str(exc),
            )
            raise
        finally:
            await scraper.close()

    def _build_geo_grid_stats(self, *, run: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        point_count = int(run.get("point_count") or 0)
        top_n = int(run.get("top_n") or 10)
        run_metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
        provider_mode = str(run.get("provider_mode") or run_metrics.get("provider_mode") or "maps_live").strip().lower()
        points: dict[int, dict[str, Any]] = {}
        businesses: dict[str, dict[str, Any]] = {}

        for item in results:
            if not isinstance(item, dict):
                continue
            point_order = int(item.get("point_order") or 0)
            rank = int(item.get("rank") or 0)
            if point_order < 1 or rank < 1:
                continue
            point_payload = points.setdefault(
                point_order,
                {
                    "point_order": point_order,
                    "point_label": item.get("point_label"),
                    "grid_row": item.get("grid_row"),
                    "grid_col": item.get("grid_col"),
                    "lat": item.get("lat"),
                    "lng": item.get("lng"),
                    "top_results": [],
                },
            )
            point_payload["top_results"].append(
                {
                    "rank": rank,
                    "business_key": item.get("business_key"),
                    "business_name": item.get("business_name"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "maps_url": item.get("maps_url"),
                }
            )

            business_key = str(item.get("business_key") or item.get("business_name_normalized") or "").strip()
            if not business_key:
                continue
            business = businesses.setdefault(
                business_key,
                {
                    "business_key": business_key,
                    "business_name": item.get("business_name"),
                    "maps_url": item.get("maps_url"),
                    "maps_url_canonical": item.get("maps_url_canonical"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "appearances": 0,
                    "ranks": [],
                    "points": [],
                    "top_1_count": 0,
                    "top_3_count": 0,
                    "top_5_count": 0,
                    "top_10_count": 0,
                    "top_20_count": 0,
                },
            )
            business["appearances"] += 1
            business["ranks"].append(rank)
            business["points"].append(
                {
                    "point_order": point_order,
                    "point_label": item.get("point_label"),
                    "lat": item.get("lat"),
                    "lng": item.get("lng"),
                    "rank": rank,
                }
            )
            if rank == 1:
                business["top_1_count"] += 1
            if rank <= 3:
                business["top_3_count"] += 1
            if rank <= 5:
                business["top_5_count"] += 1
            if rank <= 10:
                business["top_10_count"] += 1
            if rank <= 20:
                business["top_20_count"] += 1

        if point_count <= 0:
            point_count = len(points)

        business_rows: list[dict[str, Any]] = []
        for business in businesses.values():
            ranks = [int(rank) for rank in business.pop("ranks", [])]
            appearances = int(business.get("appearances") or 0)
            avg_rank = round(sum(ranks) / len(ranks), 2) if ranks else None
            best_rank = min(ranks) if ranks else None
            worst_rank = max(ranks) if ranks else None
            rank_stddev = self._population_stddev(ranks)
            coverage = round((appearances / point_count) * 100, 2) if point_count else 0.0
            missing_points = max(0, point_count - appearances)
            business_rows.append(
                {
                    **business,
                    "coverage_percent": coverage,
                    "missing_points": missing_points,
                    "avg_rank": avg_rank,
                    "best_rank": best_rank,
                    "worst_rank": worst_rank,
                    "rank_stddev": rank_stddev,
                }
            )

        business_rows.sort(
            key=lambda item: (
                -int(item.get("appearances") or 0),
                float(item.get("avg_rank") or 9999),
                str(item.get("business_name") or ""),
            )
        )
        weakest_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("avg_rank") or 0),
                int(item.get("appearances") or 0),
            ),
            reverse=True,
        )
        consistent_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("rank_stddev") or 9999),
                -int(item.get("appearances") or 0),
                float(item.get("avg_rank") or 9999),
            ),
        )
        dispersed_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("rank_stddev") or 0),
                int(item.get("appearances") or 0),
            ),
            reverse=True,
        )
        point_rows = sorted(points.values(), key=lambda item: int(item.get("point_order") or 0))
        for point in point_rows:
            point["top_results"] = sorted(
                point.get("top_results") or [],
                key=lambda item: int(item.get("rank") or 9999),
            )

        return {
            "geo_grid_run_id": run.get("geo_grid_run_id"),
            "summary": {
                "keyword": run.get("keyword"),
                "city": run.get("city"),
                "city_slug": run.get("city_slug"),
                "provider_mode": provider_mode,
                "point_count": point_count,
                "top_n": top_n,
                "total_results": len(results),
                "unique_businesses": len(business_rows),
                "visibility_score": run_metrics.get("visibility_score"),
                "share_top3": run_metrics.get("share_top3"),
                "share_top10": run_metrics.get("share_top10"),
                "share_not_found": run_metrics.get("share_not_found"),
            },
            "businesses": business_rows,
            "leaders": business_rows[:10],
            "weakest": weakest_rows[:10],
            "most_consistent": consistent_rows[:10],
            "most_dispersed": dispersed_rows[:10],
            "points": point_rows,
            "run_metrics": run_metrics,
        }

    async def _resolve_geo_grid_stats_for_public_study(self, *, benchmark: dict[str, Any]) -> dict[str, Any] | None:
        benchmark_city_slug = str(benchmark.get("city_slug") or "").strip().lower()
        if not benchmark_city_slug:
            benchmark_city = str(benchmark.get("city") or "").strip()
            if benchmark_city:
                benchmark_city_slug = self._normalize_text(benchmark_city).replace(" ", "-")
        benchmark_query = self._normalize_text(str(benchmark.get("query") or ""))

        try:
            runs_payload = await self._geo_grid_run_repository.list_runs(
                page=1,
                page_size=120,
                city_slug=benchmark_city_slug or None,
                status_filter=None,
            )
        except Exception:
            return None

        raw_items = runs_payload.get("items") if isinstance(runs_payload, dict) else []
        runs = [dict(item) for item in raw_items if isinstance(item, dict)]
        if not runs:
            return None

        candidates: list[dict[str, Any]] = []
        for run in runs:
            status = str(run.get("status") or "").strip().lower()
            if status not in {"completed", "partial"}:
                continue
            run_query = self._normalize_text(str(run.get("keyword") or ""))
            if benchmark_query and run_query:
                if benchmark_query not in run_query and run_query not in benchmark_query:
                    continue
            candidates.append(run)

        if not candidates:
            return None

        selected_run = candidates[0]
        selected_run_id = str(selected_run.get("geo_grid_run_id") or "").strip()
        if not selected_run_id:
            return None
        results = await self._geo_grid_result_repository.list_results(geo_grid_run_id=selected_run_id)
        stats = self._build_geo_grid_stats(run=selected_run, results=results)
        points = stats.get("points") if isinstance(stats.get("points"), list) else []
        return stats if points else None

    def _population_stddev(self, values: list[int]) -> float:
        if len(values) <= 1:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((float(value) - mean) ** 2 for value in values) / len(values)
        return round(variance ** 0.5, 2)

    async def select_competitors_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        max_competitors: int = 5,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        target = await self._benchmark_business_repository.get_business(
            benchmark_business_id=benchmark_business_id,
        )
        if target is None:
            raise LookupError(f"Benchmark business '{benchmark_business_id}' not found.")

        benchmark_id = str(target.get("benchmark_id") or "").strip()
        if not benchmark_id:
            raise ValueError("Benchmark business has no benchmark_id.")

        candidates_payload = await self._benchmark_business_repository.list_businesses(
            benchmark_id=benchmark_id,
            page=1,
            page_size=200,
            sort_by="review_count",
            sort_dir="desc",
        )
        candidates = candidates_payload.get("items") if isinstance(candidates_payload.get("items"), list) else []
        competitors = select_competitors_for_business(
            target,
            candidates,
            max_competitors=max_competitors,
        )
        persisted = await self._competitor_set_repository.upsert_set(
            benchmark_id=benchmark_id,
            target_business_id=str(target.get("benchmark_business_id") or benchmark_business_id),
            competitors=competitors,
            selection_version="v1",
        )
        await self._record_event(
            event_type="benchmark_competitors_selected",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "competitors": len(competitors),
            },
        )
        return self._sanitize_payload(persisted)

    async def generate_lead_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        business = await self._benchmark_business_repository.get_business(
            benchmark_business_id=benchmark_business_id,
        )
        if business is None:
            raise LookupError(f"Benchmark business '{benchmark_business_id}' not found.")

        benchmark_id = str(business.get("benchmark_id") or "").strip()
        if not benchmark_id:
            raise ValueError("Benchmark business has no benchmark_id.")

        competitor_set = await self._competitor_set_repository.get_for_business(
            target_business_id=benchmark_business_id,
        )
        if competitor_set is None:
            selected = await self.select_competitors_for_benchmark_business(
                benchmark_business_id=benchmark_business_id,
                max_competitors=5,
            )
            competitor_set = selected.get("competitor_set") if isinstance(selected.get("competitor_set"), dict) else None

        competitors = []
        if isinstance(competitor_set, dict) and isinstance(competitor_set.get("competitors"), list):
            competitors = [dict(item) for item in competitor_set.get("competitors") if isinstance(item, dict)]

        deep_study_snapshot = build_deep_study_snapshot(
            business=business,
            competitors=competitors,
        )
        initial_cta = self._resolve_lead_report_cta(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            cta=cta,
            lead_report_id=None,
        )
        html = render_lead_report_html(
            business=business,
            deep_study_snapshot=deep_study_snapshot,
            competitors=competitors,
            cta=initial_cta,
        )
        persisted = await self._lead_report_repository.upsert_for_business(
            benchmark_business_id=benchmark_business_id,
            payload={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "business_name": business.get("business_name"),
                "html": html,
                "deep_study_snapshot": deep_study_snapshot,
                "source_payload": {
                    "business": business,
                    "competitors": competitors,
                    "competitor_set_id": competitor_set.get("competitor_set_id") if isinstance(competitor_set, dict) else None,
                },
                "cta": dict(initial_cta),
            },
        )
        report = persisted.get("lead_report") if isinstance(persisted.get("lead_report"), dict) else {}
        lead_report_id = str(report.get("lead_report_id") or "").strip() or None
        resolved_cta = self._resolve_lead_report_cta(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            cta=initial_cta,
            lead_report_id=lead_report_id,
        )
        if resolved_cta != initial_cta:
            html = render_lead_report_html(
                business=business,
                deep_study_snapshot=deep_study_snapshot,
                competitors=competitors,
                cta=resolved_cta,
            )
            persisted = await self._lead_report_repository.upsert_for_business(
                benchmark_business_id=benchmark_business_id,
                payload={
                    "benchmark_id": benchmark_id,
                    "benchmark_business_id": benchmark_business_id,
                    "business_name": business.get("business_name"),
                    "html": html,
                    "deep_study_snapshot": deep_study_snapshot,
                    "source_payload": {
                        "business": business,
                        "competitors": competitors,
                        "competitor_set_id": competitor_set.get("competitor_set_id")
                        if isinstance(competitor_set, dict)
                        else None,
                    },
                    "cta": dict(resolved_cta),
                },
            )
            report = persisted.get("lead_report") if isinstance(persisted.get("lead_report"), dict) else report
        await self._record_event(
            event_type="lead_report_generated",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "lead_report_id": report.get("lead_report_id"),
                "competitors": len(competitors),
            },
        )
        return self._sanitize_payload(persisted)

    async def generate_paid_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        report_month: str | None = None,
        history: list[dict[str, Any]] | None = None,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        business = await self._benchmark_business_repository.get_business(
            benchmark_business_id=benchmark_business_id,
        )
        if business is None:
            raise LookupError(f"Benchmark business '{benchmark_business_id}' not found.")

        benchmark_id = str(business.get("benchmark_id") or "").strip()
        if not benchmark_id:
            raise ValueError("Benchmark business has no benchmark_id.")

        competitor_set = await self._competitor_set_repository.get_for_business(
            target_business_id=benchmark_business_id,
        )
        if competitor_set is None:
            selected = await self.select_competitors_for_benchmark_business(
                benchmark_business_id=benchmark_business_id,
                max_competitors=5,
            )
            competitor_set = selected.get("competitor_set") if isinstance(selected.get("competitor_set"), dict) else None

        competitors = []
        if isinstance(competitor_set, dict) and isinstance(competitor_set.get("competitors"), list):
            competitors = [dict(item) for item in competitor_set.get("competitors") if isinstance(item, dict)]

        deep_study_snapshot = build_deep_study_snapshot(
            business=business,
            competitors=competitors,
        )
        normalized_month = str(report_month or self._now_utc().strftime("%Y-%m")).strip()
        history_items = [dict(item) for item in history or [] if isinstance(item, dict)]
        html = render_paid_report_html(
            business=business,
            deep_study_snapshot=deep_study_snapshot,
            competitors=competitors,
            history=history_items,
            report_month=normalized_month,
            cta=cta,
        )
        persisted = await self._paid_report_repository.upsert_for_business_month(
            benchmark_business_id=benchmark_business_id,
            report_month=normalized_month,
            payload={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "report_month": normalized_month,
                "business_name": business.get("business_name"),
                "html": html,
                "deep_study_snapshot": deep_study_snapshot,
                "history": history_items,
                "source_payload": {
                    "business": business,
                    "competitors": competitors,
                    "competitor_set_id": competitor_set.get("competitor_set_id") if isinstance(competitor_set, dict) else None,
                },
                "cta": dict(cta or {}),
            },
        )
        report = persisted.get("paid_report") if isinstance(persisted.get("paid_report"), dict) else {}
        await self._record_event(
            event_type="paid_report_generated",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "paid_report_id": report.get("paid_report_id"),
                "report_month": normalized_month,
                "competitors": len(competitors),
                "history_points": len(history_items),
            },
        )
        return self._sanitize_payload(persisted)

    async def generate_public_study_for_benchmark_run(
        self,
        *,
        benchmark_run_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        benchmark = await self._benchmark_run_repository.get_run(benchmark_run_id=benchmark_run_id)
        if benchmark is None:
            raise LookupError(f"Benchmark run '{benchmark_run_id}' not found.")

        businesses_payload = await self._benchmark_business_repository.list_businesses(
            benchmark_id=benchmark_run_id,
            page=1,
            page_size=200,
            sort_by="discovery_rank",
            sort_dir="asc",
        )
        businesses = [
            dict(item)
            for item in businesses_payload.get("items", [])
            if isinstance(item, dict)
        ]
        geo_grid_stats = await self._resolve_geo_grid_stats_for_public_study(benchmark=benchmark)
        html = render_public_study_html(
            benchmark_run=benchmark,
            businesses=businesses,
            cta=cta,
            geo_grid_stats=geo_grid_stats,
        )
        await self._record_event(
            event_type="public_benchmark_study_generated",
            data={
                "benchmark_run_id": benchmark_run_id,
                "businesses": len(businesses),
                "geo_visibility": bool(geo_grid_stats),
                "cta_url": str((cta or {}).get("url") or "").strip() or None,
            },
        )
        return self._sanitize_payload(
            {
                "benchmark_run_id": benchmark_run_id,
                "businesses": len(businesses),
                "html": html,
            }
        )

    async def process_lead_pipeline_task(
        self,
        *,
        task_payload: CRMLeadPipelineTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_crm_lead_pipeline_task_use_case is not None:
            return await self._process_crm_lead_pipeline_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(task_payload.lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{task_payload.lead_id}' not found.")

        business_name = str(lead_doc.get("business_name") or "").strip()
        if not business_name:
            raise ValueError("Lead has no business_name to run pipeline.")

        requested_sources = tuple(task_payload.sources)
        used_sources = requested_sources
        fallback_reason: str | None = None

        try:
            queue_result = await self.business_service.enqueue_business_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=requested_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
            )
        except RuntimeError as exc:
            error_text = str(exc)
            can_fallback_to_google = (
                "tripadvisor" in requested_sources
                and "google_maps" in requested_sources
                and (
                    "Tripadvisor local worker bridge is unreachable" in error_text
                    or "TRIPADVISOR_LOCAL_WORKER_BRIDGE_ENABLED" in error_text
                )
            )
            if not can_fallback_to_google:
                raise
            used_sources = ("google_maps",)
            fallback_reason = error_text
            queue_result = await self.business_service.enqueue_business_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=used_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
            )

        jobs_by_source = queue_result.get("jobs_by_source") if isinstance(queue_result.get("jobs_by_source"), dict) else {}
        source_job_ids: list[str] = []
        for source_name in self._ALLOWED_SOURCES:
            source_job = jobs_by_source.get(source_name) if isinstance(jobs_by_source, dict) else None
            if isinstance(source_job, dict):
                source_job_id = str(source_job.get("job_id") or "").strip()
                if source_job_id:
                    source_job_ids.append(source_job_id)

        root_business_id = str(queue_result.get("business_id") or "").strip() or None
        now = self._now_utc()

        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_RUNNING.value,
                    "pipeline.business_id": root_business_id,
                    "pipeline.source_job_ids": source_job_ids,
                    "updated_at": now,
                }
            },
        )

        await self._record_event(
            event_type="lead_pipeline_started",
            lead_id=task_payload.lead_id,
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            },
        )

        return self._sanitize_payload(
            {
                "lead_id": task_payload.lead_id,
                "business_name": business_name,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            }
        )

    async def process_campaign_dispatch_task(
        self,
        *,
        task_payload: CRMCampaignDispatchTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_campaign_dispatch_task_use_case is not None:
            return await self._process_campaign_dispatch_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        database = get_database()
        campaigns = database[self._CAMPAIGNS_COLLECTION]
        messages = database[self._MESSAGES_COLLECTION]
        leads = database[self._LEADS_COLLECTION]

        message_id = self._parse_object_id(task_payload.message_id, field_name="message_id")
        campaign_id = self._parse_object_id(task_payload.campaign_id, field_name="campaign_id")

        message_doc = await messages.find_one({"_id": message_id, "campaign_id": task_payload.campaign_id})
        if message_doc is None:
            raise LookupError(f"Campaign message '{task_payload.message_id}' not found.")

        campaign_doc = await campaigns.find_one({"_id": campaign_id})
        if campaign_doc is None:
            raise LookupError(f"Campaign '{task_payload.campaign_id}' not found.")

        current_status = str(message_doc.get("status") or "").strip().lower()
        if current_status not in {CRMMessageStatus.QUEUED.value, CRMMessageStatus.FAILED.value}:
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": current_status,
                    "skipped": True,
                    "reason": "message_not_dispatchable",
                }
            )

        scheduled_at = message_doc.get("scheduled_at") if isinstance(message_doc.get("scheduled_at"), datetime) else None
        now = self._now_utc()
        if scheduled_at is not None and scheduled_at > now:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "dispatch_job_id": None,
                        "updated_at": now,
                    }
                },
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.QUEUED.value,
                    "skipped": True,
                    "reason": "not_due_yet",
                }
            )

        lead_id = str(message_doc.get("lead_id") or "").strip()
        lead_doc = await leads.find_one({"_id": self._parse_object_id(lead_id, field_name="lead_id")})
        if lead_doc is None:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "status": CRMMessageStatus.FAILED.value,
                        "error": "lead_not_found",
                        "failed_at": now,
                        "updated_at": now,
                    }
                },
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.FAILED.value,
                    "reason": "lead_not_found",
                }
            )

        allowed, reason = await self._can_send_to_lead(lead_doc=lead_doc)
        if not allowed:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "status": CRMMessageStatus.SKIPPED.value,
                        "error": reason,
                        "updated_at": now,
                        "dispatch_job_id": None,
                    }
                },
            )
            await self._record_event(
                event_type="campaign_dispatch_skipped",
                campaign_id=task_payload.campaign_id,
                lead_id=lead_id,
                message_id=task_payload.message_id,
                data={"reason": reason},
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.SKIPPED.value,
                    "reason": reason,
                }
            )

        to_email = str(message_doc.get("to_email") or "").strip()
        subject = str(message_doc.get("subject") or "").strip()
        body = str(message_doc.get("body") or "").strip()

        send_result = await asyncio.to_thread(
            self._send_resend_email,
            to_email=to_email,
            subject=subject,
            html_body=body,
        )
        provider_message_id = str(send_result.get("id") or "").strip() or None

        await messages.update_one(
            {"_id": message_id},
            {
                "$set": {
                    "status": CRMMessageStatus.SENT.value,
                    "sent_at": now,
                    "provider_message_id": provider_message_id,
                    "provider_payload": send_result,
                    "dispatch_job_id": None,
                    "updated_at": now,
                    "error": None,
                }
            },
        )

        await campaigns.update_one(
            {"_id": campaign_id},
            {
                "$inc": {"metrics.messages_sent": 1},
                "$set": {"updated_at": now},
            },
        )

        await self._record_event(
            event_type="campaign_message_sent",
            campaign_id=task_payload.campaign_id,
            lead_id=lead_id,
            message_id=task_payload.message_id,
            data={
                "provider": "resend",
                "provider_message_id": provider_message_id,
                "job_id": str(job_id) if job_id is not None else None,
            },
        )

        return self._sanitize_payload(
            {
                "campaign_id": task_payload.campaign_id,
                "message_id": task_payload.message_id,
                "status": CRMMessageStatus.SENT.value,
                "provider_message_id": provider_message_id,
                "provider_response": send_result,
            }
        )

    async def sync_lead_pipeline_refs(self, *, lead_id: str) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        database = get_database()
        leads = database[self._LEADS_COLLECTION]
        jobs = database[self._JOBS_COLLECTION]

        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        source_job_ids = pipeline.get("source_job_ids") if isinstance(pipeline.get("source_job_ids"), list) else []
        source_job_ids = [str(item).strip() for item in source_job_ids if str(item).strip()]
        if not source_job_ids:
            return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

        analysis_job_doc = (
            await jobs.find(
                {
                    "queue_name": "analysis",
                    "job_type": "analysis_generate",
                    "payload.source_job_id": {"$in": source_job_ids},
                }
            )
            .sort([("updated_at", -1), ("_id", -1)])
            .limit(1)
            .to_list(length=1)
        )
        latest_analysis_job = analysis_job_doc[0] if analysis_job_doc else None

        report_job_doc: dict[str, Any] | None = None
        if latest_analysis_job is not None:
            analysis_job_id = str(latest_analysis_job.get("_id"))
            report_docs = (
                await jobs.find(
                    {
                        "queue_name": "report",
                        "job_type": "report_generate",
                        "payload.source_job_id": analysis_job_id,
                    }
                )
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(1)
                .to_list(length=1)
            )
            report_job_doc = report_docs[0] if report_docs else None

        update_fields: dict[str, Any] = {}
        if latest_analysis_job is not None:
            update_fields["pipeline.analysis_job_id"] = str(latest_analysis_job.get("_id"))
            update_fields["status"] = CRMLeadStatus.PIPELINE_DONE.value

        if report_job_doc is not None:
            update_fields["pipeline.report_job_id"] = str(report_job_doc.get("_id"))
            report_result = report_job_doc.get("result") if isinstance(report_job_doc.get("result"), dict) else {}
            artifacts = report_result.get("artifacts") if isinstance(report_result.get("artifacts"), dict) else {}
            update_fields["pipeline.latest_report_artifacts"] = artifacts

        if update_fields:
            update_fields["updated_at"] = self._now_utc()
            updated = await leads.find_one_and_update(
                {"_id": parsed_lead_id},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                lead_doc = updated

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

    async def _resolve_cadence_template(self, cadence_template_id: str | None) -> dict[str, Any]:
        await self._ensure_default_cadence_template()
        cadence = get_database()[self._CADENCE_COLLECTION]

        normalized_id = str(cadence_template_id or "").strip()
        if normalized_id:
            try:
                doc = await cadence.find_one({"_id": ObjectId(normalized_id)})
            except InvalidId:
                doc = await cadence.find_one({"key": normalized_id})
            if doc is not None:
                return doc

        fallback = await cadence.find_one({"key": self._DEFAULT_CADENCE_KEY})
        if fallback is None:
            raise RuntimeError("Default cadence template is missing.")
        return fallback

    async def _ensure_default_cadence_template(self) -> None:
        cadence = get_database()[self._CADENCE_COLLECTION]
        now = self._now_utc()

        default_steps = [
            CRMCadenceStep(
                step_order=1,
                step_key="d0_intro",
                delay_days=0,
                subject_template="{business_name}: te comparto un mini informe de reputación",
                body_template=(
                    "Hola,\n\n"
                    "Hemos revisado la reputación online de {business_name}.\n"
                    "Resumen rápido:\n"
                    "{mini_report}\n\n"
                    "Si te encaja, te enseño en 15 minutos cómo mejorar estos puntos.\n"
                    "{cta_url}\n\n"
                    "Si no quieres recibir más mensajes, puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=2,
                step_key="d3_recordatorio",
                delay_days=3,
                subject_template="{business_name}: un dato clave para mejorar tu reputación",
                body_template=(
                    "Hola de nuevo,\n\n"
                    "Te comparto un insight adicional de {business_name}:\n"
                    "{mini_report}\n\n"
                    "Si quieres, te lo explico en una demo corta:\n"
                    "{cta_url}\n\n"
                    "Baja automática: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=3,
                step_key="d7_cierre",
                delay_days=7,
                subject_template="Cierro hilo: {business_name}",
                body_template=(
                    "Último mensaje por aquí, prometido.\n\n"
                    "Si en otro momento quieres revisar el informe de {business_name},"
                    " aquí tienes acceso directo:\n"
                    "{cta_url}\n\n"
                    "Puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
        ]

        template = CRMCadenceTemplate(
            key=self._DEFAULT_CADENCE_KEY,
            name="Cadencia opt-in 3 toques (D0/D+3/D+7)",
            locale="es-ES",
            is_default=True,
            steps=default_steps,
            created_at=now,
            updated_at=now,
        )
        payload = template.model_dump(mode="python")
        await cadence.update_one(
            {"key": self._DEFAULT_CADENCE_KEY},
            {
                "$set": {
                    "name": payload["name"],
                    "locale": payload["locale"],
                    "is_default": True,
                    "steps": payload["steps"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "key": self._DEFAULT_CADENCE_KEY,
                },
            },
            upsert=True,
        )

    def _build_campaign_lead_query(self, audience_filter: Any) -> dict[str, Any]:
        filters = dict(audience_filter or {}) if isinstance(audience_filter, dict) else {}

        query: dict[str, Any] = {
            "legal.consent_status": CRMConsentStatus.GRANTED.value,
            "legal.do_not_contact": {"$ne": True},
            "legal.unsubscribed_at": None,
        }

        lead_statuses = filters.get("lead_statuses")
        if isinstance(lead_statuses, list):
            normalized_statuses = [str(item).strip().lower() for item in lead_statuses if str(item).strip()]
            if normalized_statuses:
                query["status"] = {"$in": normalized_statuses}

        city = str(filters.get("city") or "").strip()
        if city:
            query["city"] = {"$regex": re.escape(city), "$options": "i"}

        category = str(filters.get("category") or "").strip()
        if category:
            query["category"] = {"$regex": re.escape(category), "$options": "i"}

        lead_ids = filters.get("lead_ids")
        if isinstance(lead_ids, list):
            parsed_ids: list[ObjectId] = []
            for raw_id in lead_ids:
                raw = str(raw_id or "").strip()
                if not raw:
                    continue
                try:
                    parsed_ids.append(ObjectId(raw))
                except InvalidId:
                    continue
            if parsed_ids:
                query["_id"] = {"$in": parsed_ids}

        return query

    async def _load_suppressed_emails(self) -> set[str]:
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        docs = await suppressions.find({}, projection={"email_normalized": 1}).to_list(length=50000)
        values: set[str] = set()
        for doc in docs:
            normalized = self._normalize_email(doc.get("email_normalized"))
            if normalized:
                values.add(normalized)
        return values

    async def _discover_candidates(self, *, task_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or "").strip()
        normalized_query = self._normalize_text(query_text)
        normalized_city = self._normalize_text(task_payload.city) if task_payload.city else None
        normalized_category = self._normalize_text(task_payload.category) if task_payload.category else None
        safe_limit = max(1, min(int(task_payload.limit), 5000))

        normalized_source = str(task_payload.source or "").strip().lower()
        if normalized_source in self._LIVE_GOOGLE_DISCOVERY_ALIASES:
            normalized_source = "auto_live_google_maps"
        if normalized_source in self._LIVE_GOOGLE_DISCOVERY_SOURCES:
            live_candidates = await self._discover_candidates_live_google_maps(
                task_payload=task_payload,
                normalized_query=normalized_query,
                safe_limit=safe_limit,
            )
            if normalized_source == "live_google_maps":
                return live_candidates[:safe_limit]

            # "auto_live_*": if live returns little/no data, top-up from stored sources.
            if len(live_candidates) >= safe_limit:
                return live_candidates[:safe_limit]
            remaining = safe_limit - len(live_candidates)
            fallback_candidates = await self._discover_candidates_from_stored_sources(
                task_payload=task_payload,
                normalized_query=normalized_query,
                normalized_city=normalized_city,
                normalized_category=normalized_category,
                safe_limit=remaining,
            )
            merged = live_candidates + fallback_candidates
            return merged[:safe_limit]

        return await self._discover_candidates_from_stored_sources(
            task_payload=task_payload,
            normalized_query=normalized_query,
            normalized_city=normalized_city,
            normalized_category=normalized_category,
            safe_limit=safe_limit,
        )

    async def _discover_candidates_for_orchestrator(
        self,
        task_payload: CRMLeadDiscoveryTaskPayload,
    ) -> list[dict[str, Any]]:
        return await self._discover_candidates(task_payload=task_payload)

    async def _discover_benchmark_candidates_for_orchestrator(
        self,
        task_payload: BenchmarkLocalStudyTaskPayload,
    ) -> list[dict[str, Any]]:
        discovery_payload = CRMLeadDiscoveryTaskPayload(
            query=task_payload.query,
            city=task_payload.city,
            category=task_payload.category,
            limit=task_payload.limit,
            source=task_payload.source,
        )
        candidates = await self._discover_candidates(task_payload=discovery_payload)
        for candidate in candidates:
            candidate.setdefault("source_ref", {})
            if isinstance(candidate["source_ref"], dict):
                candidate["source_ref"]["benchmark_query"] = task_payload.query
                candidate["source_ref"]["benchmark_title"] = task_payload.title
        return candidates

    def _resolve_lead_report_cta(
        self,
        *,
        benchmark_business_id: str,
        benchmark_id: str | None,
        cta: dict[str, Any] | None,
        lead_report_id: str | None,
    ) -> dict[str, Any]:
        resolved = dict(cta or {})
        resolved.setdefault("label", "Valorar este informe")
        resolved.setdefault(
            "description",
            "Cuéntanos en 1 minuto si este analisis te ha resultado util y que mejorarias.",
        )
        url_value = str(resolved.get("url") or "").strip()
        if not url_value:
            resolved["url"] = self._build_onboarding_form_url(
                lead_report_id=lead_report_id,
                benchmark_business_id=benchmark_business_id,
                benchmark_id=benchmark_id,
            )
            return resolved

        if lead_report_id and "lead_report_id=" not in url_value:
            separator = "&" if "?" in url_value else "?"
            resolved["url"] = f"{url_value}{separator}lead_report_id={quote_plus(lead_report_id)}"
        return resolved

    def _build_onboarding_form_url(
        self,
        *,
        lead_report_id: str | None,
        benchmark_business_id: str,
        benchmark_id: str | None,
    ) -> str:
        base_url = str(settings.crm_onboarding_form_base_url or "").strip() or "/valoracion"
        params: list[tuple[str, str]] = []
        if lead_report_id:
            params.append(("lead_report_id", lead_report_id))
        if benchmark_business_id:
            params.append(("benchmark_business_id", benchmark_business_id))
        if benchmark_id:
            params.append(("benchmark_id", benchmark_id))
        if not params:
            return base_url
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}{urlencode(params)}"

    async def _enqueue_report_request_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        business_name = str(doc.get("business_name") or "").strip()
        query = str(doc.get("query") or "").strip()
        if not query:
            query = " ".join(item for item in (business_name, str(doc.get("city") or "").strip()) if item)
        if not query:
            raise ValueError("Report request has no query to enqueue.")
        queued = await self.enqueue_benchmark_study_job(
            query=query,
            city=str(doc.get("city") or "").strip() or None,
            category=str(doc.get("category") or "").strip() or None,
            limit=30,
            source="auto_live_google_maps",
            title=f"Solicitud informe: {business_name or query}",
        )
        return {
            "status": "queued",
            "job_id": str(queued.get("job_id") or "").strip() or None,
            "benchmark_run_id": str(queued.get("benchmark_run_id") or "").strip() or None,
            "failure_reason": None,
            "updated_at": self._now_utc(),
        }

    async def _discover_candidates_from_stored_sources(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        normalized_city: str | None,
        normalized_category: str | None,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        database = get_database()
        candidates: list[dict[str, Any]] = []

        sources_to_scan: list[str]
        normalized_source = str(task_payload.source or "").strip().lower()
        if normalized_source in {"research_google_maps", "research"}:
            sources_to_scan = ["research"]
        elif normalized_source in {"businesses", "existing_businesses"}:
            sources_to_scan = ["businesses"]
        elif normalized_source in {"auto", "all", ""}:
            sources_to_scan = ["research", "businesses"]
        else:
            sources_to_scan = ["research", "businesses"]

        if "research" in sources_to_scan:
            research = database[self._RESEARCH_LEADS_COLLECTION]
            research_query: dict[str, Any] = {}
            if normalized_category:
                research_query["category"] = {"$regex": re.escape(task_payload.category or ""), "$options": "i"}
            if normalized_city:
                research_query["$or"] = [
                    {"address": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                    {"term_key": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                ]

            raw_docs = (
                await research.find(research_query)
                .sort([("processed_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in raw_docs:
                name = str(doc.get("name") or "").strip()
                if not name:
                    continue
                searchable = self._normalize_text(" ".join([
                    name,
                    str(doc.get("address") or ""),
                    str(doc.get("category") or ""),
                    str(doc.get("term_key") or ""),
                ]))
                if normalized_query and normalized_query not in searchable:
                    continue

                candidates.append(
                    {
                        "business_name": name,
                        "category": str(doc.get("category") or "").strip() or None,
                        "address": str(doc.get("address") or "").strip() or None,
                        "city": self._extract_city_from_address(str(doc.get("address") or "").strip() or None),
                        "phone": str(doc.get("phone") or "").strip() or None,
                        "email": str(doc.get("email") or "").strip() or None,
                        "website": str(doc.get("website") or "").strip() or None,
                        "source": "research_google_maps",
                        "source_ref": {
                            "listing_id": str(doc.get("listing_id") or "").strip() or None,
                            "term_id": str(doc.get("term_id") or "").strip() or None,
                            "term_key": str(doc.get("term_key") or "").strip() or None,
                            "maps_url": str(doc.get("maps_url") or "").strip() or None,
                        },
                        "rating": doc.get("rating"),
                        "review_count": doc.get("review_count"),
                    }
                )
                if len(candidates) >= safe_limit:
                    return candidates

        if "businesses" in sources_to_scan and len(candidates) < safe_limit:
            businesses = database[self._BUSINESSES_COLLECTION]
            business_query: dict[str, Any] = {}
            if normalized_category:
                business_query["listing.categories"] = {"$regex": re.escape(task_payload.category or ""), "$options": "i"}

            business_docs = (
                await businesses.find(business_query)
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in business_docs:
                name = str(doc.get("name") or "").strip()
                if not name:
                    continue
                listing = doc.get("listing") if isinstance(doc.get("listing"), dict) else {}
                address = str(listing.get("address") or "").strip() or None
                searchable = self._normalize_text(" ".join([
                    name,
                    str(address or ""),
                    " ".join([str(item) for item in (listing.get("categories") or []) if str(item).strip()]),
                ]))
                if normalized_query and normalized_query not in searchable:
                    continue
                if normalized_city and normalized_city not in self._normalize_text(address):
                    continue

                candidates.append(
                    {
                        "business_name": name,
                        "category": ", ".join([str(item) for item in (listing.get("categories") or []) if str(item).strip()]) or None,
                        "address": address,
                        "city": self._extract_city_from_address(address),
                        "phone": str(listing.get("phone") or "").strip() or None,
                        "email": None,
                        "website": str(listing.get("website") or "").strip() or None,
                        "source": str(doc.get("source") or "google_maps"),
                        "source_ref": {
                            "business_id": str(doc.get("_id")),
                            "name_normalized": str(doc.get("name_normalized") or "").strip() or None,
                        },
                        "rating": listing.get("overall_rating"),
                        "review_count": listing.get("total_reviews"),
                    }
                )
                if len(candidates) >= safe_limit:
                    return candidates

        return candidates[:safe_limit]

    async def _discover_candidates_live_google_maps(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or "").strip()
        city_text = str(task_payload.city or "").strip()
        search_query = query_text if not city_text else f"{query_text} {city_text}".strip()
        if not search_query:
            return []

        scraper = BusinessService.build_default_scraper()
        max_scroll_rounds = min(180, max(20, int(safe_limit // 2) + 10))
        scroll_wait_ms = max(400, int(settings.scraper_html_scroll_min_interval_s * 1000))
        collected: dict[str, dict[str, Any]] = {}

        try:
            await scraper.start()
            await self._search_google_maps_query(scraper=scraper, query=search_query)
            feed_found = await self._wait_for_results_feed(scraper=scraper, timeout_ms=16_000)
            if not feed_found:
                listing_name = ""
                for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
                    locator = scraper.page.locator(selector).first
                    try:
                        if await locator.is_visible():
                            listing_name = str(await locator.inner_text()).strip()
                            break
                    except Exception:
                        continue

                current_url = str(scraper.page.url or "").strip()
                if listing_name and "/maps/place/" in current_url:
                    canonical_url = self._canonicalize_maps_url(current_url)
                    fallback_candidates = [
                        {
                            "business_name": listing_name,
                            "category": str(task_payload.category or "").strip() or None,
                            "address": None,
                            "city": str(task_payload.city or "").strip() or None,
                            "phone": None,
                            "email": None,
                            "website": None,
                            "source": "google_maps_live_discovery",
                            "source_ref": {
                                "maps_url": current_url,
                                "maps_url_canonical": canonical_url or current_url,
                                "discovery_query": search_query,
                                "source_card_label": None,
                                "discovery_mode": "live_google_maps_auto_scroll",
                            },
                            "rating": None,
                            "review_count": None,
                        }
                    ]
                    return await self._enrich_live_google_maps_candidates(
                        scraper=scraper,
                        candidates=fallback_candidates,
                    )
                return []

            stable_rounds = 0
            for _ in range(max_scroll_rounds):
                before = len(collected)
                visible_items = await self._collect_visible_google_maps_results(scraper=scraper)
                for item in visible_items:
                    name = str(item.get("name") or "").strip()
                    raw_url = str(item.get("maps_url") or "").strip()
                    canonical_url = self._canonicalize_maps_url(raw_url)
                    if not name or not canonical_url:
                        continue
                    key = f"{canonical_url}|{self._normalize_text(name)}"
                    if key in collected:
                        continue
                    source_card_label = str(item.get("source_card_label") or "").strip() or None

                    collected[key] = {
                        "business_name": name,
                        "category": str(task_payload.category or "").strip() or None,
                        "address": None,
                        "city": str(task_payload.city or "").strip() or None,
                        "phone": None,
                        "email": None,
                        "website": None,
                        "source": "google_maps_live_discovery",
                        "source_ref": {
                            "maps_url": raw_url,
                            "maps_url_canonical": canonical_url,
                            "discovery_query": search_query,
                            "source_card_label": source_card_label,
                            "discovery_mode": "live_google_maps_auto_scroll",
                        },
                        "rating": item.get("rating"),
                        "review_count": item.get("review_count"),
                    }

                if len(collected) >= safe_limit:
                    break

                if len(collected) == before:
                    growth_detected = await self._wait_for_results_feed_growth(
                        scraper=scraper,
                        min_wait_ms=900,
                        max_wait_ms=4_200,
                    )
                    if growth_detected:
                        stable_rounds = 0
                        continue
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                if stable_rounds >= 5:
                    break

                await self._scroll_google_maps_results(scraper=scraper)
                await scraper.page.wait_for_timeout(scroll_wait_ms)

            candidates = list(collected.values())
            candidates.sort(key=lambda item: self._normalize_text(str(item.get("business_name") or "")))
            if normalized_query:
                # Keep cards that better match user query tokens first.
                query_tokens = set(normalized_query.split())
                candidates.sort(
                    key=lambda item: len(
                        query_tokens
                        & set(self._normalize_text(str(item.get("business_name") or "")).split())
                    ),
                    reverse=True,
                )
            top_candidates = candidates[:safe_limit]
            return await self._enrich_live_google_maps_candidates(
                scraper=scraper,
                candidates=top_candidates,
            )
        finally:
            await scraper.close()

    async def _wait_for_results_feed(self, *, scraper: GoogleMapsScraper, timeout_ms: int = 15_000) -> bool:
        deadline = asyncio.get_running_loop().time() + (max(1, int(timeout_ms)) / 1000.0)
        while asyncio.get_running_loop().time() < deadline:
            for selector in SELECTOR_PATTERNS["RESULTS_FEED"]:
                locator = scraper.page.locator(selector).first
                try:
                    if await locator.is_visible():
                        return True
                except Exception:
                    continue
            await scraper.page.wait_for_timeout(220)
        return False

    async def _first_visible_from_patterns(
        self,
        *,
        scraper: GoogleMapsScraper,
        key: str,
        timeout_ms: int = 1_200,
    ) -> Any | None:
        for selector in SELECTOR_PATTERNS[key]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return locator
            except Exception:
                continue
            try:
                await locator.wait_for(state="visible", timeout=max(1, timeout_ms))
                return locator
            except Exception:
                continue
        return None

    async def _search_google_maps_query(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        # Align discovery with the same startup flow used in regular Maps scraping:
        # navigate/normalize home state first, then dismiss consent if still present.
        await scraper._go_to_maps_home()
        await scraper._dismiss_google_consent_if_present()
        search_input = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_INPUT", timeout_ms=8_000)
        if search_input is None:
            raise RuntimeError("No se encontró el input de búsqueda de Google Maps para discovery live.")

        await scraper._human_click(search_input)
        await scraper.page.keyboard.press("Control+A")
        await scraper.page.keyboard.press("Backspace")
        await scraper._human_type(search_input, query)
        await scraper.page.wait_for_timeout(300)

        search_button = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_BUTTON", timeout_ms=2_000)
        if search_button is None:
            await scraper.page.keyboard.press("Enter")
        else:
            await scraper._human_click(search_button)

    async def _search_google_maps_query_from_current_view(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        await scraper._dismiss_google_consent_if_present()
        search_input = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_INPUT", timeout_ms=8_000)
        if search_input is None:
            raise RuntimeError("No se encontró el input de búsqueda de Google Maps para GeoGrid.")

        await scraper._human_click(search_input)
        await scraper.page.keyboard.press("Control+A")
        await scraper.page.keyboard.press("Backspace")
        await scraper._human_type(search_input, query)
        await scraper.page.wait_for_timeout(250)

        search_button = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_BUTTON", timeout_ms=2_000)
        if search_button is None:
            await scraper.page.keyboard.press("Enter")
        else:
            await scraper._human_click(search_button)

    async def _discover_geo_grid_point_results(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
    ) -> list[dict[str, Any]]:
        lat = float(point.get("lat"))
        lng = float(point.get("lng"))
        point_order = int(point.get("order") or 0)
        point_label = str(point.get("label") or f"Punto {point_order}").strip()
        safe_top_n = max(1, min(100, int(top_n or 10)))
        center_url = f"https://www.google.com/maps/@{lat},{lng},15z?hl=es"
        await scraper.page.goto(center_url, wait_until="domcontentloaded", timeout=settings.scraper_timeout_ms)
        await scraper.page.wait_for_timeout(700)
        await self._search_google_maps_query_from_current_view(scraper=scraper, query=keyword)
        feed_found = await self._wait_for_results_feed(scraper=scraper, timeout_ms=16_000)
        if not feed_found:
            fallback = await self._extract_geo_grid_single_listing_result(
                scraper=scraper,
                keyword=keyword,
                point_order=point_order,
                point_label=point_label,
                lat=lat,
                lng=lng,
            )
            return [fallback] if fallback else []

        collected: dict[str, dict[str, Any]] = {}
        stable_rounds = 0
        max_scroll_rounds = min(80, max(8, int(safe_top_n // 3) + 6))
        scroll_wait_ms = max(350, int(settings.scraper_html_scroll_min_interval_s * 1000))
        for _ in range(max_scroll_rounds):
            before = len(collected)
            for item in await self._collect_visible_google_maps_results(scraper=scraper):
                name = str(item.get("name") or "").strip()
                raw_url = str(item.get("maps_url") or "").strip()
                canonical_url = self._canonicalize_maps_url(raw_url)
                if not name:
                    continue
                key = canonical_url or self._normalize_text(name)
                if not key or key in collected:
                    continue
                rank = len(collected) + 1
                if rank > safe_top_n:
                    break
                collected[key] = {
                    "rank": rank,
                    "visible_top10": rank <= 10,
                    "provider_mode": "maps_live",
                    "business_key": key,
                    "business_name": name,
                    "maps_url": raw_url or None,
                    "maps_url_canonical": canonical_url or None,
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "category": None,
                    "source_ref": {
                        "keyword": keyword,
                        "point_order": point_order,
                        "point_label": point_label,
                        "lat": lat,
                        "lng": lng,
                        "row": point.get("row"),
                        "col": point.get("col"),
                        "source_card_label": item.get("source_card_label"),
                        "collection_mode": "geo_grid_feed",
                    },
                }

            if len(collected) >= safe_top_n:
                break
            if len(collected) == before:
                growth_detected = await self._wait_for_results_feed_growth(
                    scraper=scraper,
                    min_wait_ms=700,
                    max_wait_ms=3_600,
                )
                if growth_detected:
                    stable_rounds = 0
                    continue
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 4:
                break
            await self._scroll_google_maps_results(scraper=scraper)
            await scraper.page.wait_for_timeout(scroll_wait_ms)

        return list(collected.values())[:safe_top_n]

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
        lat = float(point.get("lat"))
        lng = float(point.get("lng"))
        point_order = int(point.get("order") or 0)
        point_label = str(point.get("label") or f"Punto {point_order}").strip()
        safe_top_n = max(1, min(100, int(top_n or 10)))
        safe_radius_m = max(100, int(radius_m or 1000))
        safe_throttle_ms = max(100, int(throttle_ms or 1200))

        uule = generate_uule_v2(lat=lat, lng=lng, radius_m=safe_radius_m)
        gl = str(settings.geo_grid_uule_gl or "es").strip().lower() or "es"
        hl = str(settings.geo_grid_uule_hl or "es").strip().lower() or "es"
        search_url = (
            "https://www.google.com/search?"
            f"q={quote_plus(keyword)}"
            "&tbm=lcl"
            f"&uule={quote_plus(uule)}"
            f"&gl={quote_plus(gl)}"
            f"&hl={quote_plus(hl)}"
            f"&num={max(20, safe_top_n)}"
        )

        await scraper.page.goto(search_url, wait_until="domcontentloaded", timeout=settings.scraper_timeout_ms)
        await scraper.page.wait_for_timeout(safe_throttle_ms)

        raw_items = await scraper.page.evaluate(
            """
            () => {
              const text = (value) => String(value || "").replace(/\\s+/g, " ").trim();
              const getNameFromText = (value) => {
                const clean = text(value);
                if (!clean) return "";
                return clean.split("·")[0].split("|")[0].split("\\n")[0].trim();
              };

              const cards = Array.from(document.querySelectorAll(".VkpGBb, .rllt__details, .cXedhc"));
              const anchors = cards.length
                ? cards.flatMap((card) => Array.from(card.querySelectorAll("a[href*='/maps/place/']")))
                : Array.from(document.querySelectorAll("a[href*='/maps/place/']"));

              const rows = [];
              for (const anchor of anchors) {
                if (!(anchor instanceof HTMLAnchorElement)) continue;
                const href = text(anchor.href);
                if (!href || !href.includes("/maps/place/")) continue;
                const card = anchor.closest(".VkpGBb, .rllt__details, .cXedhc, div[role='article'], div[jscontroller]") || anchor.parentElement;
                const cardText = text(card ? card.innerText : "");
                const anchorText = text(anchor.innerText || anchor.getAttribute("aria-label") || "");
                const name = getNameFromText(anchorText) || getNameFromText(cardText);
                if (!name) continue;
                rows.push({
                  name,
                  maps_url: href,
                  snippet: cardText || anchorText,
                });
              }

              return rows;
            }
            """
        )

        deduped: dict[str, dict[str, Any]] = {}
        for item in raw_items if isinstance(raw_items, list) else []:
            if not isinstance(item, dict):
                continue
            business_name = str(item.get("name") or "").strip()
            raw_url = str(item.get("maps_url") or "").strip()
            if not business_name:
                continue
            canonical_url = self._canonicalize_maps_url(raw_url)
            key = canonical_url or self._normalize_text(business_name)
            if not key or key in deduped:
                continue
            snippet = str(item.get("snippet") or "").strip()
            rating = self._parse_rating_text(snippet)
            review_count = self._parse_reviews_count_text(snippet)
            deduped[key] = {
                "business_name": business_name,
                "maps_url": raw_url or None,
                "maps_url_canonical": canonical_url or None,
                "rating": rating,
                "review_count": review_count,
                "category": None,
            }
            if len(deduped) >= safe_top_n:
                break

        if not deduped:
            return await self._discover_geo_grid_point_results(
                scraper=scraper,
                keyword=keyword,
                point=point,
                top_n=safe_top_n,
            )

        payloads: list[dict[str, Any]] = []
        for index, item in enumerate(deduped.values(), start=1):
            payloads.append(
                {
                    "rank": index,
                    "visible_top10": index <= 10,
                    "provider_mode": "uule",
                    "business_key": item.get("maps_url_canonical") or self._normalize_text(item.get("business_name")),
                    "business_name": item.get("business_name"),
                    "maps_url": item.get("maps_url"),
                    "maps_url_canonical": item.get("maps_url_canonical"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "category": item.get("category"),
                    "source_ref": {
                        "keyword": keyword,
                        "point_order": point_order,
                        "point_label": point_label,
                        "lat": lat,
                        "lng": lng,
                        "row": point.get("row"),
                        "col": point.get("col"),
                        "uule": uule,
                        "uule_radius_m": safe_radius_m,
                        "gl": gl,
                        "hl": hl,
                        "collection_mode": "geo_grid_uule_local_pack",
                    },
                }
            )
        return payloads[:safe_top_n]

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
        current_url = str(scraper.page.url or "").strip()
        if "/maps/place/" not in current_url:
            return None
        listing_name = ""
        for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    listing_name = str(await locator.inner_text()).strip()
                    break
            except Exception:
                continue
        if not listing_name:
            return None
        canonical_url = self._canonicalize_maps_url(current_url)
        return {
            "rank": 1,
            "visible_top10": True,
            "provider_mode": "maps_live",
            "business_key": canonical_url or self._normalize_text(listing_name),
            "business_name": listing_name,
            "maps_url": current_url,
            "maps_url_canonical": canonical_url or None,
            "rating": self._parse_rating_text(await self._safe_listing_text(scraper=scraper, key="LISTING_RATING")),
            "review_count": self._parse_reviews_count_text(
                await self._safe_listing_text(scraper=scraper, key="LISTING_TOTAL_REVIEWS")
            ),
            "category": None,
            "source_ref": {
                "keyword": keyword,
                "point_order": point_order,
                "point_label": point_label,
                "lat": lat,
                "lng": lng,
                "row": point_order,
                "col": 1,
                "collection_mode": "geo_grid_single_listing_fallback",
            },
        }

    async def _safe_listing_text(self, *, scraper: GoogleMapsScraper, key: str) -> str | None:
        for selector in SELECTOR_PATTERNS.get(key, []):
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return str(await locator.inner_text()).strip()
            except Exception:
                continue
        return None

    async def _read_results_feed_metrics(self, *, scraper: GoogleMapsScraper) -> dict[str, Any]:
        raw = await scraper.page.evaluate(
            """
            () => {
              const isVisible = (node) => {
                if (!(node instanceof HTMLElement)) return false;
                const style = window.getComputedStyle(node);
                if (!style) return false;
                return style.display !== "none" && style.visibility !== "hidden";
              };

              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );
              let bestFeed = null;
              let bestAnchors = [];
              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchors = Array.from(candidate.querySelectorAll("a[href*='/maps/place/']"));
                if (anchors.length > bestAnchors.length) {
                  bestAnchors = anchors;
                  bestFeed = candidate;
                }
              }
              if (!(bestFeed instanceof HTMLElement)) {
                return { found: false };
              }

              const anchorCount = bestAnchors.length;
              const loadingProgress = Array.from(
                document.querySelectorAll("[role='progressbar'], div[aria-busy='true'], div[aria-label*='cargando' i], div[aria-label*='loading' i]")
              ).some(isVisible);
              const loadingSkeleton = Array.from(
                bestFeed.querySelectorAll(".q0z1yb.CiOaN, .UJwFBf, .uQ4NLd")
              ).some(isVisible);
              const loading = loadingProgress || loadingSkeleton;

              const scrollHeight = Number(bestFeed.scrollHeight || 0);
              const scrollTop = Number(bestFeed.scrollTop || 0);
              const clientHeight = Number(bestFeed.clientHeight || 0);
              const atBottom = scrollTop + clientHeight >= scrollHeight - 6;

              return {
                found: true,
                anchor_count: anchorCount,
                loading: Boolean(loading),
                scroll_height: scrollHeight,
                scroll_top: scrollTop,
                client_height: clientHeight,
                at_bottom: Boolean(atBottom),
              };
            }
            """
        )
        if not isinstance(raw, dict):
            return {"found": False}
        return {
            "found": bool(raw.get("found")),
            "anchor_count": int(raw.get("anchor_count") or 0),
            "loading": bool(raw.get("loading")),
            "scroll_height": int(raw.get("scroll_height") or 0),
            "scroll_top": int(raw.get("scroll_top") or 0),
            "client_height": int(raw.get("client_height") or 0),
            "at_bottom": bool(raw.get("at_bottom")),
        }

    async def _wait_for_results_feed_growth(
        self,
        *,
        scraper: GoogleMapsScraper,
        min_wait_ms: int,
        max_wait_ms: int,
    ) -> bool:
        min_wait_ms = max(300, int(min_wait_ms))
        max_wait_ms = max(min_wait_ms, int(max_wait_ms))
        baseline = await self._read_results_feed_metrics(scraper=scraper)
        if not baseline.get("found"):
            return False

        loop = asyncio.get_running_loop()
        started_at = loop.time()
        deadline = started_at + (max_wait_ms / 1000.0)
        stable_without_loading = 0
        poll_ms = 320

        while loop.time() < deadline:
            await scraper.page.wait_for_timeout(poll_ms)
            current = await self._read_results_feed_metrics(scraper=scraper)
            if not current.get("found"):
                return False

            anchor_grew = int(current.get("anchor_count") or 0) > int(baseline.get("anchor_count") or 0)
            geometry_grew = int(current.get("scroll_height") or 0) > int(baseline.get("scroll_height") or 0) + 8
            if anchor_grew or geometry_grew:
                return True

            elapsed_ms = int((loop.time() - started_at) * 1000)
            if current.get("loading"):
                stable_without_loading = 0
                continue

            if elapsed_ms < min_wait_ms:
                continue

            stable_without_loading += 1
            if stable_without_loading >= 2:
                return False

        return False

    async def _collect_visible_google_maps_results(self, *, scraper: GoogleMapsScraper) -> list[dict[str, Any]]:
        raw = await scraper.page.evaluate(
            """
            () => {
              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );
              let feed = null;
              let bestAnchors = [];
              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchors = Array.from(candidate.querySelectorAll("a[href*='/maps/place/']"));
                if (anchors.length > bestAnchors.length) {
                  bestAnchors = anchors;
                  feed = candidate;
                }
              }
              if (!feed) {
                return { found: false, items: [] };
              }

              const readText = (node) => {
                if (!node || !node.textContent) return "";
                return String(node.textContent).trim();
              };

              const anchors = bestAnchors.length
                ? bestAnchors
                : Array.from(feed.querySelectorAll("a[href*='/maps/place/']"));
              const items = [];
              for (const anchor of anchors) {
                const article =
                  anchor.closest("div[role='article']") ||
                  anchor.closest("div.Nv2PK") ||
                  anchor.parentElement;
                const labelFromAnchor = String(anchor.getAttribute("aria-label") || "").trim();
                const heading =
                  article && article.querySelector
                    ? article.querySelector("h3, [role='heading'], .qBF1Pd, .fontHeadlineSmall")
                    : null;
                const labelFromHeading = readText(heading);
                const labelFromArticle = String(
                  article && article.getAttribute ? article.getAttribute("aria-label") || "" : ""
                ).trim();
                const fallbackText = readText(anchor).split("\\n")[0].trim();
                const name = labelFromHeading || labelFromAnchor || labelFromArticle || fallbackText;
                const ratingAria = String(
                  (
                    article &&
                    article.querySelector &&
                    article.querySelector("[role='img'][aria-label*='estrella' i], [role='img'][aria-label*='star' i]")
                  )?.getAttribute("aria-label") || ""
                ).trim();
                const ratingText = readText(
                  article && article.querySelector ? article.querySelector(".MW4etd") : null
                );
                const reviewsText = readText(
                  article && article.querySelector ? article.querySelector(".UY7F9") : null
                );
                const href = String(anchor.href || "").trim();
                if (!name || !href) continue;
                items.push({
                  name: name,
                  maps_url: href,
                  source_card_label: labelFromArticle || labelFromAnchor || null,
                  rating_label: ratingAria || ratingText || null,
                  reviews_label: reviewsText || ratingAria || null,
                });
              }
              return { found: true, items: items };
            }
            """
        )
        if not isinstance(raw, dict):
            return []
        items = raw.get("items")
        if not isinstance(items, list):
            return []
        cleaned: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            maps_url = str(item.get("maps_url") or "").strip()
            if not name or not maps_url:
                continue
            cleaned.append(
                {
                    "name": name,
                    "maps_url": maps_url,
                    "source_card_label": str(item.get("source_card_label") or "").strip() or None,
                    "rating": self._parse_rating_text(item.get("rating_label")),
                    "review_count": self._parse_reviews_count_text(item.get("reviews_label")),
                }
            )
        return cleaned

    async def _scroll_google_maps_results(self, *, scraper: GoogleMapsScraper) -> None:
        await scraper.page.evaluate(
            """
            () => {
              const isScrollable = (el) => {
                if (!(el instanceof HTMLElement)) return false;
                const style = window.getComputedStyle(el);
                const overflowY = String(style.overflowY || "");
                const canScroll = el.scrollHeight > el.clientHeight + 20;
                return canScroll && ["auto", "scroll", "overlay"].includes(overflowY);
              };

              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );

              let best = null;
              let bestAnchorCount = -1;

              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchorCount = candidate.querySelectorAll("a[href*='/maps/place/']").length;
                if (!isScrollable(candidate)) continue;
                if (anchorCount > bestAnchorCount) {
                  best = candidate;
                  bestAnchorCount = anchorCount;
                }
              }

              if (!best) {
                // Fallback: look for a scrollable ancestor of any place anchor.
                const anchor = document.querySelector("a[href*='/maps/place/']");
                let parent = anchor ? anchor.parentElement : null;
                while (parent && parent !== document.body) {
                  if (isScrollable(parent)) {
                    best = parent;
                    break;
                  }
                  parent = parent.parentElement;
                }
              }

              if (!best) return;
              const step = Math.max(900, Math.floor(best.clientHeight * 0.9));
              const before = best.scrollTop;
              best.scrollBy({ top: step, left: 0, behavior: "auto" });
              if (best.scrollTop === before) {
                best.scrollTop = Math.min(best.scrollTop + step, best.scrollHeight);
              }
            }
            """
        )

    async def _enrich_live_google_maps_candidates(
        self,
        *,
        scraper: GoogleMapsScraper,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not candidates:
            return []

        try:
            browser_context = scraper.page.context
        except Exception:
            return candidates

        if browser_context is None:
            return candidates

        try:
            detail_page = await browser_context.new_page()
        except Exception:
            return candidates

        detail_scraper = GoogleMapsScraper(page=detail_page)
        enriched_candidates: list[dict[str, Any]] = []
        try:
            for candidate in candidates:
                enriched_candidates.append(
                    await self._enrich_live_google_maps_candidate(
                        detail_scraper=detail_scraper,
                        candidate=candidate,
                    )
                )
        finally:
            try:
                await detail_page.close()
            except Exception:
                pass
        return enriched_candidates

    async def _enrich_live_google_maps_candidate(
        self,
        *,
        detail_scraper: GoogleMapsScraper,
        candidate: dict[str, Any],
        timeout_ms: int = 11_000,
    ) -> dict[str, Any]:
        enriched = dict(candidate)
        source_ref = dict(enriched.get("source_ref") or {})
        raw_maps_url = str(source_ref.get("maps_url") or "").strip()
        canonical_maps_url = self._canonicalize_maps_url(
            raw_maps_url or str(source_ref.get("maps_url_canonical") or "").strip()
        )
        target_maps_url = raw_maps_url or canonical_maps_url
        if not target_maps_url:
            return enriched

        listing: dict[str, Any] = {}
        try:
            await detail_scraper.page.goto(target_maps_url, wait_until="domcontentloaded")
            await detail_scraper._dismiss_google_consent_if_present()
            await detail_scraper._wait_for_listing_ready(timeout_ms=max(4_000, int(timeout_ms)))
            listing = await detail_scraper.extract_listing()
        except Exception as exc:
            source_ref["maps_url"] = target_maps_url
            source_ref["maps_url_canonical"] = canonical_maps_url or target_maps_url
            source_ref["listing_primary_extract_error"] = str(exc)[:180]

        listing_fallback = await self._extract_listing_fallback_from_dom(detail_scraper=detail_scraper)
        listing = self._merge_listing_payloads(primary=listing, fallback=listing_fallback)

        listing_name = str(listing.get("business_name") or "").strip()
        listing_address = str(listing.get("address") or "").strip() or None
        listing_phone = str(listing.get("phone") or "").strip() or None
        listing_website = str(listing.get("website") or "").strip() or None
        listing_rating = listing.get("overall_rating")
        listing_review_count = listing.get("total_reviews")

        category_values_raw: list[str] = []
        raw_categories = listing.get("categories")
        if isinstance(raw_categories, list):
            for value in raw_categories:
                cleaned = str(value or "").strip()
                if cleaned:
                    category_values_raw.append(cleaned)
        category_values = self._sanitize_listing_categories(category_values_raw)
        listing_category = ", ".join(category_values) if category_values else None
        listing_primary_category = category_values[0] if category_values else None

        if listing_name:
            enriched["business_name"] = listing_name
        if listing_address:
            enriched["address"] = listing_address
            if not str(enriched.get("city") or "").strip():
                enriched["city"] = self._extract_city_from_address(listing_address)
        if listing_phone:
            enriched["phone"] = listing_phone
        if listing_website:
            enriched["website"] = listing_website
        if listing_rating is not None:
            enriched["rating"] = listing_rating
        if listing_review_count is not None:
            enriched["review_count"] = listing_review_count
        if listing_primary_category:
            enriched["category"] = listing_primary_category

        current_page_url = str(detail_scraper.page.url or "").strip()
        source_ref["maps_url"] = current_page_url or target_maps_url
        source_ref["maps_url_canonical"] = self._canonicalize_maps_url(current_page_url) or canonical_maps_url or target_maps_url
        source_ref["discovery_mode"] = "live_google_maps_auto_scroll_listing_extract"
        listing_details: dict[str, Any] = {}
        for key in ("service_options", "price_per_person", "description", "menu_url", "reservation_url"):
            value = listing.get(key)
            if isinstance(value, list):
                cleaned_items = [str(item or "").strip() for item in value if str(item or "").strip()]
                if cleaned_items:
                    listing_details[key] = cleaned_items
                continue
            cleaned_text = str(value or "").strip()
            if cleaned_text:
                listing_details[key] = cleaned_text
        if listing_category:
            listing_details["categories"] = category_values
        if listing_primary_category:
            listing_details["category"] = listing_primary_category
        if listing_details:
            source_ref["listing_details"] = listing_details

        listing_enriched = bool(
            listing_name
            or listing_address
            or listing_phone
            or listing_website
            or listing_rating is not None
            or listing_review_count is not None
        )
        source_ref["listing_enriched"] = listing_enriched
        source_ref.pop("listing_extract_error", None)
        primary_extract_error = str(source_ref.get("listing_primary_extract_error") or "").strip()
        if listing_enriched:
            source_ref.pop("listing_primary_extract_error", None)
        elif primary_extract_error:
            source_ref["listing_extract_error"] = primary_extract_error
        enriched["source_ref"] = source_ref
        return enriched

    async def _extract_listing_fallback_from_dom(self, *, detail_scraper: GoogleMapsScraper) -> dict[str, Any]:
        try:
            raw = await detail_scraper.page.evaluate(
                """
                () => {
                  const clean = (value) => {
                    if (typeof value !== "string") return "";
                    return value.replace(/\\s+/g, " ").trim();
                  };
                  const text = (el) => clean(el && el.textContent ? String(el.textContent) : "");

                  const businessName =
                    text(document.querySelector("h1")) ||
                    text(document.querySelector("[role='main'] h1")) ||
                    "";
                  const address = text(
                    document.querySelector("[data-item-id='address'] .Io6YTe") ||
                    document.querySelector("[data-item-id='address']")
                  );
                  const phone = text(
                    document.querySelector("[data-item-id^='phone:'] .Io6YTe") ||
                    document.querySelector("[data-item-id^='phone:']")
                  );
                  const websiteText = text(
                    document.querySelector("[data-item-id='authority'] .Io6YTe") ||
                    document.querySelector("[data-item-id='authority']")
                  );
                  const pickHref = (selectors) => {
                    for (const selector of selectors) {
                      const node = document.querySelector(selector);
                      if (!node) continue;
                      const href = clean(String(node.getAttribute("href") || ""));
                      if (href) return href;
                    }
                    return "";
                  };
                  let websiteHref = "";
                  const websiteAnchor = document.querySelector("[data-item-id='authority'] a[href]");
                  if (websiteAnchor && websiteAnchor.getAttribute) {
                    websiteHref = clean(String(websiteAnchor.getAttribute("href") || ""));
                  }

                  const pickAriaLabel = (nodes, expectedKeyword) => {
                    let best = "";
                    for (const node of nodes) {
                      if (!node || !node.getAttribute) continue;
                      const rawValue = String(node.getAttribute("aria-label") || "").trim();
                      if (!rawValue) continue;
                      const hasDigit = /\\d/.test(rawValue);
                      const normalized = rawValue.toLowerCase();
                      if (hasDigit && normalized.includes(expectedKeyword)) {
                        return rawValue;
                      }
                      if (hasDigit && !best) {
                        best = rawValue;
                      } else if (!best) {
                        best = rawValue;
                      }
                    }
                    return best;
                  };

                  const ratingNodes = Array.from(
                    document.querySelectorAll("[aria-label*='estrella' i], [aria-label*='star' i], [role='img'][aria-label]")
                  );
                  const reviewsNodes = Array.from(
                    document.querySelectorAll("[aria-label*='rese' i], [aria-label*='review' i], button[jsaction*='reviewChart.moreReviews']")
                  );
                  const ratingLabel = pickAriaLabel(ratingNodes, "estrella");
                  const reviewsLabel = pickAriaLabel(reviewsNodes, "rese");
                  const ratingText =
                    text(document.querySelector(".F7nice .MW4etd")) ||
                    text(document.querySelector(".AJB7ye .MW4etd")) ||
                    text(document.querySelector(".ZkP5Je .MW4etd")) ||
                    "";
                  const reviewsText =
                    text(document.querySelector(".F7nice .UY7F9")) ||
                    text(document.querySelector(".AJB7ye .UY7F9")) ||
                    text(document.querySelector(".ZkP5Je .UY7F9")) ||
                    "";

                  const categoryButtons = Array.from(
                    document.querySelectorAll("button[jsaction*='.category'], div.LBgpqf button[jsaction*='.category'], div.LBgpqf .fontBodyMedium button")
                  );
                  const categories = [];
                  const seen = new Set();
                  for (const button of categoryButtons) {
                    const value = text(button);
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (seen.has(key)) continue;
                    seen.add(key);
                    categories.push(value);
                    if (categories.length >= 6) break;
                  }
                  if (!categories.length) {
                    const headerLine = text(document.querySelector("div.LBgpqf .fontBodyMedium"));
                    if (headerLine) {
                      categories.push(headerLine);
                    }
                  }

                  const serviceOptions = [];
                  const serviceSeen = new Set();
                  const serviceNodes = Array.from(document.querySelectorAll("div.y0K5Df .LTs0Rc"));
                  for (const node of serviceNodes) {
                    const aria = clean(String(node.getAttribute("aria-label") || ""));
                    const fallback = text(node.querySelector("div[aria-hidden='true']") || node);
                    const value = aria || fallback;
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (serviceSeen.has(key)) continue;
                    serviceSeen.add(key);
                    serviceOptions.push(value);
                  }

                  const priceText =
                    text(document.querySelector(".DfOCNb .MNVeJb > div")) ||
                    text(document.querySelector("[data-item-id='price'] .Io6YTe")) ||
                    null;
                  const descriptionText = text(document.querySelector("div.y0K5Df .PYvSYb")) || null;
                  const menuUrl = pickHref([
                    "a[data-item-id='menu'][href]",
                    "[data-item-id='menu'] a[href]",
                  ]);
                  const reservationUrl = pickHref([
                    "a[data-item-id^='action:'][href]",
                    "a[href*='/maps/reserve/']",
                    "a[href*='/reserve/']",
                  ]);
                  const websiteUrl = pickHref([
                    "a[data-item-id='authority'][href]",
                    "[data-item-id='authority'] a[href]",
                  ]);

                  return {
                    business_name: businessName || null,
                    address: address || null,
                    phone: phone || null,
                    website: websiteText || websiteHref || null,
                    website_url: websiteUrl || websiteHref || null,
                    rating_label: ratingLabel || ratingText || null,
                    reviews_label: reviewsLabel || reviewsText || null,
                    categories: categories,
                    service_options: serviceOptions,
                    price_per_person: priceText || null,
                    description: descriptionText || null,
                    menu_url: menuUrl || null,
                    reservation_url: reservationUrl || null,
                  };
                }
                """
            )
        except Exception:
            return {}

        if not isinstance(raw, dict):
            return {}

        rating_value = self._parse_rating_text(raw.get("rating_label"))
        reviews_value = self._parse_reviews_count_text(raw.get("reviews_label"))

        categories: list[str] = []
        raw_categories = raw.get("categories")
        if isinstance(raw_categories, list):
            for item in raw_categories:
                cleaned = str(item or "").strip()
                if cleaned:
                    categories.append(cleaned)
        categories = self._sanitize_listing_categories(categories)

        service_options: list[str] = []
        raw_service_options = raw.get("service_options")
        if isinstance(raw_service_options, list):
            seen: set[str] = set()
            for item in raw_service_options:
                cleaned = str(item or "").strip()
                if not cleaned:
                    continue
                key = self._normalize_text(cleaned)
                if key in seen:
                    continue
                seen.add(key)
                service_options.append(cleaned)

        return {
            "business_name": str(raw.get("business_name") or "").strip() or None,
            "address": str(raw.get("address") or "").strip() or None,
            "phone": str(raw.get("phone") or "").strip() or None,
            "website": str(raw.get("website") or raw.get("website_url") or "").strip() or None,
            "overall_rating": rating_value,
            "total_reviews": reviews_value,
            "categories": categories,
            "category": categories[0] if categories else None,
            "service_options": service_options,
            "price_per_person": str(raw.get("price_per_person") or "").strip() or None,
            "description": str(raw.get("description") or "").strip() or None,
            "menu_url": str(raw.get("menu_url") or "").strip() or None,
            "reservation_url": str(raw.get("reservation_url") or "").strip() or None,
        }

    def _merge_listing_payloads(self, *, primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = dict(primary or {})
        for key in ("business_name", "address", "phone", "website"):
            if not str(merged.get(key) or "").strip():
                value = str(fallback.get(key) or "").strip()
                if value:
                    merged[key] = value

        for key in ("price_per_person", "description", "menu_url", "reservation_url"):
            if not str(merged.get(key) or "").strip():
                value = str(fallback.get(key) or "").strip()
                if value:
                    merged[key] = value

        if merged.get("overall_rating") is None and fallback.get("overall_rating") is not None:
            merged["overall_rating"] = fallback.get("overall_rating")
        if merged.get("total_reviews") is None and fallback.get("total_reviews") is not None:
            merged["total_reviews"] = fallback.get("total_reviews")

        primary_categories = merged.get("categories") if isinstance(merged.get("categories"), list) else []
        fallback_categories = fallback.get("categories") if isinstance(fallback.get("categories"), list) else []
        merged_categories = self._sanitize_listing_categories(
            [str(item or "") for item in list(primary_categories) + list(fallback_categories)]
        )
        if merged_categories:
            merged["categories"] = merged_categories
            merged["category"] = merged_categories[0]
        elif str(fallback.get("category") or "").strip() and not str(merged.get("category") or "").strip():
            merged["category"] = str(fallback.get("category") or "").strip()

        primary_service_options = (
            merged.get("service_options")
            if isinstance(merged.get("service_options"), list)
            else []
        )
        fallback_service_options = (
            fallback.get("service_options")
            if isinstance(fallback.get("service_options"), list)
            else []
        )
        merged_service_options: list[str] = []
        seen_options: set[str] = set()
        for item in list(primary_service_options) + list(fallback_service_options):
            cleaned = str(item or "").strip()
            if not cleaned:
                continue
            key = self._normalize_text(cleaned)
            if key in seen_options:
                continue
            seen_options.add(key)
            merged_service_options.append(cleaned)
        if merged_service_options:
            merged["service_options"] = merged_service_options
        return merged

    def _parse_rating_text(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.search(r"([0-5](?:[.,]\d)?)", text)
        if not match:
            return None
        raw_value = str(match.group(1) or "").replace(",", ".")
        try:
            rating = float(raw_value)
        except ValueError:
            return None
        if rating < 0.0 or rating > 5.0:
            return None
        return rating

    def _sanitize_listing_categories(self, raw_values: list[str]) -> list[str]:
        cleaned_values: list[str] = []
        seen: set[str] = set()
        for raw_value in raw_values:
            raw_text = str(raw_value or "").strip()
            if not raw_text:
                continue
            without_symbols = re.sub(r"[\uE000-\uF8FF]", " ", raw_text)
            for part in re.split(r"[,\u00b7|\u2022]+", without_symbols):
                candidate = str(part or "").strip()
                if not candidate:
                    continue
                normalized = self._normalize_text(candidate)
                if not normalized or normalized in seen:
                    continue
                if self._is_noise_category_token(normalized):
                    continue
                seen.add(normalized)
                cleaned_values.append(candidate)
                if len(cleaned_values) >= 8:
                    return cleaned_values
        return cleaned_values

    def _is_noise_category_token(self, normalized_value: str) -> bool:
        if not normalized_value:
            return True
        if len(normalized_value) > 60:
            return True
        if re.search(r"\d", normalized_value):
            return True
        if not re.search(r"[a-z]", normalized_value):
            return True

        blocked_exact = {
            "copiar",
            "guardar",
            "compartir",
            "mas",
            "me gusta",
            "anadir precio",
            "sugerir nuevo horario",
            "como llegar",
            "ver mas",
            "enviar al telefono",
            "cercano",
            "resenas",
            "review",
            "reviews",
            "ordenar",
            "buscar resenas",
            "informacion",
            "vista general",
            "carta",
        }
        if normalized_value in blocked_exact:
            return True

        blocked_fragments = (
            "sugerir",
            "horario",
            "copiar",
            "compartir",
            "me gusta",
            "google maps",
            "aviso legal",
            "publicas en google maps",
            "mas informacion",
            "reservar una mesa",
        )
        return any(fragment in normalized_value for fragment in blocked_fragments)

    def _parse_reviews_count_text(self, value: Any) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        text_no_spaces = re.sub(r"\s+", "", text)
        candidates = re.findall(r"\d[\d\.,]*", text_no_spaces)
        if not candidates:
            return None

        parsed_values: list[int] = []
        for candidate in candidates:
            digits = re.sub(r"[^0-9]", "", candidate)
            if not digits:
                continue
            try:
                parsed_values.append(int(digits))
            except ValueError:
                continue
        if not parsed_values:
            return None
        return max(parsed_values)

    def _canonicalize_maps_url(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
        except Exception:
            return raw
        path = str(parsed.path or "").strip()
        if not path:
            return raw
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    async def _upsert_lead_candidate(self, candidate: dict[str, Any]) -> str:
        leads = get_database()[self._LEADS_COLLECTION]
        business_name = str(candidate.get("business_name") or "").strip()
        if not business_name:
            return "skipped"

        business_name_normalized = self._normalize_text(business_name)
        address = str(candidate.get("address") or "").strip() or None
        email = str(candidate.get("email") or "").strip() or None
        website = str(candidate.get("website") or "").strip() or None
        email_normalized = self._normalize_email(email)
        domain_normalized = self._domain_from_email_or_website(email=email, website=website)

        lead_query: dict[str, Any] | None = None
        if email_normalized:
            lead_query = {"email_normalized": email_normalized}
        else:
            lookup_clauses: list[dict[str, Any]] = [{"business_name_normalized": business_name_normalized}]
            if address:
                lookup_clauses.append({"address": address})
            if domain_normalized:
                lookup_clauses.append({"domain_normalized": domain_normalized})
            lead_query = {"$and": lookup_clauses}

        existing = await leads.find_one(lead_query) if lead_query else None

        rating_value = candidate.get("rating")
        review_count_value = candidate.get("review_count")
        parsed_rating = self._parse_rating_text(rating_value)
        parsed_review_count = self._parse_reviews_count_text(review_count_value)
        score = self._build_lead_score(
            rating=parsed_rating,
            review_count=parsed_review_count,
            has_email=bool(email_normalized),
            has_website=bool(website),
        )
        now = self._now_utc()

        if existing is None:
            legal = CRMLeadLegalBlock(
                consent_status=CRMConsentStatus.MISSING,
                consent_proof=None,
                do_not_contact=False,
                unsubscribed_at=None,
                suppressed_reason=None,
            )
            pipeline = CRMLeadPipelineRefs(
                business_id=None,
                source_job_ids=[],
                analysis_job_id=None,
                report_job_id=None,
                latest_report_artifacts={},
            )
            lead = CRMLead(
                business_name=business_name,
                business_name_normalized=business_name_normalized,
                email=email,
                email_normalized=email_normalized,
                domain_normalized=domain_normalized,
                phone=str(candidate.get("phone") or "").strip() or None,
                website=website,
                category=str(candidate.get("category") or "").strip() or None,
                city=str(candidate.get("city") or "").strip() or None,
                address=address,
                source=str(candidate.get("source") or "unknown"),
                source_ref=dict(candidate.get("source_ref") or {}),
                rating=parsed_rating,
                review_count=parsed_review_count,
                status=CRMLeadStatus.ENRICHING if not email_normalized else CRMLeadStatus.READY,
                score=score,
                legal=legal,
                pipeline=pipeline,
                notes=[],
                tags=[],
                created_at=now,
                updated_at=now,
            )
            await leads.insert_one(lead.model_dump(mode="python"))
            return "inserted"

        set_fields: dict[str, Any] = {"updated_at": now}
        if not str(existing.get("phone") or "").strip() and str(candidate.get("phone") or "").strip():
            set_fields["phone"] = str(candidate.get("phone") or "").strip()
        if not str(existing.get("website") or "").strip() and website:
            set_fields["website"] = website
        if not str(existing.get("email") or "").strip() and email:
            set_fields["email"] = email
            set_fields["email_normalized"] = email_normalized
        if not str(existing.get("domain_normalized") or "").strip() and domain_normalized:
            set_fields["domain_normalized"] = domain_normalized
        if not str(existing.get("address") or "").strip() and address:
            set_fields["address"] = address
        if not str(existing.get("city") or "").strip() and str(candidate.get("city") or "").strip():
            set_fields["city"] = str(candidate.get("city") or "").strip()
        if not str(existing.get("category") or "").strip() and str(candidate.get("category") or "").strip():
            set_fields["category"] = str(candidate.get("category") or "").strip()

        existing_rating = self._parse_rating_text(existing.get("rating"))
        if parsed_rating is not None:
            if existing_rating is None or abs(parsed_rating - existing_rating) > 1e-9:
                set_fields["rating"] = parsed_rating

        existing_review_count = self._parse_reviews_count_text(existing.get("review_count"))
        if parsed_review_count is not None:
            if existing_review_count is None or parsed_review_count > existing_review_count:
                set_fields["review_count"] = parsed_review_count

        existing_score = float(existing.get("score") or 0.0)
        if score > existing_score:
            set_fields["score"] = score

        source_ref = existing.get("source_ref") if isinstance(existing.get("source_ref"), dict) else {}
        merged_source_ref = {**source_ref, **dict(candidate.get("source_ref") or {})}
        set_fields["source_ref"] = merged_source_ref

        status_value = str(existing.get("status") or "").strip().lower()
        if status_value in {CRMLeadStatus.NEW.value, CRMLeadStatus.ENRICHING.value} and email_normalized:
            set_fields["status"] = CRMLeadStatus.READY.value

        if len(set_fields.keys()) <= 2:
            return "skipped"

        await leads.update_one({"_id": existing.get("_id")}, {"$set": set_fields})
        return "updated"

    async def _can_send_to_lead(self, *, lead_doc: dict[str, Any]) -> tuple[bool, str]:
        legal = lead_doc.get("legal") if isinstance(lead_doc.get("legal"), dict) else {}
        consent_status = str(legal.get("consent_status") or "").strip().lower()
        consent_proof = legal.get("consent_proof") if isinstance(legal.get("consent_proof"), dict) else None
        do_not_contact = bool(legal.get("do_not_contact"))
        unsubscribed_at = legal.get("unsubscribed_at")

        if do_not_contact:
            return False, "do_not_contact"
        if unsubscribed_at is not None:
            return False, "unsubscribed"
        if consent_status != CRMConsentStatus.GRANTED.value:
            return False, "consent_not_granted"
        if not consent_proof:
            return False, "consent_proof_missing"

        email = str(lead_doc.get("email") or "").strip()
        email_normalized = self._normalize_email(email)
        if not email or not email_normalized:
            return False, "email_missing"

        suppressed = await self._is_email_suppressed(email_normalized)
        if suppressed:
            return False, "suppressed"

        return True, "ok"

    async def _is_email_suppressed(self, email_normalized: str) -> bool:
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        doc = await suppressions.find_one({"email_normalized": email_normalized}, projection={"_id": 1})
        return doc is not None

    async def _block_lead_contact(self, *, lead_id: str, reason: str) -> None:
        leads = get_database()[self._LEADS_COLLECTION]
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        now = self._now_utc()

        set_fields: dict[str, Any] = {
            "legal.do_not_contact": True,
            "legal.suppressed_reason": reason,
            "updated_at": now,
        }
        if reason == "unsubscribed":
            set_fields["legal.unsubscribed_at"] = now

        await leads.update_one({"_id": parsed_lead_id}, {"$set": set_fields})
        await self._stop_pending_messages_for_lead(lead_id=lead_id, reason=reason)

    async def _stop_pending_messages_for_lead(self, *, lead_id: str, reason: str) -> None:
        messages = get_database()[self._MESSAGES_COLLECTION]
        now = self._now_utc()
        await messages.update_many(
            {
                "lead_id": lead_id,
                "status": CRMMessageStatus.QUEUED.value,
            },
            {
                "$set": {
                    "status": CRMMessageStatus.SKIPPED.value,
                    "error": f"stopped:{reason}",
                    "updated_at": now,
                    "dispatch_job_id": None,
                }
            },
        )

    async def _upsert_suppression(self, *, email: str, reason: str, source: str) -> None:
        normalized = self._normalize_email(email)
        if not normalized:
            return
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        now = self._now_utc()

        suppression = CRMSuppression(
            email=email,
            email_normalized=normalized,
            reason=str(reason or "manual"),
            source=str(source or "system"),
            created_at=now,
            updated_at=now,
        )
        payload = suppression.model_dump(mode="python")
        await suppressions.update_one(
            {"email_normalized": normalized},
            {
                "$set": {
                    "email": payload["email"],
                    "reason": payload["reason"],
                    "source": payload["source"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "email_normalized": normalized,
                },
            },
            upsert=True,
        )

    async def _record_event(
        self,
        *,
        event_type: str,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        message_id: str | None = None,
        data: dict[str, Any] | None = None,
        actor: str = "system",
    ) -> None:
        event = CRMEvent(
            event_type=str(event_type or "event").strip(),
            lead_id=str(lead_id).strip() if lead_id else None,
            campaign_id=str(campaign_id).strip() if campaign_id else None,
            message_id=str(message_id).strip() if message_id else None,
            actor=str(actor or "system"),
            data=dict(data or {}),
            created_at=self._now_utc(),
        )
        await self._event_repository.insert(event.model_dump(mode="python"))

    async def _build_mini_report_for_lead(self, *, lead_doc: dict[str, Any]) -> str:
        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        business_id = str(pipeline.get("business_id") or "").strip()
        if not business_id:
            return "Todavía no hay informe completo, pero podemos activarlo en tu ficha en cuanto lo prefieras."

        try:
            parsed_business_id = ObjectId(business_id)
        except InvalidId:
            return "Tenemos señales de mejora en reseñas recientes y podemos enseñártelas en una demo corta."

        analyses = get_database()[self._ANALYSES_COLLECTION]
        analysis_docs = (
            await analyses.find({"business_id": str(parsed_business_id)}).sort([("created_at", -1), ("_id", -1)]).limit(1).to_list(length=1)
        )
        if not analysis_docs:
            return "Hemos detectado oportunidades claras en servicio y reputación digital que te enseñamos en 15 minutos."

        analysis_doc = analysis_docs[0]
        stats = analysis_doc.get("stats") if isinstance(analysis_doc.get("stats"), dict) else {}
        avg_rating = stats.get("avg_rating")
        response_rate = stats.get("response_rate")
        rating_text = f"valoración media {float(avg_rating):.2f}/5" if isinstance(avg_rating, (int, float)) else "valoración media disponible"
        if isinstance(response_rate, (int, float)):
            response_pct = float(response_rate) * 100 if float(response_rate) <= 1.0 else float(response_rate)
            response_text = f"tasa de respuesta {response_pct:.0f}%"
        else:
            response_text = "tasa de respuesta mejorable"
        return f"Resumen actual: {rating_text}, {response_text}."

    def _render_cadence_step(
        self,
        *,
        step: CRMCadenceStep,
        lead_doc: dict[str, Any],
        mini_report: str,
    ) -> tuple[str, str]:
        business_name = str(lead_doc.get("business_name") or "tu negocio").strip()
        lead_id = str(lead_doc.get("_id") or "").strip()

        cta_url = str(settings.crm_cta_url or "").strip() or "https://repiq.es/#pre-report-form"
        unsubscribe_base = str(settings.crm_unsubscribe_url or "").strip() or cta_url
        unsubscribe_token = self._unsubscribe_token(lead_id=lead_id, email=str(lead_doc.get("email") or ""))
        sep = "&" if "?" in unsubscribe_base else "?"
        unsubscribe_url = f"{unsubscribe_base}{sep}lead={lead_id}&token={unsubscribe_token}"

        template_context = {
            "business_name": business_name,
            "mini_report": mini_report,
            "cta_url": cta_url,
            "unsubscribe_url": unsubscribe_url,
        }
        subject = str(step.subject_template).format(**template_context)
        body_text = str(step.body_template).format(**template_context)
        body_html = self._text_to_html(body_text)
        return subject, body_html

    def _send_resend_email(self, *, to_email: str, subject: str, html_body: str) -> dict[str, Any]:
        api_key = str(settings.crm_resend_api_key or "").strip()
        from_email = str(settings.crm_resend_from_email or "").strip()
        sender_name = str(settings.crm_sender_name or "Repiq").strip() or "Repiq"
        reply_to = str(settings.crm_resend_reply_to or "").strip() or None

        if not api_key or not from_email:
            return {
                "id": f"dryrun-{hashlib.sha1(f'{to_email}-{subject}'.encode('utf-8')).hexdigest()[:16]}",
                "dry_run": True,
                "reason": "missing_resend_config",
            }

        from_header = formataddr((sender_name, from_email))
        payload: dict[str, Any] = {
            "from": from_header,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        }
        if reply_to:
            payload["reply_to"] = reply_to

        body_bytes = json.dumps(payload).encode("utf-8")
        request = Request(
            url="https://api.resend.com/emails",
            data=body_bytes,
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=20) as response:  # noqa: S310 - endpoint fixed
            raw_response = response.read().decode("utf-8", errors="ignore")
            if not raw_response.strip():
                return {"id": None, "provider_status": response.status}
            parsed = json.loads(raw_response)
            if isinstance(parsed, dict):
                return parsed
            return {"id": None, "provider_status": response.status, "raw": parsed}

    def _text_to_html(self, text: str) -> str:
        lines = [line.strip() for line in str(text or "").splitlines()]
        html_lines: list[str] = []
        for line in lines:
            if not line:
                html_lines.append("<p>&nbsp;</p>")
                continue
            safe_line = (
                line.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            html_lines.append(f"<p>{safe_line}</p>")
        return "".join(html_lines)

    def _unsubscribe_token(self, *, lead_id: str, email: str) -> str:
        secret = str(settings.crm_unsubscribe_secret or "").strip() or "crm-unsubscribe-secret"
        payload = f"{lead_id}|{email}|{secret}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    def _extract_city_from_address(self, value: str | None) -> str | None:
        cleaned = str(value or "").strip()
        if not cleaned:
            return None
        parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        if not parts:
            return None
        return parts[-1]

    def _normalize_text(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        normalized = unicodedata.normalize("NFKD", raw)
        ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
        collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_value)
        return re.sub(r"\s+", " ", collapsed).strip()

    def _normalize_email(self, value: Any) -> str | None:
        raw = str(value or "").strip().lower()
        if not raw or "@" not in raw:
            return None
        return raw

    def _normalize_utm(self, value: dict[str, Any]) -> dict[str, str | None]:
        allowed_keys = ("source", "medium", "campaign", "term", "content")
        normalized: dict[str, str | None] = {}
        for key in allowed_keys:
            raw = value.get(key)
            if raw is None:
                raw = value.get(f"utm_{key}")
            text = str(raw or "").strip()
            normalized[key] = text or None
        return normalized

    def _domain_from_email_or_website(self, *, email: str | None, website: str | None) -> str | None:
        email_norm = self._normalize_email(email)
        if email_norm and "@" in email_norm:
            return email_norm.split("@", 1)[1].strip() or None

        website_raw = str(website or "").strip().lower()
        if not website_raw:
            return None
        if not website_raw.startswith("http://") and not website_raw.startswith("https://"):
            website_raw = f"https://{website_raw}"
        parsed = urlparse(website_raw)
        host = str(parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None

    def _build_lead_score(
        self,
        *,
        rating: Any,
        review_count: Any,
        has_email: bool,
        has_website: bool,
    ) -> float:
        score = 0.0
        if isinstance(rating, (int, float)):
            rating_value = max(0.0, min(5.0, float(rating)))
            # Prefer leads with room to improve (3.2 - 4.4 window)
            if rating_value < 2.8:
                score += 20
            elif rating_value <= 4.4:
                score += 45
            else:
                score += 30
        if isinstance(review_count, (int, float)):
            reviews = max(0, int(review_count))
            if reviews >= 500:
                score += 25
            elif reviews >= 200:
                score += 20
            elif reviews >= 50:
                score += 14
            elif reviews >= 10:
                score += 8
            else:
                score += 4
        if has_email:
            score += 18
        if has_website:
            score += 12
        return round(min(100.0, score), 2)

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _serialize_mongo_doc(self, doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop("_id"))
        return payload

    def _sanitize_payload(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, list):
            return [self._sanitize_payload(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self._sanitize_payload(item) for key, item in value.items()}
        return value

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
