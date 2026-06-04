from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from src.crm.bulk_delete_crm_leads_use_case import BulkDeleteCRMLeadsUseCase
from src.crm.create_crm_campaign_use_case import CreateCRMCampaignUseCase
from src.crm.create_crm_report_feedback_use_case import CreateCRMReportFeedbackUseCase
from src.crm.create_crm_lead_use_case import CreateCRMLeadUseCase
from src.crm.create_crm_report_request_use_case import CreateCRMReportRequestUseCase
from src.crm.handle_resend_webhook_use_case import HandleResendWebhookUseCase
from src.crm.get_crm_lead_use_case import GetCRMLeadUseCase
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
from src.dependencies import (
    create_bulk_delete_crm_leads_use_case,
    create_create_crm_campaign_use_case,
    create_crm_service,
    create_create_crm_lead_use_case,
    create_create_crm_report_feedback_use_case,
    create_create_crm_report_request_use_case,
    create_get_crm_lead_use_case,
    create_handle_resend_webhook_use_case,
    create_enqueue_crm_lead_discovery_job_use_case,
    create_enqueue_crm_lead_pipeline_job_use_case,
    create_enqueue_due_campaign_dispatch_jobs_use_case,
    create_enqueue_geo_grid_study_job_use_case,
    create_launch_crm_campaign_use_case,
    create_list_crm_campaigns_use_case,
    create_list_crm_events_use_case,
    create_list_crm_leads_use_case,
    create_list_crm_messages_use_case,
    create_list_crm_report_requests_use_case,
    create_process_pending_crm_report_requests_use_case,
    create_retry_crm_report_request_use_case,
    create_update_crm_lead_use_case,
)
from src.services.crm_service import CRMService

router = APIRouter(prefix="/crm")
CRMServiceDep = Annotated[CRMService, Depends(create_crm_service)]
EnqueueCRMLeadDiscoveryJobUseCaseDep = Annotated[
    EnqueueCRMLeadDiscoveryJobUseCase,
    Depends(create_enqueue_crm_lead_discovery_job_use_case),
]
EnqueueGeoGridStudyJobUseCaseDep = Annotated[
    EnqueueGeoGridStudyJobUseCase,
    Depends(create_enqueue_geo_grid_study_job_use_case),
]
EnqueueCRMLeadPipelineJobUseCaseDep = Annotated[
    EnqueueCRMLeadPipelineJobUseCase,
    Depends(create_enqueue_crm_lead_pipeline_job_use_case),
]
EnqueueDueCampaignDispatchJobsUseCaseDep = Annotated[
    EnqueueDueCampaignDispatchJobsUseCase,
    Depends(create_enqueue_due_campaign_dispatch_jobs_use_case),
]
CreateCRMReportRequestUseCaseDep = Annotated[
    CreateCRMReportRequestUseCase,
    Depends(create_create_crm_report_request_use_case),
]
CreateCRMReportFeedbackUseCaseDep = Annotated[
    CreateCRMReportFeedbackUseCase,
    Depends(create_create_crm_report_feedback_use_case),
]
RetryCRMReportRequestUseCaseDep = Annotated[
    RetryCRMReportRequestUseCase,
    Depends(create_retry_crm_report_request_use_case),
]
ProcessPendingCRMReportRequestsUseCaseDep = Annotated[
    ProcessPendingCRMReportRequestsUseCase,
    Depends(create_process_pending_crm_report_requests_use_case),
]
CreateCRMLeadUseCaseDep = Annotated[
    CreateCRMLeadUseCase,
    Depends(create_create_crm_lead_use_case),
]
UpdateCRMLeadUseCaseDep = Annotated[
    UpdateCRMLeadUseCase,
    Depends(create_update_crm_lead_use_case),
]
BulkDeleteCRMLeadsUseCaseDep = Annotated[
    BulkDeleteCRMLeadsUseCase,
    Depends(create_bulk_delete_crm_leads_use_case),
]
CreateCRMCampaignUseCaseDep = Annotated[
    CreateCRMCampaignUseCase,
    Depends(create_create_crm_campaign_use_case),
]
LaunchCRMCampaignUseCaseDep = Annotated[
    LaunchCRMCampaignUseCase,
    Depends(create_launch_crm_campaign_use_case),
]
HandleResendWebhookUseCaseDep = Annotated[
    HandleResendWebhookUseCase,
    Depends(create_handle_resend_webhook_use_case),
]
ListCRMReportRequestsUseCaseDep = Annotated[
    ListCRMReportRequestsUseCase,
    Depends(create_list_crm_report_requests_use_case),
]
ListCRMLeadsUseCaseDep = Annotated[
    ListCRMLeadsUseCase,
    Depends(create_list_crm_leads_use_case),
]
GetCRMLeadUseCaseDep = Annotated[
    GetCRMLeadUseCase,
    Depends(create_get_crm_lead_use_case),
]
ListCRMCampaignsUseCaseDep = Annotated[
    ListCRMCampaignsUseCase,
    Depends(create_list_crm_campaigns_use_case),
]
ListCRMMessagesUseCaseDep = Annotated[
    ListCRMMessagesUseCase,
    Depends(create_list_crm_messages_use_case),
]
ListCRMEventsUseCaseDep = Annotated[
    ListCRMEventsUseCase,
    Depends(create_list_crm_events_use_case),
]


class CRMDiscoveryJobRequest(BaseModel):
    query: str
    city: str | None = None
    category: str | None = None
    limit: int = Field(default=100, ge=1, le=5000)
    source: str = "auto_live_google_maps"

    model_config = ConfigDict(extra="forbid")


class CRMGeoGridStudyRequest(BaseModel):
    keyword: str
    city_slug: str
    top_n: int = Field(default=10, ge=1, le=100)
    provider_mode: Literal["maps_live", "uule"] | None = None
    grid_size: int | None = Field(default=None, ge=3, le=21)
    grid_spacing_km: float | None = Field(default=None, gt=0.0, le=20.0)
    uule_radius_m: int | None = Field(default=None, ge=100, le=50000)
    throttle_ms: int | None = Field(default=None, ge=100, le=15000)

    model_config = ConfigDict(extra="forbid")


class CRMLeadPipelineJobRequest(BaseModel):
    force: bool = False
    sources: list[Literal["google_maps", "tripadvisor"]] | None = None
    google_maps_name: str | None = None
    tripadvisor_name: str | None = None

    model_config = ConfigDict(extra="forbid")


class CRMLeadUpdateRequest(BaseModel):
    status: str | None = None
    business_name: str | None = None
    email: str | None = None
    phone: str | None = None
    website: str | None = None
    category: str | None = None
    city: str | None = None
    address: str | None = None
    do_not_contact: bool | None = None
    consent_status: Literal["missing", "granted", "revoked", "denied"] | None = None
    suppressed_reason: str | None = None
    unsubscribed: bool | None = None
    consent_proof: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMLeadCreateRequest(BaseModel):
    business_name: str
    contact_name: str | None = None
    email: str | None = None
    phone: str | None = None
    website: str | None = None
    category: str | None = None
    city: str | None = None
    address: str | None = None
    source: str | None = None
    status: str | None = None
    notes: list[str] | None = None
    tags: list[str] | None = None
    do_not_contact: bool | None = None
    consent_status: Literal["missing", "granted", "revoked", "denied"] | None = None
    suppressed_reason: str | None = None
    unsubscribed: bool | None = None
    consent_proof: dict[str, Any] | None = None
    source_ref: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMLeadBulkDeleteRequest(BaseModel):
    lead_ids: list[str] | None = None
    delete_all_matching: bool = False
    exclude_lead_ids: list[str] | None = None
    status: str | None = None
    consent_status: str | None = None
    source: str | None = None
    q: str | None = None

    model_config = ConfigDict(extra="forbid")


class CRMCampaignCreateRequest(BaseModel):
    name: str
    description: str | None = None
    source_mode: Literal["auto", "combined", "single"] = "auto"
    selected_source: Literal["google_maps", "tripadvisor"] | None = None
    cadence_template_id: str | None = None
    audience_filter: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMReportRequestCreateRequest(BaseModel):
    business_name: str
    city: str | None = None
    category: str | None = None
    contact_name: str | None = None
    email: str
    phone: str | None = None
    website: str | None = None
    message: str | None = None
    consent_report: bool = False
    consent_marketing: bool = False
    utm: dict[str, Any] | None = None
    source_page: str | None = None

    model_config = ConfigDict(extra="forbid")


class CRMLeadReportGenerateRequest(BaseModel):
    cta: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMPaidReportGenerateRequest(BaseModel):
    report_month: str | None = None
    history: list[dict[str, Any]] | None = None
    cta: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMPublicStudyGenerateRequest(BaseModel):
    cta: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class CRMReportRequestsProcessRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=200)

    model_config = ConfigDict(extra="forbid")


class CRMReportFeedbackCreateRequest(BaseModel):
    branch: Literal["A", "B", "C"]
    answers: dict[str, Any] = Field(default_factory=dict)
    lead_id: str | None = None
    report_request_id: str | None = None
    lead_report_id: str | None = None
    benchmark_business_id: str | None = None
    report_kind: Literal["lead", "paid", "public", "unknown"] | None = None
    source_page: str | None = None
    referrer: str | None = None
    user_agent: str | None = None
    ip_hash: str | None = None

    model_config = ConfigDict(extra="forbid")


@router.post("/leads/discovery-jobs", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def enqueue_crm_lead_discovery_job(
    payload: CRMDiscoveryJobRequest,
    enqueue_crm_lead_discovery_job_use_case: EnqueueCRMLeadDiscoveryJobUseCaseDep,
) -> dict[str, Any]:
    try:
        return await enqueue_crm_lead_discovery_job_use_case.execute(
            query=payload.query,
            city=payload.city,
            category=payload.category,
            limit=payload.limit,
            source=payload.source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/report-requests", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def create_crm_report_request(
    payload: CRMReportRequestCreateRequest,
    create_crm_report_request_use_case: CreateCRMReportRequestUseCaseDep,
) -> dict[str, Any]:
    try:
        return await create_crm_report_request_use_case.execute(
            business_name=payload.business_name,
            city=payload.city,
            category=payload.category,
            contact_name=payload.contact_name,
            email=payload.email,
            phone=payload.phone,
            website=payload.website,
            message=payload.message,
            consent_report=payload.consent_report,
            consent_marketing=payload.consent_marketing,
            utm=payload.utm,
            source_page=payload.source_page,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/report-requests", tags=["CRM"])
async def list_crm_report_requests(
    list_crm_report_requests_use_case: ListCRMReportRequestsUseCaseDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status_filter: str | None = Query(default=None, alias="status"),
    q: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return await list_crm_report_requests_use_case.execute(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            q=q,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/report-requests/{report_request_id}/retry", tags=["CRM"])
async def retry_crm_report_request(
    report_request_id: str,
    retry_crm_report_request_use_case: RetryCRMReportRequestUseCaseDep,
) -> dict[str, Any]:
    try:
        return await retry_crm_report_request_use_case.execute(report_request_id=report_request_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/report-requests/process-pending", tags=["CRM"])
async def process_pending_crm_report_requests(
    payload: CRMReportRequestsProcessRequest,
    process_pending_crm_report_requests_use_case: ProcessPendingCRMReportRequestsUseCaseDep,
) -> dict[str, Any]:
    try:
        return await process_pending_crm_report_requests_use_case.execute(limit=payload.limit)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/report-feedback", status_code=status.HTTP_201_CREATED, tags=["CRM"])
async def create_crm_report_feedback(
    payload: CRMReportFeedbackCreateRequest,
    create_crm_report_feedback_use_case: CreateCRMReportFeedbackUseCaseDep,
) -> dict[str, Any]:
    try:
        return await create_crm_report_feedback_use_case.execute(
            branch=payload.branch,
            answers=payload.answers,
            lead_id=payload.lead_id,
            report_request_id=payload.report_request_id,
            lead_report_id=payload.lead_report_id,
            benchmark_business_id=payload.benchmark_business_id,
            report_kind=payload.report_kind,
            source_page=payload.source_page,
            referrer=payload.referrer,
            user_agent=payload.user_agent,
            ip_hash=payload.ip_hash,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/leads/{lead_id}/pipeline-jobs", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def enqueue_crm_lead_pipeline_job(
    lead_id: str,
    payload: CRMLeadPipelineJobRequest,
    enqueue_crm_lead_pipeline_job_use_case: EnqueueCRMLeadPipelineJobUseCaseDep,
) -> dict[str, Any]:
    try:
        return await enqueue_crm_lead_pipeline_job_use_case.execute(
            lead_id=lead_id,
            force=payload.force,
            sources=payload.sources,
            google_maps_name=payload.google_maps_name,
            tripadvisor_name=payload.tripadvisor_name,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/leads", tags=["CRM"])
async def list_crm_leads(
    list_crm_leads_use_case: ListCRMLeadsUseCaseDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status_filter: str | None = Query(default=None, alias="status"),
    consent_filter: str | None = Query(default=None, alias="consent_status"),
    source_filter: str | None = Query(default=None, alias="source"),
    q: str | None = Query(default=None),
    sort_by: Literal["updated_at", "business_name", "score", "status", "consent_status", "source"] = Query(
        default="updated_at"
    ),
    sort_dir: Literal["asc", "desc"] = Query(default="desc"),
) -> dict[str, Any]:
    try:
        return await list_crm_leads_use_case.execute(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/leads", status_code=status.HTTP_201_CREATED, tags=["CRM"])
async def create_crm_lead(
    payload: CRMLeadCreateRequest,
    create_crm_lead_use_case: CreateCRMLeadUseCaseDep,
) -> dict[str, Any]:
    try:
        return await create_crm_lead_use_case.execute(
            business_name=payload.business_name,
            contact_name=payload.contact_name,
            email=payload.email,
            phone=payload.phone,
            website=payload.website,
            category=payload.category,
            city=payload.city,
            address=payload.address,
            source=payload.source,
            status=payload.status,
            notes=payload.notes,
            tags=payload.tags,
            do_not_contact=payload.do_not_contact,
            consent_status=payload.consent_status,
            suppressed_reason=payload.suppressed_reason,
            unsubscribed=payload.unsubscribed,
            consent_proof=payload.consent_proof,
            source_ref=payload.source_ref,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/leads/{lead_id}", tags=["CRM"])
async def get_crm_lead(
    lead_id: str,
    get_crm_lead_use_case: GetCRMLeadUseCaseDep,
    sync_pipeline_refs: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        return await get_crm_lead_use_case.execute(
            lead_id=lead_id,
            sync_pipeline_refs=sync_pipeline_refs,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.patch("/leads/{lead_id}", tags=["CRM"])
async def update_crm_lead(
    lead_id: str,
    payload: CRMLeadUpdateRequest,
    update_crm_lead_use_case: UpdateCRMLeadUseCaseDep,
) -> dict[str, Any]:
    try:
        return await update_crm_lead_use_case.execute(
            lead_id=lead_id,
            updates=payload.model_dump(mode="python", exclude_none=True),
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/leads/bulk-delete", tags=["CRM"])
async def bulk_delete_crm_leads(
    payload: CRMLeadBulkDeleteRequest,
    bulk_delete_crm_leads_use_case: BulkDeleteCRMLeadsUseCaseDep,
) -> dict[str, Any]:
    try:
        return await bulk_delete_crm_leads_use_case.execute(
            lead_ids=payload.lead_ids,
            delete_all_matching=payload.delete_all_matching,
            exclude_lead_ids=payload.exclude_lead_ids,
            status_filter=payload.status,
            consent_filter=payload.consent_status,
            source_filter=payload.source,
            q=payload.q,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/campaigns", status_code=status.HTTP_201_CREATED, tags=["CRM"])
async def create_crm_campaign(
    payload: CRMCampaignCreateRequest,
    create_crm_campaign_use_case: CreateCRMCampaignUseCaseDep,
) -> dict[str, Any]:
    try:
        return await create_crm_campaign_use_case.execute(
            name=payload.name,
            description=payload.description,
            source_mode=payload.source_mode,
            selected_source=payload.selected_source,
            cadence_template_id=payload.cadence_template_id,
            audience_filter=payload.audience_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/campaigns", tags=["CRM"])
async def list_crm_campaigns(
    list_crm_campaigns_use_case: ListCRMCampaignsUseCaseDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=30, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict[str, Any]:
    try:
        return await list_crm_campaigns_use_case.execute(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/campaigns/{campaign_id}/launch", tags=["CRM"])
async def launch_crm_campaign(
    campaign_id: str,
    launch_crm_campaign_use_case: LaunchCRMCampaignUseCaseDep,
) -> dict[str, Any]:
    try:
        return await launch_crm_campaign_use_case.execute(campaign_id=campaign_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/campaigns/dispatch-due", tags=["CRM"])
async def dispatch_due_crm_messages(
    enqueue_due_campaign_dispatch_jobs_use_case: EnqueueDueCampaignDispatchJobsUseCaseDep,
    campaign_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        queued = await enqueue_due_campaign_dispatch_jobs_use_case.execute(
            campaign_id=campaign_id,
            limit=limit,
        )
        return {"queued_dispatch_jobs": queued, "campaign_id": campaign_id}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/messages", tags=["CRM"])
async def list_crm_messages(
    list_crm_messages_use_case: ListCRMMessagesUseCaseDep,
    campaign_id: str | None = Query(default=None),
    lead_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return await list_crm_messages_use_case.execute(
            campaign_id=campaign_id,
            lead_id=lead_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/events", tags=["CRM"])
async def list_crm_events(
    list_crm_events_use_case: ListCRMEventsUseCaseDep,
    lead_id: str | None = Query(default=None),
    campaign_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return await list_crm_events_use_case.execute(
            lead_id=lead_id,
            campaign_id=campaign_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/discovery-runs", tags=["CRM"])
async def list_crm_discovery_runs(
    service: CRMServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return await service.list_discovery_runs(page=page, page_size=page_size)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/discovery-runs/{discovery_run_id}", tags=["CRM"])
async def get_crm_discovery_run(discovery_run_id: str, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.get_discovery_run(discovery_run_id=discovery_run_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/geo-cities", tags=["CRM"])
async def list_crm_geo_cities(service: CRMServiceDep) -> list[dict[str, Any]]:
    try:
        return await service.list_geo_cities()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/geo-grid-runs", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def enqueue_crm_geo_grid_study(
    payload: CRMGeoGridStudyRequest,
    enqueue_geo_grid_study_job_use_case: EnqueueGeoGridStudyJobUseCaseDep,
) -> dict[str, Any]:
    try:
        return await enqueue_geo_grid_study_job_use_case.execute(
            keyword=payload.keyword,
            city_slug=payload.city_slug,
            top_n=payload.top_n,
            provider_mode=payload.provider_mode,
            grid_size=payload.grid_size,
            grid_spacing_km=payload.grid_spacing_km,
            uule_radius_m=payload.uule_radius_m,
            throttle_ms=payload.throttle_ms,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/geo-grid-runs", tags=["CRM"])
async def list_crm_geo_grid_runs(
    service: CRMServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    city_slug: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict[str, Any]:
    try:
        return await service.list_geo_grid_runs(
            page=page,
            page_size=page_size,
            city_slug=city_slug,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/geo-grid-runs/{geo_grid_run_id}", tags=["CRM"])
async def get_crm_geo_grid_run(geo_grid_run_id: str, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.get_geo_grid_run(geo_grid_run_id=geo_grid_run_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/geo-grid-runs/{geo_grid_run_id}/results", tags=["CRM"])
async def list_crm_geo_grid_results(geo_grid_run_id: str, service: CRMServiceDep) -> list[dict[str, Any]]:
    try:
        return await service.list_geo_grid_results(geo_grid_run_id=geo_grid_run_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/geo-grid-runs/{geo_grid_run_id}/stats", tags=["CRM"])
async def get_crm_geo_grid_stats(geo_grid_run_id: str, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.get_geo_grid_stats(geo_grid_run_id=geo_grid_run_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/benchmark-businesses/{benchmark_business_id}/lead-report", tags=["CRM"])
async def generate_crm_lead_report(
    benchmark_business_id: str,
    payload: CRMLeadReportGenerateRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.generate_lead_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            cta=payload.cta,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/benchmark-businesses/{benchmark_business_id}/paid-report", tags=["CRM"])
async def generate_crm_paid_report(
    benchmark_business_id: str,
    payload: CRMPaidReportGenerateRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.generate_paid_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            report_month=payload.report_month,
            history=payload.history,
            cta=payload.cta,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/benchmark-runs/{benchmark_run_id}/public-study", tags=["CRM"])
async def generate_crm_public_study(
    benchmark_run_id: str,
    payload: CRMPublicStudyGenerateRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.generate_public_study_for_benchmark_run(
            benchmark_run_id=benchmark_run_id,
            cta=payload.cta,
        )
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/webhooks/resend", tags=["CRM"])
async def crm_resend_webhook(
    payload: dict[str, Any],
    handle_resend_webhook_use_case: HandleResendWebhookUseCaseDep,
) -> dict[str, Any]:
    try:
        return await handle_resend_webhook_use_case.execute(payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
