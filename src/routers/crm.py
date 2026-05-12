from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from src.dependencies import create_crm_service
from src.services.crm_service import CRMService

router = APIRouter(prefix="/crm")
CRMServiceDep = Annotated[CRMService, Depends(create_crm_service)]


class CRMDiscoveryJobRequest(BaseModel):
    query: str
    city: str | None = None
    category: str | None = None
    limit: int = Field(default=100, ge=1, le=5000)
    source: str = "auto_live_google_maps"

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


@router.post("/leads/discovery-jobs", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def enqueue_crm_lead_discovery_job(payload: CRMDiscoveryJobRequest, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.enqueue_lead_discovery_job(
            query=payload.query,
            city=payload.city,
            category=payload.category,
            limit=payload.limit,
            source=payload.source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/leads/{lead_id}/pipeline-jobs", status_code=status.HTTP_202_ACCEPTED, tags=["CRM"])
async def enqueue_crm_lead_pipeline_job(
    lead_id: str,
    payload: CRMLeadPipelineJobRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.enqueue_lead_pipeline_job(
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
    service: CRMServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    status_filter: str | None = Query(default=None, alias="status"),
    consent_filter: str | None = Query(default=None, alias="consent_status"),
    source_filter: str | None = Query(default=None, alias="source"),
    q: str | None = Query(default=None),
    sort_by: Literal["updated_at", "business_name", "score"] = Query(default="updated_at"),
    sort_dir: Literal["asc", "desc"] = Query(default="desc"),
) -> dict[str, Any]:
    try:
        return await service.list_leads(
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


@router.get("/leads/{lead_id}", tags=["CRM"])
async def get_crm_lead(
    lead_id: str,
    service: CRMServiceDep,
    sync_pipeline_refs: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        return await service.get_lead(lead_id=lead_id, sync_pipeline_refs=sync_pipeline_refs)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.patch("/leads/{lead_id}", tags=["CRM"])
async def update_crm_lead(
    lead_id: str,
    payload: CRMLeadUpdateRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.update_lead(lead_id=lead_id, updates=payload.model_dump(mode="python", exclude_none=True))
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/leads/bulk-delete", tags=["CRM"])
async def bulk_delete_crm_leads(
    payload: CRMLeadBulkDeleteRequest,
    service: CRMServiceDep,
) -> dict[str, Any]:
    try:
        return await service.bulk_delete_leads(
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
async def create_crm_campaign(payload: CRMCampaignCreateRequest, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.create_campaign(
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
    service: CRMServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=30, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict[str, Any]:
    try:
        return await service.list_campaigns(page=page, page_size=page_size, status_filter=status_filter)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/campaigns/{campaign_id}/launch", tags=["CRM"])
async def launch_crm_campaign(campaign_id: str, service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.launch_campaign(campaign_id=campaign_id)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/campaigns/dispatch-due", tags=["CRM"])
async def dispatch_due_crm_messages(
    service: CRMServiceDep,
    campaign_id: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        queued = await service.enqueue_due_campaign_dispatch_jobs(campaign_id=campaign_id, limit=limit)
        return {"queued_dispatch_jobs": queued, "campaign_id": campaign_id}
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/messages", tags=["CRM"])
async def list_crm_messages(
    service: CRMServiceDep,
    campaign_id: str | None = Query(default=None),
    lead_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return await service.list_messages(
            campaign_id=campaign_id,
            lead_id=lead_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/events", tags=["CRM"])
async def list_crm_events(
    service: CRMServiceDep,
    lead_id: str | None = Query(default=None),
    campaign_id: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        return await service.list_events(
            lead_id=lead_id,
            campaign_id=campaign_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/webhooks/resend", tags=["CRM"])
async def crm_resend_webhook(payload: dict[str, Any], service: CRMServiceDep) -> dict[str, Any]:
    try:
        return await service.handle_resend_webhook(payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
