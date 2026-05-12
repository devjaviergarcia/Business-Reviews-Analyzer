from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CRMConsentStatus(str, Enum):
    MISSING = "missing"
    GRANTED = "granted"
    REVOKED = "revoked"
    DENIED = "denied"


class CRMLeadStatus(str, Enum):
    NEW = "new"
    ENRICHING = "enriching"
    READY = "ready"
    PIPELINE_QUEUED = "pipeline_queued"
    PIPELINE_RUNNING = "pipeline_running"
    PIPELINE_DONE = "pipeline_done"
    CONTACTABLE = "contactable"
    PAUSED = "paused"
    WON = "won"
    LOST = "lost"


class CRMCampaignStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"


class CRMMessageStatus(str, Enum):
    QUEUED = "queued"
    SENT = "sent"
    DELIVERED = "delivered"
    OPEN = "open"
    CLICK = "click"
    REPLIED = "replied"
    BOUNCED = "bounced"
    UNSUBSCRIBED = "unsubscribed"
    FAILED = "failed"
    SKIPPED = "skipped"


class CRMDiscoveryRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    PARTIAL = "partial"
    COMPLETED = "completed"
    FAILED = "failed"


class CRMConsentProof(BaseModel):
    granted_at: datetime
    source: str
    legal_text_version: str
    evidence: str

    model_config = ConfigDict(extra="forbid")


class CRMLeadLegalBlock(BaseModel):
    consent_status: CRMConsentStatus = CRMConsentStatus.MISSING
    consent_proof: CRMConsentProof | None = None
    do_not_contact: bool = False
    unsubscribed_at: datetime | None = None
    suppressed_reason: str | None = None

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class CRMLeadPipelineRefs(BaseModel):
    business_id: str | None = None
    source_job_ids: list[str] = Field(default_factory=list)
    analysis_job_id: str | None = None
    report_job_id: str | None = None
    latest_report_artifacts: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")


class CRMLead(BaseModel):
    id: str | None = None
    business_name: str
    business_name_normalized: str
    email: str | None = None
    email_normalized: str | None = None
    domain_normalized: str | None = None
    phone: str | None = None
    website: str | None = None
    category: str | None = None
    city: str | None = None
    address: str | None = None
    source: str = "unknown"
    source_ref: dict[str, Any] = Field(default_factory=dict)
    rating: float | None = None
    review_count: int | None = None
    status: CRMLeadStatus = CRMLeadStatus.NEW
    score: float = 0.0
    legal: CRMLeadLegalBlock = Field(default_factory=CRMLeadLegalBlock)
    pipeline: CRMLeadPipelineRefs = Field(default_factory=CRMLeadPipelineRefs)
    notes: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    @field_validator("business_name")
    @classmethod
    def validate_business_name(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("business_name cannot be empty.")
        return cleaned


class CRMCadenceStep(BaseModel):
    step_order: int = Field(ge=1)
    step_key: str
    delay_days: int = Field(ge=0)
    subject_template: str
    body_template: str
    stop_on_reply: bool = True
    stop_on_unsubscribe: bool = True
    stop_on_bounce: bool = True

    model_config = ConfigDict(extra="forbid")


class CRMCadenceTemplate(BaseModel):
    id: str | None = None
    key: str
    name: str
    locale: str = "es-ES"
    is_default: bool = False
    steps: list[CRMCadenceStep] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")


class CRMCampaign(BaseModel):
    id: str | None = None
    name: str
    description: str | None = None
    status: CRMCampaignStatus = CRMCampaignStatus.DRAFT
    source_mode: str = "auto"
    selected_source: str | None = None
    cadence_template_id: str | None = None
    audience_filter: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    launched_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class CRMMessage(BaseModel):
    id: str | None = None
    campaign_id: str
    lead_id: str
    step_order: int = Field(ge=1)
    step_key: str
    scheduled_at: datetime
    status: CRMMessageStatus = CRMMessageStatus.QUEUED
    to_email: str
    subject: str
    body: str
    provider: str = "resend"
    provider_message_id: str | None = None
    dispatch_job_id: str | None = None
    provider_payload: dict[str, Any] = Field(default_factory=dict)
    sent_at: datetime | None = None
    delivered_at: datetime | None = None
    opened_at: datetime | None = None
    clicked_at: datetime | None = None
    replied_at: datetime | None = None
    bounced_at: datetime | None = None
    unsubscribed_at: datetime | None = None
    failed_at: datetime | None = None
    error: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class CRMEvent(BaseModel):
    id: str | None = None
    event_type: str
    lead_id: str | None = None
    campaign_id: str | None = None
    message_id: str | None = None
    actor: str = "system"
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")


class CRMSuppression(BaseModel):
    id: str | None = None
    email: str
    email_normalized: str
    reason: str
    source: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")


class CRMDiscoveryRun(BaseModel):
    id: str | None = None
    job_id: str | None = None
    query: str
    city: str | None = None
    category: str | None = None
    source: str = "auto_live_google_maps"
    limit: int = 100
    status: CRMDiscoveryRunStatus = CRMDiscoveryRunStatus.QUEUED
    metrics: dict[str, Any] = Field(default_factory=dict)
    steps: list[dict[str, Any]] = Field(default_factory=list)
    failure_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(extra="forbid", use_enum_values=True)
