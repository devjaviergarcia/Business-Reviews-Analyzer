from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BenchmarkRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    PARTIAL = "partial"
    COMPLETED = "completed"
    FAILED = "failed"


class BenchmarkRun(BaseModel):
    id: str | None = None
    title: str | None = None
    query: str
    city: str | None = None
    category: str | None = None
    source: str = "auto_live_google_maps"
    limit: int = Field(default=100, ge=1)
    status: BenchmarkRunStatus = BenchmarkRunStatus.QUEUED
    metrics: dict[str, Any] = Field(default_factory=dict)
    failure_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("query cannot be empty.")
        return cleaned


class BenchmarkBusiness(BaseModel):
    id: str | None = None
    benchmark_id: str
    lead_id: str | None = None
    business_name: str
    business_name_normalized: str
    category: str | None = None
    city: str | None = None
    address: str | None = None
    maps_url: str | None = None
    maps_url_canonical: str | None = None
    phone: str | None = None
    website: str | None = None
    source: str = "google_maps_live_discovery"
    source_ref: dict[str, Any] = Field(default_factory=dict)
    discovery_rank: int | None = Field(default=None, ge=1)
    rating: float | None = Field(default=None, ge=0.0, le=5.0)
    review_count: int | None = Field(default=None, ge=0)
    opportunity_score: float = Field(default=0.0, ge=0.0)
    reputation_score: float = Field(default=0.0, ge=0.0)
    visibility_score: float = Field(default=0.0, ge=0.0)
    conversion_risk_score: float = Field(default=0.0, ge=0.0)
    listing_enriched: bool = False
    raw_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator("benchmark_id", "business_name", "business_name_normalized")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required text field cannot be empty.")
        return cleaned


class CompetitorCandidate(BaseModel):
    benchmark_business_id: str | None = None
    business_name: str
    maps_url: str | None = None
    discovery_rank: int | None = Field(default=None, ge=1)
    rating: float | None = Field(default=None, ge=0.0, le=5.0)
    review_count: int | None = Field(default=None, ge=0)
    website: str | None = None
    category: str | None = None
    distance_hint: str | None = None
    why_selected: str | None = None
    relative_position: str | None = None
    similarity_score: float = Field(default=0.0, ge=0.0)

    model_config = ConfigDict(extra="forbid")

    @field_validator("business_name")
    @classmethod
    def validate_business_name(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("business_name cannot be empty.")
        return cleaned


class CompetitorSet(BaseModel):
    id: str | None = None
    benchmark_id: str
    target_business_id: str
    competitors: list[CompetitorCandidate] = Field(default_factory=list)
    selection_version: str = "v1"
    source: str = "benchmark"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator("benchmark_id", "target_business_id")
    @classmethod
    def validate_required_ids(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required id field cannot be empty.")
        return cleaned


class LeadReport(BaseModel):
    id: str | None = None
    benchmark_id: str | None = None
    benchmark_business_id: str
    report_type: Literal["lead"] = "lead"
    business_name: str
    html: str
    deep_study_snapshot: dict[str, Any] = Field(default_factory=dict)
    source_payload: dict[str, Any] = Field(default_factory=dict)
    cta: dict[str, Any] = Field(default_factory=dict)
    status: Literal["generated"] = "generated"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator("benchmark_business_id", "business_name", "html")
    @classmethod
    def validate_required_report_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required report field cannot be empty.")
        return cleaned


class PaidReport(BaseModel):
    id: str | None = None
    benchmark_id: str | None = None
    benchmark_business_id: str
    report_month: str
    report_type: Literal["paid"] = "paid"
    business_name: str
    html: str
    deep_study_snapshot: dict[str, Any] = Field(default_factory=dict)
    source_payload: dict[str, Any] = Field(default_factory=dict)
    history: list[dict[str, Any]] = Field(default_factory=list)
    cta: dict[str, Any] = Field(default_factory=dict)
    status: Literal["generated"] = "generated"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator("benchmark_business_id", "report_month", "business_name", "html")
    @classmethod
    def validate_required_paid_report_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required paid report field cannot be empty.")
        return cleaned
