from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GeoGridRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    PARTIAL = "partial"
    COMPLETED = "completed"
    FAILED = "failed"


class GeoPointModel(BaseModel):
    order: int = Field(ge=1)
    label: str
    lat: float = Field(ge=-90.0, le=90.0)
    lng: float = Field(ge=-180.0, le=180.0)

    model_config = ConfigDict(extra="forbid")


class GeoCity(BaseModel):
    id: str | None = None
    city: str
    city_slug: str
    center: dict[str, float]
    points: list[GeoPointModel] = Field(default_factory=list)
    point_count: int = Field(default=0, ge=0)
    enabled: bool = True
    source: str = "local_geo_points"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator("city", "city_slug")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required text field cannot be empty.")
        return cleaned


class GeoGridRun(BaseModel):
    id: str | None = None
    keyword: str
    city: str
    city_slug: str
    center: dict[str, float]
    provider_mode: str = "maps_live"
    grid_size: int | None = None
    grid_spacing_km: float | None = None
    uule_radius_m: int | None = None
    throttle_ms: int | None = None
    top_n: int = Field(default=10, ge=1, le=100)
    point_count: int = Field(default=0, ge=0)
    total_units: int = Field(default=0, ge=0)
    completed_units: int = Field(default=0, ge=0)
    completed_points: int = Field(default=0, ge=0)
    status: GeoGridRunStatus = GeoGridRunStatus.QUEUED
    metrics: dict[str, Any] = Field(default_factory=dict)
    failure_reason: str | None = None
    job_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    @field_validator("keyword", "city", "city_slug")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required text field cannot be empty.")
        return cleaned


class GeoGridResult(BaseModel):
    id: str | None = None
    geo_grid_run_id: str
    city_slug: str
    keyword: str
    point_order: int = Field(ge=1)
    point_label: str
    grid_row: int | None = Field(default=None, ge=1)
    grid_col: int | None = Field(default=None, ge=1)
    lat: float = Field(ge=-90.0, le=90.0)
    lng: float = Field(ge=-180.0, le=180.0)
    rank: int = Field(ge=1)
    visible_top10: bool = False
    provider_mode: str | None = None
    business_key: str
    business_name: str
    business_name_normalized: str
    maps_url: str | None = None
    maps_url_canonical: str | None = None
    rating: float | None = Field(default=None, ge=0.0, le=5.0)
    review_count: int | None = Field(default=None, ge=0)
    category: str | None = None
    source_ref: dict[str, Any] = Field(default_factory=dict)
    raw_snapshot: dict[str, Any] = Field(default_factory=dict)
    captured_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(extra="forbid")

    @field_validator(
        "geo_grid_run_id",
        "city_slug",
        "keyword",
        "point_label",
        "business_key",
        "business_name",
        "business_name_normalized",
    )
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("required text field cannot be empty.")
        return cleaned
