from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class OwnerReply(BaseModel):
    text: str = ""
    relative_time: str = ""


class Review(BaseModel):
    id: str | None = None
    business_id: str | None = None
    source: str = "google_maps"
    author_name: str = ""
    rating: float = Field(default=0.0, ge=0.0, le=5.0)
    relative_time: str = ""
    text: str = ""
    owner_reply: OwnerReply | None = None
    has_text: bool = False
    has_owner_reply: bool = False
    relative_time_bucket: str = "unknown"
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Listing(BaseModel):
    address: str | None = None
    phone: str | None = None
    website: str | None = None
    overall_rating: float | None = Field(default=None, ge=0.0, le=5.0)
    total_reviews: int | None = Field(default=None, ge=0)
    categories: list[str] = Field(default_factory=list)


class Business(BaseModel):
    id: str | None = None
    name: str
    name_normalized: str
    source: str = "google_maps"
    listing: Listing | None = None
    stats: dict[str, Any] = Field(default_factory=dict)
    review_count: int = Field(default=0, ge=0)
    last_scraped_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
