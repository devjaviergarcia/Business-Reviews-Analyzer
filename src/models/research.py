from datetime import datetime, timezone
from typing import Optional
import uuid

from pydantic import BaseModel, Field


class ResearchTerm(BaseModel):
    """Represents a single Google Maps search session (term + city + timestamp)."""

    term_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    term_key: str  # URL-safe slug, e.g. "restaurante_cordoba"
    original_term: str  # Raw user input, e.g. "restaurante cordoba"
    city: Optional[str] = None  # Parsed from term if possible
    entity_type: Optional[str] = None  # Parsed from term if possible
    captured_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    listing_count: int = 0


class ResearchLead(BaseModel):
    """A single business listing captured from the Google Maps feed."""

    listing_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    term_id: str  # FK to ResearchTerm
    term_key: str  # Denormalized for easy filtering

    name: Optional[str] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    category: Optional[str] = None

    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    maps_url: Optional[str] = None

    # True when rating is in the "sweet spot" range (3.6–4.2)
    in_range: bool = False

    raw_html: Optional[str] = None
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
