from datetime import datetime, timezone

from pydantic import BaseModel, Field


class ReviewAnalysis(BaseModel):
    id: str | None = None
    business_id: str | None = None
    overall_sentiment: str
    main_topics: list[str] = Field(default_factory=list)
    strengths: list[str] = Field(default_factory=list)
    weaknesses: list[str] = Field(default_factory=list)
    suggested_owner_reply: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
