from datetime import datetime, timezone

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.config import settings
from src.database import ping_mongo_detailed

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    mongo: str
    environment: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    detail: str | None = None


@router.get("/health", response_model=HealthResponse)
async def get_health() -> JSONResponse:
    mongo_ok, mongo_detail = await ping_mongo_detailed()
    http_status = status.HTTP_200_OK if mongo_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    payload = HealthResponse(
        status="ok" if mongo_ok else "degraded",
        mongo="up" if mongo_ok else "down",
        environment=settings.app_env,
        detail=mongo_detail if not mongo_ok else None,
    )
    return JSONResponse(status_code=http_status, content=payload.model_dump(mode="json", exclude_none=True))
