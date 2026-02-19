from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

router = APIRouter(prefix="/business", tags=["business"])


class AnalyzeBusinessRequest(BaseModel):
    name: str
    force: bool = False


@router.post("/analyze")
async def analyze_business(_: AnalyzeBusinessRequest) -> None:
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Endpoint not implemented yet. It will be connected to BusinessService in next steps.",
    )


@router.get("/{business_id}")
async def get_business(business_id: str, include_listing: bool = Query(default=True)) -> None:
    _ = (business_id, include_listing)
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Endpoint not implemented yet.",
    )


@router.get("/{business_id}/reviews")
async def get_business_reviews(
    business_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = None,
) -> None:
    _ = (business_id, limit, cursor)
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Endpoint not implemented yet.",
    )
