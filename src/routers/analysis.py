from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/business", tags=["analysis"])


@router.get("/{business_id}/analysis")
async def get_business_analysis(business_id: str) -> None:
    _ = business_id
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Endpoint not implemented yet.",
    )
