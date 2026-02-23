from fastapi import APIRouter, HTTPException, Query, status

from src.services.business_service import BusinessService

router = APIRouter(prefix="/business")


@router.get("/{business_id}/analysis", tags=["Business"])
async def get_business_analysis(business_id: str) -> dict:
    service = BusinessService()
    try:
        return await service.get_business_analysis(business_id=business_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/analyses", tags=["Business"])
async def list_business_analyses(
    business_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
    service = BusinessService()
    try:
        return await service.list_business_analyses(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
