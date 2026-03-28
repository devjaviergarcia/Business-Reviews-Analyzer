from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.dependencies import create_business_query_service
from src.services.business_query_service import BusinessQueryService

router = APIRouter(prefix="/business")
BusinessQueryServiceDep = Annotated[BusinessQueryService, Depends(create_business_query_service)]


@router.get("/{business_id}/analysis", tags=["Business"])
async def get_business_analysis(business_id: str, service: BusinessQueryServiceDep) -> dict:
    try:
        return await service.get_business_analysis(business_id=business_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/report", tags=["Business"])
async def get_business_report(business_id: str, service: BusinessQueryServiceDep) -> dict:
    try:
        return await service.get_business_report(business_id=business_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/analyses", tags=["Business"])
async def list_business_analyses(
    business_id: str,
    service: BusinessQueryServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
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
