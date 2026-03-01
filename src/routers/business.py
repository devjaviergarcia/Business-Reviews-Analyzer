import asyncio
import json
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.dependencies import create_business_query_service, create_business_service
from src.services.business_service import BusinessService
from src.services.business_query_service import BusinessQueryService

router = APIRouter(prefix="/business")
BusinessServiceDep = Annotated[BusinessService, Depends(create_business_service)]
BusinessQueryServiceDep = Annotated[BusinessQueryService, Depends(create_business_query_service)]


class ScraperParamsRequest(BaseModel):
    scraper_interactive_max_rounds: int | None = Field(default=None, ge=1, le=10000)
    scraper_html_scroll_max_rounds: int | None = Field(default=None, ge=0, le=20000)
    scraper_html_stable_rounds: int | None = Field(default=None, ge=2, le=2000)

    model_config = ConfigDict(extra="forbid")


class AnalyzeBusinessRequest(BaseModel):
    name: str
    force: bool = False
    strategy: str | None = None
    force_mode: str | None = None
    scraper_params: ScraperParamsRequest | None = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def normalize_cached_to_force(cls, raw: object) -> object:
        if not isinstance(raw, dict):
            return raw

        payload = dict(raw)
        # Backward compatibility: migrate flat params to nested scraper_params.
        legacy_scraper_keys = (
            "interactive_max_rounds",
            "html_scroll_max_rounds",
            "html_stable_rounds",
        )
        has_legacy_scraper_keys = any(key in payload for key in legacy_scraper_keys)
        if has_legacy_scraper_keys:
            scraper_params = dict(payload.get("scraper_params") or {})
            if "interactive_max_rounds" in payload:
                scraper_params.setdefault(
                    "scraper_interactive_max_rounds",
                    payload.pop("interactive_max_rounds"),
                )
            if "html_scroll_max_rounds" in payload:
                scraper_params.setdefault(
                    "scraper_html_scroll_max_rounds",
                    payload.pop("html_scroll_max_rounds"),
                )
            if "html_stable_rounds" in payload:
                scraper_params.setdefault(
                    "scraper_html_stable_rounds",
                    payload.pop("html_stable_rounds"),
                )
            payload["scraper_params"] = scraper_params

        if "cached" not in payload:
            return payload

        cached_value = bool(payload.pop("cached"))
        if "force" in payload:
            force_value = bool(payload.get("force"))
            if force_value == cached_value:
                raise ValueError("Use either 'force' or 'cached', not both with conflicting meaning.")
            return payload

        # Backward compatibility:
        # cached=false => force=true, cached=true => force=false
        payload["force"] = not cached_value
        return payload


class ReanalyzeStoredReviewsRequest(BaseModel):
    dataset_id: str | None = None
    batchers: list[str] | None = None
    batch_size: int | None = None
    max_reviews_pool: int | None = None

    model_config = ConfigDict(extra="forbid")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _sse_event(event_name: str, payload: dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False, default=_json_default)}\n\n"


@router.post("/analyze", tags=["Analyze"])
async def analyze_business(payload: AnalyzeBusinessRequest, service: BusinessServiceDep) -> dict:
    scraper_params = payload.scraper_params
    try:
        return await service.analyze_business(
            name=payload.name,
            force=payload.force,
            strategy=payload.strategy,
            force_mode=payload.force_mode,
            interactive_max_rounds=(
                scraper_params.scraper_interactive_max_rounds if scraper_params else None
            ),
            html_scroll_max_rounds=(
                scraper_params.scraper_html_scroll_max_rounds if scraper_params else None
            ),
            html_stable_rounds=(
                scraper_params.scraper_html_stable_rounds if scraper_params else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/analyze/queue", status_code=status.HTTP_202_ACCEPTED, tags=["Analyze"])
async def enqueue_analyze_business(payload: AnalyzeBusinessRequest, service: BusinessServiceDep) -> dict:
    scraper_params = payload.scraper_params
    try:
        return await service.enqueue_business_analysis_job(
            name=payload.name,
            force=payload.force,
            strategy=payload.strategy,
            force_mode=payload.force_mode,
            interactive_max_rounds=(
                scraper_params.scraper_interactive_max_rounds if scraper_params else None
            ),
            html_scroll_max_rounds=(
                scraper_params.scraper_html_scroll_max_rounds if scraper_params else None
            ),
            html_stable_rounds=(
                scraper_params.scraper_html_stable_rounds if scraper_params else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/analyze/queue", tags=["Analyze"])
async def list_analyze_business_jobs(
    service: BusinessServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict:
    try:
        return await service.list_business_analysis_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/analyze/queue/{job_id}", tags=["Analyze"])
async def get_analyze_business_job(job_id: str, service: BusinessServiceDep) -> dict:
    try:
        return await service.get_business_analysis_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/analyze/queue/{job_id}/events", tags=["Analyze"])
async def stream_analyze_business_job_events(
    job_id: str,
    service: BusinessServiceDep,
    from_index: int = Query(default=0, ge=0),
    poll_seconds: float = Query(default=1.0, ge=0.2, le=5.0),
) -> StreamingResponse:
    async def event_generator():
        sent_index = max(0, int(from_index))
        while True:
            try:
                job_payload = await service.get_business_analysis_job(job_id=job_id)
            except ValueError as exc:
                yield _sse_event("error", {"job_id": job_id, "error": str(exc)})
                return
            except LookupError as exc:
                yield _sse_event("error", {"job_id": job_id, "error": str(exc)})
                return

            events = job_payload.get("events") or []
            total_events = len(events)
            if sent_index < total_events:
                for idx in range(sent_index, total_events):
                    event_payload = events[idx] if isinstance(events[idx], dict) else {"message": str(events[idx])}
                    yield _sse_event(
                        "progress",
                        {
                            "job_id": job_id,
                            "index": idx + 1,
                            "total_events": total_events,
                            "status": job_payload.get("status"),
                            **event_payload,
                        },
                    )
                sent_index = total_events

            status_value = str(job_payload.get("status", "")).strip().lower()
            if status_value in {"done", "failed"}:
                yield _sse_event(
                    "done",
                    {
                        "job_id": job_id,
                        "status": status_value,
                        "total_events": total_events,
                    },
                )
                return

            yield _sse_event(
                "heartbeat",
                {
                    "job_id": job_id,
                    "status": status_value or "unknown",
                    "total_events": total_events,
                },
            )
            await asyncio.sleep(float(poll_seconds))

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{business_id}/reanalyze", tags=["Reanalyze"])
async def reanalyze_business_from_stored_reviews(
    business_id: str,
    service: BusinessServiceDep,
    payload: ReanalyzeStoredReviewsRequest | None = None,
) -> dict:
    body = payload or ReanalyzeStoredReviewsRequest()
    try:
        return await service.reanalyze_business_from_stored_reviews(
            business_id=business_id,
            dataset_id=body.dataset_id,
            batchers=body.batchers,
            batch_size=body.batch_size,
            max_reviews_pool=body.max_reviews_pool,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/", tags=["Business"])
async def list_businesses(
    service: BusinessQueryServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    include_listing: bool = Query(default=False),
    name: str | None = Query(default=None),
) -> dict:
    try:
        return await service.list_businesses(
            page=page,
            page_size=page_size,
            include_listing=include_listing,
            name_query=name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/{business_id}", tags=["Business"])
async def get_business(
    business_id: str,
    service: BusinessQueryServiceDep,
    include_listing: bool = Query(default=True),
) -> dict:
    try:
        return await service.get_business(business_id=business_id, include_listing=include_listing)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/reviews", tags=["Business"])
async def get_business_reviews(
    business_id: str,
    service: BusinessQueryServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    rating_gte: float | None = Query(default=None, ge=0.0, le=5.0),
    rating_lte: float | None = Query(default=None, ge=0.0, le=5.0),
    order: str = Query(default="desc"),
) -> dict:
    try:
        return await service.get_business_reviews(
            business_id=business_id,
            page=page,
            page_size=page_size,
            rating_gte=rating_gte,
            rating_lte=rating_lte,
            order=order,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/snapshots", tags=["Business"])
async def get_business_snapshots(
    business_id: str,
    service: BusinessQueryServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    source: str | None = Query(default=None),
    kind: str | None = Query(default=None),
    include_empty: bool = Query(default=True),
) -> dict:
    try:
        return await service.list_business_snapshots(
            business_id=business_id,
            page=page,
            page_size=page_size,
            source=source,
            kind=kind,
            include_empty=include_empty,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
