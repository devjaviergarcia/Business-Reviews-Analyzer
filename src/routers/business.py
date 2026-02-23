import asyncio
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, model_validator

from src.services.business_service import BusinessService

router = APIRouter(prefix="/business")


class AnalyzeBusinessRequest(BaseModel):
    name: str
    force: bool = False
    strategy: str | None = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def normalize_cached_to_force(cls, raw: object) -> object:
        if not isinstance(raw, dict):
            return raw

        payload = dict(raw)
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
async def analyze_business(payload: AnalyzeBusinessRequest) -> dict:
    service = BusinessService()
    try:
        return await service.analyze_business(
            name=payload.name,
            force=payload.force,
            strategy=payload.strategy,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/analyze/queue", status_code=status.HTTP_202_ACCEPTED, tags=["Analyze"])
async def enqueue_analyze_business(payload: AnalyzeBusinessRequest) -> dict:
    service = BusinessService()
    try:
        return await service.enqueue_business_analysis_job(
            name=payload.name,
            force=payload.force,
            strategy=payload.strategy,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/analyze/queue", tags=["Analyze"])
async def list_analyze_business_jobs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict:
    service = BusinessService()
    try:
        return await service.list_business_analysis_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/analyze/queue/{job_id}", tags=["Analyze"])
async def get_analyze_business_job(job_id: str) -> dict:
    service = BusinessService()
    try:
        return await service.get_business_analysis_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/analyze/queue/{job_id}/events", tags=["Analyze"])
async def stream_analyze_business_job_events(
    job_id: str,
    from_index: int = Query(default=0, ge=0),
    poll_seconds: float = Query(default=1.0, ge=0.2, le=5.0),
) -> StreamingResponse:
    service = BusinessService()

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
    payload: ReanalyzeStoredReviewsRequest | None = None,
) -> dict:
    body = payload or ReanalyzeStoredReviewsRequest()
    service = BusinessService()
    try:
        return await service.reanalyze_business_from_stored_reviews(
            business_id=business_id,
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
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    include_listing: bool = Query(default=False),
) -> dict:
    service = BusinessService()
    try:
        return await service.list_businesses(
            page=page,
            page_size=page_size,
            include_listing=include_listing,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/{business_id}", tags=["Business"])
async def get_business(business_id: str, include_listing: bool = Query(default=True)) -> dict:
    service = BusinessService()
    try:
        return await service.get_business(business_id=business_id, include_listing=include_listing)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/reviews", tags=["Business"])
async def get_business_reviews(
    business_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
) -> dict:
    service = BusinessService()
    try:
        return await service.get_business_reviews(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
