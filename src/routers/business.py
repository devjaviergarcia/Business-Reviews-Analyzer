import asyncio
import json
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
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
    scraper_tripadvisor_max_pages: int | None = Field(default=None, ge=1, le=10000)
    scraper_tripadvisor_pages_percent: float | None = Field(default=None, gt=0, le=100)

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
            "tripadvisor_max_pages",
            "tripadvisor_pages_percent",
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
            if "tripadvisor_max_pages" in payload:
                scraper_params.setdefault(
                    "scraper_tripadvisor_max_pages",
                    payload.pop("tripadvisor_max_pages"),
                )
            if "tripadvisor_pages_percent" in payload:
                scraper_params.setdefault(
                    "scraper_tripadvisor_pages_percent",
                    payload.pop("tripadvisor_pages_percent"),
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


class ScrapeBusinessJobsRequest(AnalyzeBusinessRequest):
    sources: list[Literal["google_maps", "tripadvisor"]] | None = None
    google_maps_name: str | None = None
    tripadvisor_name: str | None = None

    model_config = ConfigDict(extra="forbid")


class ReanalyzeStoredReviewsRequest(BaseModel):
    dataset_id: str | None = None
    batchers: list[str] | None = None
    batch_size: int | None = None
    max_reviews_pool: int | None = None

    model_config = ConfigDict(extra="forbid")


class RelaunchAnalyzeBusinessJobRequest(BaseModel):
    reason: str | None = None
    force: bool = False
    restart_from_zero: bool = False

    model_config = ConfigDict(extra="forbid")


class RelaunchTripadvisorAntiBotJobsRequest(BaseModel):
    limit: int = Field(default=20, ge=1, le=200)
    reason: str | None = None
    status_filter: Literal["failed", "needs_human", "failed_or_needs_human", "all"] = (
        "failed_or_needs_human"
    )

    model_config = ConfigDict(extra="forbid")


class TripadvisorLiveCommitRequest(BaseModel):
    listing: dict[str, Any]
    reviews: list[dict[str, Any]] = Field(default_factory=list)
    commit_reason: str | None = None
    metadata: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid")


class AnalyzeStoredReviewsJobRequest(BaseModel):
    business_id: str
    dataset_id: str | None = None
    batchers: list[str] | None = None
    batch_size: int | None = Field(default=None, ge=1, le=2000)
    max_reviews_pool: int | None = Field(default=None, ge=1, le=100000)
    source_job_id: str | None = None

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
            tripadvisor_max_pages=(
                scraper_params.scraper_tripadvisor_max_pages if scraper_params else None
            ),
            tripadvisor_pages_percent=(
                scraper_params.scraper_tripadvisor_pages_percent if scraper_params else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/scrape/jobs", status_code=status.HTTP_202_ACCEPTED, tags=["Scrape"])
async def enqueue_scrape_jobs(payload: ScrapeBusinessJobsRequest, service: BusinessServiceDep) -> dict:
    scraper_params = payload.scraper_params
    try:
        return await service.enqueue_business_scrape_jobs(
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
            tripadvisor_max_pages=(
                scraper_params.scraper_tripadvisor_max_pages if scraper_params else None
            ),
            tripadvisor_pages_percent=(
                scraper_params.scraper_tripadvisor_pages_percent if scraper_params else None
            ),
            sources=payload.sources,
            google_maps_name=payload.google_maps_name,
            tripadvisor_name=payload.tripadvisor_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.get("/scrape/jobs", tags=["Scrape"])
async def list_scrape_jobs(
    service: BusinessServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict:
    try:
        return await service.list_scrape_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/scrape/jobs/tripadvisor/antibot", tags=["Scrape"])
async def list_tripadvisor_antibot_scrape_jobs(
    service: BusinessServiceDep,
    limit: int = Query(default=20, ge=1, le=200),
    status_filter: Literal["failed", "needs_human", "failed_or_needs_human", "all"] = Query(
        default="failed_or_needs_human"
    ),
) -> dict:
    try:
        return await service.list_tripadvisor_antibot_jobs(
            limit=limit,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/scrape/jobs/tripadvisor/antibot/relaunch", tags=["Scrape"])
async def relaunch_tripadvisor_antibot_scrape_jobs(
    payload: RelaunchTripadvisorAntiBotJobsRequest,
    service: BusinessServiceDep,
) -> dict:
    try:
        return await service.relaunch_tripadvisor_antibot_jobs(
            limit=payload.limit,
            reason=payload.reason,
            status_filter=payload.status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/scrape/jobs/{job_id}", tags=["Scrape"])
async def get_scrape_job(job_id: str, service: BusinessServiceDep) -> dict:
    try:
        return await service.get_scrape_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/scrape/jobs/{job_id}/comments", tags=["Scrape"])
async def list_scrape_job_comments(
    job_id: str,
    service: BusinessServiceDep,
    source: Literal["google_maps", "tripadvisor"] | None = Query(default=None),
    scrape_type: Literal["google_maps", "tripadvisor"] | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    rating_gte: float | None = Query(default=None, ge=0.0, le=5.0),
    rating_lte: float | None = Query(default=None, ge=0.0, le=5.0),
    order: str = Query(default="desc-date"),
) -> dict:
    try:
        return await service.list_scrape_job_comments(
            job_id=job_id,
            source=source,
            scrape_type=scrape_type,
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


@router.delete("/scrape/jobs/{job_id}", tags=["Scrape"])
async def delete_scrape_job(
    job_id: str,
    service: BusinessServiceDep,
    wait_active_stop_seconds: float = Query(default=10.0, ge=0.5, le=120.0),
    poll_seconds: float = Query(default=0.5, ge=0.1, le=5.0),
    force_delete_on_timeout: bool = Query(default=True),
) -> dict:
    try:
        return await service.delete_scrape_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post("/scrape/jobs/{job_id}/relaunch", tags=["Scrape"])
async def relaunch_scrape_job(
    job_id: str,
    service: BusinessServiceDep,
    payload: RelaunchAnalyzeBusinessJobRequest | None = None,
) -> dict:
    reason = payload.reason if payload else None
    force = bool(payload.force) if payload else False
    restart_from_zero = bool(payload.restart_from_zero) if payload else False
    try:
        return await service.relaunch_scrape_job(
            job_id=job_id,
            reason=reason,
            force=force,
            restart_from_zero=restart_from_zero,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.post("/scrape/jobs/{job_id}/commit-live", tags=["Scrape"])
async def commit_tripadvisor_live_capture(
    job_id: str,
    payload: TripadvisorLiveCommitRequest,
    service: BusinessServiceDep,
) -> dict:
    try:
        return await service.commit_tripadvisor_live_capture(
            job_id=job_id,
            listing=payload.listing,
            reviews=payload.reviews,
            commit_reason=payload.commit_reason,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/scrape/jobs/{job_id}/stop", tags=["Scrape"])
async def stop_scrape_job(
    job_id: str,
    service: BusinessServiceDep,
    continue_analysis_if_google: bool = Query(default=True),
    wait_active_stop_seconds: float = Query(default=10.0, ge=0.5, le=120.0),
    poll_seconds: float = Query(default=0.5, ge=0.1, le=5.0),
) -> dict:
    try:
        return await service.stop_business_scrape_job(
            job_id=job_id,
            continue_analysis_if_google=continue_analysis_if_google,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/scrape/jobs/{job_id}/events", tags=["Scrape"])
async def stream_scrape_job_events(
    job_id: str,
    service: BusinessServiceDep,
    from_index: int = Query(default=0, ge=0),
    poll_seconds: float = Query(default=1.0, ge=0.2, le=5.0),
) -> StreamingResponse:
    async def event_generator():
        sent_index = max(0, int(from_index))
        while True:
            try:
                job_payload = await service.get_scrape_job(job_id=job_id)
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
            if status_value in {"done", "failed", "needs_human"}:
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


@router.post("/analyze/jobs", status_code=status.HTTP_202_ACCEPTED, tags=["Analyze"])
async def enqueue_analyze_job(
    payload: AnalyzeStoredReviewsJobRequest,
    service: BusinessServiceDep,
) -> dict:
    try:
        return await service.enqueue_business_analysis_generate_job(
            business_id=payload.business_id,
            dataset_id=payload.dataset_id,
            batchers=payload.batchers,
            batch_size=payload.batch_size,
            max_reviews_pool=payload.max_reviews_pool,
            source_job_id=payload.source_job_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/analyze/jobs", tags=["Analyze"])
async def list_analyze_jobs(
    service: BusinessServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict:
    try:
        return await service.list_analysis_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/analyze/jobs/{job_id}", tags=["Analyze"])
async def get_analyze_job(job_id: str, service: BusinessServiceDep) -> dict:
    try:
        return await service.get_analysis_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.delete("/analyze/jobs/{job_id}", tags=["Analyze"])
async def delete_analyze_job(
    job_id: str,
    service: BusinessServiceDep,
    wait_active_stop_seconds: float = Query(default=10.0, ge=0.5, le=120.0),
    poll_seconds: float = Query(default=0.5, ge=0.1, le=5.0),
    force_delete_on_timeout: bool = Query(default=True),
) -> dict:
    try:
        return await service.delete_analysis_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post("/analyze/jobs/{job_id}/relaunch", tags=["Analyze"])
async def relaunch_analyze_job(
    job_id: str,
    service: BusinessServiceDep,
    payload: RelaunchAnalyzeBusinessJobRequest | None = None,
) -> dict:
    reason = payload.reason if payload else None
    force = bool(payload.force) if payload else False
    restart_from_zero = bool(payload.restart_from_zero) if payload else False
    try:
        return await service.relaunch_analysis_job(
            job_id=job_id,
            reason=reason,
            force=force,
            restart_from_zero=restart_from_zero,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/analyze/jobs/{job_id}/events", tags=["Analyze"])
async def stream_analyze_job_events(
    job_id: str,
    service: BusinessServiceDep,
    from_index: int = Query(default=0, ge=0),
    poll_seconds: float = Query(default=1.0, ge=0.2, le=5.0),
) -> StreamingResponse:
    async def event_generator():
        sent_index = max(0, int(from_index))
        while True:
            try:
                job_payload = await service.get_analysis_job(job_id=job_id)
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
            if status_value in {"done", "failed", "needs_human"}:
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


@router.get("/report/jobs", tags=["Analyze"])
async def list_report_jobs(
    service: BusinessServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    status_filter: str | None = Query(default=None, alias="status"),
) -> dict:
    try:
        return await service.list_report_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/report/artifacts", tags=["Analyze"])
async def open_report_artifact(
    service: BusinessServiceDep,
    path: str = Query(..., min_length=1),
    download: bool = Query(default=False),
):
    try:
        resolved_path = service.resolve_report_artifact_path(path=path)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return FileResponse(
        path=str(resolved_path),
        filename=resolved_path.name if download else None,
    )


@router.get("/report/jobs/{job_id}", tags=["Analyze"])
async def get_report_job(job_id: str, service: BusinessServiceDep) -> dict:
    try:
        return await service.get_report_job(job_id=job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.delete("/report/jobs/{job_id}", tags=["Analyze"])
async def delete_report_job(
    job_id: str,
    service: BusinessServiceDep,
    wait_active_stop_seconds: float = Query(default=10.0, ge=0.5, le=120.0),
    poll_seconds: float = Query(default=0.5, ge=0.1, le=5.0),
    force_delete_on_timeout: bool = Query(default=True),
) -> dict:
    try:
        return await service.delete_report_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post("/report/jobs/{job_id}/relaunch", tags=["Analyze"])
async def relaunch_report_job(
    job_id: str,
    service: BusinessServiceDep,
    payload: RelaunchAnalyzeBusinessJobRequest | None = None,
) -> dict:
    reason = payload.reason if payload else None
    force = bool(payload.force) if payload else False
    restart_from_zero = bool(payload.restart_from_zero) if payload else False
    try:
        return await service.relaunch_report_job(
            job_id=job_id,
            reason=reason,
            force=force,
            restart_from_zero=restart_from_zero,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/report/jobs/{job_id}/events", tags=["Analyze"])
async def stream_report_job_events(
    job_id: str,
    service: BusinessServiceDep,
    from_index: int = Query(default=0, ge=0),
    poll_seconds: float = Query(default=1.0, ge=0.2, le=5.0),
) -> StreamingResponse:
    async def event_generator():
        sent_index = max(0, int(from_index))
        while True:
            try:
                job_payload = await service.get_report_job(job_id=job_id)
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
            if status_value in {"done", "failed", "needs_human"}:
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


@router.get("/{business_id}/sources", tags=["Business"])
async def get_business_sources_overview(
    business_id: str,
    service: BusinessServiceDep,
    comments_preview_size: int = Query(default=5, ge=1, le=20),
) -> dict:
    try:
        return await service.get_business_sources_overview(
            business_id=business_id,
            comments_preview_size=comments_preview_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get("/{business_id}/comments", tags=["Business"])
async def list_business_comments(
    business_id: str,
    service: BusinessServiceDep,
    source: Literal["google_maps", "tripadvisor"] | None = Query(default=None),
    scrape_type: Literal["google_maps", "tripadvisor"] | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    rating_gte: float | None = Query(default=None, ge=0.0, le=5.0),
    rating_lte: float | None = Query(default=None, ge=0.0, le=5.0),
    order: str = Query(default="desc-date"),
) -> dict:
    try:
        return await service.list_business_comments(
            business_id=business_id,
            source=source,
            scrape_type=scrape_type,
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


@router.get("/{business_id}/reviews", tags=["Business"])
async def get_business_reviews(
    business_id: str,
    service: BusinessQueryServiceDep,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    rating_gte: float | None = Query(default=None, ge=0.0, le=5.0),
    rating_lte: float | None = Query(default=None, ge=0.0, le=5.0),
    order: str = Query(default="desc-rating"),
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


@router.delete("/{business_id}", tags=["Business"])
async def delete_business(
    business_id: str,
    service: BusinessServiceDep,
    wait_active_stop_seconds: float = Query(default=10.0, ge=0.5, le=120.0),
    poll_seconds: float = Query(default=0.5, ge=0.1, le=5.0),
    force_delete_on_timeout: bool = Query(default=True),
    delete_related_jobs: bool = Query(default=True),
) -> dict:
    try:
        return await service.delete_business(
            business_id=business_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
            delete_related_jobs=delete_related_jobs,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


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
