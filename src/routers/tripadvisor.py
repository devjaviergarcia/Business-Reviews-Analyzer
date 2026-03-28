from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from src.dependencies import (
    create_analysis_job_service,
    create_tripadvisor_local_worker_control_service,
    create_tripadvisor_session_service,
)
from src.services.analysis_job_service import AnalysisJobService
from src.services.tripadvisor_local_worker_control_service import (
    TripadvisorLocalWorkerControlService,
)
from src.services.tripadvisor_session_service import TripadvisorSessionService

router = APIRouter(prefix="/tripadvisor", tags=["Tripadvisor Session"])

TripadvisorSessionServiceDep = Annotated[
    TripadvisorSessionService,
    Depends(create_tripadvisor_session_service),
]
AnalysisJobServiceDep = Annotated[
    AnalysisJobService,
    Depends(create_analysis_job_service),
]
TripadvisorLocalWorkerControlServiceDep = Annotated[
    TripadvisorLocalWorkerControlService,
    Depends(create_tripadvisor_local_worker_control_service),
]


class RefreshSessionStateRequest(BaseModel):
    storage_state_path: str | None = None
    profile_dir: str | None = None
    mark_human_intervention: bool = False

    model_config = ConfigDict(extra="forbid")


class ManualSessionConfirmRequest(BaseModel):
    storage_state_path: str | None = None
    profile_dir: str | None = None
    relaunch_pending_tripadvisor_jobs: bool = True
    force_relaunch_if_session_unavailable: bool = False
    relaunch_limit: int = Field(default=100, ge=1, le=500)

    model_config = ConfigDict(extra="forbid")


class MarkInvalidSessionRequest(BaseModel):
    reason: str
    increment_bot_detected: bool = False

    model_config = ConfigDict(extra="forbid")


class LaunchTripadvisorLiveSessionRequest(BaseModel):
    reason: str = "needs_human_live"
    display: str | None = None
    profile_dir: str | None = None
    job_id: str | None = None

    model_config = ConfigDict(extra="forbid")


@router.get("/session-state")
async def get_tripadvisor_session_state(service: TripadvisorSessionServiceDep) -> dict:
    return await service.get_state()


@router.post("/session-state/refresh-from-storage")
async def refresh_tripadvisor_session_state(
    payload: RefreshSessionStateRequest,
    service: TripadvisorSessionServiceDep,
) -> dict:
    try:
        return await service.refresh_from_storage_state(
            storage_state_path=payload.storage_state_path,
            profile_dir=payload.profile_dir,
            mark_human_intervention=payload.mark_human_intervention,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/session-state/manual-confirm")
async def confirm_tripadvisor_manual_session(
    payload: ManualSessionConfirmRequest,
    service: TripadvisorSessionServiceDep,
    job_service: AnalysisJobServiceDep,
) -> dict:
    try:
        session_state = await service.refresh_from_storage_state(
            storage_state_path=payload.storage_state_path,
            profile_dir=payload.profile_dir,
            mark_human_intervention=True,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    relaunch_result: dict | None = None
    should_relaunch = bool(payload.relaunch_pending_tripadvisor_jobs) and (
        bool(session_state.get("availability_now"))
        or bool(payload.force_relaunch_if_session_unavailable)
    )
    if should_relaunch:
        relaunch_result = await job_service.relaunch_jobs_waiting_human(
            queue_name="scrape_tripadvisor",
            limit=payload.relaunch_limit,
            reason="Relaunched after manual TripAdvisor session confirmation.",
        )

    return {
        "session_state": session_state,
        "relaunch": relaunch_result,
        "relaunch_forced_without_availability": bool(payload.force_relaunch_if_session_unavailable)
        and not bool(session_state.get("availability_now")),
        "relaunch_skipped": bool(payload.relaunch_pending_tripadvisor_jobs) and not should_relaunch,
    }


@router.post("/session-state/mark-invalid")
async def mark_tripadvisor_session_invalid(
    payload: MarkInvalidSessionRequest,
    service: TripadvisorSessionServiceDep,
) -> dict:
    if not str(payload.reason or "").strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="reason is required.",
        )
    return await service.mark_invalid(
        reason=payload.reason,
        increment_bot_detected=payload.increment_bot_detected,
    )


@router.get("/live-session/status")
async def get_tripadvisor_live_session_status(
    control_service: TripadvisorLocalWorkerControlServiceDep,
) -> dict:
    try:
        return await control_service.live_session_status()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.get("/live-session/log-tail")
async def get_tripadvisor_live_session_log_tail(
    control_service: TripadvisorLocalWorkerControlServiceDep,
    max_chars: int = 6000,
) -> dict:
    try:
        return await control_service.live_session_log_tail(max_chars=max_chars)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/live-session/launch")
async def launch_tripadvisor_live_session(
    payload: LaunchTripadvisorLiveSessionRequest,
    control_service: TripadvisorLocalWorkerControlServiceDep,
) -> dict:
    try:
        return await control_service.launch_live_session(
            reason=payload.reason,
            display=payload.display,
            profile_dir=payload.profile_dir,
            job_id=payload.job_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@router.post("/live-session/stop")
async def stop_tripadvisor_live_session(
    control_service: TripadvisorLocalWorkerControlServiceDep,
) -> dict:
    try:
        return await control_service.stop_live_session()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
