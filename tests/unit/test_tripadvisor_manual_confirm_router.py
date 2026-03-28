from __future__ import annotations

import asyncio
from typing import Any

from src.routers.tripadvisor import (
    ManualSessionConfirmRequest,
    confirm_tripadvisor_manual_session,
)


class _FakeTripadvisorSessionService:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    async def refresh_from_storage_state(
        self,
        *,
        storage_state_path: str | None,
        profile_dir: str | None,
        mark_human_intervention: bool,
    ) -> dict[str, Any]:
        return dict(self._state)


class _FakeAnalysisJobService:
    def __init__(self) -> None:
        self.called = False
        self.last_kwargs: dict[str, Any] | None = None

    async def relaunch_jobs_waiting_human(self, **kwargs: Any) -> dict[str, Any]:
        self.called = True
        self.last_kwargs = kwargs
        return {
            "queue_name": "scrape_tripadvisor",
            "requested_limit": kwargs.get("limit"),
            "matched_jobs": 2,
            "relaunched_jobs": ["job1", "job2"],
            "errors": [],
        }


def test_manual_confirm_forces_relaunch_when_session_not_available() -> None:
    payload = ManualSessionConfirmRequest(
        profile_dir="playwright-data-tripadvisor-worker-docker",
        relaunch_pending_tripadvisor_jobs=True,
        force_relaunch_if_session_unavailable=True,
        relaunch_limit=77,
    )
    session_service = _FakeTripadvisorSessionService(
        {
            "session_state": "invalid",
            "availability_now": False,
            "last_validation_result": "missing_cookie_expiration",
        }
    )
    job_service = _FakeAnalysisJobService()

    result = asyncio.run(
        confirm_tripadvisor_manual_session(payload, session_service, job_service)
    )

    assert job_service.called is True
    assert job_service.last_kwargs is not None
    assert job_service.last_kwargs.get("queue_name") == "scrape_tripadvisor"
    assert job_service.last_kwargs.get("limit") == 77
    assert result["relaunch_forced_without_availability"] is True
    assert result["relaunch_skipped"] is False


def test_manual_confirm_skips_relaunch_when_session_not_available_and_not_forced() -> None:
    payload = ManualSessionConfirmRequest(
        profile_dir="playwright-data-tripadvisor-worker-docker",
        relaunch_pending_tripadvisor_jobs=True,
        force_relaunch_if_session_unavailable=False,
        relaunch_limit=12,
    )
    session_service = _FakeTripadvisorSessionService(
        {
            "session_state": "invalid",
            "availability_now": False,
            "last_validation_result": "missing_cookie_expiration",
        }
    )
    job_service = _FakeAnalysisJobService()

    result = asyncio.run(
        confirm_tripadvisor_manual_session(payload, session_service, job_service)
    )

    assert job_service.called is False
    assert result["relaunch"] is None
    assert result["relaunch_forced_without_availability"] is False
    assert result["relaunch_skipped"] is True
