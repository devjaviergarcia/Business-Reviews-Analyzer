from __future__ import annotations

import asyncio
from typing import Any

from src.business_catalog.enqueue_browser_scrape_jobs_use_case import EnqueueBrowserScrapeJobsUseCase
from src.business_catalog.relaunch_browser_scrape_job_use_case import RelaunchBrowserScrapeJobUseCase


class _FakeJobService:
    def __init__(self) -> None:
        self.enqueued: list[dict[str, Any]] = []
        self.relaunch_calls: list[dict[str, Any]] = []
        self.job_payload: dict[str, Any] = {
            "_id": "job-ta-1",
            "queue_name": "scrape_tripadvisor",
            "job_type": "business_analyze",
            "payload": {
                "name": "Casa Pepe",
                "source_name": "Casa Pepe",
                "canonical_name": "Casa Pepe",
            },
        }

    async def enqueue_job(self, **kwargs: Any) -> dict[str, Any]:
        self.enqueued.append(kwargs)
        return {
            "job_id": f"job-{len(self.enqueued)}",
            "queue_name": kwargs.get("queue_name"),
            "job_type": kwargs.get("job_type"),
            "status": "queued",
        }

    async def get_job(self, *, job_id: str) -> dict[str, Any]:
        return {**self.job_payload, "_id": job_id}

    async def relaunch_job(
        self,
        *,
        job_id: str,
        reason: str,
        force: bool,
        restart_from_zero: bool,
        payload_override: dict[str, Any] | None,
    ) -> dict[str, Any]:
        self.relaunch_calls.append(
            {
                "job_id": job_id,
                "reason": reason,
                "force": force,
                "restart_from_zero": restart_from_zero,
                "payload_override": dict(payload_override or {}),
            }
        )
        return {
            "job_id": job_id,
            "queue_name": "scrape_tripadvisor",
            "job_type": "business_analyze",
            "status": "queued",
        }


class _LiveLauncher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def launch(self, *, reason: str, job_id: str, **_: Any) -> dict[str, Any]:
        payload = {"reason": reason, "job_id": job_id}
        payload.update(_)
        self.calls.append(payload)
        return {"ok": True, "live_session": {"state": "running", "pid": 1234}}


class _FakeScrapeRoundRuntime:
    def __init__(self) -> None:
        self.open_calls: list[dict[str, Any]] = []
        self.register_calls: list[dict[str, Any]] = []

    async def open_round(self, **kwargs: Any) -> dict[str, Any]:
        self.open_calls.append(dict(kwargs))
        return {"round_id": "round-1"}

    async def register_source_job(self, **kwargs: Any) -> dict[str, Any]:
        self.register_calls.append(dict(kwargs))
        return {"round_id": kwargs["scrape_round_id"]}


def test_tripadvisor_enqueue_forces_live_and_launches_live_session() -> None:
    job_service = _FakeJobService()
    live_launcher = _LiveLauncher()
    round_runtime = _FakeScrapeRoundRuntime()
    use_case = EnqueueBrowserScrapeJobsUseCase(
        job_service=job_service,
        validate_business_name=lambda value: str(value).strip(),
        normalize_text=lambda value: str(value).strip().lower(),
        resolve_reviews_strategy=lambda value: str(value or "interactive"),
        resolve_force_mode=lambda value: str(value or "fallback_existing"),
        resolve_scrape_sources=lambda sources: tuple(str(item) for item in (sources or ("tripadvisor",))),
        inspect_local_browser_runtime_on_enqueue=lambda selected_sources: asyncio.sleep(
            0, result={"available": True, "required_sources": list(selected_sources)}
        ),
        ensure_root_business_on_enqueue=lambda **_: asyncio.sleep(0, result={"_id": "biz-1"}),
        open_browser_scrape_round=round_runtime.open_round,
        register_browser_scrape_round_source_job=round_runtime.register_source_job,
        launch_tripadvisor_live_session=live_launcher.launch,
    )

    result = asyncio.run(
        use_case.execute(
            name="Casa Pepe",
            sources=["tripadvisor"],
            execution_mode="automatic",
            live_display_mode="xvfb",
            report_profile="client_audit",
            report_complexity="hydrated",
            report_cadence="one_off",
            study_resolution_mode="refresh_now",
            include_competitors=True,
            include_geogrid=True,
        )
    )

    assert job_service.enqueued[0]["execution_mode"] == "live"
    assert job_service.enqueued[0]["live_display_mode"] == "xvfb"
    assert job_service.enqueued[0]["task_payload"].execution_mode == "live"
    assert job_service.enqueued[0]["task_payload"].live_display_mode == "xvfb"
    assert job_service.enqueued[0]["task_payload"].scrape_round_id == "round-1"
    assert result["effective_execution_mode_by_source"]["tripadvisor"] == "live"
    assert result["live_display_mode"] == "xvfb"
    assert result["scrape_round_id"] == "round-1"
    assert result["analysis_request"]["report_profile"] == "client_audit"
    assert result["analysis_request"]["report_complexity"] == "hydrated"
    assert result["analysis_request"]["study_resolution_mode"] == "refresh_now"
    assert result["analysis_request"]["include_competitors"] is True
    assert result["analysis_request"]["include_geogrid"] is True
    assert result["jobs_by_source"]["tripadvisor"]["tripadvisor_live_session"]["launched"] is True
    assert live_launcher.calls[0]["job_id"] == "job-1"
    assert live_launcher.calls[0]["live_display_mode"] == "xvfb"
    assert round_runtime.register_calls[0]["source_job_id"] == "job-1"
    assert round_runtime.open_calls[0]["analysis_request"]["report_profile"] == "client_audit"
    assert round_runtime.open_calls[0]["analysis_request"]["report_complexity"] == "hydrated"
    assert round_runtime.open_calls[0]["analysis_request"]["study_resolution_mode"] == "refresh_now"
    assert round_runtime.open_calls[0]["analysis_request"]["include_geogrid"] is True


def test_tripadvisor_relaunch_forces_live_and_launches_live_session() -> None:
    job_service = _FakeJobService()
    live_launcher = _LiveLauncher()
    use_case = RelaunchBrowserScrapeJobUseCase(
        job_service=job_service,
        ensure_job_is_scrape=lambda _: None,
        ensure_tripadvisor_session_available_for_relaunch=lambda **_: asyncio.sleep(0),
        validate_business_name=lambda value: str(value).strip(),
        normalize_text=lambda value: str(value).strip().lower(),
        launch_tripadvisor_live_session=live_launcher.launch,
    )

    result = asyncio.run(
        use_case.execute(
            job_id="job-ta-1",
            execution_mode="automatic",
            live_display_mode="xvfb",
        )
    )

    assert job_service.relaunch_calls[0]["payload_override"]["execution_mode"] == "live"
    assert job_service.relaunch_calls[0]["payload_override"]["live_display_mode"] == "xvfb"
    assert result["effective_execution_mode"] == "live"
    assert result["live_display_mode"] == "xvfb"
    assert result["tripadvisor_live_session"]["launched"] is True
    assert live_launcher.calls[0]["job_id"] == "job-ta-1"
    assert live_launcher.calls[0]["live_display_mode"] == "xvfb"
