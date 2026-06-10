from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.services.business_service import BusinessService


class _FakeJobService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def enqueue_job(  # noqa: PLR0913
        self,
        *,
        task_payload: Any,
        name_normalized: str | None = None,
        queue_name: str = "scrape",
        job_type: str = "business_analyze",
        source: str | None = None,
        runtime_target: str | None = None,
        execution_mode: str | None = None,
        requested_by: str | None = None,
        fallback_policy: str | None = None,
        source_display_name: str | None = None,
    ) -> dict[str, Any]:
        payload = task_payload.model_dump(mode="python")
        self.calls.append(
            {
                "payload": payload,
                "name_normalized": name_normalized,
                "queue_name": queue_name,
                "job_type": job_type,
                "source": source,
                "runtime_target": runtime_target,
                "execution_mode": execution_mode,
                "requested_by": requested_by,
                "fallback_policy": fallback_policy,
                "source_display_name": source_display_name,
            }
        )
        return {
            "job_id": f"job-{len(self.calls)}",
            "queue_name": queue_name,
            "job_type": job_type,
            "status": "queued",
        }


class _FakeLocalBrowserWorkerRegistry:
    def __init__(self, workers: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._workers = workers or []

    async def list_live_workers(
        self,
        *,
        supported_sources: tuple[str, ...] | list[str] | None = None,
        stale_after_seconds: int = 30,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            {
                "supported_sources": list(supported_sources or []),
                "stale_after_seconds": stale_after_seconds,
            }
        )
        return list(self._workers)


def _build_service(
    *,
    job_service: _FakeJobService,
    local_browser_worker_registry: _FakeLocalBrowserWorkerRegistry,
) -> BusinessService:
    return BusinessService(
        scraper=object(),
        tripadvisor_scraper=object(),
        preprocessor=object(),
        llm_analyzer=object(),
        job_service=job_service,
        query_service=object(),
        local_browser_worker_registry=local_browser_worker_registry,
    )


def test_enqueue_tripadvisor_still_queues_when_no_local_runtime_is_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import config as config_module

    monkeypatch.setattr(
        config_module.settings,
        "tripadvisor_local_worker_autostart_on_enqueue",
        True,
    )
    monkeypatch.setattr(
        config_module.settings,
        "tripadvisor_local_worker_bridge_enabled",
        True,
    )
    monkeypatch.setattr(
        config_module.settings,
        "local_browser_worker_heartbeat_seconds",
        10,
    )

    fake_job_service = _FakeJobService()
    fake_registry = _FakeLocalBrowserWorkerRegistry()
    service = _build_service(
        job_service=fake_job_service,
        local_browser_worker_registry=fake_registry,
    )

    result = asyncio.run(
        service.enqueue_business_scrape_jobs(
            name="Godeo",
            sources=["tripadvisor"],
        )
    )

    assert len(fake_registry.calls) == 1
    assert fake_registry.calls[0]["supported_sources"] == ["tripadvisor"]
    assert len(fake_job_service.calls) == 1
    assert fake_job_service.calls[0]["queue_name"] == "scrape_tripadvisor"
    assert result["primary_source"] == "tripadvisor"
    assert result["local_browser_runtime"]["available"] is False
    assert result["local_browser_runtime"]["missing_sources"] == ["tripadvisor"]
    assert "will remain pending" in result["local_browser_runtime"]["message"].lower()


def test_enqueue_tripadvisor_reports_available_local_runtime_when_worker_exists() -> None:
    fake_job_service = _FakeJobService()
    fake_registry = _FakeLocalBrowserWorkerRegistry(
        workers=[
            {
                "worker_id": "local-browser:test",
                "state": "idle",
                "supported_sources": ["google_maps", "tripadvisor"],
                "host_name": "test-host",
                "pid": 4242,
                "last_seen_at": "2026-06-10T10:00:00Z",
                "current_job_id": None,
                "current_source": None,
                "current_execution_mode": None,
            }
        ]
    )
    service = _build_service(
        job_service=fake_job_service,
        local_browser_worker_registry=fake_registry,
    )

    result = asyncio.run(
        service.enqueue_business_scrape_jobs(
            name="Godeo",
            sources=["tripadvisor"],
        )
    )

    assert len(fake_job_service.calls) == 1
    assert result["local_browser_runtime"]["available"] is True
    assert result["local_browser_runtime"]["available_sources"] == ["tripadvisor"]
    assert result["local_browser_runtime"]["worker_count"] == 1
    assert result["local_browser_runtime"]["workers"][0]["worker_id"] == "local-browser:test"

