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
    ) -> dict[str, Any]:
        payload = task_payload.model_dump(mode="python")
        self.calls.append(
            {
                "payload": payload,
                "name_normalized": name_normalized,
                "queue_name": queue_name,
                "job_type": job_type,
            }
        )
        return {
            "job_id": f"job-{len(self.calls)}",
            "queue_name": queue_name,
            "job_type": job_type,
            "status": "queued",
        }


class _FakeLocalWorkerControlService:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._result = result or {"ok": True, "worker": {"running": True}}

    async def ensure_started(self, *, use_xvfb: bool = True, reason: str = "") -> dict[str, Any]:
        self.calls.append({"use_xvfb": use_xvfb, "reason": reason})
        return self._result


def _build_service(
    *,
    job_service: _FakeJobService,
    control_service: _FakeLocalWorkerControlService,
) -> BusinessService:
    return BusinessService(
        scraper=object(),
        tripadvisor_scraper=object(),
        preprocessor=object(),
        llm_analyzer=object(),
        job_service=job_service,
        query_service=object(),
        tripadvisor_local_worker_control_service=control_service,
    )


def test_enqueue_tripadvisor_autostarts_local_worker_when_enabled(
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

    fake_job_service = _FakeJobService()
    fake_control_service = _FakeLocalWorkerControlService()
    service = _build_service(
        job_service=fake_job_service,
        control_service=fake_control_service,
    )

    result = asyncio.run(
        service.enqueue_business_scrape_jobs(
            name="Godeo",
            sources=["tripadvisor"],
        )
    )

    assert len(fake_control_service.calls) == 1
    assert fake_control_service.calls[0]["use_xvfb"] is True
    assert len(fake_job_service.calls) == 1
    assert fake_job_service.calls[0]["queue_name"] == "scrape_tripadvisor"
    assert fake_job_service.calls[0]["payload"]["canonical_name"] == "Godeo"
    assert fake_job_service.calls[0]["payload"]["source_name"] == "Godeo"
    assert fake_job_service.calls[0]["payload"]["canonical_name_normalized"] == "godeo"
    assert result["primary_source"] == "tripadvisor"


def test_enqueue_tripadvisor_skips_autostart_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import config as config_module

    monkeypatch.setattr(
        config_module.settings,
        "tripadvisor_local_worker_autostart_on_enqueue",
        False,
    )
    monkeypatch.setattr(
        config_module.settings,
        "tripadvisor_local_worker_bridge_enabled",
        True,
    )

    fake_job_service = _FakeJobService()
    fake_control_service = _FakeLocalWorkerControlService()
    service = _build_service(
        job_service=fake_job_service,
        control_service=fake_control_service,
    )

    asyncio.run(
        service.enqueue_business_scrape_jobs(
            name="Godeo",
            sources=["tripadvisor"],
        )
    )

    assert len(fake_control_service.calls) == 0
    assert len(fake_job_service.calls) == 1


def test_enqueue_tripadvisor_fails_when_autostart_enabled_but_bridge_disabled(
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
        False,
    )

    fake_job_service = _FakeJobService()
    fake_control_service = _FakeLocalWorkerControlService()
    service = _build_service(
        job_service=fake_job_service,
        control_service=fake_control_service,
    )

    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(
            service.enqueue_business_scrape_jobs(
                name="Godeo",
                sources=["tripadvisor"],
            )
        )

    assert "bridge is disabled" in str(exc_info.value).lower()
    assert len(fake_job_service.calls) == 0
    assert len(fake_control_service.calls) == 0
