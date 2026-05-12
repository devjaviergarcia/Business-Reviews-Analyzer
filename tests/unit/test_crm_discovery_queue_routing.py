from __future__ import annotations

import asyncio
from typing import Any

from src.services.crm_service import CRMService


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
        payload_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = name_normalized, payload_override
        payload = task_payload.model_dump(mode="python")
        self.calls.append(
            {
                "queue_name": queue_name,
                "job_type": job_type,
                "payload": payload,
            }
        )
        return {
            "job_id": f"job-{len(self.calls)}",
            "queue_name": queue_name,
            "job_type": job_type,
            "status": "queued",
            "payload": payload,
        }


class _Dummy:
    pass


def _build_service(job_service: _FakeJobService) -> CRMService:
    service = CRMService(job_service=job_service, business_service=_Dummy())
    service._use_discovery_v2 = False
    service._use_repo_v2 = False
    return service


def test_discovery_live_sources_are_queued_to_scrape_google_maps() -> None:
    fake_job_service = _FakeJobService()
    service = _build_service(fake_job_service)

    result = asyncio.run(
        service.enqueue_lead_discovery_job(
            query="merienda",
            city=None,
            category=None,
            limit=100,
            source="auto_live_google_maps",
        )
    )

    assert result["queue_name"] == "scrape_google_maps"
    assert len(fake_job_service.calls) == 1
    assert fake_job_service.calls[0]["queue_name"] == "scrape_google_maps"
    assert fake_job_service.calls[0]["job_type"] == "crm_lead_discovery"


def test_discovery_auto_source_is_normalized_to_live_and_queued_to_scrape_google_maps() -> None:
    fake_job_service = _FakeJobService()
    service = _build_service(fake_job_service)

    result = asyncio.run(
        service.enqueue_lead_discovery_job(
            query="cafeterias madrid",
            city=None,
            category=None,
            limit=100,
            source="auto",
        )
    )

    assert result["queue_name"] == "scrape_google_maps"
    assert len(fake_job_service.calls) == 1
    assert fake_job_service.calls[0]["queue_name"] == "scrape_google_maps"
    assert fake_job_service.calls[0]["job_type"] == "crm_lead_discovery"
    assert fake_job_service.calls[0]["payload"]["source"] == "auto_live_google_maps"


def test_discovery_non_live_sources_stay_in_crm_queue() -> None:
    fake_job_service = _FakeJobService()
    service = _build_service(fake_job_service)

    result = asyncio.run(
        service.enqueue_lead_discovery_job(
            query="hotel",
            city=None,
            category=None,
            limit=100,
            source="research_google_maps",
        )
    )

    assert result["queue_name"] == "crm"
    assert len(fake_job_service.calls) == 1
    assert fake_job_service.calls[0]["queue_name"] == "crm"
    assert fake_job_service.calls[0]["job_type"] == "crm_lead_discovery"
