from __future__ import annotations

import asyncio
from typing import Any

from src.workers.contracts import AnalysisGenerateTaskPayload
from src.workers.scraper_worker import ScraperWorker


class _FakeJobService:
    def __init__(self) -> None:
        self.enqueue_calls: list[dict[str, Any]] = []

    async def enqueue_analysis_generate_job(self, *, task_payload) -> dict[str, Any]:
        payload = task_payload.model_dump(mode="python")
        self.enqueue_calls.append(payload)
        return {
            "job_id": "analysis-job-1",
            "status": "queued",
            "queue_name": "analysis",
            "job_type": "analysis_generate",
            "payload": payload,
        }


class _FakeBusinessService:
    def __init__(self, *, handoff_result: dict[str, Any] | None = None) -> None:
        self.job_service = _FakeJobService()
        self.handoff_result = dict(handoff_result) if isinstance(handoff_result, dict) else None
        self.handoff_calls: list[dict[str, Any]] = []

    async def scrape_business_for_analysis_pipeline(self, **kwargs) -> dict[str, Any]:
        del kwargs
        return {
            "business_id": "biz-1",
            "review_count": 12,
            "scraped_review_count": 12,
            "processed_review_count": 12,
            "stored_review_count_before": 0,
            "stored_review_count_after": 12,
            "scrape_produced_new_reviews": True,
            "analysis_dataset_id": "dataset-1",
            "source_profile_id": "source-profile-1",
            "scrape_run_id": "scrape-run-1",
        }

    async def handoff_completed_scrape_to_analysis(self, **kwargs) -> dict[str, Any]:
        self.handoff_calls.append(dict(kwargs))
        if self.handoff_result is not None:
            return dict(self.handoff_result)

        normalized_source = str(kwargs.get("source") or "").strip().lower() or None
        payload = AnalysisGenerateTaskPayload(
            business_id=str(kwargs.get("business_id") or "").strip(),
            dataset_id=str(kwargs.get("dataset_id") or "").strip() or None,
            source_profile_id=str(kwargs.get("source_profile_id") or "").strip() or None,
            scrape_run_id=str(kwargs.get("scrape_run_id") or "").strip() or None,
            source_job_id=str(kwargs.get("source_job_id") or "").strip() or None,
            source_mode="single" if normalized_source in {"google_maps", "tripadvisor"} else "auto",
            selected_source=normalized_source if normalized_source in {"google_maps", "tripadvisor"} else None,
            scrape_round_id=str(kwargs.get("scrape_round_id") or "").strip() or None,
        )
        enqueue_result = await self.job_service.enqueue_analysis_generate_job(task_payload=payload)
        return {
            "mode": "legacy_immediate",
            "scrape_round_id": payload.scrape_round_id,
            "analysis_enqueued": True,
            "waiting_for_sources": False,
            "claim_in_progress": False,
            "completed_sources": [normalized_source] if normalized_source else [],
            "pending_sources": [],
            "analysis_job_id": str(enqueue_result.get("job_id") or "").strip() or None,
            "analysis_queue_name": enqueue_result.get("queue_name"),
            "analysis_job_type": enqueue_result.get("job_type"),
            "analysis_payload": payload.model_dump(mode="python"),
        }


class _FakeBroker:
    def __init__(self) -> None:
        self.appended_events: list[dict[str, Any]] = []
        self.done_results: list[dict[str, Any]] = []
        self.handoff_called = False

    async def claim_next_job(self, *, queue_name: str) -> dict[str, Any] | None:
        del queue_name
        return None

    async def is_cancel_requested(self, *, job_id: Any) -> bool:
        del job_id
        return False

    async def append_event(
        self,
        *,
        job_id: Any,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
        status: str | None = None,
    ) -> None:
        self.appended_events.append(
            {
                "job_id": job_id,
                "stage": stage,
                "message": message,
                "data": data or {},
                "status": status,
            }
        )

    async def mark_done(self, *, job_id: Any, result: dict[str, Any]) -> None:
        self.done_results.append({"job_id": job_id, "result": result})

    async def mark_failed(self, *, job_id: Any, error: str) -> None:
        raise AssertionError(f"mark_failed should not be called (job_id={job_id}, error={error})")

    async def mark_needs_human(
        self,
        *,
        job_id: Any,
        reason: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        raise AssertionError(f"mark_needs_human should not be called (job_id={job_id}, reason={reason}, data={data})")

    async def handoff_job(self, **kwargs) -> None:
        self.handoff_called = True
        raise AssertionError(f"handoff_job should not be called: {kwargs}")


class _FakeCRMService:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def process_discovery_task(self, *, task_payload: Any, job_id: Any) -> dict[str, Any]:
        payload = task_payload.model_dump(mode="python")
        self.calls.append({"job_id": job_id, "payload": payload})
        return {
            "query": payload.get("query"),
            "source": payload.get("source"),
            "status": "completed",
            "candidates": 2,
            "inserted": 2,
            "updated": 0,
            "skipped": 0,
        }


def test_scraper_worker_queues_separate_analysis_job_and_keeps_scrape_job_done() -> None:
    fake_service = _FakeBusinessService()
    fake_broker = _FakeBroker()
    worker = ScraperWorker(service=fake_service, job_broker=fake_broker)

    job = {
        "_id": "scrape-job-1",
        "queue_name": "scrape",
        "job_type": "business_analyze",
        "payload": {
            "name": "Gamberra Smash burger",
            "force": True,
            "strategy": "scroll_copy",
            "force_mode": "fallback_existing",
        },
    }

    asyncio.run(worker._process_job(job))

    assert fake_broker.handoff_called is False
    assert len(fake_service.job_service.enqueue_calls) == 1
    enqueue_payload = fake_service.job_service.enqueue_calls[0]
    assert enqueue_payload["business_id"] == "biz-1"
    assert enqueue_payload["source_job_id"] == "scrape-job-1"

    assert len(fake_broker.done_results) == 1
    done_payload = fake_broker.done_results[0]["result"]
    assert done_payload["business_id"] == "biz-1"
    assert done_payload["analysis_handoff"]["analysis_job_id"] == "analysis-job-1"
    assert done_payload["analysis_handoff"]["queue_name"] == "analysis"
    assert done_payload["analysis_handoff"]["job_type"] == "analysis_generate"

    assert any(event["stage"] == "handoff_analysis_queued" for event in fake_broker.appended_events)


def test_scraper_worker_processes_crm_discovery_jobs_on_scrape_google_maps_queue() -> None:
    fake_service = _FakeBusinessService()
    fake_crm_service = _FakeCRMService()
    fake_broker = _FakeBroker()
    worker = ScraperWorker(service=fake_service, job_broker=fake_broker, crm_service=fake_crm_service)
    worker.queue_name = "scrape_google_maps"  # noqa: SLF001

    job = {
        "_id": "crm-discovery-job-1",
        "queue_name": "scrape_google_maps",
        "job_type": "crm_lead_discovery",
        "payload": {
            "query": "merienda",
            "limit": 100,
            "source": "auto_live_google_maps",
        },
    }

    asyncio.run(worker._process_job(job))

    assert len(fake_crm_service.calls) == 1
    assert fake_crm_service.calls[0]["job_id"] == "crm-discovery-job-1"
    assert fake_crm_service.calls[0]["payload"]["query"] == "merienda"

    assert len(fake_broker.done_results) == 1
    done_payload = fake_broker.done_results[0]["result"]
    assert done_payload["status"] == "completed"
    assert done_payload["candidates"] == 2
    assert any(event["stage"] == "crm_discovery_worker_started" for event in fake_broker.appended_events)
    assert any(event["stage"] == "crm_discovery_worker_completed" for event in fake_broker.appended_events)


def test_scraper_worker_tripadvisor_only_still_handoffs_to_analysis_single_source() -> None:
    fake_service = _FakeBusinessService()
    fake_broker = _FakeBroker()
    worker = ScraperWorker(service=fake_service, job_broker=fake_broker)
    worker._scrape_source = "tripadvisor"  # noqa: SLF001
    worker._selected_sources = ("tripadvisor",)  # noqa: SLF001

    job = {
        "_id": "scrape-job-trip-1",
        "queue_name": "scrape_tripadvisor",
        "job_type": "business_analyze",
        "payload": {
            "name": "Negocio Tripadvisor",
            "force": True,
            "strategy": "scroll_copy",
            "force_mode": "fallback_existing",
        },
    }

    asyncio.run(worker._process_job(job))

    assert len(fake_service.job_service.enqueue_calls) == 1
    enqueue_payload = fake_service.job_service.enqueue_calls[0]
    assert enqueue_payload["business_id"] == "biz-1"
    assert enqueue_payload["source_job_id"] == "scrape-job-trip-1"
    assert enqueue_payload["source_mode"] == "single"
    assert enqueue_payload["selected_source"] == "tripadvisor"

    assert len(fake_broker.done_results) == 1
    done_payload = fake_broker.done_results[0]["result"]
    assert done_payload["analysis_handoff"]["analysis_job_id"] == "analysis-job-1"
    assert any(event["stage"] == "handoff_analysis_queued" for event in fake_broker.appended_events)


def test_scraper_worker_defers_analysis_until_remaining_sources_finish_round() -> None:
    fake_service = _FakeBusinessService(
        handoff_result={
            "mode": "scrape_round",
            "scrape_round_id": "round-1",
            "analysis_enqueued": False,
            "waiting_for_sources": True,
            "claim_in_progress": False,
            "completed_sources": ["google_maps"],
            "pending_sources": ["tripadvisor"],
            "analysis_job_id": None,
            "analysis_queue_name": None,
            "analysis_job_type": None,
            "analysis_payload": None,
        }
    )
    fake_broker = _FakeBroker()
    worker = ScraperWorker(service=fake_service, job_broker=fake_broker)
    worker._scrape_source = "google_maps"  # noqa: SLF001
    worker._selected_sources = ("google_maps",)  # noqa: SLF001

    job = {
        "_id": "scrape-job-gmaps-1",
        "queue_name": "scrape_google_maps",
        "job_type": "business_analyze",
        "payload": {
            "name": "Negocio Google",
            "scrape_round_id": "round-1",
            "force": True,
            "strategy": "scroll_copy",
            "force_mode": "fallback_existing",
        },
    }

    asyncio.run(worker._process_job(job))

    assert len(fake_service.job_service.enqueue_calls) == 0
    assert len(fake_broker.done_results) == 1
    done_payload = fake_broker.done_results[0]["result"]
    assert done_payload["analysis_handoff"]["waiting_for_sources"] is True
    assert done_payload["analysis_handoff"]["pending_sources"] == ["tripadvisor"]
    assert any(event["stage"] == "handoff_analysis_waiting_round" for event in fake_broker.appended_events)
