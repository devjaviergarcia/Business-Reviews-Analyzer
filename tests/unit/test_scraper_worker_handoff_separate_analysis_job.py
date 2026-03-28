from __future__ import annotations

import asyncio
from typing import Any

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
    def __init__(self) -> None:
        self.job_service = _FakeJobService()

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
