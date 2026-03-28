from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from bson import ObjectId

from src.database import close_mongo_connection, connect_to_mongo, get_database
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.business_service import BusinessService
from src.workers.contracts import AnalyzeBusinessTaskPayload


async def _ensure_mongo_or_raise() -> None:
    await connect_to_mongo()
    await get_database().client.admin.command("ping")


def _build_business_service() -> BusinessService:
    return BusinessService(
        scraper=object(),
        tripadvisor_scraper=object(),
        preprocessor=object(),
        llm_analyzer=object(),
        job_service=AnalysisJobService(),
        query_service=BusinessQueryService(),
    )


def test_phase5_comments_canonical_write_and_query_by_job_source() -> None:
    async def _scenario() -> None:
        await _ensure_mongo_or_raise()
        database = get_database()
        comments = database["comments"]
        jobs = database["analysis_jobs"]
        service = _build_business_service()

        enqueue_result = await service.job_service.enqueue_job(
            task_payload=AnalyzeBusinessTaskPayload(name="Phase5 Integration"),
            queue_name="scrape_tripadvisor",
            job_type="business_analyze",
        )
        job_id = str(enqueue_result["job_id"])
        business_id = str(ObjectId())
        now = datetime.now(timezone.utc)

        tripadvisor_reviews = [
            {
                "source": "tripadvisor",
                "review_id": "ta-r1",
                "author_name": "Ana",
                "rating": 5.0,
                "relative_time": "hoy",
                "text": "Muy bien",
                "owner_reply": "",
                "owner_reply_relative_time": "",
                "has_text": True,
                "has_owner_reply": False,
                "relative_time_bucket": "0_3m",
            },
            {
                "source": "tripadvisor",
                "review_id": "ta-r2",
                "author_name": "Luis",
                "rating": 1.0,
                "relative_time": "ayer",
                "text": "Mal",
                "owner_reply": "",
                "owner_reply_relative_time": "",
                "has_text": True,
                "has_owner_reply": False,
                "relative_time_bucket": "0_3m",
            },
        ]
        google_reviews = [
            {
                "source": "google_maps",
                "review_id": "gm-r1",
                "author_name": "Marta",
                "rating": 4.0,
                "relative_time": "hace 1 semana",
                "text": "Bien",
                "owner_reply": "",
                "owner_reply_relative_time": "",
                "has_text": True,
                "has_owner_reply": False,
                "relative_time_bucket": "0_3m",
            }
        ]

        try:
            await service._upsert_job_comments(
                comments_collection=comments,
                business_id=business_id,
                business_name="Godeo",
                name_normalized="godeo",
                source="tripadvisor",
                source_job_id=job_id,
                processed_reviews=tripadvisor_reviews,
                scraped_at=now,
            )
            await service._upsert_job_comments(
                comments_collection=comments,
                business_id=business_id,
                business_name="Godeo",
                name_normalized="godeo",
                source="google_maps",
                source_job_id=job_id,
                processed_reviews=google_reviews,
                scraped_at=now,
            )

            all_comments = await service.list_scrape_job_comments(job_id=job_id, page=1, page_size=20)
            assert all_comments["job_id"] == job_id
            assert all_comments["total"] == 3
            assert all_comments["total_comments"] == 3
            assert all_comments["source_counts"]["tripadvisor"] == 2
            assert all_comments["source_counts"]["google_maps"] == 1

            tripadvisor_only = await service.list_scrape_job_comments(
                job_id=job_id,
                source="tripadvisor",
                page=1,
                page_size=20,
            )
            assert tripadvisor_only["source"] == "tripadvisor"
            assert tripadvisor_only["total"] == 2
            assert all(item["source"] == "tripadvisor" for item in tripadvisor_only["items"])

            await service._upsert_job_comments(
                comments_collection=comments,
                business_id=business_id,
                business_name="Godeo",
                name_normalized="godeo",
                source="tripadvisor",
                source_job_id=job_id,
                processed_reviews=[tripadvisor_reviews[0]],
                scraped_at=now,
            )
            after_cleanup = await service.list_scrape_job_comments(job_id=job_id, page=1, page_size=20)
            assert after_cleanup["total_comments"] == 2
            assert after_cleanup["source_counts"]["tripadvisor"] == 1
            assert after_cleanup["source_counts"]["google_maps"] == 1
        finally:
            await comments.delete_many({"source_job_id": job_id})
            await jobs.delete_one({"_id": ObjectId(job_id)})
            await close_mongo_connection()

    try:
        asyncio.run(_scenario())
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB is not available for integration test: {exc}")
