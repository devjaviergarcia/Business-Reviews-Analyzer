from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from src.services.business_service import BusinessService


class _FakeCommentsCollection:
    def __init__(self) -> None:
        self.update_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []

    async def update_one(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        *,
        upsert: bool = False,
    ) -> None:
        self.update_calls.append(
            {
                "query": query,
                "update": update,
                "upsert": upsert,
            }
        )

    async def delete_many(self, query: dict[str, Any]) -> None:
        self.delete_calls.append({"query": query})


def _build_service() -> BusinessService:
    return BusinessService(
        scraper=object(),
        tripadvisor_scraper=object(),
        preprocessor=object(),
        llm_analyzer=object(),
        job_service=object(),
        query_service=object(),
    )


def test_upsert_job_comments_writes_and_cleans_stale_fingerprints() -> None:
    service = _build_service()
    collection = _FakeCommentsCollection()
    now = datetime.now(timezone.utc)
    reviews = [
        {
            "source": "tripadvisor",
            "review_id": "r1",
            "author_name": "Ana",
            "rating": 5.0,
            "relative_time": "hoy",
            "text": "Excelente",
            "owner_reply": "Gracias",
            "owner_reply_relative_time": "ayer",
            "relative_time_bucket": "0_3m",
            "has_text": True,
            "has_owner_reply": True,
        },
        {
            "source": "tripadvisor",
            "review_id": "r2",
            "author_name": "Luis",
            "rating": 1.0,
            "relative_time": "ayer",
            "text": "Malo",
            "owner_reply": "",
            "owner_reply_relative_time": "",
            "relative_time_bucket": "0_3m",
            "has_text": True,
            "has_owner_reply": False,
        },
    ]

    asyncio.run(
        service._upsert_job_comments(
            comments_collection=collection,
            business_id="b1",
            business_name="Godeo",
            name_normalized="godeo",
            source="tripadvisor",
            source_job_id="69b3fd7e56d914979227814a",
            processed_reviews=reviews,
            scraped_at=now,
            source_profile_id="sp1",
            dataset_id="ds1",
            scrape_run_id="sr1",
        )
    )

    assert len(collection.update_calls) == 2
    assert all(call["upsert"] is True for call in collection.update_calls)
    assert len(collection.delete_calls) == 1
    delete_query = collection.delete_calls[0]["query"]
    assert delete_query["source_job_id"] == "69b3fd7e56d914979227814a"
    assert delete_query["source"] == "tripadvisor"
    assert "$nin" in delete_query["review_fingerprint"]
    assert len(delete_query["review_fingerprint"]["$nin"]) == 2


def test_upsert_job_comments_skips_when_source_job_id_is_missing() -> None:
    service = _build_service()
    collection = _FakeCommentsCollection()
    now = datetime.now(timezone.utc)

    asyncio.run(
        service._upsert_job_comments(
            comments_collection=collection,
            business_id="b1",
            business_name="Godeo",
            name_normalized="godeo",
            source="tripadvisor",
            source_job_id=None,
            processed_reviews=[],
            scraped_at=now,
        )
    )

    assert collection.update_calls == []
    assert collection.delete_calls == []

