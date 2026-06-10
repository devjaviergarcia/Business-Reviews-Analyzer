from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from bson import ObjectId


class BusinessScrapeRunStore:
    def __init__(
        self,
        *,
        parse_object_id: Callable[..., ObjectId],
    ) -> None:
        self._parse_object_id = parse_object_id

    async def create_scrape_run(
        self,
        *,
        scrape_runs_collection: Any,
        business_id: str,
        source_profile_id: str,
        source: str,
        strategy: str,
        force: bool,
        force_mode: str,
        now: datetime,
    ) -> dict[str, Any]:
        scrape_run_doc = {
            "business_id": business_id,
            "source_profile_id": source_profile_id,
            "source": str(source or "google_maps").strip() or "google_maps",
            "strategy": str(strategy or "scroll_copy").strip() or "scroll_copy",
            "force": bool(force),
            "force_mode": str(force_mode or "fallback_existing").strip() or "fallback_existing",
            "status": "running",
            "metrics": {
                "scraped_review_count": 0,
                "processed_review_count": 0,
                "stored_review_count_before": 0,
                "stored_review_count_after": 0,
                "dataset_review_count": 0,
            },
            "started_at": now,
            "finished_at": None,
            "created_at": now,
            "updated_at": now,
        }
        insert_result = await scrape_runs_collection.insert_one(scrape_run_doc)
        scrape_run_doc["_id"] = insert_result.inserted_id
        return scrape_run_doc

    async def create_dataset_snapshot(
        self,
        *,
        datasets_collection: Any,
        business_id: str,
        source_profile_id: str,
        source: str,
        scrape_run_id: str,
        now: datetime,
    ) -> dict[str, Any]:
        dataset_doc = {
            "business_id": business_id,
            "source_profile_id": source_profile_id,
            "source": str(source or "google_maps").strip() or "google_maps",
            "kind": "scrape_snapshot",
            "status": "collecting",
            "scrape_run_id": scrape_run_id,
            "metrics": {
                "review_count": 0,
            },
            "created_at": now,
            "updated_at": now,
        }
        insert_result = await datasets_collection.insert_one(dataset_doc)
        dataset_doc["_id"] = insert_result.inserted_id
        return dataset_doc

    async def finalize_scrape_run(
        self,
        *,
        scrape_runs_collection: Any,
        scrape_run_id: str,
        now: datetime,
        status: str,
        metrics: dict[str, Any],
        dataset_id: str | None = None,
    ) -> None:
        scrape_run_object_id = self._parse_object_id(scrape_run_id, field_name="scrape_run_id")
        set_payload: dict[str, Any] = {
            "status": str(status or "done").strip() or "done",
            "updated_at": now,
            "finished_at": now,
        }
        if dataset_id:
            set_payload["dataset_id"] = str(dataset_id).strip()
        for key, value in metrics.items():
            set_payload[f"metrics.{key}"] = value
        await scrape_runs_collection.update_one(
            {"_id": scrape_run_object_id},
            {"$set": set_payload},
        )
