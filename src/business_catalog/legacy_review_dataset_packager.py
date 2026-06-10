from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from bson import ObjectId


class LegacyReviewDatasetPackager:
    def __init__(
        self,
        *,
        parse_object_id: Callable[..., ObjectId],
    ) -> None:
        self._parse_object_id = parse_object_id

    async def package_reviews_into_dataset(
        self,
        *,
        reviews_collection: Any,
        datasets_collection: Any,
        source_profiles_collection: Any,
        business_id: str,
        source_profile_id: str,
        source: str,
        now: datetime,
    ) -> dict[str, Any]:
        normalized_source = str(source or "google_maps").strip() or "google_maps"
        legacy_dataset_doc = await datasets_collection.find_one(
            {
                "business_id": business_id,
                "source_profile_id": source_profile_id,
                "source": normalized_source,
                "kind": "legacy_packaged",
            },
            sort=[("created_at", 1), ("_id", 1)],
        )
        if legacy_dataset_doc is not None:
            return {
                "dataset_id": str(legacy_dataset_doc["_id"]),
                "migrated_count": int((legacy_dataset_doc.get("metrics") or {}).get("review_count") or 0),
                "created": False,
            }

        source_filters: list[dict[str, Any]] = [{"source": normalized_source}]
        if normalized_source == "google_maps":
            source_filters.extend(
                [
                    {"source": {"$exists": False}},
                    {"source": None},
                    {"source": ""},
                ]
            )
        legacy_query = {
            "business_id": business_id,
            "$and": [
                {
                    "$or": [
                        {"dataset_id": {"$exists": False}},
                        {"dataset_id": None},
                        {"dataset_id": ""},
                    ],
                },
                {"$or": source_filters},
            ],
        }
        legacy_count = await reviews_collection.count_documents(legacy_query)
        if legacy_count <= 0:
            return {"dataset_id": None, "migrated_count": 0, "created": False}

        dataset_doc = {
            "business_id": business_id,
            "source_profile_id": source_profile_id,
            "source": normalized_source,
            "kind": "legacy_packaged",
            "status": "migrating",
            "scrape_run_id": None,
            "metrics": {
                "review_count": legacy_count,
            },
            "created_at": now,
            "updated_at": now,
        }
        insert_result = await datasets_collection.insert_one(dataset_doc)
        dataset_id = str(insert_result.inserted_id)

        await reviews_collection.update_many(
            legacy_query,
            {
                "$set": {
                    "dataset_id": dataset_id,
                    "source_profile_id": source_profile_id,
                    "updated_at": now,
                }
            },
        )
        migrated_count = await reviews_collection.count_documents(
            {
                "business_id": business_id,
                "dataset_id": dataset_id,
            }
        )
        await datasets_collection.update_one(
            {"_id": insert_result.inserted_id},
            {
                "$set": {
                    "status": "ready" if migrated_count > 0 else "empty",
                    "metrics.review_count": migrated_count,
                    "updated_at": now,
                }
            },
        )
        source_profile_object_id = self._parse_object_id(source_profile_id, field_name="source_profile_id")
        await source_profiles_collection.update_one(
            {
                "_id": source_profile_object_id,
                "$or": [
                    {"active_dataset_id": {"$exists": False}},
                    {"active_dataset_id": None},
                    {"active_dataset_id": ""},
                ],
            },
            {
                "$set": {
                    "active_dataset_id": dataset_id,
                    "metrics.active_review_count": migrated_count,
                    "updated_at": now,
                }
            },
        )
        return {"dataset_id": dataset_id, "migrated_count": migrated_count, "created": True}
