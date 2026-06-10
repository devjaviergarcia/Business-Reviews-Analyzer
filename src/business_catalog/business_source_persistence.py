from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from pymongo import ReturnDocument

from src.models.business import OwnerReply, Review


class BusinessSourcePersistence:
    def __init__(
        self,
        *,
        build_review_fingerprint: Callable[[dict[str, Any]], str],
    ) -> None:
        self._build_review_fingerprint = build_review_fingerprint

    async def upsert_reviews(
        self,
        *,
        reviews_collection: Any,
        business_id: str,
        processed_reviews: list[dict[str, Any]],
        scraped_at: datetime,
        source_profile_id: str | None = None,
        dataset_id: str | None = None,
        scrape_run_id: str | None = None,
    ) -> None:
        for item in processed_reviews:
            owner_reply_text = str(item.get("owner_reply", "") or "").strip()
            owner_reply_time = str(item.get("owner_reply_relative_time", "") or "").strip()
            owner_reply_author_name = str(item.get("owner_reply_author_name", "") or "").strip()
            owner_reply_written_date = str(item.get("owner_reply_written_date", "") or "").strip()
            owner_reply = (
                OwnerReply(text=owner_reply_text, relative_time=owner_reply_time)
                if owner_reply_text
                else None
            )

            rating_value = float(item.get("rating", 0.0))
            rating_value = max(0.0, min(5.0, rating_value))
            review_model = Review(
                business_id=business_id,
                source=str(item.get("source", "google_maps") or "google_maps"),
                author_name=str(item.get("author_name", "") or ""),
                rating=rating_value,
                relative_time=str(item.get("relative_time", "") or ""),
                text=str(item.get("text", "") or ""),
                owner_reply=owner_reply,
                has_text=bool(item.get("has_text")),
                has_owner_reply=bool(item.get("has_owner_reply")),
                relative_time_bucket=str(item.get("relative_time_bucket", "unknown") or "unknown"),
                scraped_at=scraped_at,
            )
            review_payload = review_model.model_dump(mode="python", exclude={"id"})
            review_payload["review_id"] = item.get("review_id")
            review_payload["updated_at"] = scraped_at
            review_payload["fingerprint"] = self._build_review_fingerprint(review_payload)
            review_payload["owner_reply_author_name"] = owner_reply_author_name
            review_payload["owner_reply_written_date"] = owner_reply_written_date
            raw_card_html = str(item.get("raw_card_html", "") or "").strip()
            if raw_card_html:
                review_payload["raw_card_html"] = raw_card_html[:50_000]
            if source_profile_id:
                review_payload["source_profile_id"] = source_profile_id
            if dataset_id:
                review_payload["dataset_id"] = dataset_id
            if scrape_run_id:
                review_payload["scrape_run_id"] = scrape_run_id

            if dataset_id:
                upsert_query = {
                    "business_id": business_id,
                    "dataset_id": dataset_id,
                    "fingerprint": review_payload["fingerprint"],
                }
            else:
                upsert_query = {
                    "business_id": business_id,
                    "fingerprint": review_payload["fingerprint"],
                }

            await reviews_collection.update_one(
                upsert_query,
                {
                    "$set": review_payload,
                    "$setOnInsert": {"created_at": scraped_at},
                },
                upsert=True,
            )

    async def upsert_job_comments(
        self,
        *,
        comments_collection: Any,
        business_id: str,
        business_name: str,
        name_normalized: str,
        source: str,
        source_job_id: str | None,
        processed_reviews: list[dict[str, Any]],
        scraped_at: datetime,
        source_profile_id: str | None = None,
        dataset_id: str | None = None,
        scrape_run_id: str | None = None,
    ) -> None:
        normalized_source_job_id = str(source_job_id or "").strip()
        if not normalized_source_job_id:
            return

        normalized_source = str(source or "").strip().lower() or "google_maps"
        keep_fingerprints: set[str] = set()
        for item in processed_reviews:
            owner_reply_text = str(item.get("owner_reply", "") or "").strip()
            owner_reply_relative_time = str(item.get("owner_reply_relative_time", "") or "").strip()
            owner_reply_author_name = str(item.get("owner_reply_author_name", "") or "").strip()
            owner_reply_written_date = str(item.get("owner_reply_written_date", "") or "").strip()
            rating_value = float(item.get("rating", 0.0))
            rating_value = max(0.0, min(5.0, rating_value))
            relative_time_value = str(item.get("relative_time", "") or "").strip()
            text_value = str(item.get("text", "") or "").strip()
            review_id = str(item.get("review_id") or "").strip() or None

            fingerprint_payload = {
                "business_id": business_id,
                "source": normalized_source,
                "review_id": review_id,
                "author_name": str(item.get("author_name", "") or "").strip(),
                "rating": rating_value,
                "relative_time": relative_time_value,
                "text": text_value,
            }
            review_fingerprint = self._build_review_fingerprint(fingerprint_payload)
            keep_fingerprints.add(review_fingerprint)

            comment_payload: dict[str, Any] = {
                "source_job_id": normalized_source_job_id,
                "business_id": business_id,
                "business_name": business_name,
                "name_normalized": name_normalized,
                "source": normalized_source,
                "review_fingerprint": review_fingerprint,
                "review_id": review_id,
                "author_name": str(item.get("author_name", "") or "").strip(),
                "rating": rating_value,
                "relative_time": relative_time_value,
                "relative_time_bucket": str(item.get("relative_time_bucket", "unknown") or "unknown"),
                "text": text_value,
                "owner_reply_text": owner_reply_text,
                "owner_reply_relative_time": owner_reply_relative_time,
                "owner_reply_author_name": owner_reply_author_name,
                "owner_reply_written_date": owner_reply_written_date,
                "raw_card_html": str(item.get("raw_card_html", "") or "").strip()[:50_000],
                "has_text": bool(item.get("has_text")),
                "has_owner_reply": bool(item.get("has_owner_reply")),
                "scraped_at": scraped_at,
                "updated_at": scraped_at,
            }
            if source_profile_id:
                comment_payload["source_profile_id"] = source_profile_id
            if dataset_id:
                comment_payload["dataset_id"] = dataset_id
            if scrape_run_id:
                comment_payload["scrape_run_id"] = scrape_run_id

            await comments_collection.update_one(
                {
                    "source_job_id": normalized_source_job_id,
                    "source": normalized_source,
                    "review_fingerprint": review_fingerprint,
                },
                {
                    "$set": comment_payload,
                    "$setOnInsert": {"created_at": scraped_at},
                },
                upsert=True,
            )

        cleanup_query: dict[str, Any] = {
            "source_job_id": normalized_source_job_id,
            "source": normalized_source,
        }
        if keep_fingerprints:
            cleanup_query["review_fingerprint"] = {"$nin": sorted(keep_fingerprints)}
        await comments_collection.delete_many(cleanup_query)

    async def get_or_create_source_profile(
        self,
        *,
        source_profiles_collection: Any,
        business_id: str,
        source: str,
        name_normalized: str,
        canonical_name_normalized: str,
        source_business_name: str,
        listing_payload: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        normalized_source = str(source or "google_maps").strip() or "google_maps"
        existing = await source_profiles_collection.find_one(
            {
                "business_id": business_id,
                "source": normalized_source,
            }
        )
        if existing is not None:
            updated = await source_profiles_collection.find_one_and_update(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "name_normalized": name_normalized,
                        "canonical_name_normalized": canonical_name_normalized,
                        "source_business_name": source_business_name,
                        "source_business_name_normalized": name_normalized,
                        "latest_listing": listing_payload,
                        "updated_at": now,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
            if updated is None:
                raise RuntimeError("Failed to update source profile.")
            return updated

        source_profile_doc = {
            "business_id": business_id,
            "source": normalized_source,
            "name_normalized": name_normalized,
            "canonical_name_normalized": canonical_name_normalized,
            "source_business_name": source_business_name,
            "source_business_name_normalized": name_normalized,
            "latest_listing": listing_payload,
            "active_dataset_id": None,
            "active_scrape_run_id": None,
            "metrics": {
                "total_runs": 0,
                "active_review_count": 0,
            },
            "created_at": now,
            "updated_at": now,
        }
        insert_result = await source_profiles_collection.insert_one(source_profile_doc)
        source_profile_doc["_id"] = insert_result.inserted_id
        return source_profile_doc
