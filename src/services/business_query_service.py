from __future__ import annotations

import re
import unicodedata
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.services.query_validators import ensure_business_exists, parse_mongo_object_id


class BusinessQueryService:
    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _ANALYSES_COLLECTION = "analyses"
    _SOURCE_PROFILES_COLLECTION = "source_profiles"
    _DATASETS_COLLECTION = "datasets"
    _SCRAPE_RUNS_COLLECTION = "scrape_runs"

    async def get_business(self, business_id: str, include_listing: bool = True) -> dict:
        parsed_id = parse_mongo_object_id(business_id, field_name="business_id")
        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]

        business_doc = await businesses.find_one({"_id": parsed_id})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        review_count = await reviews.count_documents({"business_id": business_id})
        payload = self._serialize_business_doc(
            business_doc=business_doc,
            review_count=review_count,
            include_listing=include_listing,
        )
        return self._sanitize_response_payload(payload)

    async def list_businesses(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        include_listing: bool = False,
        name_query: str | None = None,
    ) -> dict:
        page_value, page_size_value = self._coerce_pagination(
            page=page, page_size=page_size, max_page_size=100
        )

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]

        query = self._build_business_name_query(name_query)
        total = await businesses.count_documents(query)
        skip = (page_value - 1) * page_size_value
        business_docs = (
            await businesses.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        latest_analysis_ids: list[ObjectId] = []
        for business_doc in business_docs:
            latest_analysis_id = business_doc.get("latest_analysis_id")
            if not latest_analysis_id:
                continue
            try:
                latest_analysis_ids.append(ObjectId(str(latest_analysis_id)))
            except (InvalidId, TypeError):
                continue

        analysis_map_by_id: dict[str, dict[str, Any]] = {}
        if latest_analysis_ids:
            analysis_docs = await analyses.find({"_id": {"$in": latest_analysis_ids}}).to_list(length=len(latest_analysis_ids))
            analysis_map_by_id = {str(doc["_id"]): doc for doc in analysis_docs}

        items = [
            self._serialize_business_summary_doc(
                business_doc=business_doc,
                latest_analysis=analysis_map_by_id.get(str(business_doc.get("latest_analysis_id", ""))),
                include_listing=include_listing,
            )
            for business_doc in business_docs
        ]
        payload = build_pagination_payload(
            items=items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["name_query"] = str(name_query or "").strip() or None
        return self._sanitize_response_payload(payload)

    async def get_business_reviews(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
        rating_gte: float | None = None,
        rating_lte: float | None = None,
        order: str = "desc",
    ) -> dict:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        min_rating, max_rating = self._normalize_rating_bounds(rating_gte=rating_gte, rating_lte=rating_lte)
        sort_direction = self._normalize_rating_order(order)

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        query: dict[str, Any] = {"business_id": business_id}
        rating_filter = self._build_rating_filter(min_rating=min_rating, max_rating=max_rating)
        if rating_filter:
            query["rating"] = rating_filter
        total = await reviews.count_documents(query)
        skip = (page_value - 1) * page_size_value
        sort_by = [("rating", sort_direction), ("_id", -1)]
        docs = (
            await reviews.find(query)
            .sort(sort_by)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        payload = build_pagination_payload(
            items=[self._serialize_review_doc(doc) for doc in docs],
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["business_id"] = business_id
        payload["rating_gte"] = min_rating
        payload["rating_lte"] = max_rating
        payload["order"] = "desc" if sort_direction == -1 else "asc"
        return self._sanitize_response_payload(payload)

    async def get_business_analysis(self, business_id: str) -> dict:
        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        analysis_doc = await analyses.find_one({"business_id": business_id}, sort=[("created_at", -1)])
        if analysis_doc is None:
            raise LookupError(f"Analysis for business '{business_id}' not found.")

        payload = self._serialize_analysis_doc(analysis_doc)
        return self._sanitize_response_payload(payload)

    async def list_business_analyses(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        query = {"business_id": business_id}
        total = await analyses.count_documents(query)
        skip = (page_value - 1) * page_size_value
        analysis_docs = (
            await analyses.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        payload = build_pagination_payload(
            items=[self._serialize_analysis_doc(doc) for doc in analysis_docs],
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["business_id"] = business_id
        return self._sanitize_response_payload(payload)

    async def list_business_snapshots(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
        source: str | None = None,
        kind: str | None = None,
        include_empty: bool = True,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        datasets = database[self._DATASETS_COLLECTION]
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        scrape_runs = database[self._SCRAPE_RUNS_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        kind_value = str(kind or "").strip().lower() or None
        if kind_value is not None and kind_value not in {"legacy_packaged", "scrape_snapshot"}:
            raise ValueError("Invalid kind. Allowed values: legacy_packaged, scrape_snapshot.")

        source_value = str(source or "").strip().lower() or None
        query: dict[str, Any] = {"business_id": business_id}
        if source_value:
            query["source"] = source_value
        if kind_value:
            query["kind"] = kind_value
        if not include_empty:
            query["status"] = {"$ne": "empty"}

        total = await datasets.count_documents(query)
        skip = (page_value - 1) * page_size_value
        dataset_docs = (
            await datasets.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        source_profile_object_ids: list[ObjectId] = []
        scrape_run_object_ids: list[ObjectId] = []
        for dataset_doc in dataset_docs:
            source_profile_id = dataset_doc.get("source_profile_id")
            scrape_run_id = dataset_doc.get("scrape_run_id")
            if isinstance(source_profile_id, str):
                try:
                    source_profile_object_ids.append(ObjectId(source_profile_id))
                except (InvalidId, TypeError):
                    pass
            if isinstance(scrape_run_id, str):
                try:
                    scrape_run_object_ids.append(ObjectId(scrape_run_id))
                except (InvalidId, TypeError):
                    pass

        source_profile_docs_all = await source_profiles.find({"business_id": business_id}).to_list(length=None)
        source_profile_docs_by_id: dict[str, dict[str, Any]] = {
            str(doc["_id"]): doc for doc in source_profile_docs_all
        }

        if source_profile_object_ids:
            source_profile_docs = await source_profiles.find({"_id": {"$in": source_profile_object_ids}}).to_list(
                length=len(source_profile_object_ids)
            )
            for doc in source_profile_docs:
                source_profile_docs_by_id[str(doc["_id"])] = doc

        scrape_runs_by_id: dict[str, dict[str, Any]] = {}
        if scrape_run_object_ids:
            scrape_run_docs = await scrape_runs.find({"_id": {"$in": scrape_run_object_ids}}).to_list(
                length=len(scrape_run_object_ids)
            )
            scrape_runs_by_id = {str(doc["_id"]): doc for doc in scrape_run_docs}

        items = [
            self._serialize_dataset_snapshot_doc(
                dataset_doc=dataset_doc,
                source_profile_doc=source_profile_docs_by_id.get(str(dataset_doc.get("source_profile_id", ""))),
                scrape_run_doc=scrape_runs_by_id.get(str(dataset_doc.get("scrape_run_id", ""))),
            )
            for dataset_doc in dataset_docs
        ]
        payload = build_pagination_payload(
            items=items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["business_id"] = business_id
        payload["source"] = source_value
        payload["kind"] = kind_value
        payload["include_empty"] = bool(include_empty)
        payload["source_profiles"] = [
            self._serialize_source_profile_doc(doc) for doc in source_profile_docs_all
        ]
        payload["total_scrape_runs"] = await scrape_runs.count_documents({"business_id": business_id})
        return self._sanitize_response_payload(payload)

    def _coerce_pagination(self, *, page: int, page_size: int, max_page_size: int) -> tuple[int, int]:
        return coerce_pagination(page=page, page_size=page_size, max_page_size=max_page_size)

    def _build_business_name_query(self, name_query: str | None) -> dict[str, Any]:
        raw = re.sub(r"\s+", " ", str(name_query or "")).strip()
        if not raw:
            return {}

        escaped_raw = re.escape(raw)
        normalized = self._normalize_text(raw)
        escaped_normalized = re.escape(normalized)

        or_clauses: list[dict[str, Any]] = [
            {"name": {"$regex": escaped_raw, "$options": "i"}},
        ]
        if escaped_normalized:
            or_clauses.append({"name_normalized": {"$regex": escaped_normalized, "$options": "i"}})
        return {"$or": or_clauses}

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _normalize_rating_bounds(
        self,
        *,
        rating_gte: float | None,
        rating_lte: float | None,
    ) -> tuple[float | None, float | None]:
        min_rating = self._coerce_rating_value(rating_gte, field_name="rating_gte")
        max_rating = self._coerce_rating_value(rating_lte, field_name="rating_lte")
        if min_rating is not None and max_rating is not None and min_rating > max_rating:
            raise ValueError("Invalid rating bounds: rating_gte cannot be greater than rating_lte.")
        return min_rating, max_rating

    def _coerce_rating_value(self, value: float | None, *, field_name: str) -> float | None:
        if value is None:
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a number between 0 and 5.") from exc
        if parsed < 0.0 or parsed > 5.0:
            raise ValueError(f"{field_name} must be between 0 and 5.")
        return parsed

    def _build_rating_filter(self, *, min_rating: float | None, max_rating: float | None) -> dict[str, Any]:
        rating_filter: dict[str, Any] = {}
        if min_rating is not None:
            rating_filter["$gte"] = min_rating
        if max_rating is not None:
            rating_filter["$lte"] = max_rating
        return rating_filter

    def _normalize_rating_order(self, order: str | None) -> int:
        normalized = str(order or "").strip().lower()
        if normalized in {"", "desc", "descending", "mayor", "highest", "high"}:
            return -1
        if normalized in {"asc", "ascending", "menor", "lowest", "low"}:
            return 1
        raise ValueError("Invalid order. Allowed values: desc, asc.")

    def _serialize_business_doc(self, *, business_doc: dict[str, Any], review_count: int, include_listing: bool) -> dict:
        payload = {
            "business_id": str(business_doc.get("_id")),
            "name": business_doc.get("name", ""),
            "name_normalized": business_doc.get("name_normalized", ""),
            "source": business_doc.get("source", "google_maps"),
            "stats": business_doc.get("stats", {}),
            "review_count": review_count,
            "last_scraped_at": business_doc.get("last_scraped_at"),
            "created_at": business_doc.get("created_at"),
            "updated_at": business_doc.get("updated_at"),
            "latest_analysis_id": business_doc.get("latest_analysis_id"),
        }
        if include_listing:
            payload["listing"] = business_doc.get("listing")
        return payload

    def _serialize_business_summary_doc(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        include_listing: bool,
    ) -> dict[str, Any]:
        listing_raw = business_doc.get("listing")
        listing = listing_raw if isinstance(listing_raw, dict) else {}

        categories_raw = listing.get("categories")
        categories = [str(item).strip() for item in categories_raw] if isinstance(categories_raw, list) else []

        review_count_raw = business_doc.get("review_count", 0)
        try:
            review_count = max(0, int(review_count_raw))
        except (TypeError, ValueError):
            review_count = 0

        payload = {
            "business_id": str(business_doc.get("_id")),
            "name": str(business_doc.get("name", "") or ""),
            "description": self._build_business_description(
                business_doc=business_doc,
                latest_analysis=latest_analysis,
                categories=categories,
            ),
            "source": business_doc.get("source", "google_maps"),
            "review_count": review_count,
            "address": listing.get("address"),
            "phone": listing.get("phone"),
            "website": listing.get("website"),
            "overall_rating": listing.get("overall_rating"),
            "total_reviews": listing.get("total_reviews"),
            "categories": categories,
            "last_scraped_at": business_doc.get("last_scraped_at"),
            "created_at": business_doc.get("created_at"),
            "updated_at": business_doc.get("updated_at"),
            "latest_analysis_id": business_doc.get("latest_analysis_id"),
        }
        if include_listing:
            payload["listing"] = listing
        return payload

    def _build_business_description(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        categories: list[str],
    ) -> str:
        if latest_analysis:
            sentiment = str(latest_analysis.get("overall_sentiment", "") or "").strip()
            topics_raw = latest_analysis.get("main_topics")
            topics = [str(item).strip() for item in topics_raw] if isinstance(topics_raw, list) else []
            topics = [item for item in topics if item][:3]
            if sentiment and topics:
                return f"Sentiment: {sentiment}. Main topics: {', '.join(topics)}."
            if sentiment:
                return f"Sentiment: {sentiment}."
            if topics:
                return f"Main topics: {', '.join(topics)}."

        filtered_categories = [item for item in categories if item][:3]
        if filtered_categories:
            return f"Categories: {', '.join(filtered_categories)}."

        business_name = str(business_doc.get("name", "") or "").strip()
        if business_name:
            return f"Google Maps business profile for {business_name}."
        return "Google Maps business profile."

    def _serialize_review_doc(self, review_doc: dict[str, Any]) -> dict:
        payload = dict(review_doc)
        payload["id"] = str(payload.pop("_id"))
        payload.pop("fingerprint", None)
        return payload

    def _serialize_analysis_doc(self, analysis_doc: dict[str, Any]) -> dict:
        payload = dict(analysis_doc)
        payload["id"] = str(payload.pop("_id"))
        return payload

    def _serialize_source_profile_doc(self, source_profile_doc: dict[str, Any]) -> dict[str, Any]:
        metrics_raw = source_profile_doc.get("metrics")
        metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
        payload = {
            "id": str(source_profile_doc.get("_id")),
            "business_id": str(source_profile_doc.get("business_id", "") or ""),
            "source": str(source_profile_doc.get("source", "") or ""),
            "name_normalized": str(source_profile_doc.get("name_normalized", "") or ""),
            "active_dataset_id": source_profile_doc.get("active_dataset_id"),
            "active_scrape_run_id": source_profile_doc.get("active_scrape_run_id"),
            "metrics": metrics,
            "created_at": source_profile_doc.get("created_at"),
            "updated_at": source_profile_doc.get("updated_at"),
        }
        return payload

    def _serialize_dataset_snapshot_doc(
        self,
        *,
        dataset_doc: dict[str, Any],
        source_profile_doc: dict[str, Any] | None,
        scrape_run_doc: dict[str, Any] | None,
    ) -> dict[str, Any]:
        metrics_raw = dataset_doc.get("metrics")
        metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
        dataset_id = str(dataset_doc.get("_id"))
        active_dataset_id = str((source_profile_doc or {}).get("active_dataset_id") or "").strip() or None

        payload: dict[str, Any] = {
            "id": dataset_id,
            "business_id": str(dataset_doc.get("business_id", "") or ""),
            "source": str(dataset_doc.get("source", "") or ""),
            "kind": str(dataset_doc.get("kind", "") or ""),
            "status": str(dataset_doc.get("status", "") or ""),
            "source_profile_id": dataset_doc.get("source_profile_id"),
            "scrape_run_id": dataset_doc.get("scrape_run_id"),
            "metrics": metrics,
            "review_count": int(metrics.get("review_count") or 0),
            "is_active": bool(active_dataset_id and active_dataset_id == dataset_id),
            "created_at": dataset_doc.get("created_at"),
            "updated_at": dataset_doc.get("updated_at"),
        }
        if source_profile_doc is not None:
            payload["source_profile"] = {
                "id": str(source_profile_doc.get("_id")),
                "source": str(source_profile_doc.get("source", "") or ""),
                "active_dataset_id": source_profile_doc.get("active_dataset_id"),
                "active_scrape_run_id": source_profile_doc.get("active_scrape_run_id"),
            }
        if scrape_run_doc is not None:
            run_metrics_raw = scrape_run_doc.get("metrics")
            run_metrics = run_metrics_raw if isinstance(run_metrics_raw, dict) else {}
            payload["scrape_run"] = {
                "id": str(scrape_run_doc.get("_id")),
                "strategy": scrape_run_doc.get("strategy"),
                "status": scrape_run_doc.get("status"),
                "force": scrape_run_doc.get("force"),
                "metrics": run_metrics,
                "started_at": scrape_run_doc.get("started_at"),
                "finished_at": scrape_run_doc.get("finished_at"),
            }
        return payload

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
