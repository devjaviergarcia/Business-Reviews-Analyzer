from __future__ import annotations

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
    ) -> dict:
        page_value, page_size_value = self._coerce_pagination(
            page=page, page_size=page_size, max_page_size=100
        )

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]

        total = await businesses.count_documents({})
        skip = (page_value - 1) * page_size_value
        business_docs = (
            await businesses.find({})
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
        return self._sanitize_response_payload(payload)

    async def get_business_reviews(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        query: dict[str, Any] = {"business_id": business_id}
        total = await reviews.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await reviews.find(query)
            .sort([("_id", -1)])
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

    def _coerce_pagination(self, *, page: int, page_size: int, max_page_size: int) -> tuple[int, int]:
        return coerce_pagination(page=page, page_size=page_size, max_page_size=max_page_size)

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

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
