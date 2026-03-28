from __future__ import annotations

import math
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
    _COMMENTS_COLLECTION = "comments"
    _ANALYSES_COLLECTION = "analyses"
    _SOURCE_PROFILES_COLLECTION = "source_profiles"
    _DATASETS_COLLECTION = "datasets"
    _SCRAPE_RUNS_COLLECTION = "scrape_runs"
    _JOBS_COLLECTION = "analysis_jobs"
    _SOURCE_ORDER = ("google_maps", "tripadvisor")
    _SCRAPE_QUEUE_TO_SOURCE = {
        "scrape_google_maps": "google_maps",
        "scrape_tripadvisor": "tripadvisor",
    }

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
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        comments = database[self._COMMENTS_COLLECTION]
        jobs = database[self._JOBS_COLLECTION]

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

        sources_available_by_business_id = await self._collect_sources_available_by_business_id(
            business_docs=business_docs,
            source_profiles=source_profiles,
            comments=comments,
            jobs=jobs,
        )

        items = [
            self._serialize_business_summary_doc(
                business_doc=business_doc,
                latest_analysis=analysis_map_by_id.get(str(business_doc.get("latest_analysis_id", ""))),
                include_listing=include_listing,
                sources_available=sources_available_by_business_id.get(str(business_doc.get("_id")), []),
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
        order: str = "desc-rating",
    ) -> dict:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        min_rating, max_rating = self._normalize_rating_bounds(rating_gte=rating_gte, rating_lte=rating_lte)
        normalized_order, sort_by = self._normalize_reviews_order(order)

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
        payload["order"] = normalized_order
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

    async def get_business_report(self, business_id: str) -> dict:
        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]

        await ensure_business_exists(businesses_collection=businesses, business_id=business_id)

        analysis_doc = await analyses.find_one({"business_id": business_id}, sort=[("created_at", -1)])
        if analysis_doc is None:
            raise LookupError(f"Analysis for business '{business_id}' not found.")

        report_payload = analysis_doc.get("advanced_report")
        if not isinstance(report_payload, dict):
            raise LookupError(
                f"Structured report for business '{business_id}' is not available yet."
            )

        payload = {
            "business_id": business_id,
            "analysis_id": str(analysis_doc.get("_id")),
            "analysis_created_at": analysis_doc.get("created_at"),
            "report_generated_at": analysis_doc.get("report_generated_at"),
            "report_intro_context": analysis_doc.get("report_intro_context"),
            "report_artifacts": analysis_doc.get("report_artifacts"),
            "report": report_payload,
            "preview_report_generated_at": analysis_doc.get("preview_report_generated_at"),
            "preview_report_artifacts": analysis_doc.get("preview_report_artifacts"),
            "preview_report": analysis_doc.get("preview_report"),
        }
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

    async def list_job_comments(
        self,
        *,
        job_id: str,
        source: str | None = None,
        scrape_type: str | None = None,
        page: int = 1,
        page_size: int = 50,
        rating_gte: float | None = None,
        rating_lte: float | None = None,
        order: str = "desc-date",
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        parsed_job_id = parse_mongo_object_id(job_id, field_name="job_id")
        normalized_job_id = str(parsed_job_id)
        min_rating, max_rating = self._normalize_rating_bounds(rating_gte=rating_gte, rating_lte=rating_lte)
        normalized_order, sort_by = self._normalize_comments_order(order)

        source_value = self._normalize_source(source)
        scrape_type_value = self._normalize_source(scrape_type)
        if source_value and scrape_type_value and source_value != scrape_type_value:
            raise ValueError("source and scrape_type cannot conflict.")
        if source_value is None and scrape_type_value is not None:
            source_value = scrape_type_value
        if source and source_value is None:
            raise ValueError("Invalid source. Allowed values: google_maps, tripadvisor.")
        if scrape_type and scrape_type_value is None:
            raise ValueError("Invalid scrape_type. Allowed values: google_maps, tripadvisor.")

        database = get_database()
        comments = database[self._COMMENTS_COLLECTION]
        query: dict[str, Any] = {"source_job_id": normalized_job_id}
        if source_value is not None:
            query["source"] = source_value
        rating_filter = self._build_rating_filter(min_rating=min_rating, max_rating=max_rating)
        if rating_filter:
            query["rating"] = rating_filter

        total = await comments.count_documents(query)
        skip = (page_value - 1) * page_size_value
        if source_value is not None:
            effective_sort_by = list(sort_by)
        else:
            effective_sort_by = [("source", 1), *sort_by]
        docs = (
            await comments.find(query)
            .sort(effective_sort_by)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        source_counts_match: dict[str, Any] = {"source_job_id": normalized_job_id}
        if rating_filter:
            source_counts_match["rating"] = rating_filter
        source_counts_raw = await comments.aggregate(
            [
                {"$match": source_counts_match},
                {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            ]
        ).to_list(length=10)
        source_counts = {
            str(item.get("_id") or "").strip() or "unknown": int(item.get("count") or 0)
            for item in source_counts_raw
            if isinstance(item, dict)
        }
        source_pagination_all = {
            source_name: {
                "total": int(count),
                "page_size": page_size_value,
                "total_pages": int(math.ceil(int(count) / page_size_value)) if int(count) > 0 else 0,
            }
            for source_name, count in source_counts.items()
        }
        source_pagination = (
            {source_value: source_pagination_all.get(source_value, {"total": 0, "page_size": page_size_value, "total_pages": 0})}
            if source_value
            else source_pagination_all
        )
        serialized_items = [self._serialize_comment_doc(doc) for doc in docs]
        items_by_source = self._group_comments_by_source(serialized_items)

        payload = build_pagination_payload(
            items=serialized_items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["job_id"] = normalized_job_id
        payload["source"] = source_value
        payload["scrape_type"] = source_value
        payload["pagination_scope"] = "source" if source_value else "all_sources"
        payload["rating_gte"] = min_rating
        payload["rating_lte"] = max_rating
        payload["order"] = normalized_order
        payload["source_counts"] = source_counts
        payload["source_pagination"] = source_pagination
        payload["items_by_source_page"] = items_by_source
        payload["available_sources"] = self._sort_sources(set(source_counts.keys()))
        payload["total_comments"] = int(sum(source_counts.values()))
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

    async def get_business_sources_overview(
        self,
        *,
        business_id: str,
        comments_preview_size: int = 5,
    ) -> dict[str, Any]:
        parsed_business_id = parse_mongo_object_id(business_id, field_name="business_id")
        normalized_business_id = str(parsed_business_id)
        preview_size = max(1, min(int(comments_preview_size), 20))

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        datasets = database[self._DATASETS_COLLECTION]
        scrape_runs = database[self._SCRAPE_RUNS_COLLECTION]
        comments = database[self._COMMENTS_COLLECTION]
        jobs = database[self._JOBS_COLLECTION]

        business_doc = await businesses.find_one({"_id": parsed_business_id})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        canonical_name_normalized = str(business_doc.get("name_normalized") or "").strip()
        source_profile_docs = (
            await source_profiles.find({"business_id": normalized_business_id})
            .sort([("source", 1), ("updated_at", -1), ("_id", -1)])
            .to_list(length=None)
        )

        comments_counts_raw = await comments.aggregate(
            [
                {"$match": {"business_id": normalized_business_id}},
                {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            ]
        ).to_list(length=10)
        comments_counts: dict[str, int] = {}
        for item in comments_counts_raw:
            if not isinstance(item, dict):
                continue
            normalized_source = self._normalize_source(item.get("_id"))
            if normalized_source is None:
                continue
            comments_counts[normalized_source] = comments_counts.get(normalized_source, 0) + int(item.get("count") or 0)

        latest_jobs_projection = {
            "_id": 1,
            "status": 1,
            "queue_name": 1,
            "job_type": 1,
            "attempts": 1,
            "error": 1,
            "progress": 1,
            "created_at": 1,
            "updated_at": 1,
            "started_at": 1,
            "finished_at": 1,
        }
        latest_jobs_docs = (
            await jobs.find(
                {
                    "queue_name": {"$in": list(self._SCRAPE_QUEUE_TO_SOURCE.keys())},
                    "$or": [
                        {"root_business_id": normalized_business_id},
                        {"payload.root_business_id": normalized_business_id},
                        {"canonical_name_normalized": canonical_name_normalized},
                        {"payload.canonical_name_normalized": canonical_name_normalized},
                        {"name_normalized": canonical_name_normalized},
                    ],
                },
                projection=latest_jobs_projection,
            )
            .sort([("updated_at", -1), ("_id", -1)])
            .to_list(length=50)
        )
        latest_job_by_source: dict[str, dict[str, Any]] = {}
        for job_doc in latest_jobs_docs:
            source_from_queue = self._source_from_queue_name(job_doc.get("queue_name"))
            if source_from_queue is None or source_from_queue in latest_job_by_source:
                continue
            latest_job_by_source[source_from_queue] = job_doc

        source_profile_by_source: dict[str, dict[str, Any]] = {}
        for source_profile_doc in source_profile_docs:
            normalized_source = self._normalize_source(source_profile_doc.get("source"))
            if normalized_source is None or normalized_source in source_profile_by_source:
                continue
            source_profile_by_source[normalized_source] = source_profile_doc

        available_sources_set: set[str] = set(source_profile_by_source.keys())
        available_sources_set.update(self._normalize_source(source) for source in comments_counts.keys())
        available_sources_set.update(latest_job_by_source.keys())
        available_sources_set.discard(None)

        fallback_source = self._normalize_source(business_doc.get("source"))
        if fallback_source and not available_sources_set:
            available_sources_set.add(fallback_source)

        available_sources = self._sort_sources(available_sources_set)
        source_items: list[dict[str, Any]] = []
        for source_value in available_sources:
            source_profile_doc = source_profile_by_source.get(source_value)
            latest_job_doc = latest_job_by_source.get(source_value)

            active_dataset_doc: dict[str, Any] | None = None
            active_scrape_run_doc: dict[str, Any] | None = None
            if source_profile_doc is not None:
                active_dataset_id = str(source_profile_doc.get("active_dataset_id") or "").strip()
                if active_dataset_id:
                    try:
                        active_dataset_doc = await datasets.find_one({"_id": ObjectId(active_dataset_id)})
                    except (InvalidId, TypeError):
                        active_dataset_doc = None

                active_scrape_run_id = str(source_profile_doc.get("active_scrape_run_id") or "").strip()
                if active_scrape_run_id:
                    try:
                        active_scrape_run_doc = await scrape_runs.find_one({"_id": ObjectId(active_scrape_run_id)})
                    except (InvalidId, TypeError):
                        active_scrape_run_doc = None

            latest_comments_docs = (
                await comments.find(
                    {
                        "business_id": normalized_business_id,
                        "source": source_value,
                    }
                )
                .sort([("scraped_at", -1), ("updated_at", -1), ("_id", -1)])
                .limit(preview_size)
                .to_list(length=preview_size)
            )
            source_items.append(
                {
                    "source": source_value,
                    "source_profile": (
                        self._serialize_source_profile_doc(source_profile_doc)
                        if isinstance(source_profile_doc, dict)
                        else None
                    ),
                    "latest_job": self._serialize_job_compact_doc(latest_job_doc) if latest_job_doc else None,
                    "active_dataset": (
                        self._serialize_dataset_snapshot_doc(
                            dataset_doc=active_dataset_doc,
                            source_profile_doc=source_profile_doc,
                            scrape_run_doc=active_scrape_run_doc,
                        )
                        if isinstance(active_dataset_doc, dict) and isinstance(source_profile_doc, dict)
                        else None
                    ),
                    "comments_count": int(comments_counts.get(source_value, 0)),
                    "latest_comments": [self._serialize_comment_doc(doc) for doc in latest_comments_docs],
                }
            )

        return self._sanitize_response_payload(
            {
                "business_id": normalized_business_id,
                "name": str(business_doc.get("name") or ""),
                "name_normalized": canonical_name_normalized or None,
                "available_sources": available_sources,
                "sources": source_items,
                "source_counts": comments_counts,
                "total_comments": int(sum(comments_counts.values())),
            }
        )

    async def list_business_comments(
        self,
        *,
        business_id: str,
        source: str | None = None,
        scrape_type: str | None = None,
        page: int = 1,
        page_size: int = 50,
        rating_gte: float | None = None,
        rating_lte: float | None = None,
        order: str = "desc-date",
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        parsed_business_id = parse_mongo_object_id(business_id, field_name="business_id")
        normalized_business_id = str(parsed_business_id)
        min_rating, max_rating = self._normalize_rating_bounds(rating_gte=rating_gte, rating_lte=rating_lte)
        normalized_order, sort_by = self._normalize_comments_order(order)

        source_value = self._normalize_source(source)
        scrape_type_value = self._normalize_source(scrape_type)
        if source_value and scrape_type_value and source_value != scrape_type_value:
            raise ValueError("source and scrape_type cannot conflict.")
        if source_value is None and scrape_type_value is not None:
            source_value = scrape_type_value
        if source and source_value is None:
            raise ValueError("Invalid source. Allowed values: google_maps, tripadvisor.")
        if scrape_type and scrape_type_value is None:
            raise ValueError("Invalid scrape_type. Allowed values: google_maps, tripadvisor.")

        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        comments = database[self._COMMENTS_COLLECTION]
        await ensure_business_exists(businesses_collection=businesses, business_id=normalized_business_id)

        query: dict[str, Any] = {"business_id": normalized_business_id}
        if source_value is not None:
            query["source"] = source_value
        rating_filter = self._build_rating_filter(min_rating=min_rating, max_rating=max_rating)
        if rating_filter:
            query["rating"] = rating_filter

        total = await comments.count_documents(query)
        skip = (page_value - 1) * page_size_value
        if source_value is not None:
            effective_sort_by = list(sort_by)
        else:
            effective_sort_by = [("source", 1), *sort_by]
        docs = (
            await comments.find(query)
            .sort(effective_sort_by)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        source_counts_match: dict[str, Any] = {"business_id": normalized_business_id}
        if rating_filter:
            source_counts_match["rating"] = rating_filter
        source_counts_raw = await comments.aggregate(
            [
                {"$match": source_counts_match},
                {"$group": {"_id": "$source", "count": {"$sum": 1}}},
            ]
        ).to_list(length=10)
        source_counts = {
            str(item.get("_id") or "").strip() or "unknown": int(item.get("count") or 0)
            for item in source_counts_raw
            if isinstance(item, dict)
        }
        source_pagination_all = {
            source_name: {
                "total": int(count),
                "page_size": page_size_value,
                "total_pages": int(math.ceil(int(count) / page_size_value)) if int(count) > 0 else 0,
            }
            for source_name, count in source_counts.items()
        }
        source_pagination = (
            {source_value: source_pagination_all.get(source_value, {"total": 0, "page_size": page_size_value, "total_pages": 0})}
            if source_value
            else source_pagination_all
        )
        serialized_items = [self._serialize_comment_doc(doc) for doc in docs]
        items_by_source = self._group_comments_by_source(serialized_items)

        payload = build_pagination_payload(
            items=serialized_items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        payload["business_id"] = normalized_business_id
        payload["source"] = source_value
        payload["scrape_type"] = source_value
        payload["pagination_scope"] = "source" if source_value else "all_sources"
        payload["rating_gte"] = min_rating
        payload["rating_lte"] = max_rating
        payload["order"] = normalized_order
        payload["source_counts"] = source_counts
        payload["source_pagination"] = source_pagination
        payload["items_by_source_page"] = items_by_source
        payload["available_sources"] = self._sort_sources(set(source_counts.keys()))
        payload["total_comments"] = int(sum(source_counts.values()))
        return self._sanitize_response_payload(payload)

    async def _collect_sources_available_by_business_id(
        self,
        *,
        business_docs: list[dict[str, Any]],
        source_profiles: Any,
        comments: Any,
        jobs: Any,
    ) -> dict[str, list[str]]:
        business_ids = [str(doc.get("_id")) for doc in business_docs if doc.get("_id") is not None]
        if not business_ids:
            return {}

        sources_by_business_id: dict[str, set[str]] = {business_id: set() for business_id in business_ids}

        source_profile_docs = await source_profiles.find(
            {"business_id": {"$in": business_ids}},
            projection={"business_id": 1, "source": 1},
        ).to_list(length=None)
        for source_profile_doc in source_profile_docs:
            business_id = str(source_profile_doc.get("business_id") or "").strip()
            source_value = self._normalize_source(source_profile_doc.get("source"))
            if business_id in sources_by_business_id and source_value:
                sources_by_business_id[business_id].add(source_value)

        comments_sources_raw = await comments.aggregate(
            [
                {"$match": {"business_id": {"$in": business_ids}}},
                {"$group": {"_id": {"business_id": "$business_id", "source": "$source"}}},
            ]
        ).to_list(length=None)
        for item in comments_sources_raw:
            if not isinstance(item, dict):
                continue
            source_ref = item.get("_id")
            if not isinstance(source_ref, dict):
                continue
            business_id = str(source_ref.get("business_id") or "").strip()
            source_value = self._normalize_source(source_ref.get("source"))
            if business_id in sources_by_business_id and source_value:
                sources_by_business_id[business_id].add(source_value)

        job_projection = {
            "queue_name": 1,
            "root_business_id": 1,
            "payload.root_business_id": 1,
            "canonical_name_normalized": 1,
            "payload.canonical_name_normalized": 1,
            "name_normalized": 1,
        }
        job_docs = await jobs.find(
            {
                "queue_name": {"$in": list(self._SCRAPE_QUEUE_TO_SOURCE.keys())},
                "$or": [
                    {"root_business_id": {"$in": business_ids}},
                    {"payload.root_business_id": {"$in": business_ids}},
                ],
            },
            projection=job_projection,
        ).to_list(length=None)
        for job_doc in job_docs:
            source_value = self._source_from_queue_name(job_doc.get("queue_name"))
            if source_value is None:
                continue
            root_business_id = str(job_doc.get("root_business_id") or "").strip()
            payload = job_doc.get("payload") if isinstance(job_doc.get("payload"), dict) else {}
            payload_business_id = str(payload.get("root_business_id") or "").strip()
            if root_business_id in sources_by_business_id:
                sources_by_business_id[root_business_id].add(source_value)
            if payload_business_id in sources_by_business_id:
                sources_by_business_id[payload_business_id].add(source_value)

        business_ids_by_name_normalized: dict[str, set[str]] = {}
        for business_doc in business_docs:
            business_id = str(business_doc.get("_id") or "").strip()
            normalized_name = str(business_doc.get("name_normalized") or "").strip()
            if not business_id or not normalized_name:
                continue
            bucket = business_ids_by_name_normalized.setdefault(normalized_name, set())
            bucket.add(business_id)

        if business_ids_by_name_normalized:
            normalized_names = list(business_ids_by_name_normalized.keys())
            job_docs_by_name = await jobs.find(
                {
                    "queue_name": {"$in": list(self._SCRAPE_QUEUE_TO_SOURCE.keys())},
                    "$or": [
                        {"canonical_name_normalized": {"$in": normalized_names}},
                        {"payload.canonical_name_normalized": {"$in": normalized_names}},
                        {"name_normalized": {"$in": normalized_names}},
                    ],
                },
                projection=job_projection,
            ).to_list(length=None)
            for job_doc in job_docs_by_name:
                source_value = self._source_from_queue_name(job_doc.get("queue_name"))
                if source_value is None:
                    continue

                payload = job_doc.get("payload") if isinstance(job_doc.get("payload"), dict) else {}
                job_names = {
                    str(job_doc.get("canonical_name_normalized") or "").strip(),
                    str(job_doc.get("name_normalized") or "").strip(),
                    str(payload.get("canonical_name_normalized") or "").strip(),
                }
                for normalized_name in job_names:
                    if not normalized_name:
                        continue
                    business_ids_for_name = business_ids_by_name_normalized.get(normalized_name, set())
                    for business_id in business_ids_for_name:
                        sources_by_business_id[business_id].add(source_value)

        return {
            business_id: self._sort_sources(sources)
            for business_id, sources in sources_by_business_id.items()
        }

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

    def _normalize_source(self, value: Any) -> str | None:
        normalized = str(value or "").strip().lower()
        if normalized in {"google_maps", "googlemaps", "google"}:
            return "google_maps"
        if normalized in {"tripadvisor", "trip_advisor"}:
            return "tripadvisor"
        return None

    def _source_from_queue_name(self, queue_name: Any) -> str | None:
        normalized_queue_name = str(queue_name or "").strip().lower()
        return self._SCRAPE_QUEUE_TO_SOURCE.get(normalized_queue_name)

    def _sort_sources(self, values: set[str]) -> list[str]:
        normalized = [source for source in values if source in self._SOURCE_ORDER]
        seen: set[str] = set()
        ordered: list[str] = []
        for source in self._SOURCE_ORDER:
            if source in normalized and source not in seen:
                ordered.append(source)
                seen.add(source)
        return ordered

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

    def _normalize_reviews_order(self, order: str | None) -> tuple[str, list[tuple[str, int]]]:
        normalized = str(order or "").strip().lower().replace("_", "-")

        # Backward compatibility aliases.
        if normalized in {"", "desc", "descending", "mayor", "highest", "high"}:
            normalized = "desc-rating"
        elif normalized in {"asc", "ascending", "menor", "lowest", "low"}:
            normalized = "asc-rating"

        if normalized == "desc-rating":
            return normalized, [("rating", -1), ("_id", -1)]
        if normalized == "asc-rating":
            return normalized, [("rating", 1), ("_id", -1)]
        if normalized == "desc-date":
            return normalized, [("created_at", -1), ("_id", -1)]
        if normalized == "asc-date":
            return normalized, [("created_at", 1), ("_id", 1)]

        raise ValueError(
            "Invalid order. Allowed values: desc-rating, asc-rating, desc-date, asc-date."
        )

    def _normalize_comments_order(self, order: str | None) -> tuple[str, list[tuple[str, int]]]:
        normalized = str(order or "").strip().lower().replace("_", "-")
        if normalized in {"", "desc", "descending", "latest", "newest"}:
            normalized = "desc-date"
        elif normalized in {"asc", "ascending", "oldest"}:
            normalized = "asc-date"

        if normalized == "desc-rating":
            return normalized, [("rating", -1), ("scraped_at", -1), ("created_at", -1), ("_id", -1)]
        if normalized == "asc-rating":
            return normalized, [("rating", 1), ("scraped_at", -1), ("created_at", -1), ("_id", -1)]
        if normalized == "desc-date":
            return normalized, [("scraped_at", -1), ("created_at", -1), ("_id", -1)]
        if normalized == "asc-date":
            return normalized, [("scraped_at", 1), ("created_at", 1), ("_id", 1)]

        raise ValueError(
            "Invalid order. Allowed values: desc-rating, asc-rating, desc-date, asc-date."
        )

    def _group_comments_by_source(self, items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {"google_maps": [], "tripadvisor": []}
        for item in items:
            source_value = self._normalize_source(item.get("source"))
            if source_value is None:
                source_value = "unknown"
            grouped.setdefault(source_value, []).append(item)
        return grouped

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
        sources_available: list[str] | None = None,
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
            "sources_available": sources_available or [],
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

    def _serialize_comment_doc(self, comment_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(comment_doc)
        payload["id"] = str(payload.pop("_id"))
        payload["scrape_type"] = self._normalize_source(payload.get("source")) or str(payload.get("source") or "")
        payload.pop("review_fingerprint", None)
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
            "canonical_name_normalized": str(source_profile_doc.get("canonical_name_normalized", "") or ""),
            "source_business_name": str(source_profile_doc.get("source_business_name", "") or ""),
            "source_business_name_normalized": str(
                source_profile_doc.get("source_business_name_normalized", "") or ""
            ),
            "active_dataset_id": source_profile_doc.get("active_dataset_id"),
            "active_scrape_run_id": source_profile_doc.get("active_scrape_run_id"),
            "metrics": metrics,
            "created_at": source_profile_doc.get("created_at"),
            "updated_at": source_profile_doc.get("updated_at"),
        }
        return payload

    def _serialize_job_compact_doc(self, job_doc: dict[str, Any]) -> dict[str, Any]:
        progress_raw = job_doc.get("progress")
        progress = progress_raw if isinstance(progress_raw, dict) else {}
        return {
            "job_id": str(job_doc.get("_id")),
            "status": str(job_doc.get("status", "") or ""),
            "queue_name": str(job_doc.get("queue_name", "") or ""),
            "job_type": str(job_doc.get("job_type", "") or ""),
            "attempts": int(job_doc.get("attempts") or 0),
            "error": str(job_doc.get("error", "") or "") or None,
            "progress": {
                "stage": str(progress.get("stage", "") or ""),
                "message": str(progress.get("message", "") or ""),
                "status": str(progress.get("status", "") or ""),
                "updated_at": progress.get("updated_at"),
            },
            "created_at": job_doc.get("created_at"),
            "updated_at": job_doc.get("updated_at"),
            "started_at": job_doc.get("started_at"),
            "finished_at": job_doc.get("finished_at"),
        }

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
