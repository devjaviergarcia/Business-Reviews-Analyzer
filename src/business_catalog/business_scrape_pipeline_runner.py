from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from bson import ObjectId
from pymongo import ReturnDocument

from src.database import get_database
from src.models.business import Listing
from src.pipeline.preprocessor import ReviewPreprocessor

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None] | None


@dataclass(frozen=True)
class BusinessScrapePipelineRequest:
    source_business_name: str
    canonical_business_name: str
    selected_strategy: str
    selected_force_mode: str
    selected_sources: tuple[str, ...]
    normalized_source_job_id: str | None
    effective_tripadvisor_max_pages: int | None
    effective_tripadvisor_pages_percent: float | None
    canonical_name_normalized: str
    source_name_normalized: str
    normalized_root_business_id: str | None
    root_business_object_id: ObjectId | None
    force: bool
    interactive_max_rounds: int | None
    html_scroll_max_rounds: int | None
    html_stable_rounds: int | None


@dataclass(frozen=True)
class BusinessScrapePipelineCollections:
    businesses: Any
    reviews: Any
    comments: Any
    source_profiles: Any
    datasets: Any
    scrape_runs: Any


@dataclass(frozen=True)
class ExistingBusinessReviewSnapshot:
    business_doc: dict[str, Any] | None
    stored_review_count_before: int
    stored_selected_review_count_before: int
    stored_selected_review_counts_before: dict[str, int]


class BusinessScrapePipelineRunner:
    def __init__(
        self,
        *,
        validate_business_name: Callable[[str], str],
        resolve_reviews_strategy: Callable[[str | None], str],
        resolve_force_mode: Callable[[str | None], str],
        resolve_scrape_sources: Callable[[tuple[str, ...] | list[str] | None], tuple[str, ...]],
        resolve_optional_int_override: Callable[..., int],
        resolve_optional_float_override: Callable[..., float],
        normalize_text: Callable[[str], str],
        parse_object_id: Callable[..., ObjectId],
        emit_progress: Callable[..., Awaitable[None]],
        sanitize_response_payload: Callable[[dict[str, Any]], dict[str, Any]],
        normalize_scraped_review: Callable[[dict[str, Any]], dict[str, Any]],
        build_source_progress_callback: Callable[..., Callable[[dict[str, Any]], Awaitable[None] | None] | None],
        scrape_google_maps_business_page: Callable[..., Awaitable[tuple[dict[str, Any], list[dict[str, Any]]]]],
        scrape_tripadvisor_business_page: Callable[..., Awaitable[tuple[dict[str, Any], list[dict[str, Any]]]]],
        get_or_create_source_profile: Callable[..., Awaitable[dict[str, Any]]],
        package_legacy_reviews_into_dataset: Callable[..., Awaitable[dict[str, Any]]],
        create_scrape_run: Callable[..., Awaitable[dict[str, Any]]],
        create_dataset_snapshot: Callable[..., Awaitable[dict[str, Any]]],
        upsert_reviews: Callable[..., Awaitable[None]],
        upsert_job_comments: Callable[..., Awaitable[None]],
        finalize_scrape_run: Callable[..., Awaitable[None]],
        preprocessor: ReviewPreprocessor,
        primary_source: str,
        scrape_sources: tuple[str, ...],
        businesses_collection_name: str,
        reviews_collection_name: str,
        comments_collection_name: str,
        source_profiles_collection_name: str,
        datasets_collection_name: str,
        scrape_runs_collection_name: str,
        scrape_bot_detected_error_type: type[Exception],
    ) -> None:
        self._validate_business_name = validate_business_name
        self._resolve_reviews_strategy = resolve_reviews_strategy
        self._resolve_force_mode = resolve_force_mode
        self._resolve_scrape_sources = resolve_scrape_sources
        self._resolve_optional_int_override = resolve_optional_int_override
        self._resolve_optional_float_override = resolve_optional_float_override
        self._normalize_text = normalize_text
        self._parse_object_id = parse_object_id
        self._emit_progress = emit_progress
        self._sanitize_response_payload = sanitize_response_payload
        self._normalize_scraped_review = normalize_scraped_review
        self._build_source_progress_callback = build_source_progress_callback
        self._scrape_google_maps_business_page = scrape_google_maps_business_page
        self._scrape_tripadvisor_business_page = scrape_tripadvisor_business_page
        self._get_or_create_source_profile = get_or_create_source_profile
        self._package_legacy_reviews_into_dataset = package_legacy_reviews_into_dataset
        self._create_scrape_run = create_scrape_run
        self._create_dataset_snapshot = create_dataset_snapshot
        self._upsert_reviews = upsert_reviews
        self._upsert_job_comments = upsert_job_comments
        self._finalize_scrape_run = finalize_scrape_run
        self._preprocessor = preprocessor
        self._primary_source = primary_source
        self._scrape_sources = scrape_sources
        self._businesses_collection_name = businesses_collection_name
        self._reviews_collection_name = reviews_collection_name
        self._comments_collection_name = comments_collection_name
        self._source_profiles_collection_name = source_profiles_collection_name
        self._datasets_collection_name = datasets_collection_name
        self._scrape_runs_collection_name = scrape_runs_collection_name
        self._scrape_bot_detected_error_type = scrape_bot_detected_error_type

    async def run(
        self,
        name: str,
        *,
        canonical_name: str | None = None,
        source_name: str | None = None,
        root_business_id: str | None = None,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        sources: tuple[str, ...] | list[str] | None = None,
        preloaded_source_payloads: dict[str, dict[str, Any]] | None = None,
        source_job_id: str | None = None,
        progress_callback: ProgressCallback = None,
    ) -> dict[str, Any]:
        request = self._normalize_request(
            name=name,
            canonical_name=canonical_name,
            source_name=source_name,
            root_business_id=root_business_id,
            force=force,
            strategy=strategy,
            force_mode=force_mode,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            tripadvisor_max_pages=tripadvisor_max_pages,
            tripadvisor_pages_percent=tripadvisor_pages_percent,
            sources=sources,
            source_job_id=source_job_id,
        )
        collections = self._resolve_collections()
        now = datetime.now(timezone.utc)

        await self._emit_progress(
            progress_callback,
            "scrape_pipeline_started",
            "Scrape stage started.",
            {
                "name": request.canonical_business_name,
                "source_name": request.source_business_name,
                "canonical_name": request.canonical_business_name,
                "root_business_id": request.normalized_root_business_id,
                "strategy": request.selected_strategy,
                "force": bool(request.force),
                "force_mode": request.selected_force_mode,
                "interactive_max_rounds": request.interactive_max_rounds,
                "html_scroll_max_rounds": request.html_scroll_max_rounds,
                "html_stable_rounds": request.html_stable_rounds,
                "tripadvisor_max_pages": request.effective_tripadvisor_max_pages,
                "tripadvisor_pages_percent": request.effective_tripadvisor_pages_percent,
                "sources": list(request.selected_sources),
            },
        )

        business_lookup_query = self._build_business_lookup_query(request=request)
        existing_snapshot = await self._load_existing_business_review_snapshot(
            collections=collections,
            business_lookup_query=business_lookup_query,
            selected_sources=request.selected_sources,
        )
        cached_response = await self._build_cached_scrape_response_if_available(
            request=request,
            existing_snapshot=existing_snapshot,
            progress_callback=progress_callback,
        )
        if cached_response is not None:
            return cached_response

        normalized_preloaded_source_payloads = self._normalize_preloaded_source_payloads(
            preloaded_source_payloads=preloaded_source_payloads,
            selected_sources=request.selected_sources,
        )
        source_results = await self._build_preloaded_source_results(
            normalized_preloaded_source_payloads=normalized_preloaded_source_payloads,
            progress_callback=progress_callback,
        )

        failed_sources: dict[str, str] = {}
        failed_source_errors: dict[str, Exception] = {}
        await self._run_source_scrapes(
            request=request,
            source_results=source_results,
            failed_sources=failed_sources,
            failed_source_errors=failed_source_errors,
            progress_callback=progress_callback,
        )
        self._raise_if_all_sources_failed(
            source_results=source_results,
            failed_sources=failed_sources,
            failed_source_errors=failed_source_errors,
        )

        return await self._persist_scrape_results(
            request=request,
            collections=collections,
            business_lookup_query=business_lookup_query,
            existing_snapshot=existing_snapshot,
            source_results=source_results,
            failed_sources=failed_sources,
            now=now,
            progress_callback=progress_callback,
        )

    def _normalize_request(
        self,
        *,
        name: str,
        canonical_name: str | None,
        source_name: str | None,
        root_business_id: str | None,
        force: bool,
        strategy: str | None,
        force_mode: str | None,
        interactive_max_rounds: int | None,
        html_scroll_max_rounds: int | None,
        html_stable_rounds: int | None,
        tripadvisor_max_pages: int | None,
        tripadvisor_pages_percent: float | None,
        sources: tuple[str, ...] | list[str] | None,
        source_job_id: str | None,
    ) -> BusinessScrapePipelineRequest:
        source_business_name = (
            self._validate_business_name(source_name)
            if isinstance(source_name, str) and source_name.strip()
            else self._validate_business_name(name)
        )
        canonical_business_name = (
            self._validate_business_name(canonical_name)
            if isinstance(canonical_name, str) and canonical_name.strip()
            else source_business_name
        )
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        selected_sources = self._resolve_scrape_sources(sources)
        normalized_source_job_id = str(source_job_id or "").strip() or None
        effective_tripadvisor_max_pages = self._resolve_optional_int_override(
            value=tripadvisor_max_pages,
            fallback=25,
            min_value=1,
            field_name="tripadvisor_max_pages",
        ) if tripadvisor_max_pages is not None else None
        effective_tripadvisor_pages_percent = self._resolve_optional_float_override(
            value=tripadvisor_pages_percent,
            min_value=0.1,
            max_value=100.0,
            field_name="tripadvisor_pages_percent",
        ) if tripadvisor_pages_percent is not None else None
        canonical_name_normalized = self._normalize_text(canonical_business_name)
        source_name_normalized = self._normalize_text(source_business_name)
        normalized_root_business_id = str(root_business_id or "").strip() or None
        root_business_object_id: ObjectId | None = None
        if normalized_root_business_id:
            try:
                root_business_object_id = self._parse_object_id(
                    normalized_root_business_id,
                    field_name="root_business_id",
                )
            except ValueError:
                root_business_object_id = None
        return BusinessScrapePipelineRequest(
            source_business_name=source_business_name,
            canonical_business_name=canonical_business_name,
            selected_strategy=selected_strategy,
            selected_force_mode=selected_force_mode,
            selected_sources=selected_sources,
            normalized_source_job_id=normalized_source_job_id,
            effective_tripadvisor_max_pages=effective_tripadvisor_max_pages,
            effective_tripadvisor_pages_percent=effective_tripadvisor_pages_percent,
            canonical_name_normalized=canonical_name_normalized,
            source_name_normalized=source_name_normalized,
            normalized_root_business_id=normalized_root_business_id,
            root_business_object_id=root_business_object_id,
            force=bool(force),
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
        )

    def _resolve_collections(self) -> BusinessScrapePipelineCollections:
        database = get_database()
        return BusinessScrapePipelineCollections(
            businesses=database[self._businesses_collection_name],
            reviews=database[self._reviews_collection_name],
            comments=database[self._comments_collection_name],
            source_profiles=database[self._source_profiles_collection_name],
            datasets=database[self._datasets_collection_name],
            scrape_runs=database[self._scrape_runs_collection_name],
        )

    def _build_business_lookup_query(self, *, request: BusinessScrapePipelineRequest) -> dict[str, Any]:
        if request.root_business_object_id is not None:
            return {"_id": request.root_business_object_id}
        return {"name_normalized": request.canonical_name_normalized}

    async def _load_existing_business_review_snapshot(
        self,
        *,
        collections: BusinessScrapePipelineCollections,
        business_lookup_query: dict[str, Any],
        selected_sources: tuple[str, ...],
    ) -> ExistingBusinessReviewSnapshot:
        existing_business_doc = await collections.businesses.find_one(business_lookup_query)
        stored_review_count_before = 0
        stored_selected_review_count_before = 0
        stored_selected_review_counts_before: dict[str, int] = {source: 0 for source in selected_sources}
        if existing_business_doc:
            existing_business_id = str(existing_business_doc["_id"])
            stored_review_count_before = await collections.reviews.count_documents({"business_id": existing_business_id})
            selected_counts_raw = await collections.reviews.aggregate(
                [
                    {
                        "$match": {
                            "business_id": existing_business_id,
                            "source": {"$in": list(selected_sources)},
                        }
                    },
                    {"$group": {"_id": "$source", "count": {"$sum": 1}}},
                ]
            ).to_list(length=len(selected_sources))
            for item in selected_counts_raw:
                if not isinstance(item, dict):
                    continue
                source_value = str(item.get("_id") or "").strip().lower()
                if source_value not in stored_selected_review_counts_before:
                    continue
                stored_selected_review_counts_before[source_value] = int(item.get("count") or 0)
            stored_selected_review_count_before = int(sum(stored_selected_review_counts_before.values()))
        return ExistingBusinessReviewSnapshot(
            business_doc=existing_business_doc,
            stored_review_count_before=stored_review_count_before,
            stored_selected_review_count_before=stored_selected_review_count_before,
            stored_selected_review_counts_before=stored_selected_review_counts_before,
        )

    async def _build_cached_scrape_response_if_available(
        self,
        *,
        request: BusinessScrapePipelineRequest,
        existing_snapshot: ExistingBusinessReviewSnapshot,
        progress_callback: ProgressCallback,
    ) -> dict[str, Any] | None:
        existing_business_doc = existing_snapshot.business_doc
        if not existing_business_doc or request.force:
            return None

        existing_business_id = str(existing_business_doc["_id"])
        missing_sources = [
            source
            for source in request.selected_sources
            if int(existing_snapshot.stored_selected_review_counts_before.get(source, 0)) <= 0
        ]
        existing_review_count = existing_snapshot.stored_selected_review_count_before
        if existing_review_count <= 0 or missing_sources:
            return None

        listing_payload = existing_business_doc.get("listing") if isinstance(existing_business_doc.get("listing"), dict) else {}
        await self._emit_progress(
            progress_callback,
            "scrape_pipeline_cache_hit",
            "Skipping scrape because stored reviews already exist for selected sources.",
            {
                "business_id": existing_business_id,
                "review_count": existing_review_count,
                "stored_review_count_before": existing_snapshot.stored_review_count_before,
                "stored_selected_review_count_before": existing_snapshot.stored_selected_review_count_before,
                "source_review_counts": existing_snapshot.stored_selected_review_counts_before,
                "sources": list(request.selected_sources),
            },
        )
        return self._sanitize_response_payload(
            {
                "business_id": existing_business_id,
                "name": str(existing_business_doc.get("name", "") or request.canonical_business_name),
                "canonical_name": request.canonical_business_name,
                "source_name": request.source_business_name,
                "cached_scrape": True,
                "strategy": request.selected_strategy,
                "force_mode": request.selected_force_mode,
                "listing": listing_payload,
                "stats": existing_business_doc.get("stats", {}),
                "review_count": existing_review_count,
                "stored_review_count_before": existing_snapshot.stored_review_count_before,
                "stored_review_count_after": existing_snapshot.stored_review_count_before,
                "stored_selected_review_count_before": existing_snapshot.stored_selected_review_count_before,
                "stored_selected_review_count_after": existing_snapshot.stored_selected_review_count_before,
                "source_review_counts": existing_snapshot.stored_selected_review_counts_before,
                "scrape_produced_new_reviews": False,
                "scraped_review_count": existing_business_doc.get("scraped_review_count"),
                "processed_review_count": existing_business_doc.get("processed_review_count"),
                "listing_total_reviews": listing_payload.get("total_reviews") if isinstance(listing_payload, dict) else None,
                "sources": {},
                "failed_sources": {},
            }
        )

    def _normalize_preloaded_source_payloads(
        self,
        *,
        preloaded_source_payloads: dict[str, dict[str, Any]] | None,
        selected_sources: tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        normalized_preloaded_source_payloads: dict[str, dict[str, Any]] = {}
        if not isinstance(preloaded_source_payloads, dict):
            return normalized_preloaded_source_payloads

        for raw_source, raw_payload in preloaded_source_payloads.items():
            source_key = str(raw_source or "").strip().lower()
            if source_key not in selected_sources or source_key not in self._scrape_sources:
                continue
            if not isinstance(raw_payload, dict):
                raise ValueError(
                    f"Invalid preloaded_source_payloads[{raw_source!r}]. Expected an object with 'listing' and 'reviews'."
                )
            listing_value = raw_payload.get("listing")
            reviews_value = raw_payload.get("reviews")
            if not isinstance(listing_value, dict):
                raise ValueError(
                    f"Invalid preloaded listing for source={source_key}. Expected an object."
                )
            if not isinstance(reviews_value, list):
                raise ValueError(
                    f"Invalid preloaded reviews for source={source_key}. Expected an array."
                )
            normalized_preloaded_source_payloads[source_key] = {
                "listing": dict(listing_value),
                "reviews": [dict(item) for item in reviews_value if isinstance(item, dict)],
            }
        return normalized_preloaded_source_payloads

    async def _build_preloaded_source_results(
        self,
        *,
        normalized_preloaded_source_payloads: dict[str, dict[str, Any]],
        progress_callback: ProgressCallback,
    ) -> dict[str, dict[str, Any]]:
        source_results: dict[str, dict[str, Any]] = {}
        for source, preloaded_payload in normalized_preloaded_source_payloads.items():
            listing_payload = Listing(**preloaded_payload["listing"]).model_dump(mode="python")
            raw_reviews = [
                {
                    **item,
                    "source": str(item.get("source") or source),
                }
                for item in preloaded_payload["reviews"]
            ]
            normalized_raw_reviews = [self._normalize_scraped_review(item) for item in raw_reviews]
            processed_reviews = self._preprocessor.process(normalized_raw_reviews)
            source_results[source] = {
                "listing_payload": listing_payload,
                "raw_reviews": raw_reviews,
                "processed_reviews": processed_reviews,
                "scraped_review_count": len(raw_reviews),
                "processed_review_count": len(processed_reviews),
                "stats": self._preprocessor.compute_stats(processed_reviews),
            }
            await self._emit_progress(
                progress_callback,
                "scrape_source_preloaded",
                "Using preloaded source payload.",
                {
                    "source": source,
                    "scraped_review_count": len(raw_reviews),
                    "processed_review_count": len(processed_reviews),
                },
            )
        return source_results

    async def _run_source_scrapes(
        self,
        *,
        request: BusinessScrapePipelineRequest,
        source_results: dict[str, dict[str, Any]],
        failed_sources: dict[str, str],
        failed_source_errors: dict[str, Exception],
        progress_callback: ProgressCallback,
    ) -> None:
        source_tasks: dict[str, asyncio.Task[tuple[dict[str, Any], list[dict[str, Any]]]]] = {}
        if "google_maps" in request.selected_sources and "google_maps" not in source_results:
            source_tasks["google_maps"] = asyncio.create_task(
                self._scrape_google_maps_business_page(
                    request.source_business_name,
                    strategy=request.selected_strategy,
                    interactive_max_rounds=request.interactive_max_rounds,
                    html_scroll_max_rounds=request.html_scroll_max_rounds,
                    html_stable_rounds=request.html_stable_rounds,
                    progress_callback=self._build_source_progress_callback(
                        progress_callback=progress_callback,
                        source="google_maps",
                    ),
                )
            )
        if "tripadvisor" in request.selected_sources and "tripadvisor" not in source_results:
            source_tasks["tripadvisor"] = asyncio.create_task(
                self._scrape_tripadvisor_business_page(
                    request.source_business_name,
                    max_pages=request.effective_tripadvisor_max_pages,
                    pages_percent=request.effective_tripadvisor_pages_percent,
                    progress_callback=self._build_source_progress_callback(
                        progress_callback=progress_callback,
                        source="tripadvisor",
                    ),
                )
            )
        if not source_tasks:
            return

        gathered = await asyncio.gather(*source_tasks.values(), return_exceptions=True)
        for source, result in zip(source_tasks.keys(), gathered):
            if isinstance(result, Exception):
                failed_sources[source] = str(result)
                failed_source_errors[source] = result
                await self._emit_progress(
                    progress_callback,
                    "scrape_source_failed",
                    "Source scrape failed.",
                    {"source": source, "error": str(result)},
                )
                continue
            listing, raw_reviews = result
            listing_payload = Listing(**listing).model_dump(mode="python")
            normalized_raw_reviews = [self._normalize_scraped_review(item) for item in raw_reviews]
            processed_reviews = self._preprocessor.process(normalized_raw_reviews)
            source_results[source] = {
                "listing_payload": listing_payload,
                "raw_reviews": raw_reviews,
                "processed_reviews": processed_reviews,
                "scraped_review_count": len(raw_reviews),
                "processed_review_count": len(processed_reviews),
                "stats": self._preprocessor.compute_stats(processed_reviews),
            }

    def _raise_if_all_sources_failed(
        self,
        *,
        source_results: dict[str, dict[str, Any]],
        failed_sources: dict[str, str],
        failed_source_errors: dict[str, Exception],
    ) -> None:
        if source_results:
            return
        bot_failed_sources = [
            source_name
            for source_name, source_error in failed_source_errors.items()
            if isinstance(source_error, self._scrape_bot_detected_error_type)
        ]
        if bot_failed_sources:
            raise self._scrape_bot_detected_error_type(
                "Anti-bot challenge detected. "
                + "; ".join(
                    f"{source}: {failed_sources.get(source, 'unknown anti-bot error')}"
                    for source in bot_failed_sources
                )
            )
        raise RuntimeError(
            "All configured sources failed during scrape stage. "
            + "; ".join(f"{source}: {error}" for source, error in failed_sources.items())
        )

    async def _persist_scrape_results(
        self,
        *,
        request: BusinessScrapePipelineRequest,
        collections: BusinessScrapePipelineCollections,
        business_lookup_query: dict[str, Any],
        existing_snapshot: ExistingBusinessReviewSnapshot,
        source_results: dict[str, dict[str, Any]],
        failed_sources: dict[str, str],
        now: datetime,
        progress_callback: ProgressCallback,
    ) -> dict[str, Any]:
        primary_source = self._primary_source if self._primary_source in source_results else next(iter(source_results))
        primary_result = source_results[primary_source]
        listing_payload = primary_result["listing_payload"]
        stats = primary_result["stats"]
        scraped_review_count = sum(int(payload.get("scraped_review_count", 0)) for payload in source_results.values())
        processed_review_count = sum(int(payload.get("processed_review_count", 0)) for payload in source_results.values())

        business_doc = await collections.businesses.find_one_and_update(
            business_lookup_query,
            {
                "$set": {
                    "name": request.canonical_business_name,
                    "name_normalized": request.canonical_name_normalized,
                    "source": primary_source,
                    "listing": listing_payload,
                    "stats": stats,
                    "review_count": processed_review_count,
                    "scraped_review_count": scraped_review_count,
                    "processed_review_count": processed_review_count,
                    "last_scraped_at": now,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if business_doc is None:
            raise RuntimeError("Failed to upsert business document during scrape stage.")

        business_id = str(business_doc["_id"])
        source_runtime = await self._persist_source_runtime(
            request=request,
            collections=collections,
            business_id=business_id,
            source_results=source_results,
            now=now,
        )
        dataset_review_count_total = sum(int(runtime.get("dataset_review_count", 0)) for runtime in source_runtime.values())
        review_count = await collections.reviews.count_documents({"business_id": business_id})
        scrape_produced_new_reviews = bool(
            any(int(runtime.get("dataset_review_count", 0)) > 0 for runtime in source_runtime.values())
        )
        strict_rescrape_failed = bool(
            request.force
            and request.selected_force_mode == "strict_rescrape"
            and not scrape_produced_new_reviews
        )
        for runtime in source_runtime.values():
            await self._finalize_scrape_run(
                scrape_runs_collection=collections.scrape_runs,
                scrape_run_id=str(runtime["scrape_run_id"]),
                now=now,
                status="failed" if strict_rescrape_failed else "done",
                metrics={
                    "scraped_review_count": int(runtime.get("scraped_review_count", 0)),
                    "processed_review_count": int(runtime.get("processed_review_count", 0)),
                    "stored_review_count_before": existing_snapshot.stored_review_count_before,
                    "stored_review_count_after": review_count,
                    "dataset_review_count": int(runtime.get("dataset_review_count", 0)),
                },
                dataset_id=str(runtime["scrape_dataset_id"]),
            )

        active_runtime = self._select_active_runtime(primary_source=primary_source, source_runtime=source_runtime)
        source_profile_id = str(active_runtime["source_profile_id"])
        scrape_run_id = str(active_runtime["scrape_run_id"])
        scrape_dataset_id = str(active_runtime["scrape_dataset_id"])
        scrape_dataset_review_count = int(active_runtime["dataset_review_count"])
        legacy_dataset_id = active_runtime.get("legacy_dataset_id")
        fallback_active_dataset_id = active_runtime.get("fallback_active_dataset_id")
        analysis_dataset_id = (
            scrape_dataset_id
            if len(source_runtime) == 1 and scrape_dataset_review_count > 0
            else fallback_active_dataset_id if len(source_runtime) == 1
            else None
        )
        business_active_dataset_id = (
            scrape_dataset_id
            if scrape_dataset_review_count > 0
            else fallback_active_dataset_id
        )
        await collections.businesses.update_one(
            {"_id": business_doc["_id"]},
            {
                "$set": {
                    "review_count": review_count,
                    "active_dataset_id": business_active_dataset_id,
                    "updated_at": now,
                }
            },
        )

        if strict_rescrape_failed:
            await self._emit_progress(
                progress_callback,
                "scrape_pipeline_strict_rescrape_failed",
                "Strict rescrape mode failed because no new reviews were scraped.",
                {
                    "business_id": business_id,
                    "strategy": request.selected_strategy,
                    "force_mode": request.selected_force_mode,
                    "scraped_review_count": scraped_review_count,
                    "dataset_review_count": dataset_review_count_total,
                    "dataset_id": scrape_dataset_id,
                    "analysis_dataset_id": analysis_dataset_id,
                    "legacy_dataset_id": legacy_dataset_id,
                    "sources": source_runtime,
                    "failed_sources": failed_sources,
                },
            )
            raise RuntimeError(
                "Strict rescrape mode is enabled and scrape produced 0 reviews. "
                "No fallback to stored reviews was applied."
            )

        if not scrape_produced_new_reviews and review_count > 0:
            await self._emit_progress(
                progress_callback,
                "scrape_pipeline_no_new_reviews",
                "Scrape produced no new reviews; continuing with stored reviews.",
                {
                    "business_id": business_id,
                    "stored_review_count_before": existing_snapshot.stored_review_count_before,
                    "stored_review_count_after": review_count,
                    "analysis_dataset_id": analysis_dataset_id,
                    "legacy_dataset_id": legacy_dataset_id,
                    "sources": source_runtime,
                    "failed_sources": failed_sources,
                },
            )

        await self._emit_progress(
            progress_callback,
            "scrape_pipeline_persisted",
            "Scrape stage persisted listing and reviews.",
            {
                "business_id": business_id,
                "review_count": review_count,
                "scraped_review_count": scraped_review_count,
                "processed_review_count": processed_review_count,
                "stored_review_count_before": existing_snapshot.stored_review_count_before,
                "stored_review_count_after": review_count,
                "dataset_review_count": dataset_review_count_total,
                "dataset_id": scrape_dataset_id,
                "analysis_dataset_id": analysis_dataset_id,
                "legacy_dataset_id": legacy_dataset_id,
                "source_profile_id": source_profile_id,
                "scrape_run_id": scrape_run_id,
                "scrape_produced_new_reviews": scrape_produced_new_reviews,
                "source_job_id": request.normalized_source_job_id,
                "sources": source_runtime,
                "failed_sources": failed_sources,
            },
        )
        return self._sanitize_response_payload(
            {
                "business_id": business_id,
                "name": request.canonical_business_name,
                "canonical_name": request.canonical_business_name,
                "source_name": request.source_business_name,
                "cached_scrape": False,
                "strategy": request.selected_strategy,
                "force_mode": request.selected_force_mode,
                "listing": listing_payload,
                "stats": stats,
                "review_count": review_count,
                "scraped_review_count": scraped_review_count,
                "processed_review_count": processed_review_count,
                "stored_review_count_before": existing_snapshot.stored_review_count_before,
                "stored_review_count_after": review_count,
                "dataset_review_count": dataset_review_count_total,
                "dataset_id": scrape_dataset_id,
                "analysis_dataset_id": analysis_dataset_id,
                "legacy_dataset_id": legacy_dataset_id,
                "source_profile_id": source_profile_id,
                "scrape_run_id": scrape_run_id,
                "scrape_produced_new_reviews": scrape_produced_new_reviews,
                "listing_total_reviews": listing_payload.get("total_reviews"),
                "source_job_id": request.normalized_source_job_id,
                "sources": source_runtime,
                "failed_sources": failed_sources,
            }
        )

    async def _persist_source_runtime(
        self,
        *,
        request: BusinessScrapePipelineRequest,
        collections: BusinessScrapePipelineCollections,
        business_id: str,
        source_results: dict[str, dict[str, Any]],
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        source_runtime: dict[str, dict[str, Any]] = {}
        for source in (item for item in request.selected_sources if item in source_results):
            payload = source_results[source]
            source_profile = await self._get_or_create_source_profile(
                source_profiles_collection=collections.source_profiles,
                business_id=business_id,
                source=source,
                name_normalized=request.source_name_normalized,
                canonical_name_normalized=request.canonical_name_normalized,
                source_business_name=request.source_business_name,
                listing_payload=payload["listing_payload"],
                now=now,
            )
            source_profile_id = str(source_profile["_id"])

            legacy_dataset_result = await self._package_legacy_reviews_into_dataset(
                reviews_collection=collections.reviews,
                datasets_collection=collections.datasets,
                source_profiles_collection=collections.source_profiles,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                now=now,
            )
            legacy_dataset_id = legacy_dataset_result.get("dataset_id")

            scrape_run = await self._create_scrape_run(
                scrape_runs_collection=collections.scrape_runs,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                strategy=request.selected_strategy,
                force=bool(request.force),
                force_mode=request.selected_force_mode,
                now=now,
            )
            scrape_run_id = str(scrape_run["_id"])
            await collections.source_profiles.update_one(
                {"_id": source_profile["_id"]},
                {
                    "$inc": {"metrics.total_runs": 1},
                    "$set": {"updated_at": now},
                },
            )

            scrape_dataset = await self._create_dataset_snapshot(
                datasets_collection=collections.datasets,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                scrape_run_id=scrape_run_id,
                now=now,
            )
            scrape_dataset_id = str(scrape_dataset["_id"])

            await self._upsert_reviews(
                reviews_collection=collections.reviews,
                business_id=business_id,
                processed_reviews=payload["processed_reviews"],
                scraped_at=now,
                source_profile_id=source_profile_id,
                dataset_id=scrape_dataset_id,
                scrape_run_id=scrape_run_id,
            )
            await self._upsert_job_comments(
                comments_collection=collections.comments,
                business_id=business_id,
                business_name=request.canonical_business_name,
                name_normalized=request.canonical_name_normalized,
                source=source,
                source_job_id=request.normalized_source_job_id,
                processed_reviews=payload["processed_reviews"],
                scraped_at=now,
                source_profile_id=source_profile_id,
                dataset_id=scrape_dataset_id,
                scrape_run_id=scrape_run_id,
            )
            scrape_dataset_review_count = await collections.reviews.count_documents(
                {"business_id": business_id, "dataset_id": scrape_dataset_id}
            )
            dataset_status = "ready" if scrape_dataset_review_count > 0 else "empty"
            await collections.datasets.update_one(
                {"_id": scrape_dataset["_id"]},
                {
                    "$set": {
                        "status": dataset_status,
                        "metrics.review_count": scrape_dataset_review_count,
                        "updated_at": now,
                    }
                },
            )
            if scrape_dataset_review_count > 0:
                await collections.source_profiles.update_one(
                    {"_id": source_profile["_id"]},
                    {
                        "$set": {
                            "active_dataset_id": scrape_dataset_id,
                            "active_scrape_run_id": scrape_run_id,
                            "metrics.active_review_count": scrape_dataset_review_count,
                            "updated_at": now,
                        }
                    },
                )

            fallback_active_dataset_id = str(
                source_profile.get("active_dataset_id") or legacy_dataset_id or ""
            ).strip() or None
            source_runtime[source] = {
                "source": source,
                "source_profile_id": source_profile_id,
                "legacy_dataset_id": legacy_dataset_id,
                "scrape_run_id": scrape_run_id,
                "scrape_dataset_id": scrape_dataset_id,
                "dataset_review_count": scrape_dataset_review_count,
                "scraped_review_count": payload["scraped_review_count"],
                "processed_review_count": payload["processed_review_count"],
                "stats": payload["stats"],
                "listing_payload": payload["listing_payload"],
                "fallback_active_dataset_id": fallback_active_dataset_id,
            }
        return source_runtime

    def _select_active_runtime(
        self,
        *,
        primary_source: str,
        source_runtime: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        primary_runtime = source_runtime[primary_source]
        active_runtime = primary_runtime
        if int(primary_runtime.get("dataset_review_count", 0)) <= 0:
            for runtime in source_runtime.values():
                if int(runtime.get("dataset_review_count", 0)) > 0:
                    active_runtime = runtime
                    break
        return active_runtime
