from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from pymongo import ReturnDocument

from src.database import get_database
from src.models.business import Listing


class AnalyzeBusinessUseCase:
    def __init__(
        self,
        *,
        preprocessor,
        llm_analyzer,
        validate_business_name: Callable[[str], str],
        resolve_reviews_strategy: Callable[[str | None], str],
        normalize_text: Callable[[str], str],
        emit_progress: Callable[
            [Callable[[dict[str, Any]], Awaitable[None] | None] | None, str, str, dict[str, Any] | None],
            Awaitable[None],
        ],
        build_cached_response: Callable[..., Awaitable[dict[str, Any] | None]],
        scrape_business_page: Callable[..., Awaitable[tuple[dict, list[dict]]]],
        normalize_scraped_review: Callable[[dict[str, Any]], dict[str, Any]],
        upsert_reviews: Callable[..., Awaitable[None]],
        sanitize_response_payload: Callable[[Any], Any],
        businesses_collection_name: str,
        reviews_collection_name: str,
        analyses_collection_name: str,
    ) -> None:
        self.preprocessor = preprocessor
        self.llm_analyzer = llm_analyzer
        self._validate_business_name = validate_business_name
        self._resolve_reviews_strategy = resolve_reviews_strategy
        self._normalize_text = normalize_text
        self._emit_progress = emit_progress
        self._build_cached_response = build_cached_response
        self._scrape_business_page = scrape_business_page
        self._normalize_scraped_review = normalize_scraped_review
        self._upsert_reviews = upsert_reviews
        self._sanitize_response_payload = sanitize_response_payload
        self._businesses_collection_name = businesses_collection_name
        self._reviews_collection_name = reviews_collection_name
        self._analyses_collection_name = analyses_collection_name

    async def execute(
        self,
        *,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict[str, Any]:
        business_name = self._validate_business_name(name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        name_normalized = self._normalize_text(business_name)
        database = get_database()
        now = datetime.now(timezone.utc)

        businesses = database[self._businesses_collection_name]
        reviews = database[self._reviews_collection_name]
        analyses = database[self._analyses_collection_name]

        await self._emit_progress(
            progress_callback,
            "analysis_started",
            "Analysis job started.",
            {
                "name": business_name,
                "strategy": selected_strategy,
                "force": bool(force),
                "interactive_max_rounds": interactive_max_rounds,
                "html_scroll_max_rounds": html_scroll_max_rounds,
                "html_stable_rounds": html_stable_rounds,
            },
        )

        if not force:
            cached_payload = await self._build_cached_response(
                businesses=businesses,
                reviews=reviews,
                analyses=analyses,
                name_normalized=name_normalized,
                strategy=selected_strategy,
            )
            if cached_payload is not None:
                await self._emit_progress(
                    progress_callback,
                    "cache_hit",
                    "Returning cached analysis result.",
                    {"strategy": selected_strategy},
                )
                return cached_payload

        listing, raw_reviews = await self._scrape_business_page(
            business_name,
            strategy=selected_strategy,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            progress_callback=progress_callback,
        )
        listing_payload = Listing(**listing).model_dump(mode="python")
        scraped_review_count = len(raw_reviews)

        await self._emit_progress(
            progress_callback,
            "scrape_completed",
            "Scraping finished.",
            {"scraped_review_count": scraped_review_count},
        )

        normalized_raw_reviews = [self._normalize_scraped_review(item) for item in raw_reviews]
        processed_reviews = self.preprocessor.process(normalized_raw_reviews)
        processed_review_count = len(processed_reviews)
        stats = self.preprocessor.compute_stats(processed_reviews)

        await self._emit_progress(
            progress_callback,
            "preprocess_completed",
            "Preprocessing completed.",
            {
                "processed_review_count": processed_review_count,
                "avg_rating": stats.get("avg_rating"),
            },
        )

        await self._emit_progress(
            progress_callback,
            "llm_analysis_started",
            "Running LLM analysis.",
            {"processed_review_count": processed_review_count},
        )
        analysis = await self.llm_analyzer.analyze(
            business_name=business_name,
            reviews=processed_reviews,
            stats=stats,
        )

        await self._emit_progress(
            progress_callback,
            "llm_analysis_completed",
            "LLM analysis completed.",
            {"overall_sentiment": analysis.overall_sentiment},
        )

        business_doc = await businesses.find_one_and_update(
            {"name_normalized": name_normalized},
            {
                "$set": {
                    "name": business_name,
                    "name_normalized": name_normalized,
                    "source": "google_maps",
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
            raise RuntimeError("Failed to upsert business document.")

        business_id = str(business_doc["_id"])
        await self._upsert_reviews(
            reviews_collection=reviews,
            business_id=business_id,
            processed_reviews=processed_reviews,
            scraped_at=now,
        )
        review_count = await reviews.count_documents({"business_id": business_id})

        analysis_payload = analysis.model_dump(mode="python")
        analysis_payload["business_id"] = business_id
        analysis_payload["created_at"] = now
        inserted_analysis = await analyses.insert_one(analysis_payload)

        await businesses.update_one(
            {"_id": business_doc["_id"]},
            {
                "$set": {
                    "latest_analysis_id": str(inserted_analysis.inserted_id),
                    "updated_at": now,
                }
            },
        )

        await self._emit_progress(
            progress_callback,
            "db_persist_completed",
            "Data persisted in MongoDB.",
            {"business_id": business_id, "review_count": review_count},
        )

        payload = {
            "business_id": business_id,
            "name": business_name,
            "cached": False,
            "strategy": selected_strategy,
            "listing": listing_payload,
            "stats": stats,
            "review_count": review_count,
            "scraped_review_count": scraped_review_count,
            "processed_review_count": processed_review_count,
            "listing_total_reviews": listing_payload.get("total_reviews"),
            "analysis": analysis_payload,
        }
        await self._emit_progress(
            progress_callback,
            "analysis_completed",
            "Analysis completed successfully.",
            {"business_id": business_id, "review_count": review_count},
        )
        return self._sanitize_response_payload(payload)
