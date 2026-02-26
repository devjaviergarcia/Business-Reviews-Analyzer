from __future__ import annotations

import asyncio
import hashlib
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.config import settings
from src.database import get_database
from src.models.business import Listing, OwnerReply, Review
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.scraper.google_maps import GoogleMapsScraper
from src.services.analyze_business_use_case import AnalyzeBusinessUseCase
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.reanalyze_use_case import ReanalyzeUseCase


class BusinessService:
    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"
    _SUPPORTED_REANALYZE_BATCHERS = {
        "latest_text",
        "balanced_rating",
        "low_rating_focus",
        "high_rating_focus",
    }
    _SUPPORTED_REVIEW_STRATEGIES = {
        "interactive",
        "scroll_copy",
    }

    def __init__(
        self,
        *,
        scraper: GoogleMapsScraper | None = None,
        preprocessor: ReviewPreprocessor | None = None,
        llm_analyzer: ReviewLLMAnalyzer | None = None,
        job_service: AnalysisJobService | None = None,
        query_service: BusinessQueryService | None = None,
        analyze_use_case: AnalyzeBusinessUseCase | None = None,
        reanalyze_use_case: ReanalyzeUseCase | None = None,
    ) -> None:
        self.scraper = scraper or type(self).build_default_scraper()
        self.preprocessor = preprocessor or ReviewPreprocessor()
        self.llm_analyzer = llm_analyzer or ReviewLLMAnalyzer()
        self.job_service = job_service or AnalysisJobService()
        self.query_service = query_service or BusinessQueryService()
        self.analyze_use_case = analyze_use_case or self._build_analyze_use_case()
        self.reanalyze_use_case = reanalyze_use_case or self._build_reanalyze_use_case()

    def _build_analyze_use_case(self) -> AnalyzeBusinessUseCase:
        return AnalyzeBusinessUseCase(
            preprocessor=self.preprocessor,
            llm_analyzer=self.llm_analyzer,
            validate_business_name=self._validate_business_name,
            resolve_reviews_strategy=self._resolve_reviews_strategy,
            normalize_text=self._normalize_text,
            emit_progress=self._emit_progress,
            build_cached_response=self._build_cached_response,
            scrape_business_page=self._scrape_business_page,
            normalize_scraped_review=self._normalize_scraped_review,
            upsert_reviews=self._upsert_reviews,
            sanitize_response_payload=self._sanitize_response_payload,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
        )

    def _build_reanalyze_use_case(self) -> ReanalyzeUseCase:
        return ReanalyzeUseCase(
            preprocessor=self.preprocessor,
            llm_analyzer=self.llm_analyzer,
            parse_object_id=self._parse_object_id,
            resolve_reanalysis_batchers=self._resolve_reanalysis_batchers,
            normalize_stored_review=self._normalize_stored_review,
            serialize_review_doc=self._serialize_review_doc,
            build_reanalysis_batches=self._build_reanalysis_batches,
            analysis_quality_score=self._analysis_quality_score,
            merge_reanalysis_runs=self._merge_reanalysis_runs,
            sanitize_response_payload=self._sanitize_response_payload,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
        )

    @classmethod
    def build_default_scraper(cls) -> GoogleMapsScraper:
        default_strategy = "scroll_copy"
        return GoogleMapsScraper(
            headless=settings.scraper_headless,
            incognito=settings.scraper_incognito,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir=settings.scraper_user_data_dir,
            browser_channel=settings.scraper_browser_channel,
            maps_url=settings.scraper_maps_url,
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
            stealth_mode=settings.scraper_stealth_mode,
            harden_headless=settings.scraper_harden_headless,
            extra_chromium_args=settings.scraper_extra_chromium_args,
            reviews_strategy=default_strategy,
        )

    async def analyze_business(
        self,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict:
        return await self.analyze_use_case.execute(
            name=name,
            force=force,
            strategy=strategy,
            progress_callback=progress_callback,
        )

    async def get_business(self, business_id: str, include_listing: bool = True) -> dict:
        return await self.query_service.get_business(business_id=business_id, include_listing=include_listing)

    async def list_businesses(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        include_listing: bool = False,
    ) -> dict:
        return await self.query_service.list_businesses(
            page=page,
            page_size=page_size,
            include_listing=include_listing,
        )

    async def get_business_reviews(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        return await self.query_service.get_business_reviews(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )

    async def get_business_analysis(self, business_id: str) -> dict:
        return await self.query_service.get_business_analysis(business_id=business_id)

    async def list_business_analyses(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        return await self.query_service.list_business_analyses(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )

    async def reanalyze_business_from_stored_reviews(
        self,
        business_id: str,
        *,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
    ) -> dict:
        return await self.reanalyze_use_case.execute(
            business_id=business_id,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
        )

    async def enqueue_business_analysis_job(
        self,
        name: str,
        force: bool = False,
        strategy: str | None = None,
    ) -> dict:
        business_name = self._validate_business_name(name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        name_normalized = self._normalize_text(business_name)
        return await self.job_service.enqueue_job(
            name=business_name,
            name_normalized=name_normalized,
            force=bool(force),
            strategy=selected_strategy,
        )

    async def get_business_analysis_job(self, job_id: str) -> dict:
        return await self.job_service.get_job(job_id=job_id)

    async def list_business_analysis_jobs(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        status_filter: str | None = None,
    ) -> dict:
        return await self.job_service.list_jobs(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )

    async def _build_cached_response(
        self,
        *,
        businesses,
        reviews,
        analyses,
        name_normalized: str,
        strategy: str,
    ) -> dict | None:
        business_doc = await businesses.find_one({"name_normalized": name_normalized})
        if not business_doc:
            return None

        business_id = str(business_doc["_id"])
        latest_analysis = await analyses.find_one(
            {"business_id": business_id},
            sort=[("created_at", -1)],
        )
        if latest_analysis is None:
            return None

        latest_analysis.pop("_id", None)
        review_count = await reviews.count_documents({"business_id": business_id})

        payload = {
            "business_id": business_id,
            "name": business_doc.get("name", ""),
            "cached": True,
            "strategy": strategy,
            "listing": business_doc.get("listing"),
            "stats": business_doc.get("stats", {}),
            "review_count": review_count,
            "scraped_review_count": business_doc.get("scraped_review_count"),
            "processed_review_count": business_doc.get("processed_review_count"),
            "listing_total_reviews": (business_doc.get("listing") or {}).get("total_reviews"),
            "analysis": latest_analysis,
        }
        return self._sanitize_response_payload(payload)

    async def _scrape_business_page(
        self,
        business_name: str,
        *,
        strategy: str,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> tuple[dict, list[dict]]:
        async def _scraper_progress(event: dict[str, Any]) -> None:
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_progress",
                "Review scrolling in progress.",
                event,
            )

        await self._emit_progress(
            progress_callback,
            "scraper_starting",
            "Starting browser and scraper.",
            {"strategy": strategy},
        )
        await self.scraper.start()
        try:
            await self._emit_progress(
                progress_callback,
                "scraper_search_started",
                "Searching business on Google Maps.",
                {"query": business_name},
            )
            await self.scraper.search_business(business_name)
            await self._emit_progress(
                progress_callback,
                "scraper_search_completed",
                "Business page opened.",
                {"query": business_name},
            )

            listing = await self.scraper.extract_listing()
            await self._emit_progress(
                progress_callback,
                "scraper_listing_completed",
                "Listing extracted.",
                {
                    "business_name": listing.get("business_name"),
                    "total_reviews": listing.get("total_reviews"),
                },
            )

            reviews = await self.scraper.extract_reviews(
                strategy=strategy,
                max_rounds=max(1, settings.scraper_interactive_max_rounds),
                html_scroll_max_rounds=max(0, settings.scraper_html_scroll_max_rounds),
                html_stable_rounds=max(2, settings.scraper_html_stable_rounds),
                html_min_interval_s=max(0.1, settings.scraper_html_scroll_min_interval_s),
                html_max_interval_s=max(
                    max(0.1, settings.scraper_html_scroll_min_interval_s),
                    settings.scraper_html_scroll_max_interval_s,
                ),
                progress_callback=_scraper_progress,
            )
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_completed",
                "Reviews extracted.",
                {"scraped_review_count": len(reviews)},
            )
            return listing, reviews
        finally:
            await self.scraper.close()

    def _resolve_reviews_strategy(self, strategy: str | None) -> str:
        if strategy is None:
            return "scroll_copy"

        raw_value = str(strategy or "").strip()
        normalized = (
            self._normalize_text(raw_value)
            .replace("-", "_")
            .replace(" ", "_")
        )
        if normalized in {"", "default"}:
            normalized = "scroll_copy"
        if normalized not in self._SUPPORTED_REVIEW_STRATEGIES:
            supported = ", ".join(sorted(self._SUPPORTED_REVIEW_STRATEGIES))
            raise ValueError(f"Unknown strategy '{raw_value}'. Supported: {supported}.")
        return normalized

    async def _emit_progress(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        stage: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        if callback is None:
            return
        payload = {
            "stage": stage,
            "message": message,
            "data": data or {},
            "created_at": datetime.now(timezone.utc),
        }
        try:
            maybe_awaitable = callback(payload)
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        except Exception:
            # Progress callback errors must not affect core flow.
            return

    async def _upsert_reviews(
        self,
        *,
        reviews_collection,
        business_id: str,
        processed_reviews: list[dict],
        scraped_at: datetime,
    ) -> None:
        for item in processed_reviews:
            owner_reply_text = str(item.get("owner_reply", "") or "").strip()
            owner_reply_time = str(item.get("owner_reply_relative_time", "") or "").strip()
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
            review_payload["fingerprint"] = self._review_fingerprint(review_payload)

            await reviews_collection.update_one(
                {"business_id": business_id, "fingerprint": review_payload["fingerprint"]},
                {
                    "$set": review_payload,
                    "$setOnInsert": {"created_at": scraped_at},
                },
                upsert=True,
            )

    def _validate_business_name(self, name: str) -> str:
        cleaned = re.sub(r"\s+", " ", str(name or "")).strip()
        if not cleaned:
            raise ValueError("Business name is required.")
        if len(cleaned) < 3:
            raise ValueError("Business name must contain at least 3 characters.")
        return cleaned

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _normalize_scraped_review(self, review: dict[str, Any]) -> dict[str, Any]:
        item = dict(review)
        owner_reply = item.get("owner_reply")
        if isinstance(owner_reply, dict):
            item["owner_reply"] = str(owner_reply.get("text", "") or "").strip()
            item["owner_reply_relative_time"] = str(owner_reply.get("relative_time", "") or "").strip()
        elif isinstance(owner_reply, str):
            item["owner_reply"] = owner_reply.strip()
            item["owner_reply_relative_time"] = ""
        else:
            item["owner_reply"] = ""
            item["owner_reply_relative_time"] = ""
        return item

    def _normalize_stored_review(self, review: dict[str, Any]) -> dict[str, Any]:
        item = dict(review)
        owner_reply = item.get("owner_reply")
        if isinstance(owner_reply, dict):
            item["owner_reply"] = str(owner_reply.get("text", "") or "").strip()
            item["owner_reply_relative_time"] = str(owner_reply.get("relative_time", "") or "").strip()
        elif isinstance(owner_reply, str):
            item["owner_reply"] = owner_reply.strip()
            item["owner_reply_relative_time"] = str(item.get("owner_reply_relative_time", "") or "").strip()
        else:
            item["owner_reply"] = ""
            item["owner_reply_relative_time"] = ""

        item["source"] = str(item.get("source", "google_maps") or "google_maps")
        item["author_name"] = str(item.get("author_name", "") or "").strip()
        item["text"] = str(item.get("text", "") or "").strip()
        item["relative_time"] = str(item.get("relative_time", "") or "").strip()
        item["review_id"] = str(item.get("review_id") or item.get("id") or "").strip() or None
        return item

    def _resolve_reanalysis_batchers(self, batchers: list[str] | None) -> list[str]:
        source = batchers if batchers else settings.analysis_reanalyze_default_batchers
        normalized: list[str] = []
        for raw in source:
            value = self._normalize_text(str(raw or "")).replace("-", "_").replace(" ", "_")
            if not value:
                continue
            if value not in self._SUPPORTED_REANALYZE_BATCHERS:
                supported = ", ".join(sorted(self._SUPPORTED_REANALYZE_BATCHERS))
                raise ValueError(f"Unknown batcher '{raw}'. Supported: {supported}.")
            if value not in normalized:
                normalized.append(value)

        if not normalized:
            raise ValueError("At least one valid batcher is required.")
        return normalized

    def _build_reanalysis_batches(
        self,
        reviews: list[dict[str, Any]],
        *,
        batcher_names: list[str],
        batch_size: int,
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        if not reviews:
            return []

        batch_size = max(10, min(batch_size, 120))
        text_reviews = [item for item in reviews if bool(item.get("has_text") or item.get("text"))]
        source_reviews = text_reviews or reviews

        batches: list[tuple[str, list[dict[str, Any]]]] = []
        for batcher_name in batcher_names:
            if batcher_name == "latest_text":
                selected = source_reviews[:batch_size]
            elif batcher_name == "low_rating_focus":
                selected = self._build_priority_batch(
                    source_reviews,
                    batch_size=batch_size,
                    primary_predicate=lambda item: self._safe_rating(item) <= 3.0,
                )
            elif batcher_name == "high_rating_focus":
                selected = self._build_priority_batch(
                    source_reviews,
                    batch_size=batch_size,
                    primary_predicate=lambda item: self._safe_rating(item) >= 4.0,
                )
            elif batcher_name == "balanced_rating":
                selected = self._build_balanced_rating_batch(source_reviews, batch_size=batch_size)
            else:
                selected = []

            if selected:
                batches.append((batcher_name, selected))

        return batches

    def _build_priority_batch(
        self,
        reviews: list[dict[str, Any]],
        *,
        batch_size: int,
        primary_predicate,
    ) -> list[dict[str, Any]]:
        primary: list[dict[str, Any]] = []
        secondary: list[dict[str, Any]] = []

        for item in reviews:
            if primary_predicate(item):
                primary.append(item)
            else:
                secondary.append(item)

        return (primary + secondary)[:batch_size]

    def _build_balanced_rating_batch(self, reviews: list[dict[str, Any]], *, batch_size: int) -> list[dict[str, Any]]:
        buckets: dict[int, list[dict[str, Any]]] = {star: [] for star in range(1, 6)}
        for item in reviews:
            rating = self._safe_rating(item)
            star = int(round(rating))
            star = min(max(star, 1), 5)
            buckets[star].append(item)

        selected: list[dict[str, Any]] = []
        used_ids: set[str] = set()

        while len(selected) < batch_size:
            added = False
            for star in range(5, 0, -1):
                if not buckets[star]:
                    continue
                candidate = buckets[star].pop(0)
                identity = self._review_identity(candidate)
                if identity in used_ids:
                    continue
                used_ids.add(identity)
                selected.append(candidate)
                added = True
                if len(selected) >= batch_size:
                    break
            if not added:
                break

        if len(selected) >= batch_size:
            return selected[:batch_size]

        for item in reviews:
            identity = self._review_identity(item)
            if identity in used_ids:
                continue
            used_ids.add(identity)
            selected.append(item)
            if len(selected) >= batch_size:
                break

        return selected[:batch_size]

    def _review_identity(self, review: dict[str, Any]) -> str:
        parts = [
            str(review.get("review_id", "") or ""),
            str(review.get("id", "") or ""),
            self._normalize_text(str(review.get("author_name", "") or "")),
            self._normalize_text(str(review.get("text", "") or ""))[:120],
            str(round(self._safe_rating(review), 1)),
        ]
        return "|".join(parts)

    def _safe_rating(self, review: dict[str, Any]) -> float:
        try:
            value = float(review.get("rating", 0.0))
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(5.0, value))

    def _analysis_quality_score(self, analysis_payload: dict[str, Any]) -> float:
        score = 0.0
        sentiment = str(analysis_payload.get("overall_sentiment", "")).strip().lower()
        if sentiment in {"positive", "mixed", "negative"}:
            score += 1.0

        main_topics = analysis_payload.get("main_topics") or []
        strengths = analysis_payload.get("strengths") or []
        weaknesses = analysis_payload.get("weaknesses") or []
        reply = str(analysis_payload.get("suggested_owner_reply", "") or "").strip()

        score += min(len(main_topics), 8) * 1.2
        score += min(len(strengths), 8) * 1.0
        score += min(len(weaknesses), 8) * 0.8
        score += min(len(reply), 320) / 80.0
        return score

    def _merge_reanalysis_runs(self, run_results: list[dict[str, Any]]) -> dict[str, Any]:
        if not run_results:
            raise RuntimeError("No reanalysis runs available to merge.")

        sentiment_counter: Counter[str] = Counter()
        for run in run_results:
            sentiment = str(run.get("analysis", {}).get("overall_sentiment", "")).strip().lower()
            if sentiment in {"positive", "mixed", "negative"}:
                sentiment_counter[sentiment] += 1
        overall_sentiment = sentiment_counter.most_common(1)[0][0] if sentiment_counter else "mixed"

        main_topics = self._merge_reanalysis_terms(run_results, key="main_topics", limit=8)
        strengths = self._merge_reanalysis_terms(run_results, key="strengths", limit=8)
        weaknesses = self._merge_reanalysis_terms(run_results, key="weaknesses", limit=8)

        best_run = max(run_results, key=lambda run: float(run.get("quality_score", 0.0)))
        suggested_owner_reply = str(best_run.get("analysis", {}).get("suggested_owner_reply", "") or "").strip()
        if not suggested_owner_reply:
            for run in run_results:
                fallback_reply = str(run.get("analysis", {}).get("suggested_owner_reply", "") or "").strip()
                if fallback_reply:
                    suggested_owner_reply = fallback_reply
                    break
        if not suggested_owner_reply:
            suggested_owner_reply = (
                "Gracias por las reseñas. Estamos revisando vuestra experiencia para mejorar el servicio."
            )

        return {
            "overall_sentiment": overall_sentiment,
            "main_topics": main_topics,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "suggested_owner_reply": suggested_owner_reply,
        }

    def _merge_reanalysis_terms(self, run_results: list[dict[str, Any]], *, key: str, limit: int) -> list[str]:
        score_by_term: Counter[str] = Counter()
        display_value_by_term: dict[str, str] = {}

        for run in run_results:
            terms = run.get("analysis", {}).get(key) or []
            if not isinstance(terms, list):
                continue
            for index, raw_term in enumerate(terms):
                term = str(raw_term or "").strip()
                normalized = self._normalize_text(term)
                if not normalized:
                    continue
                if normalized not in display_value_by_term:
                    display_value_by_term[normalized] = term
                score_by_term[normalized] += max(1, 10 - index)

        ranked = sorted(score_by_term.items(), key=lambda item: item[1], reverse=True)
        return [display_value_by_term[normalized] for normalized, _ in ranked[:limit]]

    def _review_fingerprint(self, review: dict[str, Any]) -> str:
        parts = [
            str(review.get("business_id", "")),
            str(review.get("source", "")),
            str(review.get("review_id", "")),
            self._normalize_text(str(review.get("author_name", ""))),
            str(review.get("rating", 0.0)),
            self._normalize_text(str(review.get("relative_time", ""))),
            self._normalize_text(str(review.get("text", ""))),
        ]
        base = "|".join(parts)
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _coerce_pagination(self, *, page: int, page_size: int, max_page_size: int) -> tuple[int, int]:
        try:
            page_value = int(page)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid page. It must be an integer >= 1.") from exc
        try:
            page_size_value = int(page_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid page_size. It must be an integer >= 1.") from exc

        if page_value < 1:
            raise ValueError("Invalid page. It must be >= 1.")
        if page_size_value < 1:
            raise ValueError("Invalid page_size. It must be >= 1.")
        return page_value, min(page_size_value, max_page_size)

    def _pagination_payload(self, *, items: list[dict[str, Any]], page: int, page_size: int, total: int) -> dict[str, Any]:
        total_value = max(0, int(total))
        total_pages = ((total_value + page_size - 1) // page_size) if total_value else 0
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total_value,
            "total_pages": total_pages,
            "has_next": bool(total_pages and page < total_pages),
            "has_prev": bool(total_pages and page > 1),
        }

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

    def _serialize_analysis_job_doc(self, job_doc: dict[str, Any]) -> dict:
        payload = dict(job_doc)
        payload["job_id"] = str(payload.pop("_id"))
        return payload

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
