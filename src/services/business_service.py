from __future__ import annotations

import asyncio
import hashlib
import time
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
from src.scraper.tripadvisor import TripadvisorScraper
from src.services.analyze_business_use_case import AnalyzeBusinessUseCase
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.reanalyze_use_case import ReanalyzeUseCase
from src.workers.contracts import AnalysisGenerateTaskPayload, AnalyzeBusinessTaskPayload


class BusinessService:
    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"
    _SOURCE_PROFILES_COLLECTION = "source_profiles"
    _DATASETS_COLLECTION = "datasets"
    _SCRAPE_RUNS_COLLECTION = "scrape_runs"
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
    _SUPPORTED_FORCE_MODES = {
        "fallback_existing",
        "strict_rescrape",
    }
    _SCRAPE_SOURCES = ("google_maps", "tripadvisor")
    _PRIMARY_SOURCE = "google_maps"
    _ACTIVE_JOB_STATUSES = {"running", "retrying", "partial"}

    def __init__(
        self,
        *,
        scraper: GoogleMapsScraper | None = None,
        tripadvisor_scraper: TripadvisorScraper | None = None,
        preprocessor: ReviewPreprocessor | None = None,
        llm_analyzer: ReviewLLMAnalyzer | None = None,
        job_service: AnalysisJobService | None = None,
        query_service: BusinessQueryService | None = None,
        analyze_use_case: AnalyzeBusinessUseCase | None = None,
        reanalyze_use_case: ReanalyzeUseCase | None = None,
    ) -> None:
        self.scraper = scraper or type(self).build_default_scraper()
        self.tripadvisor_scraper = tripadvisor_scraper or type(self).build_default_tripadvisor_scraper()
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

    @classmethod
    def build_default_tripadvisor_scraper(cls) -> TripadvisorScraper:
        return TripadvisorScraper(
            headless=settings.scraper_headless,
            incognito=settings.scraper_incognito,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir="playwright-data-tripadvisor",
            browser_channel=settings.scraper_browser_channel,
            tripadvisor_url="https://www.tripadvisor.es",
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
            stealth_mode=settings.scraper_stealth_mode,
            harden_headless=settings.scraper_harden_headless,
            extra_chromium_args=settings.scraper_extra_chromium_args,
        )

    async def analyze_business(
        self,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict:
        del tripadvisor_max_pages, tripadvisor_pages_percent
        selected_force_mode = self._resolve_force_mode(force_mode)
        if selected_force_mode != "fallback_existing":
            raise ValueError(
                "force_mode is supported only in queued pipeline mode. "
                "Use POST /business/analyze/queue for strict rescrape behavior."
            )
        return await self.analyze_use_case.execute(
            name=name,
            force=force,
            strategy=strategy,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            progress_callback=progress_callback,
        )

    async def scrape_business_for_analysis_pipeline(
        self,
        name: str,
        *,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        sources: tuple[str, ...] | list[str] | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict[str, Any]:
        business_name = self._validate_business_name(name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        selected_sources = self._resolve_scrape_sources(sources)
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
        name_normalized = self._normalize_text(business_name)
        database = get_database()
        now = datetime.now(timezone.utc)

        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        datasets = database[self._DATASETS_COLLECTION]
        scrape_runs = database[self._SCRAPE_RUNS_COLLECTION]

        await self._emit_progress(
            progress_callback,
            "scrape_pipeline_started",
            "Scrape stage started.",
            {
                "name": business_name,
                "strategy": selected_strategy,
                "force": bool(force),
                "force_mode": selected_force_mode,
                "interactive_max_rounds": interactive_max_rounds,
                "html_scroll_max_rounds": html_scroll_max_rounds,
                "html_stable_rounds": html_stable_rounds,
                "tripadvisor_max_pages": effective_tripadvisor_max_pages,
                "tripadvisor_pages_percent": effective_tripadvisor_pages_percent,
                "sources": list(selected_sources),
            },
        )

        existing_business_doc = await businesses.find_one({"name_normalized": name_normalized})
        stored_review_count_before = 0
        if existing_business_doc:
            stored_review_count_before = await reviews.count_documents(
                {"business_id": str(existing_business_doc["_id"])}
            )
        if existing_business_doc and not force:
            existing_business_id = str(existing_business_doc["_id"])
            existing_review_count = stored_review_count_before
            if existing_review_count > 0:
                listing_payload = existing_business_doc.get("listing") if isinstance(existing_business_doc.get("listing"), dict) else {}
                await self._emit_progress(
                    progress_callback,
                    "scrape_pipeline_cache_hit",
                    "Skipping scrape because stored reviews already exist.",
                    {"business_id": existing_business_id, "review_count": existing_review_count},
                )
                return self._sanitize_response_payload(
                    {
                        "business_id": existing_business_id,
                        "name": str(existing_business_doc.get("name", "") or business_name),
                        "cached_scrape": True,
                        "strategy": selected_strategy,
                        "force_mode": selected_force_mode,
                        "listing": listing_payload,
                        "stats": existing_business_doc.get("stats", {}),
                        "review_count": existing_review_count,
                        "stored_review_count_before": stored_review_count_before,
                        "stored_review_count_after": existing_review_count,
                        "scrape_produced_new_reviews": False,
                        "scraped_review_count": existing_business_doc.get("scraped_review_count"),
                        "processed_review_count": existing_business_doc.get("processed_review_count"),
                        "listing_total_reviews": listing_payload.get("total_reviews") if isinstance(listing_payload, dict) else None,
                        "sources": {},
                        "failed_sources": {},
                    }
                )

        source_tasks: dict[str, asyncio.Task[tuple[dict[str, Any], list[dict[str, Any]]]]] = {}
        if "google_maps" in selected_sources:
            source_tasks["google_maps"] = asyncio.create_task(
                self._scrape_business_page(
                    business_name,
                    strategy=selected_strategy,
                    interactive_max_rounds=interactive_max_rounds,
                    html_scroll_max_rounds=html_scroll_max_rounds,
                    html_stable_rounds=html_stable_rounds,
                    progress_callback=self._build_source_progress_callback(
                        progress_callback=progress_callback,
                        source="google_maps",
                    ),
                )
            )
        if "tripadvisor" in selected_sources:
            source_tasks["tripadvisor"] = asyncio.create_task(
                self._scrape_tripadvisor_business_page(
                    business_name,
                    max_pages=effective_tripadvisor_max_pages,
                    pages_percent=effective_tripadvisor_pages_percent,
                    progress_callback=self._build_source_progress_callback(
                        progress_callback=progress_callback,
                        source="tripadvisor",
                    ),
                )
            )
        source_results: dict[str, dict[str, Any]] = {}
        failed_sources: dict[str, str] = {}
        gathered = await asyncio.gather(*source_tasks.values(), return_exceptions=True)
        for source, result in zip(source_tasks.keys(), gathered):
            if isinstance(result, Exception):
                failed_sources[source] = str(result)
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
            processed_reviews = self.preprocessor.process(normalized_raw_reviews)
            source_results[source] = {
                "listing_payload": listing_payload,
                "raw_reviews": raw_reviews,
                "processed_reviews": processed_reviews,
                "scraped_review_count": len(raw_reviews),
                "processed_review_count": len(processed_reviews),
                "stats": self.preprocessor.compute_stats(processed_reviews),
            }

        if not source_results:
            raise RuntimeError(
                "All configured sources failed during scrape stage. "
                + "; ".join(f"{source}: {error}" for source, error in failed_sources.items())
            )

        primary_source = self._PRIMARY_SOURCE if self._PRIMARY_SOURCE in source_results else next(iter(source_results))
        primary_result = source_results[primary_source]
        listing_payload = primary_result["listing_payload"]
        stats = primary_result["stats"]
        scraped_review_count = sum(
            int(payload.get("scraped_review_count", 0)) for payload in source_results.values()
        )
        processed_review_count = sum(
            int(payload.get("processed_review_count", 0)) for payload in source_results.values()
        )

        business_doc = await businesses.find_one_and_update(
            {"name_normalized": name_normalized},
            {
                "$set": {
                    "name": business_name,
                    "name_normalized": name_normalized,
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
        source_runtime: dict[str, dict[str, Any]] = {}
        dataset_review_count_total = 0
        for source in (item for item in selected_sources if item in source_results):
            payload = source_results[source]
            source_profile = await self._get_or_create_source_profile(
                source_profiles_collection=source_profiles,
                business_id=business_id,
                source=source,
                name_normalized=name_normalized,
                listing_payload=payload["listing_payload"],
                now=now,
            )
            source_profile_id = str(source_profile["_id"])

            legacy_dataset_result = await self._package_legacy_reviews_into_dataset(
                reviews_collection=reviews,
                datasets_collection=datasets,
                source_profiles_collection=source_profiles,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                now=now,
            )
            legacy_dataset_id = legacy_dataset_result.get("dataset_id")

            scrape_run = await self._create_scrape_run(
                scrape_runs_collection=scrape_runs,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                strategy=selected_strategy,
                force=bool(force),
                force_mode=selected_force_mode,
                now=now,
            )
            scrape_run_id = str(scrape_run["_id"])
            await source_profiles.update_one(
                {"_id": source_profile["_id"]},
                {
                    "$inc": {"metrics.total_runs": 1},
                    "$set": {"updated_at": now},
                },
            )

            scrape_dataset = await self._create_dataset_snapshot(
                datasets_collection=datasets,
                business_id=business_id,
                source_profile_id=source_profile_id,
                source=source,
                scrape_run_id=scrape_run_id,
                now=now,
            )
            scrape_dataset_id = str(scrape_dataset["_id"])

            await self._upsert_reviews(
                reviews_collection=reviews,
                business_id=business_id,
                processed_reviews=payload["processed_reviews"],
                scraped_at=now,
                source_profile_id=source_profile_id,
                dataset_id=scrape_dataset_id,
                scrape_run_id=scrape_run_id,
            )
            scrape_dataset_review_count = await reviews.count_documents(
                {"business_id": business_id, "dataset_id": scrape_dataset_id}
            )
            dataset_review_count_total += scrape_dataset_review_count
            dataset_status = "ready" if scrape_dataset_review_count > 0 else "empty"
            await datasets.update_one(
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
                await source_profiles.update_one(
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

        review_count = await reviews.count_documents({"business_id": business_id})
        scrape_produced_new_reviews = bool(
            any(int(runtime.get("dataset_review_count", 0)) > 0 for runtime in source_runtime.values())
        )
        strict_rescrape_failed = bool(
            bool(force)
            and selected_force_mode == "strict_rescrape"
            and not scrape_produced_new_reviews
        )
        for runtime in source_runtime.values():
            await self._finalize_scrape_run(
                scrape_runs_collection=scrape_runs,
                scrape_run_id=str(runtime["scrape_run_id"]),
                now=now,
                status="failed" if strict_rescrape_failed else "done",
                metrics={
                    "scraped_review_count": int(runtime.get("scraped_review_count", 0)),
                    "processed_review_count": int(runtime.get("processed_review_count", 0)),
                    "stored_review_count_before": stored_review_count_before,
                    "stored_review_count_after": review_count,
                    "dataset_review_count": int(runtime.get("dataset_review_count", 0)),
                },
                dataset_id=str(runtime["scrape_dataset_id"]),
            )

        primary_runtime = source_runtime[primary_source]
        active_runtime = primary_runtime
        if int(primary_runtime.get("dataset_review_count", 0)) <= 0:
            for runtime in source_runtime.values():
                if int(runtime.get("dataset_review_count", 0)) > 0:
                    active_runtime = runtime
                    break

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
        await businesses.update_one(
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
                    "strategy": selected_strategy,
                    "force_mode": selected_force_mode,
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
                    "stored_review_count_before": stored_review_count_before,
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
                "stored_review_count_before": stored_review_count_before,
                "stored_review_count_after": review_count,
                "dataset_review_count": dataset_review_count_total,
                "dataset_id": scrape_dataset_id,
                "analysis_dataset_id": analysis_dataset_id,
                "legacy_dataset_id": legacy_dataset_id,
                "source_profile_id": source_profile_id,
                "scrape_run_id": scrape_run_id,
                "scrape_produced_new_reviews": scrape_produced_new_reviews,
                "sources": source_runtime,
                "failed_sources": failed_sources,
            },
        )
        return self._sanitize_response_payload(
            {
                "business_id": business_id,
                "name": business_name,
                "cached_scrape": False,
                "strategy": selected_strategy,
                "force_mode": selected_force_mode,
                "listing": listing_payload,
                "stats": stats,
                "review_count": review_count,
                "scraped_review_count": scraped_review_count,
                "processed_review_count": processed_review_count,
                "stored_review_count_before": stored_review_count_before,
                "stored_review_count_after": review_count,
                "dataset_review_count": dataset_review_count_total,
                "dataset_id": scrape_dataset_id,
                "analysis_dataset_id": analysis_dataset_id,
                "legacy_dataset_id": legacy_dataset_id,
                "source_profile_id": source_profile_id,
                "scrape_run_id": scrape_run_id,
                "scrape_produced_new_reviews": scrape_produced_new_reviews,
                "listing_total_reviews": listing_payload.get("total_reviews"),
                "sources": source_runtime,
                "failed_sources": failed_sources,
            }
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
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
    ) -> dict:
        return await self.reanalyze_use_case.execute(
            business_id=business_id,
            dataset_id=dataset_id,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
        )

    async def enqueue_business_analysis_job(
        self,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
    ) -> dict:
        business_name = self._validate_business_name(name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        name_normalized = self._normalize_text(business_name)
        task_payload = AnalyzeBusinessTaskPayload(
            name=business_name,
            force=bool(force),
            strategy=selected_strategy,
            force_mode=selected_force_mode,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            tripadvisor_max_pages=tripadvisor_max_pages,
            tripadvisor_pages_percent=tripadvisor_pages_percent,
        )
        google_job = await self.job_service.enqueue_job(
            task_payload=task_payload,
            name_normalized=name_normalized,
            queue_name="scrape_google_maps",
            job_type="business_analyze",
        )
        tripadvisor_job = await self.job_service.enqueue_job(
            task_payload=task_payload,
            name_normalized=name_normalized,
            queue_name="scrape_tripadvisor",
            job_type="business_analyze",
        )
        primary_job_id = str(google_job.get("job_id", "")).strip()
        return self._sanitize_response_payload(
            {
                "job_id": primary_job_id,
                "primary_job_id": primary_job_id,
                "status": "queued",
                "name": business_name,
                "jobs_by_source": {
                    "google_maps": google_job,
                    "tripadvisor": tripadvisor_job,
                },
            }
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

    async def delete_business_analysis_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    async def stop_business_scrape_job(
        self,
        *,
        job_id: str,
        continue_analysis_if_google: bool = True,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
    ) -> dict[str, Any]:
        job_payload = await self.job_service.get_job(job_id=job_id)
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()
        job_type = str(job_payload.get("job_type") or "").strip().lower()
        if job_type != "business_analyze" or queue_name not in {"scrape", "scrape_google_maps", "scrape_tripadvisor"}:
            raise ValueError(
                "Only scraping jobs can be stopped with this endpoint "
                "(queue_name in scrape/scrape_google_maps/scrape_tripadvisor)."
            )

        cancel_result = await self.job_service.request_job_cancellation(
            job_id=job_id,
            reason="Manual scrape stop requested via API.",
        )

        safe_wait_seconds = max(0.5, float(wait_active_stop_seconds))
        safe_poll_seconds = max(0.1, float(poll_seconds))
        started_wait_at = time.monotonic()
        timed_out_waiting_stop = False
        final_job_payload = cancel_result

        while True:
            try:
                current = await self.job_service.get_job(job_id=job_id)
            except LookupError:
                break
            final_job_payload = current
            current_status = str(current.get("status") or "").strip().lower()
            if current_status not in self._ACTIVE_JOB_STATUSES:
                break
            if (time.monotonic() - started_wait_at) >= safe_wait_seconds:
                timed_out_waiting_stop = True
                break
            await asyncio.sleep(safe_poll_seconds)

        is_google_scrape_queue = queue_name in {"scrape", "scrape_google_maps"}
        analysis_already_handed_off = (
            str(final_job_payload.get("queue_name") or "").strip().lower() == "analysis"
            or str(final_job_payload.get("job_type") or "").strip().lower() == "analysis_generate"
        )
        continue_analysis_requested = bool(continue_analysis_if_google and is_google_scrape_queue)

        analysis_enqueue_result: dict[str, Any] | None = None
        continue_analysis_note: str | None = None
        if continue_analysis_requested and not analysis_already_handed_off:
            business_id = await self._resolve_business_id_for_scrape_job(final_job_payload)
            if business_id:
                analysis_task_payload = AnalysisGenerateTaskPayload(
                    business_id=business_id,
                    source_job_id=str(job_id),
                )
                analysis_enqueue_result = await self.job_service.enqueue_analysis_generate_job(
                    task_payload=analysis_task_payload
                )
                continue_analysis_note = "Analysis job was enqueued after stopping scrape."
            else:
                continue_analysis_note = (
                    "Scrape stop requested, but analysis could not be enqueued yet "
                    "because no business_id was resolved from current data."
                )
        elif continue_analysis_requested and analysis_already_handed_off:
            continue_analysis_note = "Analysis flow was already handed off before stop completion."
        elif not continue_analysis_requested:
            continue_analysis_note = "No analysis continuation was requested for this source."

        return self._sanitize_response_payload(
            {
                "job_id": str(job_id),
                "queue_name": queue_name,
                "cancel_requested": True,
                "status": final_job_payload.get("status"),
                "timed_out_waiting_stop": timed_out_waiting_stop,
                "continue_analysis_requested": continue_analysis_requested,
                "analysis_already_handed_off": analysis_already_handed_off,
                "analysis_enqueue_result": analysis_enqueue_result,
                "note": continue_analysis_note,
            }
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
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> tuple[dict, list[dict]]:
        effective_interactive_max_rounds = self._resolve_optional_int_override(
            value=interactive_max_rounds,
            fallback=max(1, settings.scraper_interactive_max_rounds),
            min_value=1,
            field_name="interactive_max_rounds",
        )
        effective_html_scroll_max_rounds = self._resolve_optional_int_override(
            value=html_scroll_max_rounds,
            fallback=max(0, settings.scraper_html_scroll_max_rounds),
            min_value=0,
            field_name="html_scroll_max_rounds",
        )
        effective_html_stable_rounds = self._resolve_optional_int_override(
            value=html_stable_rounds,
            fallback=max(2, settings.scraper_html_stable_rounds),
            min_value=2,
            field_name="html_stable_rounds",
        )

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

            await self._emit_progress(
                progress_callback,
                "scraper_reviews_started",
                "Starting reviews extraction.",
                {
                    "strategy": strategy,
                    "interactive_max_rounds": effective_interactive_max_rounds,
                    "html_scroll_max_rounds": effective_html_scroll_max_rounds,
                    "html_stable_rounds": effective_html_stable_rounds,
                },
            )
            reviews = await self.scraper.extract_reviews(
                strategy=strategy,
                max_rounds=effective_interactive_max_rounds,
                html_scroll_max_rounds=effective_html_scroll_max_rounds,
                html_stable_rounds=effective_html_stable_rounds,
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

    def _build_source_progress_callback(
        self,
        *,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        source: str,
    ) -> Callable[[dict[str, Any]], Awaitable[None] | None] | None:
        if progress_callback is None:
            return None

        async def _source_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "scraper_source_progress")
            message = str(event.get("message", "") or "Scraper source progress.")
            data = event.get("data") if isinstance(event.get("data"), dict) else {}
            payload_data = {"source": source, **data}
            await self._emit_progress(
                progress_callback,
                stage,
                message,
                payload_data,
            )

        return _source_progress

    async def _scrape_tripadvisor_business_page(
        self,
        business_name: str,
        *,
        max_pages: int | None = None,
        pages_percent: float | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        scraper = self.tripadvisor_scraper

        async def _scraper_progress(event: dict[str, Any]) -> None:
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_progress",
                "Review pagination in progress.",
                event,
            )

        await self._emit_progress(
            progress_callback,
            "scraper_starting",
            "Starting browser and scraper.",
            {"source": "tripadvisor"},
        )
        await scraper.start()
        try:
            await self._emit_progress(
                progress_callback,
                "scraper_search_started",
                "Searching business on TripAdvisor.",
                {"source": "tripadvisor", "query": business_name},
            )
            await scraper.search_business(business_name)
            await self._emit_progress(
                progress_callback,
                "scraper_search_completed",
                "Business page opened.",
                {"source": "tripadvisor", "query": business_name},
            )

            listing = await scraper.extract_listing()
            await self._emit_progress(
                progress_callback,
                "scraper_listing_completed",
                "Listing extracted.",
                {
                    "source": "tripadvisor",
                    "business_name": listing.get("business_name"),
                    "total_reviews": listing.get("total_reviews"),
                },
            )

            await self._emit_progress(
                progress_callback,
                "scraper_reviews_started",
                "Starting reviews extraction.",
                {
                    "source": "tripadvisor",
                    "tripadvisor_max_pages": max_pages,
                    "tripadvisor_pages_percent": pages_percent,
                },
            )
            reviews = await scraper.extract_reviews(
                max_rounds=0,
                html_scroll_max_rounds=0,
                html_stable_rounds=6,
                html_min_interval_s=max(0.2, settings.scraper_html_scroll_min_interval_s),
                html_max_interval_s=max(
                    max(0.2, settings.scraper_html_scroll_min_interval_s),
                    settings.scraper_html_scroll_max_interval_s,
                ),
                max_pages=max_pages,
                max_pages_percent=pages_percent,
                progress_callback=_scraper_progress,
            )
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_completed",
                "Reviews extracted.",
                {"source": "tripadvisor", "scraped_review_count": len(reviews)},
            )
            return listing, reviews
        finally:
            await scraper.close()

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

    def _resolve_scrape_sources(self, sources: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        if sources is None:
            return tuple(self._SCRAPE_SOURCES)

        normalized_sources: list[str] = []
        for raw in sources:
            normalized = (
                self._normalize_text(str(raw or ""))
                .replace("-", "_")
                .replace(" ", "_")
            )
            if not normalized:
                continue
            if normalized not in self._SCRAPE_SOURCES:
                supported = ", ".join(self._SCRAPE_SOURCES)
                raise ValueError(f"Unknown scrape source '{raw}'. Supported: {supported}.")
            if normalized not in normalized_sources:
                normalized_sources.append(normalized)

        if not normalized_sources:
            raise ValueError("At least one scrape source is required.")
        return tuple(normalized_sources)

    async def _resolve_business_id_for_scrape_job(self, job_payload: dict[str, Any]) -> str | None:
        result_payload = job_payload.get("result")
        if isinstance(result_payload, dict):
            result_business_id = str(result_payload.get("business_id") or "").strip()
            if result_business_id:
                return result_business_id

        payload_data = job_payload.get("payload")
        payload_name = ""
        if isinstance(payload_data, dict):
            payload_name = str(payload_data.get("name") or "").strip()
        if not payload_name:
            payload_name = str(job_payload.get("name") or "").strip()
        if not payload_name:
            return None

        name_normalized = self._normalize_text(payload_name)
        database = get_database()
        businesses = database[self._BUSINESSES_COLLECTION]
        business_doc = await businesses.find_one({"name_normalized": name_normalized}, projection={"_id": 1})
        if business_doc is None:
            return None
        return str(business_doc.get("_id") or "").strip() or None

    def _resolve_force_mode(self, force_mode: str | None) -> str:
        if force_mode is None:
            return "fallback_existing"

        raw_value = str(force_mode or "").strip()
        normalized = (
            self._normalize_text(raw_value)
            .replace("-", "_")
            .replace(" ", "_")
        )
        if normalized in {"", "default"}:
            normalized = "fallback_existing"
        if normalized not in self._SUPPORTED_FORCE_MODES:
            supported = ", ".join(sorted(self._SUPPORTED_FORCE_MODES))
            raise ValueError(f"Unknown force_mode '{raw_value}'. Supported: {supported}.")
        return normalized

    def _resolve_optional_int_override(
        self,
        *,
        value: int | None,
        fallback: int,
        min_value: int,
        field_name: str,
    ) -> int:
        if value is None:
            return int(fallback)
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be an integer.") from exc
        if parsed < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}.")
        return parsed

    def _resolve_optional_float_override(
        self,
        *,
        value: float | None,
        min_value: float,
        max_value: float,
        field_name: str,
    ) -> float:
        if value is None:
            raise ValueError(f"{field_name} is required when override validation is requested.")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a number.") from exc
        if parsed < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}.")
        if parsed > max_value:
            raise ValueError(f"{field_name} must be <= {max_value}.")
        return parsed

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
        source_profile_id: str | None = None,
        dataset_id: str | None = None,
        scrape_run_id: str | None = None,
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

    async def _get_or_create_source_profile(
        self,
        *,
        source_profiles_collection,
        business_id: str,
        source: str,
        name_normalized: str,
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

    async def _package_legacy_reviews_into_dataset(
        self,
        *,
        reviews_collection,
        datasets_collection,
        source_profiles_collection,
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

    async def _create_scrape_run(
        self,
        *,
        scrape_runs_collection,
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

    async def _create_dataset_snapshot(
        self,
        *,
        datasets_collection,
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

    async def _finalize_scrape_run(
        self,
        *,
        scrape_runs_collection,
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
