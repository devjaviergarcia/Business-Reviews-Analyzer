from __future__ import annotations

import asyncio
import html
import hashlib
import os
import re
import random
import time
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.config import settings
from src.database import get_database
from src.models.business import Listing, OwnerReply, Review
from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.scraper.google_maps import GoogleMapsScraper
from src.scraper.tripadvisor import TripadvisorScraper
from src.services.analyze_business_use_case import AnalyzeBusinessUseCase
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_query_service import BusinessQueryService
from src.services.tripadvisor_local_worker_control_service import (
    TripadvisorLocalWorkerControlService,
)
from src.services.reanalyze_use_case import ReanalyzeUseCase
from src.services.tripadvisor_session_service import TripadvisorSessionService
from src.workers.contracts import (
    AnalysisJobStatus,
    AnalysisGenerateTaskPayload,
    AnalyzeBusinessTaskPayload,
    parse_analyze_business_payload,
)


class ScrapeBotDetectedError(RuntimeError):
    """Raised when an anti-bot challenge is detected during scraping."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


class ScrapeNeedsHumanInterventionError(RuntimeError):
    """Raised when scraping must pause for human intervention."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


class BusinessService:
    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _COMMENTS_COLLECTION = "comments"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"
    _SOURCE_PROFILES_COLLECTION = "source_profiles"
    _DATASETS_COLLECTION = "datasets"
    _SCRAPE_RUNS_COLLECTION = "scrape_runs"
    _SCRAPE_DIAGNOSTICS_COLLECTION = "scrape_diagnostics"
    _ANTI_BOT_KEYWORDS = (
        "bot",
        "captcha",
        "robot",
        "verify you are human",
        "verifica que eres humano",
        "tráfico inusual",
        "unusual traffic",
        "security check",
        "automated access",
        "access denied",
        "forbidden",
        "blocked",
        "challenge",
        "not a robot",
        "no soy un robot",
    )
    _ANTI_BOT_STRONG_KEYWORDS = (
        "verify you are human",
        "verifica que eres humano",
        "tráfico inusual",
        "unusual traffic",
        "security check",
        "automated access",
        "access denied",
        "forbidden",
        "blocked",
        "not a robot",
        "no soy un robot",
    )
    _ANTI_BOT_CAPTCHA_COMPANION_KEYWORDS = (
        "challenge",
        "verify",
        "human",
        "security",
        "blocked",
        "denied",
        "tráfico inusual",
        "unusual traffic",
    )
    _ANTI_BOT_ROBOT_MARKERS = (
        "captcha__robot",
        "data-dd-captcha-robot",
        "no a un robot",
        "not a robot",
        "no soy un robot",
        "i am not a robot",
    )
    _ANTI_BOT_DATADOME_STRUCTURAL_MARKERS = (
        "ddv1-captcha-container",
        "captcha__frame",
        "captcha__human",
        "data-dd-captcha-human",
        "slidercontainer",
        "slidertext",
        "slidertarget",
        "slidermask",
        "captcha__puzzle",
        "captcha__audio",
    )
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
    _PROJECT_ROOT = Path(__file__).resolve().parents[2]
    _ARTIFACTS_ROOT = (_PROJECT_ROOT / "artifacts").resolve()

    def __init__(
        self,
        *,
        scraper: GoogleMapsScraper | None = None,
        tripadvisor_scraper: TripadvisorScraper | None = None,
        preprocessor: ReviewPreprocessor | None = None,
        llm_analyzer: ReviewLLMAnalyzer | None = None,
        report_builder: AdvancedBusinessReportBuilder | None = None,
        job_service: AnalysisJobService | None = None,
        query_service: BusinessQueryService | None = None,
        analyze_use_case: AnalyzeBusinessUseCase | None = None,
        reanalyze_use_case: ReanalyzeUseCase | None = None,
        tripadvisor_local_worker_control_service: TripadvisorLocalWorkerControlService | None = None,
    ) -> None:
        self.scraper = scraper or type(self).build_default_scraper()
        self.tripadvisor_scraper = tripadvisor_scraper or type(self).build_default_tripadvisor_scraper()
        self.preprocessor = preprocessor or ReviewPreprocessor()
        self.llm_analyzer = llm_analyzer or ReviewLLMAnalyzer()
        self.report_builder = report_builder or AdvancedBusinessReportBuilder()
        self.job_service = job_service or AnalysisJobService()
        self.query_service = query_service or BusinessQueryService()
        self.analyze_use_case = analyze_use_case or self._build_analyze_use_case()
        self.reanalyze_use_case = reanalyze_use_case or self._build_reanalyze_use_case()
        self.tripadvisor_local_worker_control_service = (
            tripadvisor_local_worker_control_service or TripadvisorLocalWorkerControlService()
        )

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
            build_advanced_report=self._build_advanced_report,
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
            build_advanced_report=self._build_advanced_report,
            businesses_collection_name=self._BUSINESSES_COLLECTION,
            reviews_collection_name=self._REVIEWS_COLLECTION,
            analyses_collection_name=self._ANALYSES_COLLECTION,
        )

    async def _build_advanced_report(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        reviews: list[dict[str, Any]],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        database = get_database()
        return await self.report_builder.build(
            business_id=str(business_id),
            business_name=str(business_name or "").strip(),
            listing=listing if isinstance(listing, dict) else {},
            stats=stats if isinstance(stats, dict) else {},
            reviews=reviews if isinstance(reviews, list) else [],
            analysis_payload=analysis_payload if isinstance(analysis_payload, dict) else {},
            businesses_collection=database[self._BUSINESSES_COLLECTION],
            analyses_collection=database[self._ANALYSES_COLLECTION],
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
            user_data_dir=settings.scraper_tripadvisor_user_data_dir,
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
                "Use POST /business/scrape/jobs for strict rescrape behavior."
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
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict[str, Any]:
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
        database = get_database()
        now = datetime.now(timezone.utc)

        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]
        comments = database[self._COMMENTS_COLLECTION]
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        datasets = database[self._DATASETS_COLLECTION]
        scrape_runs = database[self._SCRAPE_RUNS_COLLECTION]

        await self._emit_progress(
            progress_callback,
            "scrape_pipeline_started",
            "Scrape stage started.",
            {
                "name": canonical_business_name,
                "source_name": source_business_name,
                "canonical_name": canonical_business_name,
                "root_business_id": normalized_root_business_id,
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

        business_lookup_query: dict[str, Any]
        if root_business_object_id is not None:
            business_lookup_query = {"_id": root_business_object_id}
        else:
            business_lookup_query = {"name_normalized": canonical_name_normalized}

        existing_business_doc = await businesses.find_one(business_lookup_query)
        stored_review_count_before = 0
        stored_selected_review_count_before = 0
        stored_selected_review_counts_before: dict[str, int] = {
            source: 0 for source in selected_sources
        }
        if existing_business_doc:
            existing_business_id = str(existing_business_doc["_id"])
            stored_review_count_before = await reviews.count_documents(
                {"business_id": existing_business_id}
            )
            selected_counts_raw = await reviews.aggregate(
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
        if existing_business_doc and not force:
            existing_business_id = str(existing_business_doc["_id"])
            missing_sources = [
                source
                for source in selected_sources
                if int(stored_selected_review_counts_before.get(source, 0)) <= 0
            ]
            existing_review_count = stored_selected_review_count_before
            if existing_review_count > 0 and not missing_sources:
                listing_payload = existing_business_doc.get("listing") if isinstance(existing_business_doc.get("listing"), dict) else {}
                await self._emit_progress(
                    progress_callback,
                    "scrape_pipeline_cache_hit",
                    "Skipping scrape because stored reviews already exist for selected sources.",
                    {
                        "business_id": existing_business_id,
                        "review_count": existing_review_count,
                        "stored_review_count_before": stored_review_count_before,
                        "stored_selected_review_count_before": stored_selected_review_count_before,
                        "source_review_counts": stored_selected_review_counts_before,
                        "sources": list(selected_sources),
                    },
                )
                return self._sanitize_response_payload(
                    {
                        "business_id": existing_business_id,
                        "name": str(existing_business_doc.get("name", "") or canonical_business_name),
                        "canonical_name": canonical_business_name,
                        "source_name": source_business_name,
                        "cached_scrape": True,
                        "strategy": selected_strategy,
                        "force_mode": selected_force_mode,
                        "listing": listing_payload,
                        "stats": existing_business_doc.get("stats", {}),
                        "review_count": existing_review_count,
                        "stored_review_count_before": stored_review_count_before,
                        "stored_review_count_after": stored_review_count_before,
                        "stored_selected_review_count_before": stored_selected_review_count_before,
                        "stored_selected_review_count_after": stored_selected_review_count_before,
                        "source_review_counts": stored_selected_review_counts_before,
                        "scrape_produced_new_reviews": False,
                        "scraped_review_count": existing_business_doc.get("scraped_review_count"),
                        "processed_review_count": existing_business_doc.get("processed_review_count"),
                        "listing_total_reviews": listing_payload.get("total_reviews") if isinstance(listing_payload, dict) else None,
                        "sources": {},
                        "failed_sources": {},
                    }
                )

        source_results: dict[str, dict[str, Any]] = {}
        failed_sources: dict[str, str] = {}
        failed_source_errors: dict[str, Exception] = {}
        normalized_preloaded_source_payloads: dict[str, dict[str, Any]] = {}
        if isinstance(preloaded_source_payloads, dict):
            for raw_source, raw_payload in preloaded_source_payloads.items():
                source_key = str(raw_source or "").strip().lower()
                if source_key not in selected_sources:
                    continue
                if source_key not in self._SCRAPE_SOURCES:
                    continue
                if not isinstance(raw_payload, dict):
                    raise ValueError(
                        f"Invalid preloaded_source_payloads[{raw_source!r}]. "
                        "Expected an object with 'listing' and 'reviews'."
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

        for source in selected_sources:
            preloaded_payload = normalized_preloaded_source_payloads.get(source)
            if preloaded_payload is None:
                continue
            listing_payload = Listing(**preloaded_payload["listing"]).model_dump(mode="python")
            raw_reviews = [
                {
                    **item,
                    "source": str(item.get("source") or source),
                }
                for item in preloaded_payload["reviews"]
            ]
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

        source_tasks: dict[str, asyncio.Task[tuple[dict[str, Any], list[dict[str, Any]]]]] = {}
        if "google_maps" in selected_sources and "google_maps" not in source_results:
            source_tasks["google_maps"] = asyncio.create_task(
                self._scrape_business_page(
                    source_business_name,
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
        if "tripadvisor" in selected_sources and "tripadvisor" not in source_results:
            source_tasks["tripadvisor"] = asyncio.create_task(
                self._scrape_tripadvisor_business_page(
                    source_business_name,
                    max_pages=effective_tripadvisor_max_pages,
                    pages_percent=effective_tripadvisor_pages_percent,
                    progress_callback=self._build_source_progress_callback(
                        progress_callback=progress_callback,
                        source="tripadvisor",
                    ),
                )
            )
        if source_tasks:
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
            bot_failed_sources = [
                source_name
                for source_name, source_error in failed_source_errors.items()
                if isinstance(source_error, ScrapeBotDetectedError)
            ]
            if bot_failed_sources:
                raise ScrapeBotDetectedError(
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
            business_lookup_query,
            {
                "$set": {
                    "name": canonical_business_name,
                    "name_normalized": canonical_name_normalized,
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
                name_normalized=source_name_normalized,
                canonical_name_normalized=canonical_name_normalized,
                source_business_name=source_business_name,
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
            await self._upsert_job_comments(
                comments_collection=comments,
                business_id=business_id,
                business_name=canonical_business_name,
                name_normalized=canonical_name_normalized,
                source=source,
                source_job_id=normalized_source_job_id,
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
                "source_job_id": normalized_source_job_id,
                "sources": source_runtime,
                "failed_sources": failed_sources,
            },
        )
        return self._sanitize_response_payload(
            {
                "business_id": business_id,
                "name": canonical_business_name,
                "canonical_name": canonical_business_name,
                "source_name": source_business_name,
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
                "source_job_id": normalized_source_job_id,
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

    async def get_business_sources_overview(
        self,
        *,
        business_id: str,
        comments_preview_size: int = 5,
    ) -> dict[str, Any]:
        return await self.query_service.get_business_sources_overview(
            business_id=business_id,
            comments_preview_size=comments_preview_size,
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
        return await self.query_service.list_business_comments(
            business_id=business_id,
            source=source,
            scrape_type=scrape_type,
            page=page,
            page_size=page_size,
            rating_gte=rating_gte,
            rating_lte=rating_lte,
            order=order,
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

    async def enqueue_business_scrape_jobs(
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
        sources: tuple[str, ...] | list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict:
        business_name = self._validate_business_name(name)
        canonical_name_normalized = self._normalize_text(business_name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        selected_sources = self._resolve_scrape_sources(sources)
        await self._ensure_tripadvisor_worker_started_on_enqueue(selected_sources=selected_sources)
        root_business_doc = await self._ensure_root_business_on_enqueue(
            canonical_name=business_name,
            canonical_name_normalized=canonical_name_normalized,
        )
        root_business_id = str(root_business_doc.get("_id") or "").strip() or None

        source_names: dict[str, str] = {}
        for source in selected_sources:
            raw_source_name = (
                google_maps_name
                if source == "google_maps"
                else tripadvisor_name if source == "tripadvisor" else None
            )
            resolved_name = (
                self._validate_business_name(raw_source_name)
                if isinstance(raw_source_name, str) and raw_source_name.strip()
                else business_name
            )
            source_names[source] = resolved_name

        queue_by_source = {
            "google_maps": "scrape_google_maps",
            "tripadvisor": "scrape_tripadvisor",
        }
        jobs_by_source: dict[str, dict[str, Any]] = {}
        for source in selected_sources:
            source_business_name = source_names[source]
            source_name_normalized = self._normalize_text(source_business_name)
            task_payload = AnalyzeBusinessTaskPayload(
                name=source_business_name,
                canonical_name=business_name,
                canonical_name_normalized=canonical_name_normalized,
                source_name=source_business_name,
                source_name_normalized=source_name_normalized,
                root_business_id=root_business_id,
                force=bool(force),
                strategy=selected_strategy,
                force_mode=selected_force_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                tripadvisor_max_pages=tripadvisor_max_pages,
                tripadvisor_pages_percent=tripadvisor_pages_percent,
            )
            queued_job = await self.job_service.enqueue_job(
                task_payload=task_payload,
                name_normalized=source_name_normalized,
                queue_name=queue_by_source[source],
                job_type="business_analyze",
            )
            jobs_by_source[source] = queued_job

        primary_source = selected_sources[0]
        primary_job_id = str((jobs_by_source.get(primary_source) or {}).get("job_id", "")).strip()
        return self._sanitize_response_payload(
            {
                "job_id": primary_job_id,
                "primary_job_id": primary_job_id,
                "primary_source": primary_source,
                "status": "queued",
                "name": business_name,
                "canonical_name": business_name,
                "canonical_name_normalized": canonical_name_normalized,
                "business_id": root_business_id,
                "sources_requested": list(selected_sources),
                "source_names": source_names,
                "jobs_by_source": jobs_by_source,
            }
        )

    async def _ensure_root_business_on_enqueue(
        self,
        *,
        canonical_name: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        try:
            businesses = get_database()[self._BUSINESSES_COLLECTION]
        except RuntimeError:
            # Unit-test friendly fallback when database lifecycle is mocked out.
            return {
                "_id": None,
                "name": canonical_name,
                "name_normalized": canonical_name_normalized,
                "source": "multi_source",
                "created_at": now,
                "updated_at": now,
            }
        business_doc = await businesses.find_one_and_update(
            {"name_normalized": canonical_name_normalized},
            {
                "$set": {
                    "name": canonical_name,
                    "name_normalized": canonical_name_normalized,
                    "source": "multi_source",
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "listing": {},
                    "stats": {},
                    "review_count": 0,
                    "scraped_review_count": 0,
                    "processed_review_count": 0,
                    "last_scraped_at": None,
                    "active_dataset_id": None,
                    "created_at": now,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if business_doc is None:
            raise RuntimeError("Failed to create or fetch root business while enqueueing scrape jobs.")
        return business_doc

    async def _ensure_tripadvisor_worker_started_on_enqueue(
        self,
        *,
        selected_sources: tuple[str, ...],
    ) -> None:
        if "tripadvisor" not in selected_sources:
            return
        if not settings.tripadvisor_local_worker_autostart_on_enqueue:
            return
        if not settings.tripadvisor_local_worker_bridge_enabled:
            raise RuntimeError(
                "Tripadvisor local worker autostart is enabled, but bridge is disabled. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_ENABLED=true."
            )
        bridge_result = await self.tripadvisor_local_worker_control_service.ensure_started(
            use_xvfb=True,
            reason="business_scrape_jobs_enqueue",
        )
        worker_payload = bridge_result.get("worker")
        if isinstance(worker_payload, dict) and worker_payload.get("running") is True:
            return
        raise RuntimeError(
            "Tripadvisor local worker bridge did not confirm a running worker. "
            f"Bridge response: {bridge_result}"
        )

    async def enqueue_business_analysis_generate_job(
        self,
        *,
        business_id: str,
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
        source_job_id: str | None = None,
    ) -> dict[str, Any]:
        parsed_business_id = self._parse_object_id(business_id, field_name="business_id")
        businesses = get_database()[self._BUSINESSES_COLLECTION]
        business_doc = await businesses.find_one({"_id": parsed_business_id}, projection={"_id": 1})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        payload = AnalysisGenerateTaskPayload(
            business_id=str(parsed_business_id),
            dataset_id=str(dataset_id or "").strip() or None,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
            source_job_id=str(source_job_id or "").strip() or None,
        )
        return await self.job_service.enqueue_analysis_generate_job(task_payload=payload)

    async def get_scrape_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(job_payload)
        return job_payload

    async def commit_tripadvisor_live_capture(
        self,
        *,
        job_id: str,
        listing: dict[str, Any],
        reviews: list[dict[str, Any]],
        commit_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(listing, dict):
            raise ValueError("listing must be an object.")
        if not isinstance(reviews, list):
            raise ValueError("reviews must be an array.")

        existing_job = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing_job)
        queue_name = str(existing_job.get("queue_name") or "").strip().lower()
        if queue_name != "scrape_tripadvisor":
            raise ValueError("live commit is supported only for scrape_tripadvisor jobs.")

        status_value = str(existing_job.get("status") or "").strip().lower()
        if status_value == "done":
            return self._sanitize_response_payload(
                {
                    "job_id": str(job_id),
                    "status": "done",
                    "already_done": True,
                    "result": existing_job.get("result"),
                }
            )

        task_payload = parse_analyze_business_payload(existing_job)
        source_business_name = self._validate_business_name(task_payload.source_name or task_payload.name)
        canonical_business_name = self._validate_business_name(task_payload.canonical_name or task_payload.name)
        normalized_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        normalized_reviews = [dict(item) for item in reviews if isinstance(item, dict)]

        job_object_id = self._parse_object_id(job_id, field_name="job_id")
        await self.job_service.append_event(
            job_id=job_object_id,
            stage="live_commit_started",
            message="Live TripAdvisor capture commit started.",
            status=AnalysisJobStatus.RUNNING,
            data={
                "source": "tripadvisor",
                "review_count_received": len(normalized_reviews),
                "commit_reason": str(commit_reason or "").strip() or "live_session_capture",
                "metadata": normalized_metadata,
            },
        )

        async def _job_progress(event: dict[str, Any]) -> None:
            stage_value = str(event.get("stage", "") or "live_commit_progress")
            message_value = str(event.get("message", "") or "Live commit in progress.")
            raw_data = event.get("data")
            data_value = raw_data if isinstance(raw_data, dict) else {}
            await self.job_service.append_event(
                job_id=job_object_id,
                stage=f"live_{stage_value}",
                message=message_value,
                status=AnalysisJobStatus.RUNNING,
                data={"source": "tripadvisor", "live_commit": True, **data_value},
            )

        try:
            scrape_result = await self.scrape_business_for_analysis_pipeline(
                name=source_business_name,
                canonical_name=canonical_business_name,
                source_name=source_business_name,
                root_business_id=task_payload.root_business_id,
                force=True,
                strategy=task_payload.strategy,
                force_mode=task_payload.force_mode,
                interactive_max_rounds=task_payload.interactive_max_rounds,
                html_scroll_max_rounds=task_payload.html_scroll_max_rounds,
                html_stable_rounds=task_payload.html_stable_rounds,
                tripadvisor_max_pages=task_payload.tripadvisor_max_pages,
                tripadvisor_pages_percent=task_payload.tripadvisor_pages_percent,
                sources=("tripadvisor",),
                preloaded_source_payloads={
                    "tripadvisor": {
                        "listing": dict(listing),
                        "reviews": normalized_reviews,
                    }
                },
                source_job_id=str(job_id),
                progress_callback=_job_progress,
            )
        except Exception as exc:
            await self.job_service.append_event(
                job_id=job_object_id,
                stage="live_commit_failed",
                message="Live TripAdvisor capture commit failed.",
                status=AnalysisJobStatus.NEEDS_HUMAN,
                data={
                    "source": "tripadvisor",
                    "error": str(exc),
                },
            )
            raise

        result_payload = dict(scrape_result)
        result_payload["pipeline"] = {
            "worker": "live_commit",
            "source": "tripadvisor",
            "queue_name": "scrape_tripadvisor",
            "mode": "live_commit",
        }
        result_payload["live_commit"] = {
            "committed": True,
            "review_count_received": len(normalized_reviews),
            "commit_reason": str(commit_reason or "").strip() or "live_session_capture",
            "metadata": normalized_metadata,
        }

        await self.job_service.mark_done(job_id=job_object_id, result=result_payload)
        return self._sanitize_response_payload(
            {
                "job_id": str(job_id),
                "status": "done",
                "already_done": False,
                "result": result_payload,
            }
        )

    async def get_analysis_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(job_payload)
        return job_payload

    async def get_report_job(self, job_id: str) -> dict:
        job_payload = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(job_payload)
        return job_payload

    async def list_scrape_jobs(
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
            queue_names=["scrape_google_maps", "scrape_tripadvisor", "scrape"],
            job_type_filter="business_analyze",
        )

    async def list_scrape_job_comments(
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
        await self.get_scrape_job(job_id=job_id)
        return await self.query_service.list_job_comments(
            job_id=job_id,
            source=source,
            scrape_type=scrape_type,
            page=page,
            page_size=page_size,
            rating_gte=rating_gte,
            rating_lte=rating_lte,
            order=order,
        )

    async def list_tripadvisor_antibot_jobs(
        self,
        *,
        limit: int = 20,
        status_filter: str = "failed_or_needs_human",
    ) -> dict[str, Any]:
        normalized_status_filter = str(status_filter or "failed_or_needs_human").strip().lower()
        status_filter_map: dict[str, set[str] | None] = {
            "failed_or_needs_human": {"failed", "needs_human"},
            "failed": {"failed"},
            "needs_human": {"needs_human"},
            "all": None,
        }
        if normalized_status_filter not in status_filter_map:
            allowed_values = ", ".join(sorted(status_filter_map.keys()))
            raise ValueError(
                f"Invalid status_filter={status_filter!r}. Allowed values: {allowed_values}."
            )

        safe_limit = max(1, min(int(limit), 200))
        statuses = status_filter_map[normalized_status_filter]
        query: dict[str, Any] = {
            "queue_name": "scrape_tripadvisor",
            "job_type": "business_analyze",
        }
        if statuses is not None:
            query["status"] = {"$in": sorted(statuses)}

        scan_limit = min(2000, max(safe_limit * 8, safe_limit))
        jobs_collection = get_database()[self._JOBS_COLLECTION]
        docs = (
            await jobs_collection.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .limit(scan_limit)
            .to_list(length=scan_limit)
        )

        items: list[dict[str, Any]] = []
        for doc in docs:
            summary = self._summarize_tripadvisor_antibot_job(doc)
            if summary is None:
                continue
            items.append(summary)
            if len(items) >= safe_limit:
                break

        return self._sanitize_response_payload(
            {
                "limit": safe_limit,
                "status_filter": normalized_status_filter,
                "scanned_jobs": len(docs),
                "matched_jobs": len(items),
                "items": items,
            }
        )

    async def relaunch_tripadvisor_antibot_jobs(
        self,
        *,
        limit: int = 20,
        reason: str | None = None,
        status_filter: str = "failed_or_needs_human",
    ) -> dict[str, Any]:
        await self._ensure_tripadvisor_session_available_for_relaunch(
            operation="relaunch_tripadvisor_antibot_jobs",
        )
        list_result = await self.list_tripadvisor_antibot_jobs(
            limit=limit,
            status_filter=status_filter,
        )
        items = list_result.get("items") if isinstance(list_result, dict) else []
        if not isinstance(items, list):
            items = []

        safe_limit = max(1, min(int(limit), 200))
        relaunched: list[str] = []
        errors: list[dict[str, str]] = []
        for item in items[:safe_limit]:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("job_id") or "").strip()
            if not job_id:
                continue
            try:
                await self.job_service.relaunch_job(
                    job_id=job_id,
                    reason=reason or "Relaunched latest TripAdvisor anti-bot failed jobs via API.",
                )
                relaunched.append(job_id)
            except Exception as exc:  # noqa: BLE001
                errors.append({"job_id": job_id, "error": str(exc)})

        return self._sanitize_response_payload(
            {
                "requested_limit": safe_limit,
                "status_filter": str(status_filter or "failed_or_needs_human").strip().lower(),
                "matched_jobs": len(items),
                "relaunched_jobs": relaunched,
                "errors": errors,
            }
        )

    async def list_analysis_jobs(
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
            queue_names=["analysis"],
            job_type_filter="analysis_generate",
        )

    async def list_report_jobs(
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
            queue_names=["report"],
            job_type_filter="report_generate",
        )

    def resolve_report_artifact_path(self, *, path: str) -> Path:
        raw_value = str(path or "").strip()
        if not raw_value:
            raise ValueError("Artifact path is required.")

        candidate_values: list[Path] = []
        as_path = Path(raw_value)
        if as_path.is_absolute():
            candidate_values.append(as_path)
        else:
            candidate_values.append(self._PROJECT_ROOT / as_path)

        if raw_value.startswith("/app/"):
            candidate_values.append(self._PROJECT_ROOT / raw_value.removeprefix("/app/"))

        checked_candidates: list[str] = []
        for candidate in candidate_values:
            try:
                resolved = candidate.expanduser().resolve(strict=True)
            except (FileNotFoundError, RuntimeError, OSError):
                checked_candidates.append(str(candidate))
                continue
            if not resolved.is_file():
                checked_candidates.append(str(resolved))
                continue
            try:
                resolved.relative_to(self._ARTIFACTS_ROOT)
            except ValueError:
                checked_candidates.append(str(resolved))
                continue
            return resolved

        checked_text = ", ".join(checked_candidates[:4]) if checked_candidates else raw_value
        raise FileNotFoundError(f"Artifact not found or out of allowed scope: {checked_text}")

    async def delete_business(
        self,
        *,
        business_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
        delete_related_jobs: bool = True,
    ) -> dict[str, Any]:
        parsed_business_id = self._parse_object_id(business_id, field_name="business_id")
        normalized_business_id = str(parsed_business_id)
        database = get_database()

        businesses = database[self._BUSINESSES_COLLECTION]
        reviews = database[self._REVIEWS_COLLECTION]
        comments = database[self._COMMENTS_COLLECTION]
        analyses = database[self._ANALYSES_COLLECTION]
        source_profiles = database[self._SOURCE_PROFILES_COLLECTION]
        datasets = database[self._DATASETS_COLLECTION]
        scrape_runs = database[self._SCRAPE_RUNS_COLLECTION]
        jobs_collection = database[self._JOBS_COLLECTION]

        business_doc = await businesses.find_one({"_id": parsed_business_id})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        canonical_name_normalized = str(business_doc.get("name_normalized") or "").strip()
        deleted_jobs: list[dict[str, Any]] = []
        job_delete_errors: list[dict[str, str]] = []

        if delete_related_jobs:
            jobs_query = self._build_related_business_jobs_query(
                business_id=normalized_business_id,
                canonical_name_normalized=canonical_name_normalized,
            )
            related_jobs_docs = await jobs_collection.find(
                jobs_query,
                projection={"_id": 1},
            ).to_list(length=None)
            for job_doc in related_jobs_docs:
                current_job_id = str(job_doc.get("_id") or "").strip()
                if not current_job_id:
                    continue
                try:
                    delete_result = await self.job_service.delete_job(
                        job_id=current_job_id,
                        wait_active_stop_seconds=wait_active_stop_seconds,
                        poll_seconds=poll_seconds,
                        force_delete_on_timeout=force_delete_on_timeout,
                    )
                    deleted_jobs.append(
                        {
                            "job_id": current_job_id,
                            "status_at_delete": delete_result.get("status_at_delete"),
                            "forced_delete": bool(delete_result.get("forced_delete")),
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    job_delete_errors.append(
                        {
                            "job_id": current_job_id,
                            "error": str(exc),
                        }
                    )

        reviews_result = await reviews.delete_many({"business_id": normalized_business_id})
        comments_result = await comments.delete_many({"business_id": normalized_business_id})
        analyses_result = await analyses.delete_many({"business_id": normalized_business_id})
        source_profiles_result = await source_profiles.delete_many({"business_id": normalized_business_id})
        datasets_result = await datasets.delete_many({"business_id": normalized_business_id})
        scrape_runs_result = await scrape_runs.delete_many({"business_id": normalized_business_id})
        business_delete_result = await businesses.delete_one({"_id": parsed_business_id})

        if business_delete_result.deleted_count == 0:
            raise RuntimeError(f"Business '{business_id}' could not be deleted.")

        return self._sanitize_response_payload(
            {
                "business_id": normalized_business_id,
                "deleted": True,
                "business_name": str(business_doc.get("name") or ""),
                "canonical_name_normalized": canonical_name_normalized or None,
                "delete_related_jobs": bool(delete_related_jobs),
                "jobs": {
                    "deleted_count": len(deleted_jobs),
                    "deleted_jobs": deleted_jobs,
                    "errors": job_delete_errors,
                },
                "collections": {
                    "businesses_deleted": int(business_delete_result.deleted_count),
                    "reviews_deleted": int(reviews_result.deleted_count),
                    "comments_deleted": int(comments_result.deleted_count),
                    "analyses_deleted": int(analyses_result.deleted_count),
                    "source_profiles_deleted": int(source_profiles_result.deleted_count),
                    "datasets_deleted": int(datasets_result.deleted_count),
                    "scrape_runs_deleted": int(scrape_runs_result.deleted_count),
                },
            }
        )

    async def delete_scrape_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    def _build_related_business_jobs_query(
        self,
        *,
        business_id: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        clauses: list[dict[str, Any]] = [
            {"payload.business_id": business_id},
            {"business_id": business_id},
            {"root_business_id": business_id},
            {"payload.root_business_id": business_id},
        ]
        if canonical_name_normalized:
            clauses.extend(
                [
                    {"canonical_name_normalized": canonical_name_normalized},
                    {"payload.canonical_name_normalized": canonical_name_normalized},
                    {"name_normalized": canonical_name_normalized},
                    {"payload.name_normalized": canonical_name_normalized},
                ]
            )
        return {"$or": clauses}

    async def delete_analysis_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    async def delete_report_job(
        self,
        *,
        job_id: str,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
        force_delete_on_timeout: bool = True,
    ) -> dict:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(existing)
        return await self.job_service.delete_job(
            job_id=job_id,
            wait_active_stop_seconds=wait_active_stop_seconds,
            poll_seconds=poll_seconds,
            force_delete_on_timeout=force_delete_on_timeout,
        )

    async def relaunch_scrape_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing)
        queue_name = str(existing.get("queue_name") or "").strip().lower()
        if queue_name == "scrape_tripadvisor":
            await self._ensure_tripadvisor_session_available_for_relaunch(
                operation="relaunch_tripadvisor_job",
                job_id=job_id,
            )
        return await self.job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force) or bool(restart_from_zero),
            restart_from_zero=bool(restart_from_zero),
        )

    async def relaunch_analysis_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_analysis(existing)
        if restart_from_zero:
            raise ValueError("restart_from_zero is supported only for scrape jobs.")
        return await self.job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force),
            restart_from_zero=False,
        )

    async def relaunch_report_job(
        self,
        *,
        job_id: str,
        reason: str | None = None,
        force: bool = False,
        restart_from_zero: bool = False,
    ) -> dict[str, Any]:
        existing = await self.job_service.get_job(job_id=job_id)
        self._ensure_job_is_report(existing)
        if restart_from_zero:
            raise ValueError("restart_from_zero is not supported for report jobs.")
        return await self.job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force),
            restart_from_zero=False,
        )

    async def stop_business_scrape_job(
        self,
        *,
        job_id: str,
        continue_analysis_if_google: bool = True,
        wait_active_stop_seconds: float = 10.0,
        poll_seconds: float = 0.5,
    ) -> dict[str, Any]:
        job_payload = await self.get_scrape_job(job_id=job_id)
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()

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
        result_payload = final_job_payload.get("result") if isinstance(final_job_payload.get("result"), dict) else {}
        analysis_handoff_payload = (
            result_payload.get("analysis_handoff")
            if isinstance(result_payload, dict) and isinstance(result_payload.get("analysis_handoff"), dict)
            else {}
        )
        analysis_handoff_job_id = str(analysis_handoff_payload.get("analysis_job_id") or "").strip()
        handoff_event_present = any(
            (
                isinstance(event, dict)
                and str(event.get("stage") or "").strip().lower() == "handoff_analysis_queued"
            )
            for event in (final_job_payload.get("events") or [])
        )
        analysis_already_handed_off = (
            str(final_job_payload.get("queue_name") or "").strip().lower() == "analysis"
            or str(final_job_payload.get("job_type") or "").strip().lower() == "analysis_generate"
            or bool(analysis_handoff_job_id)
            or handoff_event_present
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
                "analysis_handoff_job_id": analysis_handoff_job_id or None,
                "analysis_enqueue_result": analysis_enqueue_result,
                "note": continue_analysis_note,
            }
        )

    def _ensure_job_is_scrape(self, job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()
        job_type = str(job_payload.get("job_type") or "").strip().lower()
        if job_type != "business_analyze" or queue_name not in {
            "scrape",
            "scrape_google_maps",
            "scrape_tripadvisor",
        }:
            raise ValueError(
                "Job is not a scrape job. Expected job_type=business_analyze and "
                "queue_name in scrape/scrape_google_maps/scrape_tripadvisor."
            )

    def _ensure_job_is_analysis(self, job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()
        job_type = str(job_payload.get("job_type") or "").strip().lower()
        if job_type != "analysis_generate" or queue_name != "analysis":
            raise ValueError(
                "Job is not an analysis job. Expected job_type=analysis_generate and queue_name=analysis."
            )

    def _ensure_job_is_report(self, job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()
        job_type = str(job_payload.get("job_type") or "").strip().lower()
        if job_type != "report_generate" or queue_name != "report":
            raise ValueError(
                "Job is not a report job. Expected job_type=report_generate and queue_name=report."
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
        session_service = TripadvisorSessionService()
        session_state = await session_service.ensure_available()
        if not bool(session_state.get("availability_now")):
            recovery_context = self._build_tripadvisor_recovery_context(
                reason_code="tripadvisor_session_unavailable",
                session_state=session_state,
                user_reason="Tripadvisor session is not available.",
            )
            human_message = str(recovery_context.get("human_message") or "Tripadvisor session is not available.")
            await self._emit_progress(
                progress_callback,
                "scraper_needs_human",
                human_message,
                {
                    "source": "tripadvisor",
                    **recovery_context,
                },
            )
            raise ScrapeNeedsHumanInterventionError(
                human_message,
                context=recovery_context,
            )
        stage_timeout_seconds = max(1, int(settings.scraper_tripadvisor_stage_timeout_seconds))
        configured_reviews_time_limit = settings.scraper_tripadvisor_reviews_time_limit_seconds
        if configured_reviews_time_limit is None:
            reviews_time_limit_seconds = float(stage_timeout_seconds)
        else:
            try:
                parsed_reviews_time_limit = float(configured_reviews_time_limit)
            except (TypeError, ValueError):
                parsed_reviews_time_limit = float(stage_timeout_seconds)
            reviews_time_limit_seconds = (
                parsed_reviews_time_limit
                if parsed_reviews_time_limit > 0
                else float(stage_timeout_seconds)
            )
        stage_elapsed_seconds: dict[str, float] = {}
        current_stage = "init"

        async def _scraper_progress(event: dict[str, Any]) -> None:
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_progress",
                "Review pagination in progress.",
                event,
            )

        async def _run_stage(
            *,
            stage: str,
            operation: Callable[[], Awaitable[Any]],
        ) -> Any:
            nonlocal current_stage
            current_stage = stage
            started_at = time.monotonic()
            try:
                result = await asyncio.wait_for(operation(), timeout=float(stage_timeout_seconds))
                elapsed_seconds = round(time.monotonic() - started_at, 3)
                stage_elapsed_seconds[stage] = elapsed_seconds
                await self._emit_progress(
                    progress_callback,
                    "scraper_stage_timing",
                    "Tripadvisor stage completed.",
                    {
                        "source": "tripadvisor",
                        "stage_name": stage,
                        "elapsed_seconds": elapsed_seconds,
                        "stage_timeout_seconds": stage_timeout_seconds,
                    },
                )
                return result
            except asyncio.TimeoutError as exc:
                elapsed_seconds = round(time.monotonic() - started_at, 3)
                stage_elapsed_seconds[stage] = elapsed_seconds
                diagnostic_payload = await self._record_tripadvisor_stage_timeout_diagnostic(
                    business_name=business_name,
                    stage=stage,
                    timeout_seconds=stage_timeout_seconds,
                    elapsed_seconds=elapsed_seconds,
                    scraper=scraper,
                    error=f"Stage '{stage}' timed out after {stage_timeout_seconds}s.",
                )
                await self._emit_progress(
                    progress_callback,
                    "scraper_stage_timeout",
                    "Tripadvisor stage timed out.",
                    {
                        "source": "tripadvisor",
                        "query": business_name,
                        "stage_name": stage,
                        "elapsed_seconds": elapsed_seconds,
                        "stage_timeout_seconds": stage_timeout_seconds,
                        "diagnostic_id": diagnostic_payload.get("diagnostic_id"),
                        "diagnostic_persist_error": diagnostic_payload.get("persist_error"),
                        "bot_match_count": diagnostic_payload.get("bot_match_count", 0),
                        "anti_bot_detected": bool(diagnostic_payload.get("anti_bot_detected")),
                        "page_url": diagnostic_payload.get("page_url"),
                    },
                )
                diagnostic_id = str(diagnostic_payload.get("diagnostic_id") or "").strip() or "n/a"
                if bool(diagnostic_payload.get("anti_bot_detected")):
                    await session_service.mark_invalid(
                        reason=f"Anti-bot challenge detected during stage '{stage}'.",
                        increment_bot_detected=True,
                    )
                    updated_state = await session_service.get_state()
                    recovery_context = self._build_tripadvisor_recovery_context(
                        reason_code="tripadvisor_antibot_detected",
                        session_state=updated_state,
                        user_reason=(
                            f"Tripadvisor anti-bot challenge detected during stage '{stage}' "
                            f"(diagnostic_id={diagnostic_id})."
                        ),
                        stage=stage,
                        diagnostic_id=diagnostic_id,
                    )
                    raise ScrapeBotDetectedError(
                        str(recovery_context.get("human_message")),
                        context=recovery_context,
                    ) from exc
                raise RuntimeError(
                    f"Tripadvisor stage '{stage}' timed out after {stage_timeout_seconds}s "
                    f"(diagnostic_id={diagnostic_id})."
                ) from exc

        async def _run_stage_without_hard_timeout(
            *,
            stage: str,
            operation: Callable[[], Awaitable[Any]],
        ) -> Any:
            nonlocal current_stage
            current_stage = stage
            started_at = time.monotonic()
            result = await operation()
            elapsed_seconds = round(time.monotonic() - started_at, 3)
            stage_elapsed_seconds[stage] = elapsed_seconds
            await self._emit_progress(
                progress_callback,
                "scraper_stage_timing",
                "Tripadvisor stage completed.",
                {
                    "source": "tripadvisor",
                    "stage_name": stage,
                    "elapsed_seconds": elapsed_seconds,
                    "stage_timeout_seconds": stage_timeout_seconds,
                    "timeout_mode": "soft",
                },
            )
            return result

        start_delay_seconds = self._resolve_effective_tripadvisor_start_delay_seconds()
        await self._emit_progress(
            progress_callback,
            "scraper_starting",
            "Starting browser and scraper.",
            {
                "source": "tripadvisor",
                "stage_timeout_seconds": stage_timeout_seconds,
                "start_delay_seconds": start_delay_seconds,
                "start_delay_min_seconds": settings.scraper_tripadvisor_start_delay_min_seconds,
                "start_delay_max_seconds": settings.scraper_tripadvisor_start_delay_max_seconds,
            },
        )
        try:
            await _run_stage(stage="start", operation=scraper.start)
            await self._emit_progress(
                progress_callback,
                "scraper_started",
                "Browser and scraper started.",
                {
                    "source": "tripadvisor",
                    "elapsed_seconds": stage_elapsed_seconds.get("start"),
                },
            )
            if start_delay_seconds > 0:
                await self._emit_progress(
                    progress_callback,
                    "scraper_start_delay_started",
                    "Waiting before starting Tripadvisor search.",
                    {
                        "source": "tripadvisor",
                        "start_delay_seconds": start_delay_seconds,
                        "start_delay_min_seconds": settings.scraper_tripadvisor_start_delay_min_seconds,
                        "start_delay_max_seconds": settings.scraper_tripadvisor_start_delay_max_seconds,
                    },
                )
                await _run_stage(
                    stage="start_delay",
                    operation=lambda: asyncio.sleep(start_delay_seconds),
                )
            await self._emit_progress(
                progress_callback,
                "scraper_search_started",
                "Searching business on TripAdvisor.",
                {
                    "source": "tripadvisor",
                    "query": business_name,
                    "stage_timeout_seconds": stage_timeout_seconds,
                },
            )
            await _run_stage(stage="search", operation=lambda: scraper.search_business(business_name))
            await self._emit_progress(
                progress_callback,
                "scraper_search_completed",
                "Business page opened.",
                {
                    "source": "tripadvisor",
                    "query": business_name,
                    "elapsed_seconds": stage_elapsed_seconds.get("search"),
                },
            )

            listing = await _run_stage(stage="listing", operation=scraper.extract_listing)
            await self._emit_progress(
                progress_callback,
                "scraper_listing_completed",
                "Listing extracted.",
                {
                    "source": "tripadvisor",
                    "business_name": listing.get("business_name"),
                    "total_reviews": listing.get("total_reviews"),
                    "elapsed_seconds": stage_elapsed_seconds.get("listing"),
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
                    "stage_timeout_seconds": stage_timeout_seconds,
                    "reviews_time_limit_seconds": reviews_time_limit_seconds,
                },
            )
            reviews = await _run_stage_without_hard_timeout(
                stage="reviews",
                operation=lambda: scraper.extract_reviews(
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
                    max_duration_seconds=reviews_time_limit_seconds,
                    progress_callback=_scraper_progress,
                ),
            )
            if stage_elapsed_seconds.get("reviews", 0.0) >= float(reviews_time_limit_seconds):
                await self._emit_progress(
                    progress_callback,
                    "scraper_reviews_time_limit_reached",
                    "Tripadvisor reviews stopped by time limit; keeping collected data.",
                    {
                        "source": "tripadvisor",
                        "query": business_name,
                        "stage_name": "reviews",
                        "elapsed_seconds": stage_elapsed_seconds.get("reviews"),
                        "stage_timeout_seconds": stage_timeout_seconds,
                        "reviews_time_limit_seconds": reviews_time_limit_seconds,
                        "scraped_review_count": len(reviews),
                    },
                )
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_completed",
                "Reviews extracted.",
                {
                    "source": "tripadvisor",
                    "scraped_review_count": len(reviews),
                    "elapsed_seconds": stage_elapsed_seconds.get("reviews"),
                    "stage_elapsed_seconds": stage_elapsed_seconds,
                },
            )
            return listing, reviews
        except ScrapeBotDetectedError:
            raise
        except Exception as exc:  # noqa: BLE001
            if "diagnostic_id=" in str(exc):
                raise

            diagnostic_payload = await self._record_tripadvisor_failure_diagnostic(
                business_name=business_name,
                stage=current_stage,
                scraper=scraper,
                error=str(exc),
            )
            diagnostic_id = str(diagnostic_payload.get("diagnostic_id") or "").strip()
            await self._emit_progress(
                progress_callback,
                "scraper_stage_error",
                "Tripadvisor stage failed.",
                {
                    "source": "tripadvisor",
                    "query": business_name,
                    "stage_name": current_stage,
                    "diagnostic_id": diagnostic_payload.get("diagnostic_id"),
                    "diagnostic_persist_error": diagnostic_payload.get("persist_error"),
                    "bot_match_count": diagnostic_payload.get("bot_match_count", 0),
                    "anti_bot_detected": bool(diagnostic_payload.get("anti_bot_detected")),
                    "page_url": diagnostic_payload.get("page_url"),
                },
            )
            if diagnostic_id:
                if bool(diagnostic_payload.get("anti_bot_detected")):
                    await session_service.mark_invalid(
                        reason=f"Anti-bot challenge detected during stage '{current_stage}'.",
                        increment_bot_detected=True,
                    )
                    updated_state = await session_service.get_state()
                    recovery_context = self._build_tripadvisor_recovery_context(
                        reason_code="tripadvisor_antibot_detected",
                        session_state=updated_state,
                        user_reason=(
                            f"Tripadvisor anti-bot challenge detected during stage '{current_stage}' "
                            f"(diagnostic_id={diagnostic_id})."
                        ),
                        stage=current_stage,
                        diagnostic_id=diagnostic_id,
                    )
                    raise ScrapeBotDetectedError(
                        str(recovery_context.get("human_message")),
                        context=recovery_context,
                    ) from exc
                raise RuntimeError(
                    f"Tripadvisor stage '{current_stage}' failed: {exc} "
                    f"(diagnostic_id={diagnostic_id})."
                ) from exc
            raise
        finally:
            await scraper.close()

    def _resolve_effective_tripadvisor_start_delay_seconds(self) -> float:
        fixed = max(0.0, float(settings.scraper_tripadvisor_start_delay_seconds))
        minimum = settings.scraper_tripadvisor_start_delay_min_seconds
        maximum = settings.scraper_tripadvisor_start_delay_max_seconds

        if minimum is None and maximum is None:
            return fixed

        lower = fixed if minimum is None else max(0.0, float(minimum))
        upper = fixed if maximum is None else max(0.0, float(maximum))
        if upper < lower:
            lower, upper = upper, lower
        if abs(upper - lower) < 1e-9:
            return lower
        return random.uniform(lower, upper)

    async def _ensure_tripadvisor_session_available_for_relaunch(
        self,
        *,
        operation: str,
        job_id: str | None = None,
    ) -> None:
        session_service = TripadvisorSessionService()
        session_state = await session_service.ensure_available()
        if bool(session_state.get("availability_now")):
            return
        recovery_context = self._build_tripadvisor_recovery_context(
            reason_code="tripadvisor_session_unavailable",
            session_state=session_state,
            user_reason=(
                "Tripadvisor session is not available; relaunching now will fail again."
            ),
        )
        operation_label = str(operation or "tripadvisor_operation").strip()
        job_suffix = f" (job_id={job_id})" if str(job_id or "").strip() else ""
        raise ValueError(
            f"Cannot execute {operation_label}{job_suffix}. "
            f"{recovery_context.get('human_message')}"
        )

    def _resolve_tripadvisor_profile_dir_hint(self) -> str:
        local_hint = str(os.getenv("SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL") or "").strip()
        if local_hint:
            return local_hint
        raw_profile_dir = str(settings.scraper_tripadvisor_user_data_dir or "").strip()
        if not raw_profile_dir:
            return "playwright-data-tripadvisor-worker-docker"
        if "worker" not in raw_profile_dir.lower():
            return "playwright-data-tripadvisor-worker-docker"
        if raw_profile_dir.startswith("/app/"):
            return raw_profile_dir.replace("/app/", "", 1)
        return raw_profile_dir

    def _build_tripadvisor_recovery_context(
        self,
        *,
        reason_code: str,
        session_state: dict[str, Any] | None,
        user_reason: str,
        stage: str | None = None,
        diagnostic_id: str | None = None,
    ) -> dict[str, Any]:
        state = session_state if isinstance(session_state, dict) else {}
        session_state_value = str(state.get("session_state") or "invalid").strip().lower() or "invalid"
        availability_now = bool(state.get("availability_now"))
        last_validation = str(state.get("last_validation_result") or "unknown").strip() or "unknown"
        session_cookie_expires_at = state.get("session_cookie_expires_at")
        last_human_intervention_at = state.get("last_human_intervention_at")
        last_error = str(state.get("last_error") or "").strip() or None
        profile_dir_hint = self._resolve_tripadvisor_profile_dir_hint()
        recovery_commands = [
            "./scripts/tripadvisor_ctl.sh human",
            f"./scripts/tripadvisor_ctl.sh session-confirm {profile_dir_hint} true",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id>",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id> --force",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id> --from-zero",
            "./scripts/tripadvisor_ctl.sh trace <job_id> 0",
        ]
        recovery_steps = [
            "Abre una sesión manual de TripAdvisor para resolver captcha/login.",
            "Cierra la ventana al terminar para que el proceso manual finalice.",
            "Confirma la sesión en backend para marcar availability_now=true.",
            "Relanza el job y revisa trazas en tiempo real.",
        ]
        reason_bits = [
            f"session_state={session_state_value}",
            f"availability_now={str(availability_now).lower()}",
            f"last_validation_result={last_validation}",
        ]
        if session_cookie_expires_at:
            reason_bits.append(f"session_cookie_expires_at={session_cookie_expires_at}")
        if last_error:
            reason_bits.append(f"last_error={last_error}")
        if stage:
            reason_bits.append(f"stage={stage}")
        if diagnostic_id:
            reason_bits.append(f"diagnostic_id={diagnostic_id}")
        reason_summary = "; ".join(reason_bits)
        human_message = (
            f"{user_reason} Motivo técnico: {reason_summary}. "
            "Acción requerida: ejecutar intervención humana de TripAdvisor. "
            f"Pasos: 1) {recovery_commands[0]} 2) {recovery_commands[1]} "
            f"3) {recovery_commands[2]} 4) {recovery_commands[3]}"
        )
        payload: dict[str, Any] = {
            "source": "tripadvisor",
            "reason_code": str(reason_code or "tripadvisor_action_required"),
            "reason": str(user_reason),
            "reason_summary": reason_summary,
            "human_message": human_message,
            "required_action": "manual_tripadvisor_intervention",
            "session_state": session_state_value,
            "availability_now": availability_now,
            "last_validation_result": last_validation,
            "session_cookie_expires_at": session_cookie_expires_at,
            "last_human_intervention_at": last_human_intervention_at,
            "last_error": last_error,
            "recovery_steps": recovery_steps,
            "recovery_commands": recovery_commands,
            "profile_dir_hint": profile_dir_hint,
        }
        if stage:
            payload["stage"] = stage
        if diagnostic_id:
            payload["diagnostic_id"] = diagnostic_id
        return payload

    async def _record_tripadvisor_failure_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        scraper: TripadvisorScraper,
        error: str,
        diagnostic_type: str = "stage_error",
        timeout_seconds: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        snapshot = await self._capture_tripadvisor_snapshot(scraper=scraper)
        full_html = str(snapshot.get("html") or "")
        anti_bot_scan_text = self._extract_antibot_scan_text(full_html)
        max_html_length = 200_000
        html_truncated = len(full_html) > max_html_length
        stored_html = full_html[:max_html_length] if html_truncated else full_html
        bot_snippets = self._extract_keyword_context_snippets(
            full_html,
            keyword="bot",
            max_matches=8,
            context_chars=140,
        )
        anti_bot_matches = self._extract_anti_bot_keyword_matches(anti_bot_scan_text)
        anti_bot_detected, anti_bot_detection_rule = self._detect_tripadvisor_antibot(
            html_text=full_html,
            keyword_matches=anti_bot_matches,
        )
        anti_bot_match_count = (
            sum(len(items) for items in anti_bot_matches.values()) if anti_bot_detected else 0
        )

        doc = {
            "source": "tripadvisor",
            "diagnostic_type": diagnostic_type,
            "business_name": business_name,
            "stage": stage,
            "timeout_seconds": int(timeout_seconds) if timeout_seconds is not None else None,
            "elapsed_seconds": float(elapsed_seconds) if elapsed_seconds is not None else None,
            "error": str(error or "").strip(),
            "page_url": str(snapshot.get("url") or ""),
            "page_title": str(snapshot.get("title") or ""),
            "html_snapshot": stored_html,
            "html_snapshot_length": len(full_html),
            "html_snapshot_truncated": bool(html_truncated),
            "keyword_matches": {
                "keyword": "bot",
                "count": len(bot_snippets),
                "snippets": bot_snippets,
            },
            "anti_bot": {
                "detected": anti_bot_detected,
                "detection_rule": anti_bot_detection_rule,
                "total_matches": anti_bot_match_count,
                "keywords": anti_bot_matches,
            },
            "capture_errors": list(snapshot.get("capture_errors") or []),
            "created_at": now,
            "updated_at": now,
        }

        diagnostics_collection = get_database()[self._SCRAPE_DIAGNOSTICS_COLLECTION]
        try:
            insert_result = await diagnostics_collection.insert_one(doc)
            diagnostic_id = str(insert_result.inserted_id)
            persist_error = None
        except Exception as exc:  # noqa: BLE001
            diagnostic_id = None
            persist_error = str(exc)

        return {
            "diagnostic_id": diagnostic_id,
            "persist_error": persist_error,
            "page_url": str(snapshot.get("url") or ""),
            "bot_match_count": len(bot_snippets),
            "anti_bot_detected": anti_bot_detected,
            "anti_bot_detection_rule": anti_bot_detection_rule,
            "anti_bot_match_count": anti_bot_match_count,
        }

    def _detect_tripadvisor_antibot(
        self,
        *,
        html_text: str,
        keyword_matches: dict[str, list[str]],
    ) -> tuple[bool, str]:
        html_text_lower = str(html_text or "").lower()

        robot_matches: list[str] = []
        for keyword in ("robot", "not a robot", "no soy un robot"):
            robot_matches.extend(keyword_matches.get(keyword) or [])
        robot_text_lower = " ".join(str(snippet or "") for snippet in robot_matches).lower()
        robot_marker_hits = sorted(
            {
                marker
                for marker in self._ANTI_BOT_ROBOT_MARKERS
                if marker in html_text_lower or marker in robot_text_lower
            }
        )
        has_robot_word = bool(
            re.search(r"\brobot\b", robot_text_lower)
        )
        has_robot_signal = bool(robot_matches) or bool(robot_marker_hits) or has_robot_word
        if not has_robot_signal:
            return False, "robot_keyword_missing"

        datadome_structure_hits = sorted(
            {
                marker
                for marker in self._ANTI_BOT_DATADOME_STRUCTURAL_MARKERS
                if marker in html_text_lower
            }
        )
        if datadome_structure_hits:
            return True, f"robot_with_datadome_structure:{','.join(datadome_structure_hits)}"

        explicit_challenge_markers = sorted(
            {
                marker
                for marker in (
                    "geo.captcha-delivery.com/captcha/",
                    "captcha/?initialcid=",
                    "ct.captcha-delivery.com/c.js",
                    "datadome captcha",
                )
                if marker in html_text_lower
            }
        )
        if explicit_challenge_markers:
            return True, f"explicit_challenge_markers:{','.join(explicit_challenge_markers)}"

        strong_keywords = {
            keyword
            for keyword in self._ANTI_BOT_STRONG_KEYWORDS
            if keyword_matches.get(keyword)
        }
        if strong_keywords:
            return True, f"strong_keywords:{','.join(sorted(strong_keywords))}"

        captcha_matches = keyword_matches.get("captcha") or []
        if captcha_matches:
            captcha_text_lower = " ".join(str(snippet or "") for snippet in captcha_matches).lower()
            provider_markers = (
                "captcha-delivery.com",
                "datadome",
                "captcha/?initialcid=",
                "ct.captcha-delivery.com/c.js",
                "data-dd-captcha",
                "ddv1-captcha-container",
            )
            provider_hits = sorted(
                {
                    marker
                    for marker in provider_markers
                    if marker in html_text_lower or marker in captcha_text_lower
                }
            )
            if provider_hits:
                return True, f"captcha_provider_markers:{','.join(provider_hits)}"
            companions = [
                marker
                for marker in self._ANTI_BOT_CAPTCHA_COMPANION_KEYWORDS
                if marker in html_text_lower or marker in captcha_text_lower
            ]
            if companions:
                return True, f"captcha_with_companion:{','.join(sorted(set(companions)))}"
            return False, "captcha_without_companion_or_robot"

        return False, "no_strong_signal_with_robot"

    async def _record_tripadvisor_stage_timeout_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        timeout_seconds: int,
        elapsed_seconds: float,
        scraper: TripadvisorScraper,
        error: str,
    ) -> dict[str, Any]:
        return await self._record_tripadvisor_failure_diagnostic(
            business_name=business_name,
            stage=stage,
            scraper=scraper,
            error=error,
            diagnostic_type="stage_timeout",
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )

    async def _capture_tripadvisor_snapshot(
        self,
        *,
        scraper: TripadvisorScraper,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "url": "",
            "title": "",
            "html": "",
            "capture_errors": [],
        }
        page = None
        try:
            page = scraper.page
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"page_unavailable: {exc}")
            return payload

        try:
            payload["url"] = str(page.url or "")
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"url_read_failed: {exc}")
        try:
            payload["title"] = str(await page.title() or "")
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"title_read_failed: {exc}")
        try:
            main_html = str(await page.content() or "")
            payload["html"] = main_html
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"html_capture_failed: {exc}")
            main_html = ""

        # DataDome challenge details often live inside iframes. Include their HTML so
        # anti-bot detection can read "robot"/captcha markers even when top-level HTML
        # is just a wrapper.
        try:
            frames = list(getattr(page, "frames", []) or [])
            frame_chunks: list[str] = []
            for index, frame in enumerate(frames):
                try:
                    if frame == page.main_frame:
                        continue
                except Exception:
                    pass
                try:
                    frame_url = str(getattr(frame, "url", "") or "")
                except Exception:
                    frame_url = ""
                try:
                    frame_html = str(await frame.content() or "")
                except Exception as exc:  # noqa: BLE001
                    payload["capture_errors"].append(
                        f"frame_html_capture_failed[{index}] ({frame_url}): {exc}"
                    )
                    continue
                if not frame_html:
                    continue
                escaped_url = html.escape(frame_url, quote=True)
                frame_chunks.append(
                    (
                        f"\n<!-- frame_snapshot index={index} url={escaped_url} -->\n"
                        f"{frame_html}\n"
                    )
                )
            if frame_chunks:
                payload["html"] = (
                    f"{main_html}\n<!-- frame_snapshots_begin -->"
                    f"{''.join(frame_chunks)}\n<!-- frame_snapshots_end -->"
                )
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"frame_snapshot_capture_failed: {exc}")
        return payload

    def _extract_anti_bot_keyword_matches(self, text: str) -> dict[str, list[str]]:
        matches: dict[str, list[str]] = {}
        for keyword in self._ANTI_BOT_KEYWORDS:
            snippets = self._extract_keyword_context_snippets(
                text,
                keyword=keyword,
                max_matches=6,
                context_chars=140,
            )
            if snippets:
                matches[keyword] = snippets
        return matches

    def _extract_antibot_scan_text(self, html_text: str) -> str:
        raw = str(html_text or "")
        if not raw:
            return ""
        without_embedded = re.sub(
            r"(?is)<(script|style|noscript|svg|iframe)[^>]*>.*?</\1>",
            " ",
            raw,
        )
        without_comments = re.sub(r"(?is)<!--.*?-->", " ", without_embedded)
        text_only = re.sub(r"(?is)<[^>]+>", " ", without_comments)
        normalized = html.unescape(text_only)
        return re.sub(r"\s+", " ", normalized).strip()

    def _extract_keyword_context_snippets(
        self,
        text: str,
        *,
        keyword: str,
        max_matches: int = 8,
        context_chars: int = 120,
    ) -> list[str]:
        haystack = str(text or "")
        needle = str(keyword or "").strip()
        if not haystack or not needle:
            return []

        snippets: list[str] = []
        context_size = max(20, int(context_chars))
        limit = max(1, int(max_matches))
        word_pattern = re.compile(rf"\b{re.escape(needle)}\b", flags=re.IGNORECASE)

        for match in word_pattern.finditer(haystack):
            start = max(0, match.start() - context_size)
            end = min(len(haystack), match.end() + context_size)
            snippet = re.sub(r"\s+", " ", haystack[start:end]).strip()
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= limit:
                return snippets

        # Avoid noisy false-positives for short words (e.g. "bot" inside "optout",
        # "robot" inside "robots"). For plain words/phrases we keep strict word-boundary
        # matching only; fallback substring matching is reserved for symbol-heavy needles
        # where boundaries are not reliable (e.g. '/recaptcha/').
        has_symbol = bool(re.search(r"[^\w\s]", needle, flags=re.UNICODE))
        if not has_symbol:
            return snippets

        fallback_pattern = re.compile(re.escape(needle), flags=re.IGNORECASE)
        for match in fallback_pattern.finditer(haystack):
            start = max(0, match.start() - context_size)
            end = min(len(haystack), match.end() + context_size)
            snippet = re.sub(r"\s+", " ", haystack[start:end]).strip()
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= limit:
                break
        return snippets

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
        payload_canonical_name = ""
        payload_root_business_id = ""
        if isinstance(payload_data, dict):
            payload_name = str(payload_data.get("name") or "").strip()
            payload_canonical_name = str(payload_data.get("canonical_name") or "").strip()
            payload_root_business_id = str(payload_data.get("root_business_id") or "").strip()
        if not payload_root_business_id:
            payload_root_business_id = str(job_payload.get("root_business_id") or "").strip()
        if payload_root_business_id:
            try:
                parsed = self._parse_object_id(payload_root_business_id, field_name="root_business_id")
                return str(parsed)
            except ValueError:
                pass
        if not payload_name:
            payload_name = str(job_payload.get("name") or "").strip()
        if not payload_canonical_name:
            payload_canonical_name = str(job_payload.get("canonical_name") or "").strip()

        lookup_name = payload_canonical_name or payload_name
        if not lookup_name:
            return None

        name_normalized = self._normalize_text(lookup_name)
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
            review_payload["fingerprint"] = self._review_fingerprint(review_payload)
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

    async def _upsert_job_comments(
        self,
        *,
        comments_collection,
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
            review_fingerprint = self._review_fingerprint(fingerprint_payload)
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

    async def _get_or_create_source_profile(
        self,
        *,
        source_profiles_collection,
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
            item["owner_reply_author_name"] = str(
                item.get("owner_reply_author_name", "") or owner_reply.get("author_name", "") or ""
            ).strip()
            item["owner_reply_written_date"] = str(
                item.get("owner_reply_written_date", "") or owner_reply.get("written_date", "") or ""
            ).strip()
        elif isinstance(owner_reply, str):
            item["owner_reply"] = owner_reply.strip()
            item["owner_reply_relative_time"] = ""
            item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
            item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()
        else:
            item["owner_reply"] = ""
            item["owner_reply_relative_time"] = ""
            item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
            item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()
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
        item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
        item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()

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

    def _summarize_tripadvisor_antibot_job(
        self,
        job_doc: dict[str, Any],
    ) -> dict[str, Any] | None:
        events_value = job_doc.get("events")
        events = events_value if isinstance(events_value, list) else []
        antibot_events: list[dict[str, Any]] = []
        for index, event in enumerate(events, start=1):
            if not isinstance(event, dict):
                continue
            summary = self._extract_tripadvisor_antibot_event_summary(event, index=index)
            if summary is None:
                continue
            antibot_events.append(summary)

        if not antibot_events:
            progress = job_doc.get("progress") if isinstance(job_doc.get("progress"), dict) else {}
            progress_message = str(progress.get("message") or "")
            error_message = str(job_doc.get("error") or "")
            if not (
                self._looks_like_antibot_text(progress_message)
                or self._looks_like_antibot_text(error_message)
            ):
                return None

            fallback_diagnostic_id = (
                self._extract_diagnostic_id_from_text(progress_message)
                or self._extract_diagnostic_id_from_text(error_message)
            )
            antibot_events.append(
                {
                    "index": None,
                    "status": str(job_doc.get("status") or "").strip().lower(),
                    "stage": str(progress.get("stage") or "").strip().lower() or "failed",
                    "message": progress_message or error_message or "Anti-bot related failure detected.",
                    "created_at": progress.get("updated_at") or job_doc.get("updated_at"),
                    "reason_code": "tripadvisor_antibot_detected",
                    "diagnostic_id": fallback_diagnostic_id,
                }
            )

        attempts_value = job_doc.get("attempts")
        attempts = attempts_value if isinstance(attempts_value, int) else 0
        status_value = str(job_doc.get("status") or "").strip().lower()
        return {
            "job_id": str(job_doc.get("_id")),
            "queue_name": str(job_doc.get("queue_name") or "").strip().lower(),
            "job_type": str(job_doc.get("job_type") or "").strip().lower(),
            "name": str(job_doc.get("name") or "").strip(),
            "name_normalized": str(job_doc.get("name_normalized") or "").strip(),
            "status": status_value,
            "attempts": attempts,
            "updated_at": job_doc.get("updated_at"),
            "first_antibot_event": antibot_events[0],
            "latest_antibot_event": antibot_events[-1],
            "antibot_event_count": len(antibot_events),
            "relaunch_eligible": status_value not in self._ACTIVE_JOB_STATUSES,
        }

    def _extract_tripadvisor_antibot_event_summary(
        self,
        event: dict[str, Any],
        *,
        index: int,
    ) -> dict[str, Any] | None:
        stage = str(event.get("stage") or "").strip().lower()
        status = str(event.get("status") or "").strip().lower()
        message = str(event.get("message") or "")
        created_at = event.get("created_at")
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        reason_code = str(data.get("reason_code") or "").strip().lower()
        diagnostic_id = str(data.get("diagnostic_id") or "").strip() or None

        message_error = str(data.get("error") or "")
        message_reason = str(data.get("reason") or "")
        anti_bot_detected = bool(data.get("anti_bot_detected"))
        anti_bot_flag = bool(data.get("anti_bot"))
        failure_like_stages = {
            "failed",
            "needs_human",
            "scraper_stage_error",
            "scraper_stage_timeout",
            "scrape_source_failed",
        }
        message_indicates_antibot = (
            self._looks_like_antibot_text(message)
            or self._looks_like_antibot_text(message_error)
            or self._looks_like_antibot_text(message_reason)
        )

        event_matches_antibot = (
            reason_code == "tripadvisor_antibot_detected"
            or anti_bot_detected
            or anti_bot_flag
            or (stage in failure_like_stages and message_indicates_antibot)
        )
        if not event_matches_antibot:
            return None

        extracted_diagnostic_id = (
            diagnostic_id
            or self._extract_diagnostic_id_from_text(message)
            or self._extract_diagnostic_id_from_text(message_error)
        )
        return {
            "index": index,
            "status": status or None,
            "stage": stage or None,
            "message": message or None,
            "created_at": created_at,
            "reason_code": reason_code or None,
            "diagnostic_id": extracted_diagnostic_id,
        }

    def _looks_like_antibot_text(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        text = value.strip().lower()
        if not text:
            return False
        strong_markers = (
            "anti-bot",
            "antibot",
            "captcha",
            "verify you are human",
            "verifica que eres humano",
            "tráfico inusual",
            "unusual traffic",
            "automated access",
            "security check",
            "challenge detected",
        )
        if any(marker in text for marker in strong_markers):
            return True
        return bool(re.search(r"\bbot\b", text))

    def _extract_diagnostic_id_from_text(self, value: str) -> str | None:
        text = str(value or "")
        if not text:
            return None
        match = re.search(r"diagnostic_id=([A-Za-z0-9_-]+)", text)
        if not match:
            return None
        return str(match.group(1)).strip() or None

    def _sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self._sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_response_payload(item) for item in value]
        return value
