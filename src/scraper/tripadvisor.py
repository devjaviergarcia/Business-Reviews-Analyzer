from __future__ import annotations

import asyncio
import json
import math
import random
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any, Awaitable, Callable
from urllib.parse import urljoin

from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)
from playwright_stealth import Stealth


@dataclass(slots=True)
class _SearchCandidate:
    title: str
    href: str
    score: float


class TripadvisorScraper:
    def __init__(
        self,
        page: Page | None = None,
        *,
        headless: bool = False,
        slow_mo_ms: int = 50,
        user_data_dir: str = "playwright-data-tripadvisor",
        browser_channel: str | None = None,
        tripadvisor_url: str = "https://www.tripadvisor.es",
        timeout_ms: int = 30000,
        min_click_delay_ms: int = 700,
        max_click_delay_ms: int = 1500,
        min_key_delay_ms: int = 35,
        max_key_delay_ms: int = 95,
        max_reviews_open_seconds: float = 3.0,
        max_seconds_per_reviews_page: float = 10.0,
        stealth_mode: bool = True,
        harden_headless: bool = True,
        extra_chromium_args: list[str] | None = None,
        incognito: bool = False,
    ) -> None:
        self._page = page
        self._external_page = page is not None

        self._headless = False
        self._slow_mo_ms = slow_mo_ms
        self._user_data_dir = user_data_dir
        self._browser_channel = (browser_channel or "").strip() or None
        self._tripadvisor_url = tripadvisor_url
        self._timeout_ms = timeout_ms
        self._min_click_delay_ms = max(120, min(700, int(min_click_delay_ms)))
        self._max_click_delay_ms = max(self._min_click_delay_ms, min(1500, int(max_click_delay_ms)))
        self._min_key_delay_ms = max(5, min(60, int(min_key_delay_ms)))
        self._max_key_delay_ms = max(self._min_key_delay_ms, min(120, int(max_key_delay_ms)))
        self._max_reviews_open_seconds = max(0.8, float(max_reviews_open_seconds))
        self._max_seconds_per_reviews_page = max(2.0, float(max_seconds_per_reviews_page))
        self._stealth_mode = stealth_mode
        self._harden_headless = harden_headless
        self._extra_chromium_args = list(extra_chromium_args or [])
        self._incognito = incognito
        self._project_root = Path(__file__).resolve().parents[2]

        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._last_click_ts: float | None = None
        self._rng = random.Random()
        self._cookies_checked_once = False
        self._consent_checked_once = False
        self._location_prompt_checked_once = False
        self._stealth = Stealth(
            navigator_languages_override=("es-ES", "es"),
            navigator_platform_override="Win32",
        )

    def bind_page(self, page: Page) -> None:
        self._page = page
        self._external_page = True

    async def __aenter__(self) -> TripadvisorScraper:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    @property
    def page(self) -> Page:
        return self._require_page()

    async def start(self) -> Page:
        if self._page is not None:
            return self._page

        self._assert_event_loop_compatible_for_playwright()
        self._playwright = await async_playwright().start()

        if self._incognito:
            launch_options: dict[str, Any] = {
                "headless": self._headless,
                "slow_mo": self._slow_mo_ms,
                "args": self._build_chromium_args(),
            }
            if self._browser_channel:
                launch_options["channel"] = self._browser_channel
            try:
                self._browser = await self._playwright.chromium.launch(**launch_options)
            except Exception:
                if not self._browser_channel:
                    raise
                launch_options.pop("channel", None)
                self._browser = await self._playwright.chromium.launch(**launch_options)

            self._context = await self._browser.new_context(
                viewport={"width": 1366, "height": 900},
                locale="es-ES",
                timezone_id="Europe/Madrid",
            )
        else:
            user_data_dir = self._resolve_user_data_dir()
            launch_options: dict[str, Any] = {
                "user_data_dir": str(user_data_dir),
                "headless": self._headless,
                "slow_mo": self._slow_mo_ms,
                "viewport": {"width": 1366, "height": 900},
                "locale": "es-ES",
                "timezone_id": "Europe/Madrid",
                "args": self._build_chromium_args(),
            }
            if self._browser_channel:
                launch_options["channel"] = self._browser_channel
            try:
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
            except Exception:
                if not self._browser_channel:
                    raise
                launch_options.pop("channel", None)
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)

        if self._stealth_mode and self._context is not None:
            await self._stealth.apply_stealth_async(self._context)
        if self._context is not None:
            await self._context.add_init_script(self._block_geolocation_init_script())
        if self._context is not None:
            self._context.set_default_timeout(self._timeout_ms)

        if self._context is None:
            raise RuntimeError("Playwright context was not initialized.")

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await self._go_to_home()
        return self._page

    async def close(self) -> None:
        if not self._external_page and self._context is not None:
            await self._context.close()
        if not self._external_page and self._browser is not None:
            await self._browser.close()
        if not self._external_page and self._playwright is not None:
            await self._playwright.stop()

        self._context = None
        self._browser = None
        self._playwright = None
        self._page = None
        self._external_page = False
        self._last_click_ts = None

    async def search_business(
        self,
        name: str,
        *,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> None:
        query = self._clean_text(name)
        if not query:
            raise ValueError("Business query is empty.")

        page = await self.start()
        started_at = monotonic()
        direct_listing_url = self._resolve_direct_listing_target_url(query)

        async def _emit_search_progress(event: str, *, step: str, step_started_at: float) -> None:
            await self._emit_progress(
                progress_callback,
                {
                    "event": event,
                    "source": "tripadvisor",
                    "step": step,
                    "elapsed_step_s": round(monotonic() - step_started_at, 3),
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )

        if direct_listing_url:
            open_direct_started_at = monotonic()
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_direct_url_detected",
                    "source": "tripadvisor",
                    "input": query,
                    "target_url": direct_listing_url,
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )
            await page.goto(direct_listing_url, wait_until="domcontentloaded")
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
            await _emit_search_progress(
                "tripadvisor_search_listing_opened",
                step="open_direct_url",
                step_started_at=open_direct_started_at,
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_completed",
                    "source": "tripadvisor",
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )
            return

        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

        search_input_selectors = (
            "form[role='search'] input[type='search'][name='q']",
            "form[action='/Search'] input[type='search'][name='q']",
            "input[role='searchbox'][name='q']",
            "input[type='search'][name='q'][aria-label*='Buscar' i]",
            "input[type='search'][name='q'][title='Buscar']",
            "input[type='search'][name='q']",
            "input[name='q'][type='search']",
        )
        open_search_button_selectors = (
            "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
            "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
            "button[type='submit'][aria-label*='Buscar' i]",
        )

        typing_started_at = monotonic()
        search_input = await self._find_first_optional_visible(
            search_input_selectors,
            timeout_ms=7000,
        )
        if search_input is None:
            open_search_button = await self._find_first_optional_visible(
                open_search_button_selectors,
                timeout_ms=3500,
            )
            if open_search_button is not None:
                try:
                    await self._human_click(open_search_button)
                    await page.wait_for_timeout(self._rng.randint(180, 460))
                except Exception:
                    pass
            search_input = await self._find_first_optional_visible(
                search_input_selectors,
                timeout_ms=7000,
            )
        if search_input is None:
            page_title = ""
            try:
                page_title = self._clean_text(await page.title())
            except Exception:
                page_title = ""
            raise RuntimeError(
                "Tripadvisor search input not found after retries. "
                f"url={self._clean_text(page.url)} title={page_title!r}"
            )
        await self._human_click(search_input)
        await self._human_type(search_input, query)
        await page.wait_for_timeout(self._rng.randint(250, 700))
        await _emit_search_progress(
            "tripadvisor_search_query_typed",
            step="type_query",
            step_started_at=typing_started_at,
        )

        typeahead_started_at = monotonic()
        opened_from_typeahead = await self._open_exact_typeahead_result(query)
        if opened_from_typeahead:
            await _emit_search_progress(
                "tripadvisor_search_typeahead_exact_match_opened",
                step="open_typeahead_exact",
                step_started_at=typeahead_started_at,
            )
            await _emit_search_progress(
                "tripadvisor_search_listing_opened",
                step="open_listing",
                step_started_at=typeahead_started_at,
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_completed",
                    "source": "tripadvisor",
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )
            return

        submit_started_at = monotonic()
        submit_button = await self._find_first_visible(
            (
                "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
                "div.bOfFT button[type='submit'][aria-label*='Buscar' i]",
                "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
                "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
                "button[type='submit'][formaction='/Search']",
                "form[role='search'] button[type='submit']",
            ),
            timeout_ms=6000,
        )
        await self._human_click(submit_button)
        await _emit_search_progress(
            "tripadvisor_search_submitted",
            step="submit_query",
            step_started_at=submit_started_at,
        )

        results_ready_started_at = monotonic()
        await self._wait_after_navigation()
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        await _emit_search_progress(
            "tripadvisor_search_results_ready",
            step="results_ready",
            step_started_at=results_ready_started_at,
        )

        open_listing_started_at = monotonic()
        await self._open_best_search_result(query)
        await _emit_search_progress(
            "tripadvisor_search_listing_opened",
            step="open_listing",
            step_started_at=open_listing_started_at,
        )
        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_search_completed",
                "source": "tripadvisor",
                "elapsed_total_s": round(monotonic() - started_at, 3),
                "page_url": page.url,
            },
        )

    async def extract_listing(self) -> dict[str, Any]:
        page = self._require_page()
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()

        name = await self._safe_locator_inner_text(page.locator("h1").first)
        json_ld_entity = await self._extract_primary_json_ld_entity()

        if json_ld_entity:
            if not name:
                name = self._clean_text(json_ld_entity.get("name"))
            address = self._address_from_json_ld(json_ld_entity.get("address"))
            phone = self._clean_text(json_ld_entity.get("telephone"))
            website = self._clean_text(json_ld_entity.get("url"))
            aggregate = json_ld_entity.get("aggregateRating") or {}
            rating = self._parse_rating(aggregate.get("ratingValue"))
            total_reviews = self._parse_total_reviews(aggregate.get("reviewCount"))
            categories = self._categories_from_json_ld(json_ld_entity)
        else:
            address = ""
            phone = ""
            website = ""
            rating = None
            total_reviews = None
            categories = []

        if rating is None:
            rating_text = await self._safe_locator_inner_text(
                page.locator("[data-automation='bubbleRatingValue']").first
            )
            rating = self._parse_rating(rating_text)

        if total_reviews is None:
            reviews_count_text = await self._safe_locator_inner_text(
                page.locator("a[href='#REVIEWS'] [data-automation='bubbleReviewCount']").first
            )
            if not reviews_count_text:
                reviews_count_text = await self._safe_locator_inner_text(
                    page.locator("[data-test-target='reviews-tab'] .biGQs._P.SewaP.kSNRl.KeZJf").first
                )
            total_reviews = self._parse_total_reviews(reviews_count_text)

        return {
            "business_name": name or "",
            "address": address or None,
            "phone": phone or None,
            "website": website or None,
            "overall_rating": rating,
            "total_reviews": total_reviews,
            "categories": categories,
        }

    async def extract_reviews(
        self,
        *,
        strategy: str | None = None,
        max_rounds: int = 10,
        html_scroll_max_rounds: int = 180,
        html_stable_rounds: int = 6,
        html_min_interval_s: float = 1.0,
        html_max_interval_s: float = 2.0,
        max_pages: int | None = None,
        max_pages_percent: float | None = None,
        max_duration_seconds: float | None = None,
        include_owner_reply: bool = False,
        include_image_urls: bool = False,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> list[dict[str, Any]]:
        del strategy, html_stable_rounds
        page = self._require_page()
        await self._accept_cookies_if_present()
        await self._open_reviews_section()
        reviews_started_at = monotonic()

        effective_pages = self._resolve_effective_pages(
            max_pages=max_pages,
            max_rounds=max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
        )
        min_pause_s = max(0.2, float(html_min_interval_s))
        max_pause_s = max(min_pause_s, float(html_max_interval_s))
        pagination = await self._reviews_pagination_snapshot()
        known_total_pages = pagination.get("total_pages")
        if isinstance(known_total_pages, int) and known_total_pages > 0:
            effective_pages = min(effective_pages, known_total_pages)
            if max_pages_percent is not None:
                percent_value = float(max_pages_percent)
                percent_pages = max(1, int(math.ceil((known_total_pages * percent_value) / 100.0)))
                effective_pages = min(effective_pages, percent_pages)

        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_started",
                "max_pages": effective_pages,
                "max_pages_percent": max_pages_percent,
                "max_duration_seconds": max_duration_seconds,
                "pause_interval_s": {"min": min_pause_s, "max": max_pause_s},
                "range_start": pagination.get("range_start"),
                "range_end": pagination.get("range_end"),
                "total_results": pagination.get("total_results"),
                "current_page": pagination.get("current_page"),
                "total_pages": pagination.get("total_pages"),
            },
        )

        all_items: list[dict[str, Any]] = []
        seen: set[str] = set()
        effective_duration_limit: float | None = None
        if max_duration_seconds is not None:
            try:
                parsed_duration = float(max_duration_seconds)
            except (TypeError, ValueError):
                parsed_duration = 0.0
            if parsed_duration > 0:
                effective_duration_limit = parsed_duration

        for page_index in range(1, effective_pages + 1):
            page_collect_started_at = monotonic()
            if effective_duration_limit is not None:
                elapsed_seconds = max(0.0, monotonic() - reviews_started_at)
                if elapsed_seconds >= effective_duration_limit:
                    await self._emit_progress(
                        progress_callback,
                        {
                            "event": "tripadvisor_reviews_time_limit_reached",
                            "reason": "before_page_collection",
                            "elapsed_seconds": round(elapsed_seconds, 3),
                            "max_duration_seconds": round(effective_duration_limit, 3),
                            "last_page_index": page_index - 1,
                            "pages_target": effective_pages,
                            "total_unique_reviews": len(all_items),
                        },
                    )
                    break

            await self._expand_reviews(max_clicks=0)
            current_items = await self._extract_reviews_from_current_page(
                max_collection_seconds=self._max_seconds_per_reviews_page,
                include_owner_reply=include_owner_reply,
                include_image_urls=include_image_urls,
                ensure_reviews_open=(page_index == 1),
            )

            added_count = 0
            for item_index, item in enumerate(current_items):
                identity = self._review_identity(item)
                if not identity:
                    identity = self._review_identity_fallback(
                        review=item,
                        page_index=page_index,
                        item_index=item_index,
                    )
                if identity in seen:
                    continue
                seen.add(identity)
                all_items.append(item)
                added_count += 1

            pagination = await self._reviews_pagination_snapshot()
            current_page_num = pagination.get("current_page")
            total_pages_num = pagination.get("total_pages")
            remaining_pages: int | None = None
            if isinstance(current_page_num, int) and isinstance(total_pages_num, int):
                remaining_pages = max(0, total_pages_num - current_page_num)

            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_reviews_page_collected",
                    "page": page_index,
                    "page_elapsed_seconds": round(max(0.0, monotonic() - page_collect_started_at), 3),
                    "page_budget_seconds": round(self._max_seconds_per_reviews_page, 3),
                    "page_url": page.url,
                    "items_in_page": len(current_items),
                    "added_to_total": added_count,
                    "total_unique_reviews": len(all_items),
                    "range_start": pagination.get("range_start"),
                    "range_end": pagination.get("range_end"),
                    "total_results": pagination.get("total_results"),
                    "current_page": current_page_num,
                    "total_pages": total_pages_num,
                    "remaining_pages": remaining_pages,
                },
            )

            if effective_duration_limit is not None:
                elapsed_seconds = max(0.0, monotonic() - reviews_started_at)
                if elapsed_seconds >= effective_duration_limit:
                    await self._emit_progress(
                        progress_callback,
                        {
                            "event": "tripadvisor_reviews_time_limit_reached",
                            "reason": "after_page_collection",
                            "elapsed_seconds": round(elapsed_seconds, 3),
                            "max_duration_seconds": round(effective_duration_limit, 3),
                            "last_page_index": page_index,
                            "pages_target": effective_pages,
                            "total_unique_reviews": len(all_items),
                        },
                    )
                    break

            if page_index >= effective_pages:
                break

            try:
                moved = await self._go_next_reviews_page()
            except Exception as exc:
                moved = False
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_reviews_next_page_error",
                        "page": page_index,
                        "error": self._clean_text(str(exc)),
                        "total_unique_reviews": len(all_items),
                    },
                )
            if not moved:
                recovered = False
                if page_index < effective_pages:
                    recovered = await self._recover_reviews_and_retry_pagination(
                        page_index=page_index,
                        progress_callback=progress_callback,
                    )
                if recovered:
                    await page.wait_for_timeout(self._rng.randint(120, 260))
                    continue
                pagination = await self._reviews_pagination_snapshot()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_reviews_end_of_pagination",
                        "page": page_index,
                        "total_unique_reviews": len(all_items),
                        "current_page": pagination.get("current_page"),
                        "total_pages": pagination.get("total_pages"),
                    },
                )
                break

            # Keep transition pause short for predictable throughput between pages.
            page_pause_min_s = min(max(0.05, min_pause_s), 0.25)
            page_pause_max_s = min(max(page_pause_min_s, max_pause_s), 0.45)
            await page.wait_for_timeout(self._rng.uniform(page_pause_min_s * 1000.0, page_pause_max_s * 1000.0))

        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_completed",
                "total_unique_reviews": len(all_items),
            },
        )
        return all_items

    async def _extract_reviews_from_current_page(
        self,
        *,
        max_collection_seconds: float | None = None,
        include_owner_reply: bool = False,
        include_image_urls: bool = False,
        ensure_reviews_open: bool = True,
    ) -> list[dict[str, Any]]:
        page = self._require_page()
        page_started_at = monotonic()
        total_budget_seconds = (
            max(1.0, float(max_collection_seconds))
            if max_collection_seconds is not None
            else self._max_seconds_per_reviews_page
        )

        if ensure_reviews_open:
            await self._open_reviews_section()
        await self._prefetch_reviews_by_scroll(max_seconds=min(2.2, max(0.6, total_budget_seconds * 0.28)))

        # Fast DOM polling + progressive scroll: helps with lazy-loaded review cards.
        best_items: list[dict[str, Any]] = []
        stable_rounds = 0
        empty_rounds = 0
        while (monotonic() - page_started_at) < total_budget_seconds:
            dom_items = await self._extract_reviews_from_dom(
                include_owner_reply=include_owner_reply,
                include_image_urls=include_image_urls,
            )
            if len(dom_items) > len(best_items):
                best_items = dom_items
                stable_rounds = 0
            elif dom_items and len(dom_items) == len(best_items):
                stable_rounds += 1
            else:
                stable_rounds = max(0, stable_rounds - 1)

            if dom_items:
                empty_rounds = 0
            else:
                empty_rounds += 1

            if best_items and (stable_rounds >= 3 or len(best_items) >= 32):
                return best_items

            if empty_rounds >= 2:
                await self._prefetch_reviews_by_scroll(max_seconds=0.6)
                empty_rounds = 0
                continue

            cards = page.locator("[data-automation='reviewCard'], [data-test-target='HR_CC_CARD']")
            try:
                card_count = await cards.count()
            except Exception:
                card_count = 0
            if card_count == 0:
                cards = page.locator("[data-test-target='review-title']")
                try:
                    card_count = await cards.count()
                except Exception:
                    card_count = 0
            if card_count > 0:
                last_index = min(card_count - 1, 31)
                try:
                    await cards.nth(last_index).scroll_into_view_if_needed(timeout=900)
                except Exception:
                    pass
            try:
                await page.mouse.wheel(0, self._rng.randint(850, 1700))
            except Exception:
                pass
            await page.wait_for_timeout(95)

        if not best_items:
            # Last quick attempt in case cards rendered late.
            try:
                await self._wait_for_review_cards(timeout_ms=1500)
            except Exception:
                pass
            best_items = await self._extract_reviews_from_dom(
                include_owner_reply=include_owner_reply,
                include_image_urls=include_image_urls,
            )
        return best_items

    async def _prefetch_reviews_by_scroll(self, *, max_seconds: float) -> None:
        page = self._require_page()
        budget = max(0.4, float(max_seconds))
        deadline = monotonic() + budget
        best_count = 0
        stable_rounds = 0

        while monotonic() < deadline:
            cards = page.locator("[data-automation='reviewCard'], [data-test-target='HR_CC_CARD']")
            try:
                card_count = await cards.count()
            except Exception:
                card_count = 0
            if card_count == 0:
                cards = page.locator("[data-test-target='review-title']")
                try:
                    card_count = await cards.count()
                except Exception:
                    card_count = 0

            if card_count > best_count:
                best_count = card_count
                stable_rounds = 0
            else:
                stable_rounds += 1

            if card_count > 0:
                try:
                    await cards.nth(min(card_count - 1, 31)).scroll_into_view_if_needed(timeout=900)
                except Exception:
                    pass

            try:
                await page.mouse.wheel(0, self._rng.randint(1200, 2600))
            except Exception:
                pass

            if best_count >= 32 or (best_count >= 8 and stable_rounds >= 2):
                break
            await page.wait_for_timeout(80)

    async def _wait_for_review_cards(self, *, timeout_ms: int) -> Locator:
        page = self._require_page()
        selectors = (
            "div[data-test-target='HR_CC_CARD']",
            "div[data-test-target='reviews-tab'] [data-automation='reviewCard']:visible",
            "div.AjLYs.e[data-test-target='reviews-tab'] [data-automation='reviewCard']:visible",
            "[data-automation='reviewCard']:visible",
            "div[data-test-target='reviews-tab'] [data-test-target='review-title']",
            "[data-test-target='review-title']",
            "div[data-test-target='reviews-tab'] [data-automation='reviewCard']",
            "[data-automation='reviewCard']",
        )
        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)

        while monotonic() < deadline:
            for selector in selectors:
                locator = page.locator(selector)
                try:
                    total = await locator.count()
                except Exception:
                    continue
                if total > 0 and await self._review_cards_have_content(locator):
                    return locator

            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()

            reviews_tab = page.locator("div[data-test-target='reviews-tab']").first
            try:
                if await reviews_tab.count() > 0:
                    await reviews_tab.scroll_into_view_if_needed()
            except Exception:
                pass

            try:
                await page.mouse.wheel(0, self._rng.randint(300, 900))
            except Exception:
                pass
            await page.wait_for_timeout(220)

        fallback = page.locator("[data-automation='reviewCard']:visible")
        try:
            if await fallback.count() > 0:
                return fallback
        except Exception:
            pass
        fallback_hr = page.locator("[data-test-target='HR_CC_CARD']")
        try:
            if await fallback_hr.count() > 0:
                return fallback_hr
        except Exception:
            pass
        fallback_titles = page.locator("[data-test-target='review-title']")
        try:
            if await fallback_titles.count() > 0:
                return fallback_titles
        except Exception:
            pass
        return page.locator("[data-automation='reviewCard']")

    async def _review_cards_have_content(self, cards: Locator, *, sample_size: int = 6) -> bool:
        try:
            total = await cards.count()
        except Exception:
            return False
        for idx in range(min(total, sample_size)):
            card = cards.nth(idx)
            title = await self._safe_locator_inner_text(card.locator("[data-test-target='review-title']").first)
            body = await self._safe_locator_inner_text(card.locator("div[data-test-target='review-body']").first)
            if not body:
                body = await self._safe_locator_inner_text(card.locator("div._c div._T.FKffI").first)
            if not body:
                body = await self._safe_locator_inner_text(card.locator("div._T.FKffI").first)
            author = await self._safe_locator_inner_text(card.locator("a[href*='/Profile/']").first)
            if title or body or author:
                return True
        return False

    async def _extract_reviews_from_dom(
        self,
        *,
        include_owner_reply: bool,
        include_image_urls: bool,
    ) -> list[dict[str, Any]]:
        page = self._require_page()
        try:
            raw_items = await page.evaluate(
                """
                ({ includeOwnerReply, includeImageUrls }) => {
                  const clean = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
                  const hasBodyNode = (root) => !!(
                    root?.querySelector("[data-test-target='review-body']")
                    || root?.querySelector("div._T.FKffI")
                    || root?.querySelector("span.JguWG")
                  );
                  const parseRating = (value) => {
                    const normalized = clean(value).replace(',', '.');
                    const match = normalized.match(/(\\d+(?:\\.\\d+)?)/);
                    if (!match) return null;
                    const parsed = Number(match[1]);
                    if (!Number.isFinite(parsed)) return null;
                    if (parsed < 0 || parsed > 5) return null;
                    return parsed;
                  };
                  const pickWrittenDate = (root) => {
                    const candidates = Array.from(root.querySelectorAll('div.biGQs._P.VImYz.ncFvv.navcl, div.biGQs._P.VImYz.navcl'));
                    for (const el of candidates) {
                      const text = clean(el.textContent);
                      if (!text) continue;
                      if (/escrita el|escrito el|written|responded/i.test(text)) return text;
                    }
                    return '';
                  };
                  let cards = Array.from(document.querySelectorAll("div[data-automation='reviewCard'], div[data-test-target='HR_CC_CARD']"));
                  if (!cards.length) {
                    const roots = [];
                    const seen = new Set();
                    const titles = Array.from(document.querySelectorAll("[data-test-target='review-title'] a[href], [data-test-target='review-title']")).slice(0, 48);
                    for (const node of titles) {
                      let root = node;
                      let selectedRoot = null;
                      for (let depth = 0; depth < 10 && root; depth += 1) {
                        const hasTitle = !!root.querySelector("[data-test-target='review-title']");
                        const hasBody = hasBodyNode(root);
                        if (hasTitle && hasBody) {
                          selectedRoot = root;
                          const hasAuthorProfile = !!root.querySelector("a[href*='/Profile/']");
                          if (hasAuthorProfile) break;
                        }
                        root = root.parentElement;
                      }
                      root = selectedRoot;
                      if (!root) continue;
                      if (seen.has(root)) continue;
                      seen.add(root);
                      roots.push(root);
                    }
                    cards = roots;
                  }
                  cards = cards.slice(0, 32);
                  const items = [];
                  for (const card of cards) {
                    const titleAnchor = card.querySelector("[data-test-target='review-title'] a[href]") || card.querySelector("h3 a[href]");
                    const titleNode = card.querySelector("[data-test-target='review-title']");
                    const title = clean(titleAnchor?.textContent || titleNode?.textContent);
                    const titleHref = clean(titleAnchor?.getAttribute('href'));
                    const authorAnchor = card.querySelector("a[href*='/Profile/'].ukgoS") || card.querySelector("span.biGQs._P.ezezH a[href*='/Profile/']");
                    const authorName = clean(authorAnchor?.textContent);
                    const relativeTime = clean(
                      (
                        card.querySelector('div.VufqL.o.W')
                        || card.querySelector('div.VufqL')
                        || card.querySelector('div.ZRBpD div.biGQs._P.VImYz.AWdfh')
                        || card.querySelector('div.biGQs._P.VImYz.AWdfh')
                      )?.textContent
                    );
                    const writtenDate = pickWrittenDate(card);
                    const text = clean(
                      (card.querySelector("div[data-test-target='review-body'] span.JguWG div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div[data-test-target='review-body'] span.JguWG")
                        || card.querySelector("div[data-test-target='review-body'] div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div[data-test-target='review-body']")
                        || card.querySelector("div._c div._T.FKffI span.JguWG div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div._c div._T.FKffI span.JguWG")
                        || card.querySelector("div._c div._T.FKffI")
                        || card.querySelector("div._T.FKffI"))?.textContent
                    ).slice(0, 6000);
                    const rating = parseRating(
                      clean(card.querySelector("svg[data-automation='bubbleRatingImage'] title")?.textContent)
                      || clean(card.querySelector("title[id*='_lithium']")?.textContent)
                    );
                    const item = {
                      title_href: titleHref,
                      review_title: title,
                      author_name: authorName,
                      relative_time: relativeTime,
                      written_date: writtenDate,
                      text,
                      rating,
                      raw_card_html: String(card.outerHTML || '').slice(0, 50000),
                    };
                    if (includeImageUrls) {
                      const images = Array.from(card.querySelectorAll("button img, picture img"))
                        .map((img) => clean(img.currentSrc || img.getAttribute('src')))
                        .filter((url) => !!url && !/default-avatar/i.test(url));
                      item.image_urls = Array.from(new Set(images)).slice(0, 12);
                    }
                    if (includeOwnerReply) {
                      const replyRoot = card.querySelector("div.mahws");
                      if (replyRoot) {
                        const replyText = clean(
                          (replyRoot.querySelector("div._T.FKffI span.JguWG")
                            || replyRoot.querySelector("div._T.FKffI div.biGQs._P.VImYz.AWdfh")
                            || replyRoot.querySelector("span.JguWG"))?.textContent
                        ).slice(0, 3000);
                        const replyAuthor = clean(
                          (replyRoot.querySelector("a[href*='/Profile/'].ukgoS")
                            || replyRoot.querySelector("span.biGQs._P.ezezH"))?.textContent
                        );
                        const replyWrittenDate = pickWrittenDate(replyRoot);
                        if (replyText) {
                          item.owner_reply = {
                            text: replyText,
                            relative_time: replyWrittenDate,
                            written_date: replyWrittenDate,
                            author_name: replyAuthor,
                          };
                        }
                      }
                    }
                    if (
                      item.review_title ||
                      item.author_name ||
                      item.relative_time ||
                      item.written_date ||
                      item.text ||
                      item.title_href
                    ) {
                      items.push(item);
                    }
                  }
                  return items;
                }
                """,
                {
                    "includeOwnerReply": bool(include_owner_reply),
                    "includeImageUrls": bool(include_image_urls),
                },
            )
        except Exception:
            return []

        if not isinstance(raw_items, list):
            return []

        normalized_items: list[dict[str, Any]] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title_href = self._clean_text(str(raw.get("title_href", "") or ""))
            review_id = self._extract_review_id_from_href(title_href)
            item: dict[str, Any] = {
                "source": "tripadvisor",
                "review_id": review_id,
                "author_name": self._clean_text(str(raw.get("author_name", "") or "")),
                "rating": self._parse_rating(raw.get("rating")) or 0.0,
                "relative_time": self._clean_text(str(raw.get("relative_time", "") or "")),
                "text": self._clean_text(str(raw.get("text", "") or "")),
                "review_title": self._clean_text(str(raw.get("review_title", "") or "")),
                "written_date": self._extract_written_date_line_from_text(str(raw.get("written_date", "") or "")),
            }
            raw_card_html = str(raw.get("raw_card_html", "") or "").strip()
            if raw_card_html:
                item["raw_card_html"] = raw_card_html[:50_000]
            if include_image_urls:
                image_urls = raw.get("image_urls")
                if isinstance(image_urls, list):
                    item["image_urls"] = [
                        self._clean_text(str(url or ""))
                        for url in image_urls
                        if self._clean_text(str(url or ""))
                    ]
            if include_owner_reply:
                owner_reply = raw.get("owner_reply")
                if isinstance(owner_reply, dict):
                    owner_reply_text = self._clean_text(str(owner_reply.get("text", "") or ""))
                    if owner_reply_text:
                        owner_written = self._extract_written_date_line_from_text(
                            str(owner_reply.get("written_date", "") or owner_reply.get("relative_time", "") or "")
                        )
                        item["owner_reply"] = {
                            "text": owner_reply_text,
                            "relative_time": owner_written,
                        }
                        owner_author = self._clean_text(str(owner_reply.get("author_name", "") or ""))
                        if owner_author:
                            item["owner_reply_author_name"] = owner_author
                        if owner_written:
                            item["owner_reply_written_date"] = owner_written

            normalized_items.append(item)
        return normalized_items

    async def _go_to_home(self) -> None:
        page = self._require_page()
        await page.goto(self._tripadvisor_url, wait_until="domcontentloaded")
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        search_input_selectors = (
            "form[role='search'] input[type='search'][name='q']",
            "form[action='/Search'] input[type='search'][name='q']",
            "input[role='searchbox'][name='q']",
            "input[type='search'][name='q'][aria-label*='Buscar' i]",
            "input[type='search'][name='q']",
        )
        open_search_button_selectors = (
            "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search']",
        )
        try:
            search_input = await self._find_first_optional_visible(
                search_input_selectors,
                timeout_ms=12000,
            )
            if search_input is None:
                open_search_button = await self._find_first_optional_visible(
                    open_search_button_selectors,
                    timeout_ms=3000,
                )
                if open_search_button is not None:
                    try:
                        await self._human_click(open_search_button)
                        await page.wait_for_timeout(self._rng.randint(160, 420))
                    except Exception:
                        pass
                    await self._find_first_optional_visible(
                        search_input_selectors,
                        timeout_ms=4000,
                    )
        except Exception:
            # Do not fail startup here; search stage handles retries/selectors.
            return

    async def _open_best_search_result(self, query: str) -> None:
        page = self._require_page()
        current_url = page.url
        if self._looks_like_tripadvisor_listing_url(current_url):
            return

        await self._accept_cookies_if_present()
        await self._dismiss_location_prompt_if_present()
        cards = await self._find_search_result_cards(timeout_ms=7000)
        total_cards = await cards.count() if cards is not None else 0
        if total_cards == 0:
            fallback_href = await self._best_listing_href_from_search_results(query, min_score=0.12)
            if not fallback_href:
                raise RuntimeError("Tripadvisor search results did not render result cards (*-results-card).")
            target_url = urljoin(self._tripadvisor_url, fallback_href)
            await page.goto(target_url, wait_until="domcontentloaded")
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
            return

        assert cards is not None
        query_normalized = self._normalize_text(query)
        candidates: list[_SearchCandidate] = []

        for idx in range(min(total_cards, 25)):
            card = cards.nth(idx)
            title, href = await self._extract_card_title_and_href(card)
            if not href or not title:
                continue
            score = self._match_score(query_normalized, self._normalize_text(title))
            candidates.append(_SearchCandidate(title=title, href=href, score=score))

        if not candidates:
            fallback_href = await self._best_listing_href_from_search_results(query, min_score=0.05)
            if not fallback_href:
                raise RuntimeError("Tripadvisor search result parsing returned no selectable candidates.")
            target_url = urljoin(self._tripadvisor_url, fallback_href)
            await page.goto(target_url, wait_until="domcontentloaded")
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
            return

        best = max(candidates, key=lambda item: item.score)
        if best.score < 0.35:
            fallback_href = await self._best_listing_href_from_search_results(query, min_score=0.0)
            if fallback_href:
                target_url = urljoin(self._tripadvisor_url, fallback_href)
                await page.goto(target_url, wait_until="domcontentloaded")
                await self._wait_after_navigation()
                await self._accept_cookies_if_present()
                await self._dismiss_consent_if_present()
                await self._dismiss_location_prompt_if_present()
                return
            raise RuntimeError(
                f"No Tripadvisor result was similar enough to query '{query}'. Best='{best.title}' score={best.score:.3f}."
            )

        target_url = urljoin(self._tripadvisor_url, best.href)
        await page.goto(target_url, wait_until="domcontentloaded")
        await self._wait_after_navigation()
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

    async def _open_exact_typeahead_result(self, query: str, *, timeout_ms: int = 4500) -> bool:
        page = self._require_page()
        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)
        selectors = (
            "#typeahead_results a[role='option'][href]",
            "[data-test-attribute='typeahead-results'] a[role='option'][href]",
            "[role='listbox'] a[role='option'][href]",
            "[role='listbox'] a[href*='_Review-']",
            "a[role='option'][href*='_Review-']",
        )

        while monotonic() < deadline:
            candidates: list[tuple[str, str]] = []
            seen_hrefs: set[str] = set()
            for selector in selectors:
                anchors = page.locator(selector)
                try:
                    total = await anchors.count()
                except Exception:
                    continue
                for idx in range(min(total, 18)):
                    anchor = anchors.nth(idx)
                    href = await self._safe_locator_attribute(anchor, "href")
                    if not href or href in seen_hrefs:
                        continue
                    seen_hrefs.add(href)
                    if not self._looks_like_tripadvisor_listing_href(href):
                        continue

                    title = await self._safe_locator_inner_text(anchor.locator("div.biGQs._P.ezezH").first)
                    if not title:
                        title = await self._safe_locator_inner_text(anchor.locator("div.GWJnL").first)
                    if not title:
                        title = await self._safe_locator_attribute(anchor, "aria-label")
                    if not title:
                        title = await self._safe_locator_inner_text(anchor)
                    if not title:
                        title = self._title_from_tripadvisor_href(href)
                    if not title:
                        continue
                    candidates.append((title, href))

            selected_href = self._pick_exact_typeahead_candidate_href(query=query, candidates=candidates)
            if selected_href:
                target_url = urljoin(self._tripadvisor_url, selected_href)
                await page.goto(target_url, wait_until="domcontentloaded")
                await self._wait_after_navigation()
                await self._accept_cookies_if_present()
                await self._dismiss_consent_if_present()
                await self._dismiss_location_prompt_if_present()
                return True
            await page.wait_for_timeout(140)
        return False

    async def _find_search_result_cards(self, *, timeout_ms: int) -> Locator | None:
        page = self._require_page()
        selectors = (
            "[data-test-attribute='top-results-card']",
            "[data-test-attribute='location-results-card']",
            "[data-test-attribute$='results-card']",
            "[data-test-attribute*='results-card']",
            "[data-test-attribute$='results_card']",
            "[data-test-attribute*='results_card']",
            "[aria-label$='results_card']",
            "[aria-label*='results_card']",
        )
        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)
        while monotonic() < deadline:
            for selector in selectors:
                cards = page.locator(selector)
                try:
                    total = await cards.count()
                except Exception:
                    continue
                if total == 0:
                    continue
                for idx in range(min(total, 8)):
                    card = cards.nth(idx)
                    href = await self._safe_locator_attribute(card.locator("a[href]").first, "href")
                    if href and self._looks_like_tripadvisor_listing_href(href):
                        return cards
            await page.wait_for_timeout(160)
        return None

    async def _best_listing_href_from_search_results(self, query: str, *, min_score: float = 0.2) -> str:
        page = self._require_page()
        selectors = (
            "[data-test-attribute='all-results-section'] a[href*='_Review-']",
            "[data-test-attribute='all-results-section'] [data-test-attribute='location-results-card'] a[href]",
            "[data-test-attribute$='results-card'] a[href]",
            "[data-test-attribute*='results-card'] a[href]",
            "[data-test-attribute$='results_card'] a[href]",
            "[data-test-attribute*='results_card'] a[href]",
            "main a[href*='/Restaurant_Review-'][href*='Reviews']",
            "main a[href*='/Attraction_Review-'][href*='Reviews']",
            "main a[href*='/Hotel_Review-'][href*='Reviews']",
            "a[href*='/ShowUserReviews-']",
            "a[href*='/Restaurant_Review-'][href*='Reviews']",
            "a[href*='/Attraction_Review-'][href*='Reviews']",
            "a[href*='/Hotel_Review-'][href*='Reviews']",
            "main a[href*='_Review-']",
        )
        query_normalized = self._normalize_text(query)
        candidates: list[_SearchCandidate] = []
        seen_hrefs: set[str] = set()

        for selector in selectors:
            links = page.locator(selector)
            total = await links.count()
            for idx in range(min(total, 40)):
                link = links.nth(idx)
                href = await self._safe_locator_attribute(link, "href")
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)
                if not self._looks_like_tripadvisor_listing_href(href):
                    continue

                title = await self._safe_locator_inner_text(link)
                if not title:
                    title = await self._safe_locator_attribute(link, "aria-label")
                if not title:
                    title = self._title_from_tripadvisor_href(href)
                if not title:
                    continue

                score = self._match_score(query_normalized, self._normalize_text(title))
                href_normalized = href.lower()
                if any(token in href_normalized for token in ("/restaurant_review-", "/attraction_review-", "/hotel_review-")):
                    score = min(1.0, score + 0.1)
                candidates.append(_SearchCandidate(title=title, href=href, score=score))

        if not candidates:
            return ""

        best = max(candidates, key=lambda item: item.score)
        return best.href if best.score >= min_score else ""

    async def _open_reviews_section(self) -> None:
        page = self._require_page()
        current_url = self._clean_text(page.url)
        if "#reviews" not in current_url.lower():
            opened = False
            reviews_anchor = await self._find_first_optional_visible(
                (
                    "a[href='#REVIEWS']",
                    "a[href='#reviews']",
                    "[data-test-target='reviews-tab'] a[href*='#REVIEWS']",
                ),
                timeout_ms=1000,
            )
            if reviews_anchor is not None:
                try:
                    await reviews_anchor.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    await reviews_anchor.click(timeout=1200, force=True)
                    opened = True
                except Exception:
                    opened = False
            if not opened:
                try:
                    changed = await page.evaluate(
                        """
                        () => {
                          if (window.location.hash === '#REVIEWS') return false;
                          window.location.hash = 'REVIEWS';
                          return true;
                        }
                        """
                    )
                except Exception:
                    changed = False
                if not changed:
                    base_url = page.url.split("#", maxsplit=1)[0]
                    target_url = f"{base_url}#REVIEWS"
                    try:
                        await page.goto(target_url, wait_until="commit")
                    except Exception:
                        pass
            await page.wait_for_timeout(120)
        await self._accept_cookies_if_present(timeout_seconds=0.7)
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

    async def _reviews_ready(self, *, timeout_ms: int) -> bool:
        page = self._require_page()
        selectors = (
            "div[data-test-target='reviews-tab'] [data-automation='reviewCard']:visible",
            "div.AjLYs.e[data-test-target='reviews-tab'] [data-automation='reviewCard']:visible",
            "[data-automation='reviewCard']:visible",
            "div[data-test-target='reviews-tab'] h3[data-test-target='review-title']",
            "h3[data-test-target='review-title']",
            "[data-smoke-attr='pagination-next-arrow']",
            "a[href*='ShowUserReviews-']",
        )
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                await locator.wait_for(state="visible", timeout=timeout_ms)
                return True
            except PlaywrightTimeoutError:
                continue
            except Exception:
                continue
        return False

    async def _expand_reviews(self, *, max_clicks: int) -> None:
        if max_clicks <= 0:
            return
        page = self._require_page()
        buttons = page.locator("div[data-test-target='reviews-tab'] button:has-text('Leer')")
        total = await buttons.count()
        if total == 0:
            buttons = page.locator("button:has-text('Leer m'), button:has-text('Leer')")
            total = await buttons.count()
        clicks = 0

        for idx in range(min(total, 20)):
            if clicks >= max_clicks:
                break
            button = buttons.nth(idx)
            try:
                if not await button.is_visible():
                    continue
                label = await self._safe_locator_inner_text(button)
                normalized = self._normalize_text(label)
                if "leer mas" not in normalized:
                    continue
                try:
                    await button.scroll_into_view_if_needed()
                except Exception:
                    pass
                await button.click(timeout=1200, force=True)
                await page.wait_for_timeout(self._rng.randint(120, 280))
                clicks += 1
            except Exception:
                continue

    async def _go_next_reviews_page(self) -> bool:
        page = self._require_page()
        moved = False
        previous_url = page.url
        previous_marker = await self._first_review_marker()
        previous_range = await self._reviews_pagination_snapshot()
        previous_range_start = previous_range.get("range_start")
        previous_range_end = previous_range.get("range_end")
        previous_total_results = previous_range.get("total_results")
        previous_current_page = previous_range.get("current_page")
        previous_total_pages = previous_range.get("total_pages")
        if (
            isinstance(previous_current_page, int)
            and isinstance(previous_total_pages, int)
            and previous_current_page >= previous_total_pages
        ):
            return False
        if (
            isinstance(previous_range_end, int)
            and isinstance(previous_total_results, int)
            and previous_range_end >= previous_total_results
        ):
            return False

        current_offset = self._reviews_offset_from_href(previous_url)
        try:
            next_link = await self._next_reviews_page_link(current_offset=current_offset or 0)
        except Exception:
            next_link = None
        if next_link is not None:
            try:
                await next_link.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                await next_link.click(timeout=2200, force=True)
                moved = True
            except Exception:
                moved = False
        else:
            next_arrow = page.locator(
                "a[data-smoke-attr='pagination-next-arrow'], button[data-smoke-attr='pagination-next-arrow']"
            ).first
            if await next_arrow.count() == 0:
                next_arrow = page.locator("[data-smoke-attr='pagination-next-arrow']").first
            if await next_arrow.count() > 0:
                try:
                    if await next_arrow.is_disabled():
                        return False
                except Exception:
                    pass
                aria_disabled = await self._safe_locator_attribute(next_arrow, "aria-disabled")
                if aria_disabled.lower() == "true":
                    return False
                next_href = await self._safe_locator_attribute(next_arrow, "href")
                target_url = urljoin(self._tripadvisor_url, next_href) if next_href else ""
                if target_url and self._clean_text(target_url) != self._clean_text(previous_url):
                    await page.goto(target_url, wait_until="domcontentloaded")
                    moved = True
                else:
                    try:
                        await next_arrow.click(timeout=1800, force=True)
                        moved = True
                    except Exception:
                        moved = False
            if not moved:
                next_button = await self._find_first_optional_visible(
                    (
                        "button[data-smoke-attr='pagination-next-arrow']",
                        "a[data-smoke-attr='pagination-next-arrow']",
                        "button[aria-label*='pagina siguiente' i]",
                        "button[aria-label*='siguiente' i]",
                        "button[aria-label*='next page' i]",
                        "button[aria-label*='next' i]",
                        "a[aria-label*='pagina siguiente' i]",
                        "a[aria-label*='siguiente' i]",
                        "a[aria-label*='next page' i]",
                        "a[aria-label*='next' i]",
                    ),
                    timeout_ms=1800,
                )
                if next_button is None:
                    return False
                try:
                    try:
                        if await next_button.is_disabled():
                            return False
                    except Exception:
                        pass
                    aria_disabled = await self._safe_locator_attribute(next_button, "aria-disabled")
                    if aria_disabled.lower() == "true":
                        return False
                    await next_button.click(timeout=1800, force=True)
                    moved = True
                except Exception:
                    return False

        if not moved:
            # Fallback: navigate directly to next reviews offset URL when pagination click is flaky.
            next_offset_url = self._next_reviews_offset_url(
                current_url=previous_url,
                current_offset=current_offset or 0,
                range_start=previous_range_start if isinstance(previous_range_start, int) else None,
                range_end=previous_range_end if isinstance(previous_range_end, int) else None,
                total_results=previous_total_results if isinstance(previous_total_results, int) else None,
            )
            if next_offset_url:
                try:
                    await page.goto(next_offset_url, wait_until="domcontentloaded")
                    moved = True
                except Exception:
                    moved = False

        if not moved:
            return False

        url_changed = False
        try:
            await page.wait_for_url(lambda value: self._clean_text(value) != self._clean_text(previous_url), timeout=5500)
            url_changed = True
        except Exception:
            url_changed = False
        if not url_changed:
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5500)
            except PlaywrightTimeoutError:
                pass
        await page.wait_for_timeout(self._rng.randint(90, 220))

        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

        for _ in range(10):
            marker = await self._first_review_marker()
            range_start = (await self._reviews_pagination_snapshot()).get("range_start")
            if page.url != previous_url:
                return True
            if marker and previous_marker and marker != previous_marker:
                return True
            if (
                range_start is not None
                and previous_range_start is not None
                and range_start > previous_range_start
            ):
                return True
            await page.wait_for_timeout(120)
        return False

    async def _recover_reviews_and_retry_pagination(
        self,
        *,
        page_index: int,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> bool:
        page = self._require_page()
        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_recover_reload_started",
                "page": page_index,
                "page_url": page.url,
            },
        )
        try:
            await page.reload(wait_until="domcontentloaded", timeout=12000)
        except Exception as exc:
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_reviews_recover_reload_failed",
                    "page": page_index,
                    "error": self._clean_text(str(exc)),
                    "page_url": page.url,
                },
            )
            return False

        await page.wait_for_timeout(self._rng.randint(120, 300))
        await self._accept_cookies_if_present(timeout_seconds=0.9)
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        await self._open_reviews_section()
        await self._prefetch_reviews_by_scroll(max_seconds=0.8)

        try:
            moved = await self._go_next_reviews_page()
        except Exception as exc:
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_reviews_recover_retry_failed",
                    "page": page_index,
                    "error": self._clean_text(str(exc)),
                    "page_url": page.url,
                },
            )
            return False

        pagination = await self._reviews_pagination_snapshot()
        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_recover_retry_done",
                "page": page_index,
                "recovered": moved,
                "page_url": page.url,
                "current_page": pagination.get("current_page"),
                "total_pages": pagination.get("total_pages"),
            },
        )
        return moved

    def _next_reviews_offset_url(
        self,
        *,
        current_url: str,
        current_offset: int,
        range_start: int | None,
        range_end: int | None,
        total_results: int | None,
    ) -> str:
        value = self._clean_text(current_url)
        if not value:
            return ""

        page_size = 0
        if (
            isinstance(range_start, int)
            and isinstance(range_end, int)
            and range_start > 0
            and range_end >= range_start
        ):
            page_size = max(1, range_end - range_start + 1)
        if page_size <= 0:
            page_size = 15

        next_offset = max(0, current_offset) + page_size
        if isinstance(total_results, int) and total_results > 0 and next_offset >= total_results:
            return ""

        if re.search(r"-Reviews-or\d+-", value, flags=re.IGNORECASE):
            return re.sub(
                r"-Reviews-or\d+-",
                f"-Reviews-or{next_offset}-",
                value,
                count=1,
                flags=re.IGNORECASE,
            )
        if re.search(r"-Reviews-", value, flags=re.IGNORECASE):
            return re.sub(
                r"-Reviews-",
                f"-Reviews-or{next_offset}-",
                value,
                count=1,
                flags=re.IGNORECASE,
            )
        return ""

    async def _next_reviews_page_link(self, *, current_offset: int) -> Locator | None:
        page = self._require_page()
        active_button = await self._active_pagination_button()
        current_page_num: int | None = None
        paginator = page.locator("body")

        if active_button is not None:
            label = await self._safe_locator_attribute(active_button, "aria-label")
            if label.isdigit():
                current_page_num = int(label)
            paginator_candidate = active_button.locator(
                "xpath=ancestor::*[self::nav or self::section or self::div][.//a[@href]][1]"
            )
            if await paginator_candidate.count() > 0:
                paginator = paginator_candidate

        links = paginator.locator("a[href]")
        total = await links.count()
        if total == 0:
            return None

        best_link: Locator | None = None
        best_offset: int | None = None
        best_rank = 9

        for idx in range(min(total, 80)):
            link = links.nth(idx)
            href = await self._safe_locator_attribute(link, "href")
            if not href:
                continue
            if not self._looks_like_reviews_pagination_href(href):
                continue
            offset = self._reviews_offset_from_href(href)
            if offset is None:
                continue
            if offset <= current_offset:
                continue
            link_label = await self._safe_locator_attribute(link, "aria-label")
            label_num = int(link_label) if link_label.isdigit() else None
            is_direct_next = (
                current_page_num is not None
                and label_num is not None
                and label_num == current_page_num + 1
            )
            rank = 0 if is_direct_next else 1
            if rank < best_rank or (rank == best_rank and (best_offset is None or offset < best_offset)):
                best_rank = rank
                best_offset = offset
                best_link = link
        return best_link

    async def _active_pagination_button(self) -> Locator | None:
        page = self._require_page()
        buttons = page.locator("button[disabled][aria-label]")
        total = await buttons.count()
        if total == 0:
            return None

        for idx in range(min(total, 40)):
            button = buttons.nth(idx)
            label = await self._safe_locator_attribute(button, "aria-label")
            if not label.isdigit():
                continue
            container = button.locator(
                "xpath=ancestor::*[self::nav or self::section or self::div][.//a[contains(@href,'Reviews')]][1]"
            )
            if await container.count() > 0:
                return button
        return None

    def _looks_like_reviews_pagination_href(self, href: str) -> bool:
        value = self._clean_text(href)
        if not value:
            return False
        if re.search(r"_Review-.*-Reviews", value, flags=re.IGNORECASE):
            return True
        if re.search(r"-Reviews-or\d+-", value, flags=re.IGNORECASE):
            return True
        return False

    def _reviews_offset_from_href(self, href: str) -> int | None:
        value = self._clean_text(href)
        if not value:
            return None
        match = re.search(r"-Reviews-or(\d+)-", value, flags=re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        if re.search(r"-Reviews-", value, flags=re.IGNORECASE):
            return 0
        return None

    async def _first_review_marker(self) -> str:
        page = self._require_page()
        first_card = page.locator("div[data-test-target='reviews-tab'] [data-automation='reviewCard']").first
        if await first_card.count() == 0:
            first_card = page.locator("[data-automation='reviewCard']").first
        link = first_card.locator("a[href*='ShowUserReviews']").first
        if await link.count() == 0:
            link = page.locator("a[href*='ShowUserReviews-']").first
        if await link.count() == 0:
            return ""
        href = await self._safe_locator_attribute(link, "href")
        review_id = self._extract_review_id_from_href(href)
        if review_id:
            return review_id
        return await self._safe_locator_inner_text(link)

    async def _reviews_range_start(self) -> int | None:
        snapshot = await self._reviews_pagination_snapshot()
        value = snapshot.get("range_start")
        return value if isinstance(value, int) else None

    async def _reviews_pagination_snapshot(self) -> dict[str, int | None]:
        # Prefer DOM snapshot (current page, visible range and total results) with URL fallback.
        page = self._require_page()
        dom_snapshot: dict[str, Any] = {}
        try:
            raw = await page.evaluate(
                """
                () => {
                  const toInt = (value) => {
                    const parsed = Number.parseInt(String(value || '').trim(), 10);
                    return Number.isFinite(parsed) ? parsed : null;
                  };
                  const result = {
                    range_start: null,
                    range_end: null,
                    total_results: null,
                    current_page: null,
                    total_pages: null,
                  };

                  const paginators = Array.from(
                    document.querySelectorAll(
                      "div.lKkrl, nav[aria-label*='agin' i], nav[aria-label*='page' i], [data-smoke-attr='pagination-next-arrow']"
                    )
                  );
                  for (const root of paginators) {
                    const labels = Array.from(root.querySelectorAll("button[aria-label], a[aria-label]"))
                      .map((node) => toInt(node.getAttribute("aria-label")))
                      .filter((value) => value !== null);
                    if (!labels.length) continue;
                    const disabledCurrent = root.querySelector("button[disabled][aria-label]");
                    if (disabledCurrent) {
                      const current = toInt(disabledCurrent.getAttribute("aria-label"));
                      if (current !== null) result.current_page = current;
                    }
                    const maxLabel = Math.max(...labels);
                    if (Number.isFinite(maxLabel) && maxLabel > 0) result.total_pages = maxLabel;
                    break;
                  }

                  const textCandidates = Array.from(
                    document.querySelectorAll("div.Ci, div.biGQs._P.VImYz.ZNjnF, div.qAZoU")
                  )
                    .map((node) => (node.textContent || "").replace(/\\u00a0/g, " ").replace(/\\s+/g, " ").trim())
                    .filter((value) => value.length > 0);
                  for (const text of textCandidates) {
                    const match = text.match(/(\\d+)\\s*[-–]\\s*(\\d+)\\s*(?:de|of)\\s*(\\d+)/i);
                    if (!match) continue;
                    const start = toInt(match[1]);
                    const end = toInt(match[2]);
                    const total = toInt(match[3]);
                    if (start !== null) result.range_start = start;
                    if (end !== null) result.range_end = end;
                    if (total !== null) result.total_results = total;
                    break;
                  }

                  if (
                    result.total_pages === null &&
                    result.total_results !== null &&
                    result.range_start !== null &&
                    result.range_end !== null
                  ) {
                    const pageSize = Math.max(1, result.range_end - result.range_start + 1);
                    result.total_pages = Math.max(1, Math.ceil(result.total_results / pageSize));
                  }

                  return result;
                }
                """
            )
            if isinstance(raw, dict):
                dom_snapshot = raw
        except Exception:
            dom_snapshot = {}

        def _to_int_or_none(value: Any) -> int | None:
            try:
                if value is None:
                    return None
                parsed = int(value)
                return parsed if parsed > 0 else None
            except (TypeError, ValueError):
                return None

        range_start = _to_int_or_none(dom_snapshot.get("range_start"))
        range_end = _to_int_or_none(dom_snapshot.get("range_end"))
        total_results = _to_int_or_none(dom_snapshot.get("total_results"))
        current_page = _to_int_or_none(dom_snapshot.get("current_page"))
        total_pages = _to_int_or_none(dom_snapshot.get("total_pages"))

        offset = self._reviews_offset_from_href(page.url)
        page_size_guess = 15
        if offset is not None:
            if range_start is None:
                range_start = offset + 1
            if range_end is None:
                range_end = offset + page_size_guess
            if current_page is None:
                current_page = max(1, (offset // page_size_guess) + 1)

        if (
            total_pages is None
            and total_results is not None
            and range_start is not None
            and range_end is not None
        ):
            inferred_page_size = max(1, range_end - range_start + 1)
            total_pages = max(1, math.ceil(total_results / inferred_page_size))

        if current_page is not None and total_pages is not None and current_page > total_pages:
            total_pages = current_page

        return {
            "range_start": range_start,
            "range_end": range_end,
            "total_results": total_results,
            "current_page": current_page,
            "total_pages": total_pages,
        }

    async def _extract_primary_json_ld_entity(self) -> dict[str, Any] | None:
        page = self._require_page()
        scripts = page.locator("script[type='application/ld+json']")
        total = await scripts.count()
        entities: list[dict[str, Any]] = []

        for idx in range(min(total, 20)):
            raw = await scripts.nth(idx).text_content()
            if not raw:
                continue
            for entity in self._parse_json_ld_entities(raw):
                if not isinstance(entity, dict):
                    continue
                entities.append(entity)

        for entity in entities:
            entity_type = str(entity.get("@type", "")).lower()
            if any(
                key in entity_type
                for key in ("restaurant", "attraction", "hotel", "touristattraction", "localbusiness")
            ):
                return entity

        for entity in entities:
            if entity.get("aggregateRating"):
                return entity
        return entities[0] if entities else None

    def _parse_json_ld_entities(self, raw: str) -> list[dict[str, Any]]:
        try:
            parsed = json.loads(raw)
        except Exception:
            return []

        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            graph = parsed.get("@graph")
            if isinstance(graph, list):
                return [item for item in graph if isinstance(item, dict)]
            return [parsed]
        return []

    async def _extract_card_title_and_href(self, card: Locator) -> tuple[str, str]:
        anchors = card.locator("a[href]")
        total = await anchors.count()
        best_title = ""
        best_href = ""
        best_weight = -1

        for idx in range(min(total, 20)):
            anchor = anchors.nth(idx)
            href = await self._safe_locator_attribute(anchor, "href")
            if not href:
                continue
            title = await self._safe_locator_inner_text(anchor)
            if not title:
                title = self._title_from_tripadvisor_href(href)
            if not title:
                continue

            weight = len(title)
            normalized_href = href.lower()
            if "_review" in normalized_href:
                weight += 100
            if "opiniones" in self._normalize_text(title):
                weight -= 30
            if weight > best_weight:
                best_weight = weight
                best_title = title
                best_href = href

        return best_title, best_href

    def _looks_like_tripadvisor_listing_href(self, href: str) -> bool:
        value = self._clean_text(href).lower()
        if not value:
            return False
        return any(
            token in value
            for token in (
                "/restaurant_review-",
                "/attraction_review-",
                "/hotel_review-",
                "/showuserreviews-",
            )
        )

    def _title_from_tripadvisor_href(self, href: str) -> str:
        value = self._clean_text(href)
        if not value:
            return ""
        path = value.split("?", maxsplit=1)[0]
        slug = path.rsplit("/", maxsplit=1)[-1]
        if slug.lower().endswith(".html"):
            slug = slug[:-5]
        if "-Reviews" in slug:
            slug = slug.split("-Reviews", maxsplit=1)[-1].lstrip("-")
        if "-or" in slug:
            slug = re.sub(r"^or\d+-", "", slug, flags=re.IGNORECASE)
        if "-" in slug:
            slug = slug.split("-", maxsplit=1)[0]
        slug = slug.replace("_", " ").strip()
        return self._clean_text(slug)

    async def _extract_review_image_urls(self, card: Locator) -> list[str]:
        images = card.locator("button[aria-label*='imagen' i] img")
        total = await images.count()
        collected: list[str] = []
        seen: set[str] = set()

        for idx in range(min(total, 12)):
            image = images.nth(idx)
            src = self._clean_text(await image.get_attribute("src"))
            srcset = self._clean_text(await image.get_attribute("srcset"))
            url = src or self._first_url_from_srcset(srcset)
            if not url:
                continue
            if "default-avatar" in url.lower():
                continue
            if url in seen:
                continue
            seen.add(url)
            collected.append(url)
        return collected

    async def _extract_review_card_rating(self, card: Locator) -> float | None:
        svg = card.locator("svg[data-automation='bubbleRatingImage']").first
        rating_from_paths = await self._extract_bubble_rating_from_svg(svg)
        if rating_from_paths is not None:
            return rating_from_paths

        rating_label = await self._safe_locator_inner_text(svg.locator("title").first)
        if not rating_label:
            rating_label = await self._safe_locator_inner_text(card.locator("title[id*='_lithium']").first)
        return self._parse_rating(rating_label)

    async def _extract_bubble_rating_from_svg(self, svg: Locator) -> float | None:
        try:
            if await svg.count() == 0:
                return None
        except Exception:
            return None

        paths = svg.locator("path")
        try:
            total_paths = await paths.count()
        except Exception:
            return None
        if total_paths <= 0:
            return None

        first_d = await self._safe_locator_attribute(paths.nth(0), "d")
        normalized_first = self._normalize_svg_path_d(first_d)
        if not normalized_first:
            return None

        filled = 0
        for idx in range(min(total_paths, 5)):
            d_value = await self._safe_locator_attribute(paths.nth(idx), "d")
            if self._normalize_svg_path_d(d_value) == normalized_first:
                filled += 1

        if 1 <= filled <= 5:
            return float(filled)
        return None

    def _normalize_svg_path_d(self, value: str) -> str:
        cleaned = self._clean_text(value)
        if not cleaned:
            return ""
        return re.sub(r"\s+", "", cleaned).lower()

    async def _extract_review_author_name(self, card: Locator) -> str:
        return await self._extract_profile_display_name(
            scope=card,
            exclude_names=[],
        )

    async def _extract_profile_display_name(
        self,
        *,
        scope: Locator,
        exclude_names: list[str],
    ) -> str:
        excluded = {self._normalize_text(value) for value in exclude_names if self._clean_text(value)}
        selectors = (
            "div.QIHsu span.biGQs._P.ezezH a[href*='/Profile/']",
            "a[href*='/Profile/'].ukgoS",
            "div.QIHsu span.biGQs._P.ezezH",
            "span.biGQs._P.ezezH",
        )
        for selector in selectors:
            candidates = scope.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 10)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                normalized = self._normalize_text(text)
                if not normalized:
                    continue
                if normalized in excluded:
                    continue
                if "contribuciones" in normalized:
                    continue
                if normalized in {"leer mas", "leer menos", "read more", "read less"}:
                    continue
                return text
        return ""

    async def _extract_owner_reply(self, card: Locator, *, reviewer_author_name: str = "") -> dict[str, str] | None:
        # Fast path: Tripadvisor currently wraps owner reply in `div.mahws` inside each review card.
        block = card.locator("div.mahws").first
        try:
            if await block.count() == 0:
                block = card.locator("div[data-test-target='owner-reply']").first
        except Exception:
            block = card.locator("div.mahws").first
        if await block.count() == 0:
            return None

        author_name = await self._extract_profile_display_name(
            scope=block,
            exclude_names=[reviewer_author_name],
        )
        written_date = await self._owner_reply_written_date(block)
        reply_text = await self._owner_reply_text(block, author_name=author_name, written_date=written_date)
        if not reply_text:
            return None

        return {
            "text": reply_text,
            "relative_time": written_date or "",
            "written_date": written_date or "",
            "author_name": author_name or "",
        }

    async def _owner_reply_marker_in_scope(self, scope: Locator) -> Locator | None:
        marker_selectors = (
            "div:has-text('Esta respuesta es la opinión subjetiva del representante de la dirección')",
            "div:has-text('Esta respuesta es la opinion subjetiva del representante de la direccion')",
            "div:has-text('This response is the subjective opinion of the management representative')",
        )
        for selector in marker_selectors:
            markers = scope.locator(selector)
            try:
                total = await markers.count()
            except Exception:
                continue
            for idx in range(min(total, 6)):
                marker = markers.nth(idx)
                text = await self._safe_locator_inner_text(marker)
                if self._is_owner_reply_disclaimer(text):
                    return marker
        return None

    async def _owner_reply_written_date(self, block: Locator) -> str:
        matches: list[str] = []
        selectors = (
            "div:has-text('Escrita el')",
            "div:has-text('Escrito el')",
            "div:has-text('Responded')",
            "div:has-text('Written')",
        )
        for selector in selectors:
            candidates = block.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 6)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                extracted = self._extract_written_date_line_from_text(text)
                if extracted:
                    matches.append(extracted)

        lines = await self._locator_text_lines(block)
        for line in lines:
            extracted = self._extract_written_date_line_from_text(line)
            if extracted:
                matches.append(extracted)
        return matches[-1] if matches else ""

    def _extract_written_date_line_from_text(self, text: str) -> str:
        cleaned = self._clean_text(text)
        if not cleaned:
            return ""
        patterns = (
            r"(Escrita el .*?)(?=\s+Esta respuesta|\s+This response|$)",
            r"(Escrito el .*?)(?=\s+Esta respuesta|\s+This response|$)",
            r"(Responded .*?)(?=\s+This response|$)",
            r"(Written .*?)(?=\s+This response|$)",
        )
        for pattern in patterns:
            match = re.search(pattern, cleaned, flags=re.IGNORECASE)
            if match:
                return self._clean_text(match.group(1))
        return cleaned if self._looks_like_written_date_text(cleaned) else ""

    async def _owner_reply_text(
        self,
        block: Locator,
        *,
        author_name: str,
        written_date: str,
    ) -> str:
        candidate_selectors = (
            "div._T.FKffI span.JguWG",
            "div._T.FKffI div.biGQs._P.VImYz.AWdfh",
            "span.JguWG",
            "div.biGQs._P.VImYz.AWdfh",
        )
        best = ""
        for selector in candidate_selectors:
            candidates = block.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 8)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                if self._owner_reply_text_is_noise(text, author_name=author_name, written_date=written_date):
                    continue
                if len(text) > len(best):
                    best = text
        if best:
            return best

        lines = await self._locator_text_lines(block)
        cleaned_lines: list[str] = []
        for line in lines:
            if self._owner_reply_text_is_noise(line, author_name=author_name, written_date=written_date):
                continue
            if len(line) < 12:
                continue
            cleaned_lines.append(line)
        return " ".join(cleaned_lines).strip()

    async def _locator_text_lines(self, locator: Locator) -> list[str]:
        try:
            if await locator.count() == 0:
                return []
            raw = await locator.inner_text()
        except Exception:
            return []
        lines = [self._clean_text(line) for line in re.split(r"\n+", raw or "") if self._clean_text(line)]
        return lines

    def _owner_reply_text_is_noise(self, text: str, *, author_name: str, written_date: str) -> bool:
        normalized = self._normalize_text(text)
        if not normalized:
            return True
        if self._is_owner_reply_disclaimer(text):
            return True
        if self._looks_like_written_date_text(text):
            return True
        if normalized in {"leer mas", "leer menos", "read more", "read less"}:
            return True
        if author_name and normalized == self._normalize_text(author_name):
            return True
        if written_date and normalized == self._normalize_text(written_date):
            return True
        return False

    def _is_owner_reply_disclaimer(self, text: str) -> bool:
        normalized = self._normalize_text(text)
        return (
            "respuesta es la opinion subjetiva del representante de la direccion" in normalized
            or "response is the subjective opinion of the management representative" in normalized
        )

    def _looks_like_written_date_text(self, text: str) -> bool:
        normalized = self._normalize_text(text)
        return any(
            token in normalized
            for token in (
                "escrita el",
                "escrito el",
                "written",
                "responded",
            )
        )

    def _first_url_from_srcset(self, srcset: str) -> str:
        if not srcset:
            return ""
        first = srcset.split(",")[0].strip()
        return first.split(" ")[0].strip()

    def _address_from_json_ld(self, value: Any) -> str:
        if isinstance(value, str):
            return self._clean_text(value)
        if not isinstance(value, dict):
            return ""

        parts = [
            self._clean_text(value.get("streetAddress")),
            self._clean_text(value.get("addressLocality")),
            self._clean_text(value.get("addressRegion")),
            self._clean_text(value.get("addressCountry")),
        ]
        return ", ".join(part for part in parts if part)

    def _categories_from_json_ld(self, entity: dict[str, Any]) -> list[str]:
        categories: list[str] = []
        entity_type = self._clean_text(entity.get("@type"))
        if entity_type:
            categories.append(entity_type)
        category = self._clean_text(entity.get("category"))
        if category:
            categories.append(category)
        serves_cuisine = entity.get("servesCuisine")
        if isinstance(serves_cuisine, list):
            for item in serves_cuisine:
                text = self._clean_text(item)
                if text:
                    categories.append(text)
        elif isinstance(serves_cuisine, str):
            text = self._clean_text(serves_cuisine)
            if text:
                categories.append(text)

        unique: list[str] = []
        seen: set[str] = set()
        for item in categories:
            normalized = self._normalize_text(item)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique.append(item)
        return unique[:8]

    def _resolve_effective_pages(
        self,
        *,
        max_pages: int | None,
        max_rounds: int,
        html_scroll_max_rounds: int,
    ) -> int:
        if max_pages is not None:
            return max(1, int(max_pages))
        if max_rounds > 0:
            return max(1, int(max_rounds))
        if html_scroll_max_rounds > 0:
            return max(1, min(100, int(html_scroll_max_rounds)))
        return 25

    async def _wait_after_navigation(self) -> None:
        page = self._require_page()
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except PlaywrightTimeoutError:
            pass
        await page.wait_for_timeout(self._rng.randint(280, 760))

    async def _accept_cookies_if_present(self, *, timeout_seconds: float = 10.0, force_check: bool = False) -> None:
        if self._cookies_checked_once and not force_check:
            return
        page = self._require_page()
        accept_selectors = (
            "#onetrust-accept-btn-handler",
            "button#onetrust-accept-btn-handler",
            "#accept-recommended-btn-handler",
            "button#accept-recommended-btn-handler",
            "button:has-text('Permitirlas todas')",
            "button:has-text('Aceptar todo')",
            "button:has-text('Accept recommended')",
            "button:has-text('Allow all')",
            "button:has-text('Acepto')",
            "button:has-text('Aceptar')",
            "button:has-text('Accept all')",
        )
        open_panel_selectors = (
            "#onetrust-cookie-btn",
            "button#onetrust-cookie-btn",
            "button[aria-label='Cookies']",
        )
        panel_presence_selectors = (
            "#onetrust-banner-sdk",
            "#onetrust-accept-btn-handler",
            "#onetrust-cookie-btn",
            "#accept-recommended-btn-handler",
            "#ot-pc-content",
            "#ot-pc-title",
            "div[role='dialog'][aria-label*='privacidad' i]",
        )

        start = monotonic()
        deadline = start + max(0.4, float(timeout_seconds))
        clicked_any = False
        while monotonic() < deadline:
            scopes: list[Any] = [page, *page.frames]
            saw_cookie_ui = False
            opened_panel = False

            for scope in scopes:
                for selector in panel_presence_selectors:
                    candidates = scope.locator(selector)
                    try:
                        if await candidates.count() > 0:
                            saw_cookie_ui = True
                            break
                    except Exception:
                        continue

                for selector in accept_selectors:
                    candidates = scope.locator(selector)
                    try:
                        total = await candidates.count()
                    except Exception:
                        continue
                    for idx in range(min(total, 6)):
                        candidate = candidates.nth(idx)
                        try:
                            if not await candidate.is_visible():
                                continue
                            await candidate.click(timeout=1200, force=True)
                            clicked_any = True
                            await page.wait_for_timeout(self._rng.randint(260, 520))
                        except Exception:
                            continue

                # Some OneTrust configurations start with a floating cookie icon.
                for selector in open_panel_selectors:
                    candidates = scope.locator(selector)
                    try:
                        total = await candidates.count()
                    except Exception:
                        continue
                    for idx in range(min(total, 3)):
                        candidate = candidates.nth(idx)
                        try:
                            if not await candidate.is_visible():
                                continue
                            await candidate.click(timeout=700, force=True)
                            await page.wait_for_timeout(220)
                            opened_panel = True
                        except Exception:
                            continue

            # Fallback: JS click by id in top document.
            try:
                clicked = await page.evaluate(
                    """
                    () => {
                        const ids = [
                            '#accept-recommended-btn-handler',
                            '#onetrust-accept-btn-handler',
                        ];
                        for (const id of ids) {
                            const btn = document.querySelector(id);
                            if (!btn || btn.disabled) continue;
                            btn.click();
                            return true;
                        }
                        const byText = [...document.querySelectorAll('button')].find((btn) => {
                            const text = (btn.textContent || '').toLowerCase();
                            if (btn.disabled) return false;
                            return (
                                text.includes('permitirlas todas') ||
                                text.includes('accept all') ||
                                text.includes('allow all')
                            );
                        });
                        if (byText) {
                            byText.click();
                            return true;
                        }
                        return false;
                    }
                    """
                )
                if clicked:
                    clicked_any = True
                    await page.wait_for_timeout(self._rng.randint(240, 460))
            except Exception:
                pass

            # Exit once cookie UI is gone. If we clicked something, give UI a short grace period
            # in case a second-step modal appears and requires "Permitirlas todas".
            if not saw_cookie_ui and monotonic() - start >= (1.2 if clicked_any else 0.9):
                self._cookies_checked_once = True
                return
            if opened_panel:
                await page.wait_for_timeout(120)
            await page.wait_for_timeout(180)
        self._cookies_checked_once = True

    async def _dismiss_consent_if_present(self, *, force_check: bool = False) -> None:
        if self._consent_checked_once and not force_check:
            return
        terms = ("aceptar", "accept", "consentir", "agree")
        page = self._require_page()
        scopes: list[Any] = [page, *page.frames]
        selectors = (
            "button[aria-label]",
            "button",
            "[role='button'][aria-label]",
        )

        for scope in scopes:
            for selector in selectors:
                candidates = scope.locator(selector)
                try:
                    total = await candidates.count()
                except Exception:
                    continue
                for idx in range(min(total, 20)):
                    candidate = candidates.nth(idx)
                    try:
                        if not await candidate.is_visible():
                            continue
                        label = self._clean_text(await candidate.get_attribute("aria-label"))
                        if not label:
                            label = await self._safe_locator_inner_text(candidate)
                        normalized = self._normalize_text(label)
                        if not any(term in normalized for term in terms):
                            continue
                        if "acepto" in normalized or "accept all" in normalized:
                            await self._human_click(candidate)
                            await page.wait_for_timeout(self._rng.randint(220, 450))
                            self._consent_checked_once = True
                            return
                        if "cookies" not in normalized and "todo" not in normalized and "all" not in normalized:
                            continue
                        await self._human_click(candidate)
                        await page.wait_for_timeout(self._rng.randint(280, 520))
                        self._consent_checked_once = True
                        return
                    except Exception:
                        continue
        self._consent_checked_once = True

    async def _dismiss_location_prompt_if_present(self, *, force_check: bool = False) -> None:
        if self._location_prompt_checked_once and not force_check:
            return
        negative_terms = (
            "no gracias",
            "ahora no",
            "no permitir",
            "bloquear",
            "rechazar",
            "not now",
            "no thanks",
            "deny",
            "block",
            "dont allow",
            "do not allow",
        )
        page = self._require_page()
        scopes: list[Any] = [page, *page.frames]
        selectors = (
            "button[aria-label]",
            "button",
            "[role='button'][aria-label]",
            "[role='button']",
        )

        for scope in scopes:
            for selector in selectors:
                candidates = scope.locator(selector)
                try:
                    total = await candidates.count()
                except Exception:
                    continue
                for idx in range(min(total, 40)):
                    candidate = candidates.nth(idx)
                    try:
                        if not await candidate.is_visible():
                            continue
                        label = self._clean_text(await candidate.get_attribute("aria-label"))
                        if not label:
                            label = await self._safe_locator_inner_text(candidate)
                        normalized = self._normalize_text(label)
                        if not normalized:
                            continue
                        if not any(term in normalized for term in negative_terms):
                            continue
                        await self._human_click(candidate)
                        await page.wait_for_timeout(self._rng.randint(220, 480))
                        self._location_prompt_checked_once = True
                        return
                    except Exception:
                        continue
        self._location_prompt_checked_once = True

    async def _emit_progress(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        payload: dict[str, Any],
    ) -> None:
        if callback is None:
            return
        try:
            maybe_awaitable = callback(payload)
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        except Exception:
            return

    async def _find_first_visible(
        self,
        selectors: tuple[str, ...],
        *,
        timeout_ms: int,
    ) -> Locator:
        optional = await self._find_first_optional_visible(selectors, timeout_ms=timeout_ms)
        if optional is None:
            raise RuntimeError(f"No visible element found for selectors: {'; '.join(selectors)}")
        return optional

    async def _find_first_optional_visible(
        self,
        selectors: tuple[str, ...],
        *,
        timeout_ms: int,
    ) -> Locator | None:
        page = self._require_page()
        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)
        while monotonic() < deadline:
            for selector in selectors:
                try:
                    candidates = page.locator(selector)
                    total = await candidates.count()
                except Exception:
                    continue
                if total == 0:
                    continue
                for idx in range(min(total, 12)):
                    locator = candidates.nth(idx)
                    try:
                        if await locator.is_visible():
                            return locator
                    except Exception:
                        continue
            await page.wait_for_timeout(120)
        return None

    async def _safe_locator_inner_text(self, locator: Locator) -> str:
        try:
            if await locator.count() == 0:
                return ""
            text = await locator.inner_text()
            return self._clean_text(text)
        except Exception:
            return ""

    async def _safe_locator_attribute(self, locator: Locator, attribute: str) -> str:
        try:
            if await locator.count() == 0:
                return ""
            value = await locator.get_attribute(attribute)
            return self._clean_text(value)
        except Exception:
            return ""

    async def _first_non_empty_text(
        self,
        scope: Locator,
        *,
        selectors: tuple[str, ...],
        max_candidates_per_selector: int = 6,
    ) -> str:
        for selector in selectors:
            candidates = scope.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, max_candidates_per_selector)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if text:
                    return text
        return ""

    def _extract_review_id_from_href(self, href: str) -> str:
        if not href:
            return ""
        match = re.search(r"-r(\d+)-", href)
        if match:
            return match.group(1)
        return ""

    async def _extract_review_id_from_card(self, card: Locator) -> str:
        direct_attr_candidates = (
            await self._safe_locator_attribute(card, "data-reviewid"),
            await self._safe_locator_attribute(card, "data-review-id"),
            await self._safe_locator_attribute(card, "id"),
        )
        for candidate in direct_attr_candidates:
            extracted = self._extract_review_id_from_href(candidate)
            if extracted:
                return extracted
            normalized_candidate = self._clean_text(candidate)
            if normalized_candidate.isdigit():
                return normalized_candidate

        anchors = card.locator("a[href]")
        try:
            total = await anchors.count()
        except Exception:
            return ""
        for idx in range(min(total, 24)):
            href = await self._safe_locator_attribute(anchors.nth(idx), "href")
            extracted = self._extract_review_id_from_href(href)
            if extracted:
                return extracted
        return ""

    def _review_identity(self, review: dict[str, Any]) -> str:
        review_id = self._clean_text(str(review.get("review_id", "") or ""))
        if review_id:
            return f"id:{self._normalize_text(review_id)}"
        parts = [
            str(review.get("author_name", "") or ""),
            str(review.get("review_title", "") or ""),
            str(review.get("relative_time", "") or ""),
            str(review.get("written_date", "") or ""),
            str(review.get("text", "") or ""),
        ]
        joined = "|".join(parts).strip()
        return self._normalize_text(joined)

    def _review_identity_fallback(self, *, review: dict[str, Any], page_index: int, item_index: int) -> str:
        parts = [
            str(review.get("author_name", "") or ""),
            str(review.get("review_title", "") or ""),
            str(review.get("relative_time", "") or ""),
            str(review.get("written_date", "") or ""),
            str(review.get("text", "") or ""),
        ]
        normalized = self._normalize_text("|".join(parts))
        if normalized:
            return f"fallback:{page_index}:{item_index}:{normalized[:220]}"
        return f"fallback:{page_index}:{item_index}"

    def _match_score(self, query_normalized: str, title_normalized: str) -> float:
        if not query_normalized or not title_normalized:
            return 0.0
        if query_normalized == title_normalized:
            return 1.0
        if query_normalized in title_normalized:
            return 0.95
        if title_normalized in query_normalized:
            return 0.9

        query_tokens = set(query_normalized.split())
        title_tokens = set(title_normalized.split())
        if not query_tokens or not title_tokens:
            return 0.0
        overlap = len(query_tokens & title_tokens)
        union = len(query_tokens | title_tokens)
        jaccard = overlap / union if union else 0.0

        prefix_bonus = 0.0
        if title_normalized.startswith(query_normalized[: min(6, len(query_normalized))]):
            prefix_bonus = 0.06
        length_penalty = min(0.2, abs(len(query_normalized) - len(title_normalized)) / 120.0)
        return max(0.0, min(1.0, jaccard + prefix_bonus - length_penalty))

    def _parse_rating(self, value: Any) -> float | None:
        text = self._clean_text(value)
        if not text:
            return None
        normalized = text.replace(",", ".")
        match = re.search(r"(\d+(?:\.\d+)?)", normalized)
        if not match:
            return None
        try:
            rating = float(match.group(1))
        except ValueError:
            return None
        if rating < 0.0 or rating > 5.0:
            return None
        return rating

    def _parse_total_reviews(self, value: Any) -> int | None:
        text = self._clean_text(value)
        if not text:
            return None
        match = re.search(r"(\d[\d\.\, ]*)", text)
        if not match:
            return None
        digits = re.sub(r"[^\d]", "", match.group(1))
        if not digits:
            return None
        try:
            parsed = int(digits)
        except ValueError:
            return None
        return parsed if parsed >= 0 else None

    def _looks_like_tripadvisor_listing_url(self, url: str) -> bool:
        normalized = str(url or "").lower()
        return any(
            key in normalized
            for key in (
                "/attraction_review",
                "/restaurant_review",
                "/hotel_review",
                "/showuserreviews",
                "/attractionproductreview",
            )
        )

    def _resolve_direct_listing_target_url(self, value: str) -> str:
        candidate = self._clean_text(value)
        if not candidate:
            return ""

        normalized = candidate.lower()
        if normalized.startswith(("http://", "https://")):
            if "tripadvisor." not in normalized:
                return ""
            return candidate if self._looks_like_tripadvisor_listing_url(candidate) else ""

        if normalized.startswith("/"):
            return urljoin(self._tripadvisor_url, candidate) if self._looks_like_tripadvisor_listing_url(candidate) else ""

        if re.match(r"^[a-z]+_review-", normalized, flags=re.IGNORECASE):
            candidate = f"/{candidate}"
            return urljoin(self._tripadvisor_url, candidate) if self._looks_like_tripadvisor_listing_url(candidate) else ""

        return ""

    def _pick_exact_typeahead_candidate_href(self, *, query: str, candidates: list[tuple[str, str]]) -> str:
        query_normalized = self._normalize_text(query)
        if not query_normalized:
            return ""

        exact_matches: list[tuple[float, str]] = []
        for title, href in candidates:
            cleaned_href = self._clean_text(href)
            if not cleaned_href:
                continue
            title_normalized = self._normalize_text(title)
            if not title_normalized:
                continue
            if title_normalized != query_normalized:
                continue
            score = self._match_score(query_normalized, title_normalized)
            exact_matches.append((score, cleaned_href))

        if not exact_matches:
            return ""
        exact_matches.sort(key=lambda item: item[0], reverse=True)
        return exact_matches[0][1]

    def _normalize_text(self, value: Any) -> str:
        text = self._clean_text(value)
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _require_page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Playwright page is not configured. Call start() or bind_page(page).")
        return self._page

    def _assert_event_loop_compatible_for_playwright(self) -> None:
        if sys.platform != "win32":
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if "selector" in loop.__class__.__name__.lower():
            raise RuntimeError(
                "Playwright is not compatible with Windows SelectorEventLoop "
                "(common with uvicorn --reload). Run without --reload or in Docker/WSL."
            )

    def _resolve_user_data_dir(self) -> Path:
        path = Path(self._user_data_dir).expanduser()
        if not path.is_absolute():
            path = self._project_root / path
        return path.resolve()

    def _build_chromium_args(self) -> list[str]:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--deny-permission-prompts",
            "--disable-geolocation",
            "--window-size=1920,1080",
            "--lang=es-ES",
        ]
        if self._headless and self._harden_headless:
            args.extend(
                [
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ]
            )
        args.extend(self._extra_chromium_args)

        deduped: list[str] = []
        seen: set[str] = set()
        for arg in args:
            cleaned = str(arg or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    def _block_geolocation_init_script(self) -> str:
        return """
            (() => {
                const deniedError = {
                    code: 1,
                    message: 'User denied Geolocation',
                    PERMISSION_DENIED: 1,
                    POSITION_UNAVAILABLE: 2,
                    TIMEOUT: 3
                };

                if (navigator.geolocation) {
                    navigator.geolocation.getCurrentPosition = (_, error) => {
                        if (typeof error === 'function') error(deniedError);
                    };
                    navigator.geolocation.watchPosition = (_, error) => {
                        if (typeof error === 'function') error(deniedError);
                        return -1;
                    };
                    navigator.geolocation.clearWatch = () => {};
                }

                if (navigator.permissions && navigator.permissions.query) {
                    const originalQuery = navigator.permissions.query.bind(navigator.permissions);
                    navigator.permissions.query = (params) => {
                        if (params && params.name === 'geolocation') {
                            return Promise.resolve({ state: 'denied', onchange: null });
                        }
                        return originalQuery(params);
                    };
                }
            })();
        """

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(max(0, delay_ms) / 1000.0)

    async def _enforce_click_gap(self) -> None:
        target_gap_ms = self._rng.randint(self._min_click_delay_ms, self._max_click_delay_ms)
        if self._last_click_ts is None:
            await self._sleep_ms(self._rng.randint(120, 320))
            return
        elapsed_ms = int((monotonic() - self._last_click_ts) * 1000)
        remaining = target_gap_ms - elapsed_ms
        if remaining > 0:
            await self._sleep_ms(remaining)

    async def _human_click(self, locator: Locator) -> None:
        await self._enforce_click_gap()
        try:
            await locator.scroll_into_view_if_needed()
        except Exception:
            pass
        await locator.click()
        self._last_click_ts = monotonic()

    async def _human_type(self, locator: Locator, text: str) -> None:
        await locator.fill("")
        for char in text:
            await locator.type(char, delay=self._rng.randint(self._min_key_delay_ms, self._max_key_delay_ms))
            if self._rng.random() < 0.04:
                await self._sleep_ms(self._rng.randint(80, 220))
