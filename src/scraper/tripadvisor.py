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
        min_click_delay_ms: int = 3100,
        max_click_delay_ms: int = 5200,
        min_key_delay_ms: int = 90,
        max_key_delay_ms: int = 260,
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
        self._min_click_delay_ms = max(3001, min_click_delay_ms)
        self._max_click_delay_ms = max(self._min_click_delay_ms, max_click_delay_ms)
        self._min_key_delay_ms = max(10, min_key_delay_ms)
        self._max_key_delay_ms = max(self._min_key_delay_ms, max_key_delay_ms)
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
        self._default_user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        )
        self._stealth = Stealth(
            navigator_languages_override=("es-ES", "es"),
            navigator_platform_override="Win32",
            navigator_user_agent_override=self._default_user_agent,
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
                user_agent=self._default_user_agent,
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
                "user_agent": self._default_user_agent,
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

    async def search_business(self, name: str) -> None:
        query = self._clean_text(name)
        if not query:
            raise ValueError("Business query is empty.")

        page = await self.start()
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

        search_input = await self._find_first_visible(
            (
                "input[role='searchbox'][name='q']",
                "input[type='search'][name='q'][aria-label*='Buscar' i]",
                "input[type='search'][name='q'][title='Buscar']",
            ),
            timeout_ms=10000,
        )
        await self._human_click(search_input)
        await self._human_type(search_input, query)
        await page.wait_for_timeout(self._rng.randint(250, 700))

        submit_button = await self._find_first_visible(
            (
                "div.bOfFT button[type='submit'][aria-label*='Buscar' i]",
                "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
                "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
            ),
            timeout_ms=6000,
        )
        await self._human_click(submit_button)

        await self._wait_after_navigation()
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        await self._open_best_search_result(query)

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
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> list[dict[str, Any]]:
        del strategy, html_stable_rounds
        page = self._require_page()
        await self._accept_cookies_if_present()
        await self._open_reviews_section()

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

        for page_index in range(1, effective_pages + 1):
            await self._expand_reviews(max_clicks=1)
            current_items = await self._extract_reviews_from_current_page()

            added_count = 0
            for item in current_items:
                identity = self._review_identity(item)
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

            if page_index >= effective_pages:
                break

            moved = await self._go_next_reviews_page()
            if not moved:
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

            await page.wait_for_timeout(self._rng.uniform(min_pause_s * 1000.0, max_pause_s * 1000.0))

        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_completed",
                "total_unique_reviews": len(all_items),
            },
        )
        return all_items

    async def _extract_reviews_from_current_page(self) -> list[dict[str, Any]]:
        page = self._require_page()
        await self._reviews_ready(timeout_ms=9000)
        cards = page.locator("div[data-test-target='reviews-tab'] [data-automation='reviewCard']")
        total = await cards.count()
        if total == 0:
            cards = page.locator("[data-automation='reviewCard']")
            total = await cards.count()
        items: list[dict[str, Any]] = []

        for idx in range(total):
            card = cards.nth(idx)

            title_anchor = card.locator("h3 a[href*='ShowUserReviews']").first
            title = await self._safe_locator_inner_text(title_anchor)
            title_href = await self._safe_locator_attribute(title_anchor, "href")
            review_id = self._extract_review_id_from_href(title_href)
            if not review_id:
                review_id = self._extract_review_id_from_href(
                    await self._safe_locator_attribute(card.locator("a[href*='ShowUserReviews']").first, "href")
                )

            author_name = await self._safe_locator_inner_text(card.locator("a[href*='/Profile/']").first)
            relative_time = await self._safe_locator_inner_text(card.locator(".jXCrq").first)
            written_date = await self._safe_locator_inner_text(card.locator(".BNelO .biGQs").first)

            rating = await self._extract_review_card_rating(card)

            text = await self._safe_locator_inner_text(card.locator("div.biGQs._P.VImYz.AWdfh").first)
            if not text:
                text = await self._safe_locator_inner_text(card.locator("div._T.FKffI span.yCeTE").first)
            if not text:
                text = await self._safe_locator_inner_text(card.locator("span.yCeTE").first)

            image_urls = await self._extract_review_image_urls(card)
            owner_reply = await self._extract_owner_reply(card)

            item = {
                "source": "tripadvisor",
                "review_id": review_id,
                "author_name": author_name,
                "rating": rating if rating is not None else 0.0,
                "relative_time": relative_time,
                "text": text,
                "review_title": title,
                "written_date": written_date,
                "image_urls": image_urls,
            }
            if owner_reply is not None:
                item["owner_reply"] = {
                    "text": owner_reply.get("text", ""),
                    "relative_time": owner_reply.get("relative_time", ""),
                }
                if owner_reply.get("author_name"):
                    item["owner_reply_author_name"] = owner_reply.get("author_name")
                if owner_reply.get("written_date"):
                    item["owner_reply_written_date"] = owner_reply.get("written_date")
            items.append(item)

        return items

    async def _go_to_home(self) -> None:
        page = self._require_page()
        await page.goto(self._tripadvisor_url, wait_until="domcontentloaded")
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        await self._find_first_visible(
            (
                "input[role='searchbox'][name='q']",
                "input[type='search'][name='q'][aria-label*='Buscar' i]",
            ),
            timeout_ms=12000,
        )

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
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

        if await self._reviews_ready(timeout_ms=3500):
            return

        review_anchor = await self._find_first_optional_visible(
            (
                "a[href='#REVIEWS'][data-automation='bubbleReviewCount']",
                "a[href='#REVIEWS']",
                "a[href*='#REVIEWS']",
                "button:has-text('opiniones')",
                "[aria-label*='opiniones' i]",
            ),
            timeout_ms=6000,
        )
        if review_anchor is not None:
            await self._human_click(review_anchor)
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
            if await self._reviews_ready(timeout_ms=8000):
                return

        base_url = page.url.split("#", maxsplit=1)[0]
        if "#REVIEWS" not in page.url:
            await page.goto(f"{base_url}#REVIEWS", wait_until="domcontentloaded")
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
        if await self._reviews_ready(timeout_ms=18000):
            return

        reviews_tab = page.locator("div[data-test-target='reviews-tab']").first
        try:
            await reviews_tab.scroll_into_view_if_needed()
        except Exception:
            pass
        if await self._reviews_ready(timeout_ms=9000):
            return
        raise RuntimeError("Tripadvisor reviews section did not become available.")

    async def _reviews_ready(self, *, timeout_ms: int) -> bool:
        page = self._require_page()
        selectors = (
            "div[data-test-target='reviews-tab'] [data-automation='reviewCard']",
            "div.AjLYs.e[data-test-target='reviews-tab'] [data-automation='reviewCard']",
            "[data-automation='reviewCard']",
            "div[data-test-target='reviews-tab']",
            "[data-smoke-attr='pagination-next-arrow']",
            "a[href*='ShowUserReviews']",
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
        previous_url = page.url
        previous_marker = await self._first_review_marker()
        previous_range = await self._reviews_pagination_snapshot()
        previous_range_start = previous_range.get("range_start")
        previous_current_page = previous_range.get("current_page")
        previous_total_pages = previous_range.get("total_pages")
        if (
            isinstance(previous_current_page, int)
            and isinstance(previous_total_pages, int)
            and previous_current_page >= previous_total_pages
        ):
            return False

        current_offset = self._reviews_offset_from_href(previous_url)
        next_link = await self._next_reviews_page_link(current_offset=current_offset or 0)
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
            next_arrow = page.locator("a[data-smoke-attr='pagination-next-arrow']").first
            if await next_arrow.count() == 0:
                next_arrow = page.locator("[data-smoke-attr='pagination-next-arrow']").first
            if await next_arrow.count() == 0:
                return False
            next_href = await self._safe_locator_attribute(next_arrow, "href")
            target_url = urljoin(self._tripadvisor_url, next_href) if next_href else ""
            if target_url and self._clean_text(target_url) != self._clean_text(previous_url):
                await page.goto(target_url, wait_until="domcontentloaded")
                moved = True

        if not moved:
            return False

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=9000)
        except PlaywrightTimeoutError:
            pass
        await page.wait_for_timeout(self._rng.randint(180, 420))

        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()

        for _ in range(24):
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
            await page.wait_for_timeout(220)
        return False

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
        if await first_card.count() == 0:
            return ""
        link = first_card.locator("a[href*='ShowUserReviews']").first
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
        page = self._require_page()
        range_text = await self._safe_locator_inner_text(
            page.locator("xpath=//*[contains(translate(., 'SE', 'se'), 'se muestran los resultados')]").first
        )
        if not range_text:
            range_text = await self._safe_locator_inner_text(page.locator("xpath=//*[contains(., ' de ')]").first)

        range_start: int | None = None
        range_end: int | None = None
        total_results: int | None = None

        if range_text:
            match = re.search(
                r"(\d[\d\.\,\s]*)\s*-\s*(\d[\d\.\,\s]*)\s*de\s*(\d[\d\.\,\s]*)",
                range_text,
                flags=re.IGNORECASE,
            )
            if match:
                parts = [re.sub(r"[^\d]", "", group) for group in match.groups()]
                if all(parts):
                    range_start, range_end, total_results = (int(parts[0]), int(parts[1]), int(parts[2]))
            if range_start is None or range_end is None or total_results is None:
                numbers = [int(value) for value in re.findall(r"\d+", range_text)]
                if len(numbers) >= 3:
                    range_start, range_end, total_results = numbers[0], numbers[1], numbers[2]

        current_page: int | None = None
        active_page = await self._active_pagination_button()
        if active_page is not None:
            label = await self._safe_locator_attribute(active_page, "aria-label")
            if label.isdigit():
                current_page = int(label)

        page_size: int | None = None
        if (
            isinstance(range_start, int)
            and isinstance(range_end, int)
            and range_start > 0
            and range_end >= range_start
        ):
            page_size = max(1, range_end - range_start + 1)

        total_pages: int | None = None
        if isinstance(total_results, int) and total_results > 0 and isinstance(page_size, int) and page_size > 0:
            total_pages = max(1, (total_results + page_size - 1) // page_size)

        if current_page is None and isinstance(range_start, int) and isinstance(page_size, int) and page_size > 0:
            current_page = max(1, ((range_start - 1) // page_size) + 1)

        if current_page is not None and total_pages is not None:
            current_page = min(max(1, current_page), total_pages)

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

    async def _extract_owner_reply(self, card: Locator) -> dict[str, str] | None:
        scopes = [card]
        for level in range(1, 7):
            scope = card.locator(f"xpath=ancestor::div[{level}]").first
            if await scope.count() == 0:
                continue
            review_cards = scope.locator("[data-automation='reviewCard']")
            try:
                review_count = await review_cards.count()
            except Exception:
                continue
            if review_count == 1:
                scopes.append(scope)
            if review_count > 1:
                break

        seen_scopes: set[str] = set()
        for scope in scopes:
            marker = await self._owner_reply_marker_in_scope(scope)
            if marker is None:
                continue
            marker_text = await self._safe_locator_inner_text(marker)
            marker_key = self._normalize_text(marker_text)
            if marker_key in seen_scopes:
                continue
            seen_scopes.add(marker_key)

            block = marker.locator("xpath=ancestor::div[.//a[contains(@href,'/Profile/')]][1]").first
            if await block.count() == 0:
                block = marker

            author_name = await self._safe_locator_inner_text(block.locator("a[href*='/Profile/']").first)
            written_date = await self._owner_reply_written_date(block)
            reply_text = await self._owner_reply_text(block, author_name=author_name, written_date=written_date)
            if not reply_text:
                continue

            return {
                "text": reply_text,
                "relative_time": written_date or "",
                "written_date": written_date or "",
                "author_name": author_name or "",
            }

        return None

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
        selectors = (
            "div:has-text('Escrita el')",
            "div:has-text('Escrito el')",
            "div:has-text('Responded')",
            "div:has-text('Written')",
        )
        for selector in selectors:
            text = await self._safe_locator_inner_text(block.locator(selector).first)
            if text and self._looks_like_written_date_text(text):
                return text

        lines = await self._locator_text_lines(block)
        for line in lines:
            if self._looks_like_written_date_text(line):
                return line
        return ""

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
        await page.wait_for_timeout(self._rng.randint(900, 1800))

    async def _accept_cookies_if_present(self) -> None:
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
        deadline = start + 10.0
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
                return
            if opened_panel:
                await page.wait_for_timeout(120)
            await page.wait_for_timeout(180)

    async def _dismiss_consent_if_present(self) -> None:
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
                            await page.wait_for_timeout(self._rng.randint(700, 1300))
                            return
                        if "cookies" not in normalized and "todo" not in normalized and "all" not in normalized:
                            continue
                        await self._human_click(candidate)
                        await page.wait_for_timeout(self._rng.randint(1200, 2200))
                        return
                    except Exception:
                        continue

    async def _dismiss_location_prompt_if_present(self) -> None:
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
                        await page.wait_for_timeout(self._rng.randint(800, 1400))
                        return
                    except Exception:
                        continue

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

    def _extract_review_id_from_href(self, href: str) -> str:
        if not href:
            return ""
        match = re.search(r"-r(\d+)-", href)
        if match:
            return match.group(1)
        return ""

    def _review_identity(self, review: dict[str, Any]) -> str:
        parts = [
            str(review.get("review_id", "") or ""),
            str(review.get("author_name", "") or ""),
            str(review.get("review_title", "") or ""),
            str(review.get("relative_time", "") or ""),
            str(review.get("text", "") or ""),
        ]
        joined = "|".join(parts).strip()
        return self._normalize_text(joined)

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
            await self._sleep_ms(self._rng.randint(450, 1100))
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
            if self._rng.random() < 0.1:
                await self._sleep_ms(self._rng.randint(220, 700))

