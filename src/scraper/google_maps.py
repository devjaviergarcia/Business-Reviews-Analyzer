from __future__ import annotations

import asyncio
import html
import random
import re
import unicodedata
from pathlib import Path
from time import monotonic
from typing import Any

from playwright.async_api import (
    BrowserContext,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from src.scraper.selectors import SELECTOR_PATTERNS


class GoogleMapsScraper:
    def __init__(
        self,
        page: Page | None = None,
        *,
        headless: bool = False,
        slow_mo_ms: int = 50,
        user_data_dir: str = "playwright-data",
        browser_channel: str | None = None,
        maps_url: str = "https://www.google.com/maps?hl=es",
        timeout_ms: int = 30000,
        min_click_delay_ms: int = 3100,
        max_click_delay_ms: int = 5200,
        min_key_delay_ms: int = 90,
        max_key_delay_ms: int = 260,
        reviews_strategy: str = "interactive",
    ) -> None:
        self._page = page
        self._external_page = page is not None

        self._headless = headless
        self._slow_mo_ms = slow_mo_ms
        self._user_data_dir = user_data_dir
        self._browser_channel = (browser_channel or "").strip() or None
        self._maps_url = maps_url
        self._timeout_ms = timeout_ms
        self._min_click_delay_ms = max(3001, min_click_delay_ms)
        self._max_click_delay_ms = max(self._min_click_delay_ms, max_click_delay_ms)
        self._min_key_delay_ms = max(10, min_key_delay_ms)
        self._max_key_delay_ms = max(self._min_key_delay_ms, max_key_delay_ms)
        self._project_root = Path(__file__).resolve().parents[2]
        self._reviews_strategy = self._resolve_reviews_strategy(reviews_strategy)

        self._playwright: Playwright | None = None
        self._context: BrowserContext | None = None
        self._last_click_ts: float | None = None
        self._rng = random.Random()
        self._default_user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        )

    def bind_page(self, page: Page) -> None:
        self._page = page
        self._external_page = True

    async def __aenter__(self) -> GoogleMapsScraper:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def start(self) -> Page:
        if self._page is not None:
            return self._page

        self._playwright = await async_playwright().start()
        user_data_dir = self._resolve_user_data_dir()
        launch_options: dict[str, Any] = {
            "user_data_dir": str(user_data_dir),
            "headless": self._headless,
            "slow_mo": self._slow_mo_ms,
            "viewport": {"width": 1366, "height": 900},
            "locale": "es-ES",
            "timezone_id": "Europe/Madrid",
            "user_agent": self._default_user_agent,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if self._browser_channel:
            launch_options["channel"] = self._browser_channel

        try:
            self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
        except Exception:
            if not self._browser_channel:
                raise
            # Fallback to bundled Chromium if requested browser channel is unavailable.
            launch_options.pop("channel", None)
            self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        self._context.set_default_timeout(self._timeout_ms)

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await self._go_to_maps_home()
        return self._page

    async def close(self) -> None:
        if not self._external_page and self._context is not None:
            await self._context.close()

        if not self._external_page and self._playwright is not None:
            await self._playwright.stop()

        self._context = None
        self._playwright = None
        self._page = None
        self._external_page = False
        self._last_click_ts = None

    @property
    def page(self) -> Page:
        return self._require_page()

    async def search_business(self, name: str) -> None:
        page = await self.start()

        search_input = await self._first_visible_from_patterns("SEARCH_INPUT")
        await self._human_click(search_input)
        await self._human_type(search_input, name)
        await page.wait_for_timeout(self._rng.randint(200, 600))

        # Explicit user requirement: always click search button.
        search_button = await self._first_visible_from_patterns("SEARCH_BUTTON")
        await self._human_click(search_button)

        state = await self._wait_for_search_state()
        if state == "results":
            await self._open_first_result()

        await self._wait_for_listing_ready()

    async def extract_listing(self) -> dict:
        await self._wait_for_listing_ready()

        business_name = await self._text_from_patterns("BUSINESS_NAME")
        address = await self._text_from_patterns("LISTING_ADDRESS")
        phone = await self._text_from_patterns("LISTING_PHONE")
        website = await self._text_from_patterns("LISTING_WEBSITE")

        rating_source = await self._attribute_from_patterns("LISTING_RATING", "aria-label")
        if not rating_source:
            rating_source = await self._text_from_patterns("LISTING_RATING")

        reviews_source = await self._attribute_from_patterns("LISTING_TOTAL_REVIEWS", "aria-label")
        if not reviews_source:
            reviews_source = await self._text_from_patterns("LISTING_TOTAL_REVIEWS")

        categories_raw = await self._collect_texts("LISTING_CATEGORIES", limit=30)
        categories = [item for item in categories_raw if self._is_probable_category(item)]

        return {
            "business_name": business_name,
            "address": address,
            "phone": phone,
            "website": website,
            "overall_rating": self._parse_rating(rating_source),
            "total_reviews": self._parse_total_reviews(reviews_source),
            "categories": categories,
        }

    async def scroll_reviews(self, max_rounds: int = 10) -> None:
        if max_rounds <= 0:
            return

        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return

        last_count = await self._review_count()
        stale_rounds = 0
        page = self._require_page()

        for _ in range(max_rounds):
            await self._click_expand_buttons(max_clicks=4)
            scrolled = await self._scroll_reviews_feed_once()
            await page.wait_for_timeout(700)

            current_count = await self._review_count()
            if current_count > last_count:
                last_count = current_count
                stale_rounds = 0
            else:
                stale_rounds += 1

            if stale_rounds >= 2 or not scrolled:
                break

    async def collect_reviews_html_snapshot(
        self,
        *,
        max_rounds: int = 180,
        stable_rounds: int = 6,
        min_pause_ms: int = 750,
        max_pause_ms: int = 1900,
    ) -> str:
        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return ""

        max_rounds = max(1, max_rounds)
        stable_rounds = max(2, stable_rounds)
        min_pause_ms = max(150, min_pause_ms)
        max_pause_ms = max(min_pause_ms, max_pause_ms)

        last_count = await self._review_count()
        unchanged_rounds = 0
        last_top = -1
        last_scroll_height = -1
        page = self._require_page()

        for _ in range(max_rounds):
            metrics = await self._scroll_reviews_feed_step(step_px=self._rng.randint(380, 980))
            await page.wait_for_timeout(self._rng.randint(min_pause_ms, max_pause_ms))

            current_count = await self._review_count()
            moved = bool(metrics.get("scrolled"))
            top = int(metrics.get("scroll_top", -1))
            scroll_height = int(metrics.get("scroll_height", -1))
            count_grew = current_count > last_count
            geometry_changed = top != last_top or scroll_height != last_scroll_height

            if count_grew:
                last_count = current_count

            if moved or count_grew or geometry_changed:
                unchanged_rounds = 0
            else:
                unchanged_rounds += 1

            last_top = top
            last_scroll_height = scroll_height

            at_bottom = bool(metrics.get("at_bottom"))
            if at_bottom and unchanged_rounds >= stable_rounds:
                break

            if not metrics.get("found") and unchanged_rounds >= stable_rounds:
                break

        await page.wait_for_timeout(self._rng.randint(500, 1100))
        return await self._capture_reviews_feed_html()

    async def capture_reviews_container_html(self) -> str:
        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return ""
        return await self._capture_reviews_feed_html()

    def extract_reviews_from_html(self, reviews_html: str, limit: int | None = None) -> list[dict]:
        if not reviews_html:
            return []

        cards = self._extract_review_card_html_fragments(reviews_html)
        if limit is not None and limit > 0:
            cards = cards[:limit]

        items: list[dict[str, Any]] = []
        for card_html in cards:
            review_id = self._extract_attr_value(card_html, "data-review-id")

            author_name = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<div[^>]*class=['\"][^'\"]*d4r55[^'\"]*['\"][^>]*>(.*?)</div>",
                )
            )
            if not author_name:
                author_name = ""

            rating_label = self._extract_first_attr_value_containing(
                card_html,
                "aria-label",
                contains_terms=("estrella", "star"),
            )
            rating = self._parse_rating(rating_label)

            relative_time = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<span[^>]*class=['\"][^'\"]*rsqaWe[^'\"]*['\"][^>]*>(.*?)</span>",
                )
            )
            if not relative_time:
                relative_time = ""

            review_text = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<div[^>]*class=['\"][^'\"]*MyEned[^'\"]*['\"][^>]*>.*?<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
                )
            )
            if not review_text:
                review_text = self._strip_html_markup(
                    self._extract_first_html_fragment(
                        card_html,
                        r"<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
                    )
                )
            if not review_text:
                review_text = ""

            image_urls: list[str] = []
            seen_image_urls: set[str] = set()
            for style_value in self._extract_attr_values(card_html, "style"):
                for url in self._extract_urls_from_style(style_value):
                    if url in seen_image_urls:
                        continue
                    seen_image_urls.add(url)
                    image_urls.append(url)

            review_payload: dict[str, Any] = {
                "source": "google_maps",
                "review_id": review_id,
                "author_name": author_name,
                "rating": rating if rating is not None else 0.0,
                "relative_time": relative_time,
                "text": review_text,
                "image_urls": image_urls,
            }

            owner_reply = self._extract_owner_reply_from_card_html(card_html)
            if owner_reply is not None:
                review_payload["owner_reply"] = owner_reply

            items.append(review_payload)

        return items

    async def extract_reviews(
        self,
        *,
        strategy: str | None = None,
        max_rounds: int = 10,
        html_scroll_max_rounds: int = 180,
        html_stable_rounds: int = 6,
    ) -> list[dict]:
        selected_strategy = self._resolve_reviews_strategy(strategy)

        if selected_strategy == "scroll_copy":
            reviews_html = await self.collect_reviews_html_snapshot(
                max_rounds=html_scroll_max_rounds,
                stable_rounds=html_stable_rounds,
            )
            return self.extract_reviews_from_html(reviews_html)

        if max_rounds > 0:
            await self.scroll_reviews(max_rounds=max_rounds)
        return await self._extract_reviews_interactive()

    async def _extract_reviews_interactive(self) -> list[dict]:
        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return []
        await self._click_expand_buttons(max_clicks=8)

        cards = await self._first_available_collection("REVIEW_CARDS")
        if cards is None:
            return []

        total_cards = await cards.count()
        items: list[dict[str, Any]] = []

        for idx in range(total_cards):
            card = cards.nth(idx)

            review_id = await card.get_attribute("data-review-id")
            author_name = await self._text_from_locator(card.locator("div.d4r55").first)
            if not author_name:
                author_name = self._clean_text(await card.get_attribute("aria-label"))

            rating_label = await self._attribute_from_descendant_patterns(card, "RATING_LABEL", "aria-label")
            rating = self._parse_rating(rating_label)
            relative_time = await self._text_from_locator(card.locator("span.rsqaWe").first)
            review_text = await self._text_from_locator(card.locator(".MyEned .wiI7pd").first)
            image_urls = await self._extract_review_photo_urls(card)

            review_payload: dict[str, Any] = {
                "source": "google_maps",
                "review_id": review_id,
                "author_name": author_name or "",
                "rating": rating if rating is not None else 0.0,
                "relative_time": relative_time or "",
                "text": review_text or "",
                "image_urls": image_urls,
            }

            owner_reply = await self._extract_owner_reply(card)
            if owner_reply is not None:
                review_payload["owner_reply"] = owner_reply

            items.append(review_payload)

        return items

    def _resolve_reviews_strategy(self, strategy: str | None) -> str:
        raw_value = strategy or self._reviews_strategy
        normalized = self._normalize_text(raw_value).replace("-", "_").replace(" ", "_")

        interactive_aliases = {"interactive", "current", "legacy", "expand_click"}
        scroll_copy_aliases = {"scroll_copy", "scroll_and_copy", "html_snapshot", "snapshot"}

        if normalized in interactive_aliases:
            return "interactive"
        if normalized in scroll_copy_aliases:
            return "scroll_copy"

        raise ValueError(
            f"Unknown reviews strategy '{raw_value}'. "
            "Supported: interactive | scroll_copy"
        )

    async def _go_to_maps_home(self) -> None:
        page = self._require_page()
        await page.goto(self._maps_url, wait_until="domcontentloaded")

        search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=8000)
        if search_input is not None:
            return

        await self._dismiss_google_consent_if_present()
        search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=9000)
        if search_input is None:
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
            await self._dismiss_google_consent_if_present()
            search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=9000)

        if search_input is None:
            raise RuntimeError("Google Maps search input was not found after consent fallback.")

    def _require_page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Playwright page is not configured. Call start() or bind_page(page).")
        return self._page

    def _resolve_user_data_dir(self) -> Path:
        path = Path(self._user_data_dir).expanduser()
        if not path.is_absolute():
            path = self._project_root / path
        return path.resolve()

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(max(0, delay_ms) / 1000)

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

    async def _first_visible_from_patterns(self, key: str, timeout_ms: int = 2500) -> Locator:
        page = self._require_page()
        tried: list[str] = []

        for selector in SELECTOR_PATTERNS[key]:
            locator = page.locator(selector).first
            tried.append(selector)
            try:
                await locator.wait_for(state="visible", timeout=timeout_ms)
                return locator
            except PlaywrightTimeoutError:
                continue

        tried_msg = "; ".join(tried)
        raise RuntimeError(f"No visible element found for selector group '{key}'. Tried: {tried_msg}")

    async def _first_optional_visible_from_patterns(self, key: str, timeout_ms: int = 1200) -> Locator | None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            locator = page.locator(selector).first
            try:
                await locator.wait_for(state="visible", timeout=timeout_ms)
                return locator
            except PlaywrightTimeoutError:
                continue

        return None

    async def _dismiss_google_consent_if_present(self) -> None:
        terms = ("aceptar todo", "accept all", "i agree", "estoy de acuerdo")
        clicked = await self._click_first_by_text(terms)
        if clicked:
            await self._require_page().wait_for_timeout(self._rng.randint(1200, 2200))

    async def _first_available_collection(self, key: str) -> Locator | None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            collection = page.locator(selector)
            try:
                if await collection.count() > 0:
                    return collection
            except Exception:
                continue

        return None

    async def _is_any_visible(self, key: str) -> bool:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            locator = page.locator(selector).first
            try:
                if await locator.is_visible():
                    return True
            except Exception:
                continue

        return False

    async def _wait_for_search_state(self, timeout_ms: int = 15000) -> str:
        page = self._require_page()
        deadline = monotonic() + (timeout_ms / 1000)

        while monotonic() < deadline:
            if await self._is_any_visible("LISTING_READY"):
                return "listing"

            if await self._is_any_visible("RESULTS_FEED"):
                for selector in SELECTOR_PATTERNS["RESULT_ITEMS"]:
                    if await page.locator(selector).count() > 0:
                        return "results"

            await page.wait_for_timeout(200)

        raise RuntimeError("Search did not reach listing or results state.")

    async def _open_first_result(self) -> None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS["RESULT_ITEMS"]:
            candidates = page.locator(selector)
            total = await candidates.count()
            if total == 0:
                continue

            for idx in range(min(total, 5)):
                candidate = candidates.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                    await self._human_click(candidate)
                    await page.wait_for_timeout(self._rng.randint(450, 900))
                    return
                except Exception:
                    anchor = candidate.locator("a[href*='/maps/place/']").first
                    try:
                        if await anchor.is_visible():
                            await self._human_click(anchor)
                            await page.wait_for_timeout(self._rng.randint(450, 900))
                            return
                    except Exception:
                        pass
                    continue

        raise RuntimeError("Could not open the first search result from results feed.")

    async def _wait_for_listing_ready(self, timeout_ms: int = 15000) -> None:
        page = self._require_page()
        deadline = monotonic() + (timeout_ms / 1000)

        while monotonic() < deadline:
            if await self._is_any_visible("LISTING_READY"):
                return
            await page.wait_for_timeout(200)

        raise RuntimeError("Business listing did not become ready after search.")

    async def _ensure_reviews_open(self) -> bool:
        if await self._wait_for_reviews_ready(timeout_ms=2200):
            return True

        if await self._is_limited_maps_view():
            return False

        if not await self._has_review_entrypoint():
            return False

        page = self._require_page()

        for _ in range(3):
            clicked_tab = await self._click_first_valid_review_button_in_group("REVIEWS_TAB")
            if clicked_tab:
                await page.wait_for_timeout(self._rng.randint(900, 1700))

            if await self._wait_for_reviews_ready(timeout_ms=4500):
                return True

            clicked_button = await self._click_first_valid_review_button_in_group("REVIEWS_BUTTON")
            if clicked_button:
                await page.wait_for_timeout(self._rng.randint(900, 1700))

            if await self._wait_for_reviews_ready(timeout_ms=5500):
                return True

            # Final fallback: strict button-only scan with nested div text.
            if await self._click_review_entrypoint():
                if await self._wait_for_reviews_ready(timeout_ms=5000):
                    return True

        return await self._wait_for_reviews_ready(timeout_ms=2500)

    async def _wait_for_reviews_ready(self, timeout_ms: int = 8000) -> bool:
        page = self._require_page()
        deadline = monotonic() + (timeout_ms / 1000)

        while monotonic() < deadline:
            if await self._is_any_visible("REVIEWS_PANEL_READY"):
                # Sometimes cards appear only after first scroll in the reviews container.
                await self._scroll_reviews_feed_once()
                await page.wait_for_timeout(700)
                if await self._review_count() > 0:
                    return True
                return True

            # Fallback: accept cards only when Reviews tab is actually selected.
            if await self._is_reviews_tab_selected() and await self._review_count() > 0:
                return True

            await page.wait_for_timeout(220)

        return False

    async def _is_reviews_tab_selected(self) -> bool:
        page = self._require_page()
        selected_tabs = page.locator("[role='tablist'] button[role='tab'][aria-selected='true']")
        try:
            total = await selected_tabs.count()
        except Exception:
            return False

        for idx in range(min(total, 6)):
            tab = selected_tabs.nth(idx)
            try:
                label = await self._candidate_label(tab)
                if self._is_review_entrypoint_text(label):
                    return True
            except Exception:
                continue

        return False

    async def _has_review_entrypoint(self) -> bool:
        if await self._find_first_valid_review_button_in_group("REVIEWS_TAB") is not None:
            return True
        if await self._find_first_valid_review_button_in_group("REVIEWS_BUTTON") is not None:
            return True
        return await self._find_any_valid_review_button() is not None

    async def _review_count(self) -> int:
        cards = await self._first_available_collection("REVIEW_CARDS")
        if cards is None:
            return 0
        return await cards.count()

    async def _click_expand_buttons(self, max_clicks: int = 30) -> int:
        clicks = 0

        for selector in SELECTOR_PATTERNS["REVIEW_EXPAND"]:
            buttons = self._require_page().locator(selector)
            total = await buttons.count()
            if total == 0:
                continue

            for idx in range(total):
                if clicks >= max_clicks:
                    return clicks

                button = buttons.nth(idx)
                try:
                    if not await button.is_visible():
                        continue
                    await self._human_click(button)
                    clicks += 1
                    await self._require_page().wait_for_timeout(self._rng.randint(300, 900))
                except Exception:
                    continue

        return clicks

    async def _scroll_reviews_feed_once(self) -> bool:
        metrics = await self._scroll_reviews_feed_step()
        return bool(metrics.get("found")) and bool(metrics.get("scrolled"))

    async def _scroll_reviews_feed_step(self, step_px: int | None = None) -> dict[str, Any]:
        page = self._require_page()
        card_selectors = list(SELECTOR_PATTERNS["REVIEW_CARDS"])

        normalized_step = max(200, step_px) if step_px is not None else None
        return await page.evaluate(
            """
            (payload) => {
                const selectors = payload.selectors;
                const requestedStep = payload.stepPx;

                let card = null;
                for (const selector of selectors) {
                    card = document.querySelector(selector);
                    if (card) break;
                }

                if (!card) {
                    window.scrollBy(0, Math.max(480, window.innerHeight * 0.6));
                    return {
                        found: false,
                        scrolled: false,
                        at_bottom: true,
                        scroll_top: 0,
                        scroll_height: 0,
                        client_height: 0
                    };
                }

                let parent = card.parentElement;
                while (parent) {
                    const style = window.getComputedStyle(parent);
                    const overflowY = style.overflowY;
                    const canScroll = parent.scrollHeight > parent.clientHeight + 20;
                    if ((overflowY === "auto" || overflowY === "scroll") && canScroll) {
                        const before = parent.scrollTop;
                        const step = requestedStep && requestedStep > 0
                            ? requestedStep
                            : Math.max(420, parent.clientHeight * 0.9);
                        parent.scrollBy(0, step);
                        if (parent.scrollTop === before) {
                            parent.scrollTop = Math.min(parent.scrollTop + step, parent.scrollHeight);
                        }
                        const after = parent.scrollTop;
                        const atBottom = after + parent.clientHeight >= parent.scrollHeight - 4;
                        return {
                            found: true,
                            scrolled: after > before,
                            at_bottom: atBottom,
                            scroll_top: Math.round(after),
                            scroll_height: Math.round(parent.scrollHeight),
                            client_height: Math.round(parent.clientHeight)
                        };
                    }
                    parent = parent.parentElement;
                }

                window.scrollBy(0, Math.max(480, window.innerHeight * 0.6));
                return {
                    found: false,
                    scrolled: true,
                    at_bottom: true,
                    scroll_top: 0,
                    scroll_height: 0,
                    client_height: 0
                };
            }
            """,
            {"selectors": card_selectors, "stepPx": normalized_step},
        )

    async def _capture_reviews_feed_html(self) -> str:
        page = self._require_page()
        card_selectors = list(SELECTOR_PATTERNS["REVIEW_CARDS"])
        snapshot = await page.evaluate(
            """
            (selectors) => {
                let card = null;
                for (const selector of selectors) {
                    card = document.querySelector(selector);
                    if (card) break;
                }

                const findScrollableParent = (node) => {
                    let parent = node?.parentElement || null;
                    while (parent) {
                        const style = window.getComputedStyle(parent);
                        const overflowY = style.overflowY;
                        const canScroll = parent.scrollHeight > parent.clientHeight + 20;
                        if ((overflowY === "auto" || overflowY === "scroll") && canScroll) {
                            return parent;
                        }
                        parent = parent.parentElement;
                    }
                    return null;
                };

                const feed = findScrollableParent(card);
                if (feed) {
                    return feed.outerHTML;
                }

                const cards = [];
                for (const selector of selectors) {
                    const list = document.querySelectorAll(selector);
                    for (const node of list) {
                        cards.push(node.outerHTML);
                    }
                }
                if (cards.length > 0) {
                    return `<div data-review-feed-fallback="true">${cards.join("")}</div>`;
                }

                return "";
            }
            """,
            card_selectors,
        )
        return str(snapshot or "")

    async def _text_from_patterns(self, key: str) -> str | None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            locator = page.locator(selector).first
            text = await self._text_from_locator(locator)
            if text:
                return text

        return None

    async def _attribute_from_patterns(self, key: str, attribute: str) -> str | None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            locator = page.locator(selector).first
            try:
                if await locator.count() <= 0:
                    continue
                value = await locator.get_attribute(attribute)
                cleaned = self._clean_text(value)
                if cleaned:
                    return cleaned
            except Exception:
                continue

        return None

    async def _collect_texts(self, key: str, limit: int = 20) -> list[str]:
        page = self._require_page()
        values: list[str] = []
        seen: set[str] = set()

        for selector in SELECTOR_PATTERNS[key]:
            items = page.locator(selector)
            try:
                total = await items.count()
            except Exception:
                continue

            for idx in range(min(total, limit)):
                text = await self._text_from_locator(items.nth(idx))
                if not text:
                    continue

                norm = self._normalize_text(text)
                if norm in seen:
                    continue

                seen.add(norm)
                values.append(text)

                if len(values) >= limit:
                    return values

        return values

    async def _text_from_descendant_patterns(self, root: Locator, key: str) -> str | None:
        for selector in SELECTOR_PATTERNS[key]:
            locator = root.locator(selector).first
            text = await self._text_from_locator(locator)
            if text:
                return text

        return None

    async def _attribute_from_descendant_patterns(self, root: Locator, key: str, attribute: str) -> str | None:
        for selector in SELECTOR_PATTERNS[key]:
            locator = root.locator(selector).first
            try:
                if await locator.count() <= 0:
                    continue
                value = await locator.get_attribute(attribute)
                cleaned = self._clean_text(value)
                if cleaned:
                    return cleaned
            except Exception:
                continue

        return None

    async def _click_first_by_text(self, terms: tuple[str, ...]) -> bool:
        page = self._require_page()
        regex = re.compile("|".join(re.escape(term) for term in terms), re.IGNORECASE)

        scopes: list[Any] = [page, *page.frames]
        for scope in scopes:
            candidates: list[Locator] = [
                scope.get_by_role("button", name=regex),
                scope.get_by_role("tab", name=regex),
                scope.locator("button, [role='button'], [role='tab']").filter(has_text=regex),
            ]

            for candidate_group in candidates:
                try:
                    total = await candidate_group.count()
                except Exception:
                    continue

                if total <= 0:
                    continue

                for idx in range(min(total, 6)):
                    candidate = candidate_group.nth(idx)
                    try:
                        if not await candidate.is_visible():
                            continue
                        await self._human_click(candidate)
                        return True
                    except Exception:
                        continue

        return False

    async def _click_first_valid_review_button_in_group(self, key: str) -> bool:
        button = await self._find_first_valid_review_button_in_group(key)
        if button is None:
            return False
        await self._human_click(button)
        return True

    async def _find_first_valid_review_button_in_group(self, key: str) -> Locator | None:
        if key == "REVIEWS_TAB":
            return await self._find_valid_reviews_tab_from_tablist()

        page = self._require_page()

        for selector in SELECTOR_PATTERNS[key]:
            candidates = page.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue

            for idx in range(min(total, 10)):
                candidate = candidates.nth(idx)
                try:
                    if await self._is_valid_review_button(candidate):
                        return candidate
                except Exception:
                    continue

        return None

    async def _find_valid_reviews_tab_from_tablist(self) -> Locator | None:
        page = self._require_page()
        tablist_selectors = (
            "div[role='main'] [role='tablist']",
            "[role='tablist']",
        )

        for tablist_selector in tablist_selectors:
            tablists = page.locator(tablist_selector)
            try:
                total_tablists = await tablists.count()
            except Exception:
                continue

            for tablist_idx in range(min(total_tablists, 6)):
                tablist = tablists.nth(tablist_idx)
                try:
                    if not await tablist.is_visible():
                        continue
                except Exception:
                    continue

                tabs = tablist.locator("button[role='tab']")
                try:
                    total_tabs = await tabs.count()
                except Exception:
                    continue

                for tab_idx in range(min(total_tabs, 12)):
                    tab = tabs.nth(tab_idx)
                    try:
                        if await self._is_valid_review_button(tab, must_be_in_tablist=True):
                            return tab
                    except Exception:
                        continue

        return None

    async def _find_any_valid_review_button(self) -> Locator | None:
        # First priority: reviews tab inside a tablist.
        tab = await self._find_valid_reviews_tab_from_tablist()
        if tab is not None:
            return tab

        # Second priority: explicit review button selectors.
        return await self._find_first_valid_review_button_in_group("REVIEWS_BUTTON")

    async def _is_valid_review_button(self, candidate: Locator, *, must_be_in_tablist: bool = False) -> bool:
        try:
            if not await candidate.is_visible():
                return False
        except Exception:
            return False

        try:
            tag_name = await candidate.evaluate("el => el.tagName")
        except Exception:
            return False

        if str(tag_name).upper() != "BUTTON":
            return False

        if must_be_in_tablist and not await self._button_is_inside_tablist(candidate):
            return False

        if not await self._button_has_nested_review_div_text(candidate):
            return False

        label = await self._candidate_label(candidate)
        return self._is_review_entrypoint_text(label)

    async def _button_is_inside_tablist(self, button: Locator) -> bool:
        try:
            return bool(await button.evaluate("el => !!el.closest('[role=\"tablist\"]')"))
        except Exception:
            return False

    async def _button_has_nested_review_div_text(self, button: Locator) -> bool:
        regex = re.compile(r"rese|review", re.IGNORECASE)
        try:
            matching_divs = button.locator("div").filter(has_text=regex)
            return await matching_divs.count() > 0
        except Exception:
            return False

    async def _click_review_entrypoint(self) -> bool:
        button = await self._find_any_valid_review_button()
        if button is None:
            return False
        await self._human_click(button)
        return True

    async def _candidate_label(self, locator: Locator) -> str:
        aria = await locator.get_attribute("aria-label")
        text = await self._text_from_locator(locator)
        parts = [part for part in (aria, text) if part]
        return " ".join(parts)

    async def _text_from_locator(self, locator: Locator) -> str | None:
        try:
            if await locator.count() <= 0:
                return None
        except Exception:
            return None

        text: str | None = None
        try:
            text = await locator.inner_text()
        except Exception:
            try:
                text = await locator.text_content()
            except Exception:
                text = None

        return self._clean_text(text)

    async def _extract_owner_reply(self, card: Locator) -> dict[str, str] | None:
        block = await self._find_owner_reply_block(card)
        if block is None:
            return None

        reply_time = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TIME")
        reply_text = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TEXT")

        if not reply_text:
            raw_block_text = await self._text_from_locator(block)
            if raw_block_text:
                lines = [line.strip() for line in re.split(r"\n+", raw_block_text) if line.strip()]
                cleaned_lines: list[str] = []
                for line in lines:
                    if self._is_owner_reply_label(line):
                        continue
                    if reply_time and self._normalize_text(line) == self._normalize_text(reply_time):
                        continue
                    cleaned_lines.append(line)
                if cleaned_lines:
                    reply_text = " ".join(cleaned_lines)

        if not reply_text:
            return None

        return {"text": reply_text, "relative_time": reply_time or ""}

    async def _find_owner_reply_block(self, card: Locator) -> Locator | None:
        for selector in SELECTOR_PATTERNS["OWNER_REPLY_BLOCK"]:
            candidates = card.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue

            if total <= 0:
                continue

            # Owner reply usually appears at the end of the review block.
            for idx in range(min(total - 1, 5), -1, -1):
                candidate = candidates.nth(idx)
                if await self._looks_like_owner_reply_block(candidate):
                    return candidate

        return None

    async def _looks_like_owner_reply_block(self, block: Locator) -> bool:
        label = await self._text_from_descendant_patterns(block, "OWNER_REPLY_LABEL")
        reply_text = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TEXT")

        if label and self._is_owner_reply_label(label):
            return True

        block_text = await self._text_from_locator(block)
        if block_text and self._is_owner_reply_label(block_text):
            return True

        if reply_text:
            try:
                child_divs = block.locator(":scope > div")
                first_child_span_count = await child_divs.nth(0).locator("span").count()
                child_count = await child_divs.count()
                if child_count >= 2 and first_child_span_count > 0:
                    return True
            except Exception:
                pass

        return False

    async def _extract_review_photo_urls(self, card: Locator) -> list[str]:
        buttons = card.locator("button[data-photo-index][data-review-id]")
        urls: list[str] = []
        seen: set[str] = set()

        try:
            total = await buttons.count()
        except Exception:
            return urls

        for idx in range(total):
            style = await buttons.nth(idx).get_attribute("style")
            for url in self._extract_urls_from_style(style):
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)

        return urls

    def _extract_urls_from_style(self, style: str | None) -> list[str]:
        if not style:
            return []

        matches = re.findall(r"url\(([^)]+)\)", style)
        urls: list[str] = []
        for match in matches:
            cleaned = match.strip().strip("'\"")
            cleaned = html.unescape(cleaned)
            if cleaned:
                urls.append(cleaned)
        return urls

    def _extract_review_card_html_fragments(self, reviews_html: str) -> list[str]:
        open_tag_pattern = re.compile(
            r"<div\b[^>]*\bdata-review-id\s*=\s*(['\"])(?P<review_id>[^\"']+)\1[^>]*>",
            re.IGNORECASE,
        )
        div_tag_pattern = re.compile(r"</?div\b[^>]*>", re.IGNORECASE)

        fragments: list[str] = []
        seen_review_ids: set[str] = set()

        for match in open_tag_pattern.finditer(reviews_html):
            review_id = self._clean_text(match.group("review_id"))
            if not review_id or review_id in seen_review_ids:
                continue

            depth = 1
            end_index: int | None = None
            for div_match in div_tag_pattern.finditer(reviews_html, match.end()):
                token = div_match.group(0).lower()
                if token.startswith("</div"):
                    depth -= 1
                else:
                    depth += 1

                if depth == 0:
                    end_index = div_match.end()
                    break

            if end_index is None:
                continue

            fragments.append(reviews_html[match.start() : end_index])
            seen_review_ids.add(review_id)

        return fragments

    def _extract_attr_value(self, source: str, attribute: str) -> str | None:
        values = self._extract_attr_values(source, attribute)
        return values[0] if values else None

    def _extract_attr_values(self, source: str, attribute: str) -> list[str]:
        pattern = re.compile(
            rf"\b{re.escape(attribute)}\s*=\s*(['\"])(.*?)\1",
            re.IGNORECASE | re.DOTALL,
        )
        values: list[str] = []
        for match in pattern.finditer(source):
            raw_value = html.unescape(match.group(2))
            cleaned = self._clean_text(raw_value)
            if cleaned:
                values.append(cleaned)
        return values

    def _extract_first_attr_value_containing(
        self,
        source: str,
        attribute: str,
        *,
        contains_terms: tuple[str, ...],
    ) -> str | None:
        for value in self._extract_attr_values(source, attribute):
            normalized = self._normalize_text(value)
            if any(term in normalized for term in contains_terms):
                return value
        return None

    def _extract_first_html_fragment(self, source: str, pattern: str) -> str | None:
        match = re.search(pattern, source, re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return match.group(1)

    def _strip_html_markup(self, value: str | None) -> str | None:
        if not value:
            return None
        without_tags = re.sub(r"<[^>]+>", " ", value)
        decoded = html.unescape(without_tags)
        return self._clean_text(decoded)

    def _extract_owner_reply_from_card_html(self, card_html: str) -> dict[str, str] | None:
        marker_pattern = re.compile(
            r"(Respuesta del propietario|Owner response|Response from the owner)",
            re.IGNORECASE,
        )
        marker = marker_pattern.search(card_html)
        if marker is None:
            return None

        after_marker = card_html[marker.end() :]

        reply_time = self._strip_html_markup(
            self._extract_first_html_fragment(
                after_marker,
                r"<span[^>]*class=['\"][^'\"]*DZSIDd[^'\"]*['\"][^>]*>(.*?)</span>",
            )
        )
        reply_text = self._strip_html_markup(
            self._extract_first_html_fragment(
                after_marker,
                r"<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
            )
        )
        if not reply_text:
            return None

        return {"text": reply_text, "relative_time": reply_time or ""}

    def _parse_rating(self, value: str | None) -> float | None:
        if not value:
            return None

        cleaned = self._normalize_text(value)
        match = re.search(r"(\d+(?:[.,]\d+)?)", cleaned)
        if not match:
            return None

        number = match.group(1).replace(",", ".")
        try:
            rating = float(number)
        except ValueError:
            return None

        if 0.0 <= rating <= 5.0:
            return rating

        return None

    def _parse_total_reviews(self, value: str | None) -> int | None:
        if not value:
            return None

        candidates = re.findall(r"\d[\d.,\s]*", value)
        if not candidates:
            return None

        numbers: list[int] = []
        for candidate in candidates:
            digits = re.sub(r"\D", "", candidate)
            if not digits:
                continue
            try:
                numbers.append(int(digits))
            except ValueError:
                continue

        if not numbers:
            return None

        high_confidence = [number for number in numbers if number >= 10]
        if high_confidence:
            return max(high_confidence)

        return max(numbers)

    def _is_probable_category(self, value: str) -> bool:
        normalized = self._normalize_text(value)
        if not normalized:
            return False

        if len(normalized) > 35:
            return False

        if re.search(r"\d", normalized):
            return False

        blocked_terms = {
            "copiar",
            "guardar",
            "compartir",
            "como llegar",
            "escribir una resena",
            "resenas",
            "informacion",
            "vista general",
            "carta",
            "ordenar",
            "buscar resenas",
            "reviews",
        }
        return normalized not in blocked_terms

    def _is_owner_reply_label(self, value: str) -> bool:
        normalized = self._normalize_text(value)
        keywords = (
            "respuesta del propietario",
            "owner response",
            "response from the owner",
        )
        return any(keyword in normalized for keyword in keywords)

    def _is_review_entrypoint_text(self, value: str | None) -> bool:
        normalized = self._normalize_text(value or "")
        if not normalized:
            return False

        if "rese" not in normalized and "review" not in normalized:
            return False

        blocked_tokens = (
            "aviso legal",
            "avisos legales",
            "mas informacion sobre los avisos legales",
            "publicas en google maps",
            "public reviews",
            "escribir una resena",
            "write a review",
            "resumen de resenas",
            "review summary",
            "acciones en la resena",
            "compartir resena",
            "share review",
        )
        return not any(token in normalized for token in blocked_tokens)

    async def _is_limited_maps_view(self) -> bool:
        page = self._require_page()
        try:
            return await page.evaluate(
                """
                () => {
                    const text = (document.body?.innerText || '').toLowerCase();
                    return (
                        text.includes('vista limitada de google maps') ||
                        text.includes('limited view of google maps')
                    );
                }
                """
            )
        except Exception:
            return False

    def _clean_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", value).strip()
        return cleaned or None

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized
