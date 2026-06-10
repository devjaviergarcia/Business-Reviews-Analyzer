from __future__ import annotations

from time import monotonic

from playwright.async_api import Locator


class TripadvisorBrowserReviewsPageCollectionFacet:
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
