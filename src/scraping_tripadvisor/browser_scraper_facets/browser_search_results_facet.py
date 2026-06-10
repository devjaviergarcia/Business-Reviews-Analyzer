from __future__ import annotations

from urllib.parse import urljoin

from src.scraping_tripadvisor.browser_scraper_types import TripadvisorSearchCandidate


class TripadvisorBrowserSearchResultsFacet:
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
        candidates: list[TripadvisorSearchCandidate] = []

        for idx in range(min(total_cards, 25)):
            card = cards.nth(idx)
            title, href = await self._extract_card_title_and_href(card)
            if not href or not title:
                continue
            score = self._match_score(query_normalized, self._normalize_text(title))
            candidates.append(TripadvisorSearchCandidate(title=title, href=href, score=score))

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
        candidates: list[TripadvisorSearchCandidate] = []
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
                candidates.append(TripadvisorSearchCandidate(title=title, href=href, score=score))

        if not candidates:
            return ""

        best = max(candidates, key=lambda item: item.score)
        return best.href if best.score >= min_score else ""
