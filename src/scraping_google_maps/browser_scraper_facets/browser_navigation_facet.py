from __future__ import annotations

from typing import Any

from playwright.async_api import Locator, TimeoutError as PlaywrightTimeoutError

from src.scraping_google_maps.selectors import SELECTOR_PATTERNS


class GoogleMapsBrowserNavigationFacet:

    async def search_business(self, name: str) -> None:
        page = await self.start()
        await self._dismiss_google_consent_if_present()

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
        clicked = await self._click_cookie_accept_by_aria_label()
        if not clicked:
            clicked = await self._click_first_by_text(terms)
        if clicked:
            await self._require_page().wait_for_timeout(self._rng.randint(1200, 2200))

    async def _click_cookie_accept_by_aria_label(self) -> bool:
        page = self._require_page()
        scopes: list[Any] = [page, *page.frames]
        candidates_selector = "button[aria-label], [role='button'][aria-label]"

        for scope in scopes:
            candidates = scope.locator(candidates_selector)
            try:
                total = await candidates.count()
            except Exception:
                continue

            for idx in range(min(total, 20)):
                candidate = candidates.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                    aria_label = self._clean_text(await candidate.get_attribute("aria-label")) or ""
                    if not self._is_cookie_accept_label(aria_label):
                        continue
                    await self._human_click(candidate)
                    return True
                except Exception:
                    continue

        return False

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

    async def _reviews_panel_root_locator(self) -> Locator | None:
        page = self._require_page()

        for selector in SELECTOR_PATTERNS["REVIEWS_PANEL_READY"]:
            markers = page.locator(selector)
            try:
                total = await markers.count()
            except Exception:
                continue

            for idx in range(min(total, 10)):
                marker = markers.nth(idx)
                try:
                    if not await marker.is_visible():
                        continue
                except Exception:
                    continue

                role_main_root = marker.locator("xpath=ancestor::*[@role='main'][1]")
                try:
                    if await role_main_root.count() > 0:
                        return role_main_root.first
                except Exception:
                    pass

                generic_root = marker.locator("xpath=ancestor::div[1]")
                try:
                    if await generic_root.count() > 0:
                        return generic_root.first
                except Exception:
                    continue

        return None

    async def _panel_scoped_collection(self, key: str) -> Locator | None:
        panel_root = await self._reviews_panel_root_locator()
        if panel_root is None:
            return None

        for selector in SELECTOR_PATTERNS[key]:
            collection = panel_root.locator(selector)
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

        # Second priority: explicit "more reviews" summary button.
        more_reviews = await self._find_more_reviews_summary_button()
        if more_reviews is not None:
            return more_reviews

        # Third priority: generic review button selectors.
        return await self._find_first_valid_review_button_in_group("REVIEWS_BUTTON")

    async def _find_more_reviews_summary_button(self) -> Locator | None:
        page = self._require_page()

        selectors = (
            *SELECTOR_PATTERNS["MORE_REVIEWS_BUTTON"],
            "button[aria-label]",
            "button",
        )
        for selector in selectors:
            candidates = page.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue

            for idx in range(min(total, 50)):
                candidate = candidates.nth(idx)
                try:
                    if not await candidate.is_visible():
                        continue
                except Exception:
                    continue

                label = await self._candidate_label(candidate)
                if not self._is_more_reviews_label(label):
                    continue

                try:
                    tag_name = await candidate.evaluate("el => el.tagName")
                except Exception:
                    continue
                if str(tag_name).upper() != "BUTTON":
                    continue

                return candidate

        return None

    async def _click_more_reviews_summary_button(self) -> bool:
        button = await self._find_more_reviews_summary_button()
        if button is None:
            return False
        await self._human_click(button)
        return True

    async def _has_more_reviews_summary_button_visible(self) -> bool:
        return await self._find_more_reviews_summary_button() is not None

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

    async def _outer_html_from_locator(self, locator: Locator) -> str:
        try:
            if await locator.count() <= 0:
                return ""
        except Exception:
            return ""
        try:
            value = await locator.evaluate("node => node?.outerHTML || ''")
        except Exception:
            return ""
        return str(value or "").strip()

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
