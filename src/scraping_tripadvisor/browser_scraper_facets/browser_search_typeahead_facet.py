from __future__ import annotations

from time import monotonic
from urllib.parse import urljoin


class TripadvisorBrowserSearchTypeaheadFacet:
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
