from __future__ import annotations

from typing import Any, Awaitable, Callable
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError


class TripadvisorBrowserReviewsNavigationFacet:
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
