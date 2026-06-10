from __future__ import annotations

import math
import re
from typing import Any

from playwright.async_api import Locator


class TripadvisorBrowserReviewsPaginationStateFacet:
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
