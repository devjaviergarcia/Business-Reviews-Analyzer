from __future__ import annotations

import re
from typing import Any

from playwright.async_api import Locator


class TripadvisorBrowserReviewIdentityFacet:
    def _first_url_from_srcset(self, srcset: str) -> str:
        if not srcset:
            return ""
        first = srcset.split(",")[0].strip()
        return first.split(" ")[0].strip()

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
