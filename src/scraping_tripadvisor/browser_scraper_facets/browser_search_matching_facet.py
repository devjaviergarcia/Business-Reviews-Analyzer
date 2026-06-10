from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import Locator


class TripadvisorBrowserSearchMatchingFacet:

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
