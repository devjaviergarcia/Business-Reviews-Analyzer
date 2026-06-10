from __future__ import annotations

import json
from typing import Any


class TripadvisorBrowserListingFacet:

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
