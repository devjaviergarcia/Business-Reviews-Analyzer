from __future__ import annotations

from typing import Any

from playwright.async_api import Locator


class GoogleMapsBrowserListingFacet:

    async def extract_listing(self) -> dict:
        await self._wait_for_listing_ready()

        business_name = await self._text_from_patterns("BUSINESS_NAME")
        address = await self._text_from_patterns("LISTING_ADDRESS")
        phone = await self._text_from_patterns("LISTING_PHONE")
        website = await self._text_from_patterns("LISTING_WEBSITE")
        listing_details = await self._extract_listing_details_from_dom()

        rating_source = await self._attribute_from_patterns("LISTING_RATING", "aria-label")
        if not rating_source:
            rating_source = await self._text_from_patterns("LISTING_RATING")
        if not rating_source:
            rating_source = str(listing_details.get("rating_label") or "").strip() or None

        reviews_source = await self._attribute_from_patterns("LISTING_TOTAL_REVIEWS", "aria-label")
        if not reviews_source:
            reviews_source = await self._text_from_patterns("LISTING_TOTAL_REVIEWS")
        if not reviews_source:
            reviews_source = str(listing_details.get("reviews_label") or "").strip() or None

        categories_raw = await self._collect_texts("LISTING_CATEGORIES", limit=20)
        dom_categories = listing_details.get("categories")
        if isinstance(dom_categories, list):
            categories_raw.extend(str(item or "").strip() for item in dom_categories)
        categories = self._normalize_listing_categories(categories_raw)

        service_options: list[str] = []
        raw_service_options = listing_details.get("service_options")
        if isinstance(raw_service_options, list):
            seen_service_options: set[str] = set()
            for item in raw_service_options:
                cleaned = self._clean_text(str(item or ""))
                if not cleaned:
                    continue
                key = self._normalize_text(cleaned)
                if key in seen_service_options:
                    continue
                seen_service_options.add(key)
                service_options.append(cleaned)

        price_per_person = self._clean_text(str(listing_details.get("price_per_person") or ""))
        menu_url = self._clean_text(str(listing_details.get("menu_url") or ""))
        reservation_url = self._clean_text(str(listing_details.get("reservation_url") or ""))
        description = self._clean_text(str(listing_details.get("description") or ""))
        if not website:
            website = self._clean_text(str(listing_details.get("website_url") or ""))

        return {
            "business_name": business_name,
            "address": address,
            "phone": phone,
            "website": website,
            "overall_rating": self._parse_rating(rating_source),
            "total_reviews": self._parse_total_reviews(reviews_source),
            "categories": categories,
            "category": categories[0] if categories else None,
            "service_options": service_options,
            "price_per_person": price_per_person,
            "menu_url": menu_url,
            "reservation_url": reservation_url,
            "description": description,
        }

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

    async def _extract_listing_details_from_dom(self) -> dict[str, Any]:
        page = self._require_page()
        try:
            raw = await page.evaluate(
                """
                () => {
                  const clean = (value) => {
                    if (typeof value !== "string") return "";
                    return value.replace(/\\s+/g, " ").trim();
                  };
                  const text = (node) => clean(node && node.textContent ? String(node.textContent) : "");

                  const pickHref = (selectors) => {
                    for (const selector of selectors) {
                      const node = document.querySelector(selector);
                      if (!node) continue;
                      const href = clean(String(node.getAttribute("href") || ""));
                      if (href) return href;
                    }
                    return "";
                  };

                  const categoryValues = [];
                  const seenCategory = new Set();
                  const categoryCandidates = Array.from(
                    document.querySelectorAll(
                      "button[jsaction*='.category'], div.LBgpqf button[jsaction*='.category'], div.LBgpqf .fontBodyMedium button"
                    )
                  );
                  for (const node of categoryCandidates) {
                    const value = text(node);
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (seenCategory.has(key)) continue;
                    seenCategory.add(key);
                    categoryValues.push(value);
                  }
                  if (!categoryValues.length) {
                    const headerText = text(document.querySelector("div.LBgpqf .fontBodyMedium"));
                    if (headerText) {
                      categoryValues.push(headerText);
                    }
                  }

                  const serviceOptions = [];
                  const seenService = new Set();
                  const serviceNodes = Array.from(document.querySelectorAll("div.y0K5Df .LTs0Rc"));
                  for (const node of serviceNodes) {
                    const aria = clean(String(node.getAttribute("aria-label") || ""));
                    let value = aria;
                    if (!value) {
                      value = text(node.querySelector("div[aria-hidden='true']") || node);
                    }
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (seenService.has(key)) continue;
                    seenService.add(key);
                    serviceOptions.push(value);
                  }

                  const ratingLabel = clean(
                    String(
                      (
                        document.querySelector("[role='img'][aria-label*='estrella' i]") ||
                        document.querySelector("[role='img'][aria-label*='star' i]")
                      )?.getAttribute("aria-label") || ""
                    )
                  );
                  const reviewsLabel = clean(
                    String(
                      (
                        document.querySelector("button[jsaction*='reviewChart.moreReviews']") ||
                        document.querySelector("[aria-label*='rese' i]") ||
                        document.querySelector("[aria-label*='review' i]")
                      )?.getAttribute("aria-label") || ""
                    )
                  );

                  const priceText =
                    text(document.querySelector(".DfOCNb .MNVeJb > div")) ||
                    text(document.querySelector("[data-item-id='price'] .Io6YTe")) ||
                    "";

                  const descriptionText =
                    text(document.querySelector("div.y0K5Df .PYvSYb")) ||
                    "";

                  const websiteUrl = pickHref([
                    "a[data-item-id='authority'][href]",
                    "[data-item-id='authority'] a[href]",
                  ]);
                  const menuUrl = pickHref([
                    "a[data-item-id='menu'][href]",
                    "[data-item-id='menu'] a[href]",
                  ]);
                  const reservationUrl = pickHref([
                    "a[data-item-id^='action:'][href]",
                    "a[href*='/maps/reserve/']",
                    "a[href*='/reserve/']",
                  ]);

                  return {
                    categories: categoryValues,
                    service_options: serviceOptions,
                    rating_label: ratingLabel || null,
                    reviews_label: reviewsLabel || null,
                    price_per_person: priceText || null,
                    description: descriptionText || null,
                    website_url: websiteUrl || null,
                    menu_url: menuUrl || null,
                    reservation_url: reservationUrl || null,
                  };
                }
                """
            )
        except Exception:
            return {}
        if not isinstance(raw, dict):
            return {}
        return raw

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
