from __future__ import annotations

from typing import Any, Awaitable, Callable
from urllib.parse import quote_plus

from src.crm.benchmark import generate_uule_v2
from src.scraping_google_maps import GoogleMapsScraper
from src.scraping_google_maps.selectors import SELECTOR_PATTERNS


CanonicalizeMapsUrlFn = Callable[[str], str]
NormalizeTextFn = Callable[[Any], str]
ParseRatingTextFn = Callable[[Any], float | None]
ParseReviewsCountTextFn = Callable[[Any], int | None]
SearchFromCurrentViewFn = Callable[..., Awaitable[None]]
WaitForResultsFeedFn = Callable[..., Awaitable[bool]]
CollectVisibleResultsFn = Callable[..., Awaitable[list[dict[str, Any]]]]
WaitForResultsFeedGrowthFn = Callable[..., Awaitable[bool]]
ScrollResultsFn = Callable[..., Awaitable[None]]
ExtractSingleListingResultFn = Callable[..., Awaitable[dict[str, Any] | None]]
TimeoutMsFn = Callable[[], int]
ScrollIntervalSecondsFn = Callable[[], float]
GeoGridGlFn = Callable[[], str]
GeoGridHlFn = Callable[[], str]


class GoogleMapsGeoGridRuntime:
    def __init__(
        self,
        *,
        canonicalize_maps_url: CanonicalizeMapsUrlFn,
        normalize_text: NormalizeTextFn,
        parse_rating_text: ParseRatingTextFn,
        parse_reviews_count_text: ParseReviewsCountTextFn,
        search_from_current_view: SearchFromCurrentViewFn,
        wait_for_results_feed: WaitForResultsFeedFn,
        collect_visible_results: CollectVisibleResultsFn,
        wait_for_results_feed_growth: WaitForResultsFeedGrowthFn,
        scroll_results: ScrollResultsFn,
        extract_single_listing_result: ExtractSingleListingResultFn,
        timeout_ms: TimeoutMsFn,
        scroll_interval_seconds: ScrollIntervalSecondsFn,
        geo_grid_gl: GeoGridGlFn,
        geo_grid_hl: GeoGridHlFn,
    ) -> None:
        self._canonicalize_maps_url = canonicalize_maps_url
        self._normalize_text = normalize_text
        self._parse_rating_text = parse_rating_text
        self._parse_reviews_count_text = parse_reviews_count_text
        self._search_from_current_view = search_from_current_view
        self._wait_for_results_feed = wait_for_results_feed
        self._collect_visible_results = collect_visible_results
        self._wait_for_results_feed_growth = wait_for_results_feed_growth
        self._scroll_results = scroll_results
        self._extract_single_listing_result = extract_single_listing_result
        self._timeout_ms = timeout_ms
        self._scroll_interval_seconds = scroll_interval_seconds
        self._geo_grid_gl = geo_grid_gl
        self._geo_grid_hl = geo_grid_hl

    async def discover_geo_grid_point_results(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
    ) -> list[dict[str, Any]]:
        lat = float(point.get("lat"))
        lng = float(point.get("lng"))
        point_order = int(point.get("order") or 0)
        point_label = str(point.get("label") or f"Punto {point_order}").strip()
        safe_top_n = max(1, min(100, int(top_n or 10)))
        center_url = f"https://www.google.com/maps/@{lat},{lng},15z?hl=es"
        await scraper.page.goto(center_url, wait_until="domcontentloaded", timeout=self._timeout_ms())
        await scraper.page.wait_for_timeout(700)
        await self._search_from_current_view(scraper=scraper, query=keyword)
        feed_found = await self._wait_for_results_feed(scraper=scraper, timeout_ms=16_000)
        if not feed_found:
            fallback = await self._extract_single_listing_result(
                scraper=scraper,
                keyword=keyword,
                point_order=point_order,
                point_label=point_label,
                lat=lat,
                lng=lng,
            )
            return [fallback] if fallback else []

        collected: dict[str, dict[str, Any]] = {}
        stable_rounds = 0
        max_scroll_rounds = min(80, max(8, int(safe_top_n // 3) + 6))
        scroll_wait_ms = max(350, int(self._scroll_interval_seconds() * 1000))

        for _ in range(max_scroll_rounds):
            before = len(collected)
            for item in await self._collect_visible_results(scraper=scraper):
                name = str(item.get("name") or "").strip()
                raw_url = str(item.get("maps_url") or "").strip()
                canonical_url = self._canonicalize_maps_url(raw_url)
                if not name:
                    continue
                key = canonical_url or self._normalize_text(name)
                if not key or key in collected:
                    continue
                rank = len(collected) + 1
                if rank > safe_top_n:
                    break
                collected[key] = {
                    "rank": rank,
                    "visible_top10": rank <= 10,
                    "provider_mode": "maps_live",
                    "business_key": key,
                    "business_name": name,
                    "maps_url": raw_url or None,
                    "maps_url_canonical": canonical_url or None,
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "category": None,
                    "source_ref": {
                        "keyword": keyword,
                        "point_order": point_order,
                        "point_label": point_label,
                        "lat": lat,
                        "lng": lng,
                        "row": point.get("row"),
                        "col": point.get("col"),
                        "source_card_label": item.get("source_card_label"),
                        "collection_mode": "geo_grid_feed",
                    },
                }

            if len(collected) >= safe_top_n:
                break
            if len(collected) == before:
                growth_detected = await self._wait_for_results_feed_growth(
                    scraper=scraper,
                    min_wait_ms=700,
                    max_wait_ms=3_600,
                )
                if growth_detected:
                    stable_rounds = 0
                    continue
                stable_rounds += 1
            else:
                stable_rounds = 0
            if stable_rounds >= 4:
                break
            await self._scroll_results(scraper=scraper)
            await scraper.page.wait_for_timeout(scroll_wait_ms)

        return list(collected.values())[:safe_top_n]

    async def discover_geo_grid_point_results_uule(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
        radius_m: int,
        throttle_ms: int,
    ) -> list[dict[str, Any]]:
        lat = float(point.get("lat"))
        lng = float(point.get("lng"))
        point_order = int(point.get("order") or 0)
        point_label = str(point.get("label") or f"Punto {point_order}").strip()
        safe_top_n = max(1, min(100, int(top_n or 10)))
        safe_radius_m = max(100, int(radius_m or 1000))
        safe_throttle_ms = max(100, int(throttle_ms or 1200))

        uule = generate_uule_v2(lat=lat, lng=lng, radius_m=safe_radius_m)
        gl = str(self._geo_grid_gl() or "es").strip().lower() or "es"
        hl = str(self._geo_grid_hl() or "es").strip().lower() or "es"
        search_url = (
            "https://www.google.com/search?"
            f"q={quote_plus(keyword)}"
            "&tbm=lcl"
            f"&uule={quote_plus(uule)}"
            f"&gl={quote_plus(gl)}"
            f"&hl={quote_plus(hl)}"
            f"&num={max(20, safe_top_n)}"
        )

        await scraper.page.goto(search_url, wait_until="domcontentloaded", timeout=self._timeout_ms())
        await scraper.page.wait_for_timeout(safe_throttle_ms)

        raw_items = await scraper.page.evaluate(
            """
            () => {
              const text = (value) => String(value || "").replace(/\\s+/g, " ").trim();
              const getNameFromText = (value) => {
                const clean = text(value);
                if (!clean) return "";
                return clean.split("·")[0].split("|")[0].split("\\n")[0].trim();
              };

              const cards = Array.from(document.querySelectorAll(".VkpGBb, .rllt__details, .cXedhc"));
              const anchors = cards.length
                ? cards.flatMap((card) => Array.from(card.querySelectorAll("a[href*='/maps/place/']")))
                : Array.from(document.querySelectorAll("a[href*='/maps/place/']"));

              const rows = [];
              for (const anchor of anchors) {
                if (!(anchor instanceof HTMLAnchorElement)) continue;
                const href = text(anchor.href);
                if (!href || !href.includes("/maps/place/")) continue;
                const card = anchor.closest(".VkpGBb, .rllt__details, .cXedhc, div[role='article'], div[jscontroller]") || anchor.parentElement;
                const cardText = text(card ? card.innerText : "");
                const anchorText = text(anchor.innerText || anchor.getAttribute("aria-label") || "");
                const name = getNameFromText(anchorText) || getNameFromText(cardText);
                if (!name) continue;
                rows.push({
                  name,
                  maps_url: href,
                  snippet: cardText || anchorText,
                });
              }

              return rows;
            }
            """
        )

        deduped: dict[str, dict[str, Any]] = {}
        for item in raw_items if isinstance(raw_items, list) else []:
            if not isinstance(item, dict):
                continue
            business_name = str(item.get("name") or "").strip()
            raw_url = str(item.get("maps_url") or "").strip()
            if not business_name:
                continue
            canonical_url = self._canonicalize_maps_url(raw_url)
            key = canonical_url or self._normalize_text(business_name)
            if not key or key in deduped:
                continue
            snippet = str(item.get("snippet") or "").strip()
            rating = self._parse_rating_text(snippet)
            review_count = self._parse_reviews_count_text(snippet)
            deduped[key] = {
                "business_name": business_name,
                "maps_url": raw_url or None,
                "maps_url_canonical": canonical_url or None,
                "rating": rating,
                "review_count": review_count,
                "category": None,
            }
            if len(deduped) >= safe_top_n:
                break

        if not deduped:
            return await self.discover_geo_grid_point_results(
                scraper=scraper,
                keyword=keyword,
                point=point,
                top_n=safe_top_n,
            )

        payloads: list[dict[str, Any]] = []
        for index, item in enumerate(deduped.values(), start=1):
            payloads.append(
                {
                    "rank": index,
                    "visible_top10": index <= 10,
                    "provider_mode": "uule",
                    "business_key": item.get("maps_url_canonical")
                    or self._normalize_text(item.get("business_name")),
                    "business_name": item.get("business_name"),
                    "maps_url": item.get("maps_url"),
                    "maps_url_canonical": item.get("maps_url_canonical"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "category": item.get("category"),
                    "source_ref": {
                        "keyword": keyword,
                        "point_order": point_order,
                        "point_label": point_label,
                        "lat": lat,
                        "lng": lng,
                        "row": point.get("row"),
                        "col": point.get("col"),
                        "uule": uule,
                        "uule_radius_m": safe_radius_m,
                        "gl": gl,
                        "hl": hl,
                        "collection_mode": "geo_grid_uule_local_pack",
                    },
                }
            )
        return payloads[:safe_top_n]

    async def extract_geo_grid_single_listing_result(
        self,
        *,
        scraper: GoogleMapsScraper,
        keyword: str,
        point_order: int,
        point_label: str,
        lat: float,
        lng: float,
    ) -> dict[str, Any] | None:
        current_url = str(scraper.page.url or "").strip()
        if "/maps/place/" not in current_url:
            return None

        listing_name = ""
        for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    listing_name = str(await locator.inner_text()).strip()
                    break
            except Exception:
                continue
        if not listing_name:
            return None

        canonical_url = self._canonicalize_maps_url(current_url)
        return {
            "rank": 1,
            "visible_top10": True,
            "provider_mode": "maps_live",
            "business_key": canonical_url or self._normalize_text(listing_name),
            "business_name": listing_name,
            "maps_url": current_url,
            "maps_url_canonical": canonical_url or None,
            "rating": self._parse_rating_text(await self.safe_listing_text(scraper=scraper, key="LISTING_RATING")),
            "review_count": self._parse_reviews_count_text(
                await self.safe_listing_text(scraper=scraper, key="LISTING_TOTAL_REVIEWS")
            ),
            "category": None,
            "source_ref": {
                "keyword": keyword,
                "point_order": point_order,
                "point_label": point_label,
                "lat": lat,
                "lng": lng,
                "row": point_order,
                "col": 1,
                "collection_mode": "geo_grid_single_listing_fallback",
            },
        }

    async def safe_listing_text(self, *, scraper: GoogleMapsScraper, key: str) -> str | None:
        for selector in SELECTOR_PATTERNS.get(key, []):
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return str(await locator.inner_text()).strip()
            except Exception:
                continue
        return None
