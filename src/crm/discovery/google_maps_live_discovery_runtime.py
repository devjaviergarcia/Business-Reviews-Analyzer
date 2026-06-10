from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from src.scraping_google_maps import GoogleMapsScraper
from src.scraping_google_maps.selectors import SELECTOR_PATTERNS
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


ScraperFactory = Callable[[], GoogleMapsScraper]
NormalizeTextFn = Callable[[Any], str]
CanonicalizeMapsUrlFn = Callable[[str], str]
ParseRatingTextFn = Callable[[Any], float | None]
ParseReviewsCountTextFn = Callable[[Any], int | None]
SanitizeListingCategoriesFn = Callable[[list[str]], list[str]]
ExtractCityFromAddressFn = Callable[[str | None], str | None]
MergeListingPayloadsFn = Callable[..., dict[str, Any]]
ScrollIntervalSecondsFn = Callable[[], float]
ResolveFirstVisiblePatternFn = Callable[..., Awaitable[Any | None]]
SearchGoogleMapsQueryFn = Callable[..., Awaitable[None]]
WaitForResultsFeedFn = Callable[..., Awaitable[bool]]
WaitForResultsFeedGrowthFn = Callable[..., Awaitable[bool]]
CollectVisibleResultsFn = Callable[..., Awaitable[list[dict[str, Any]]]]
ScrollResultsFn = Callable[..., Awaitable[None]]
EnrichCandidatesFn = Callable[..., Awaitable[list[dict[str, Any]]]]


class GoogleMapsLiveDiscoveryRuntime:
    def __init__(
        self,
        *,
        scraper_factory: ScraperFactory,
        normalize_text: NormalizeTextFn,
        canonicalize_maps_url: CanonicalizeMapsUrlFn,
        parse_rating_text: ParseRatingTextFn,
        parse_reviews_count_text: ParseReviewsCountTextFn,
        sanitize_listing_categories: SanitizeListingCategoriesFn,
        extract_city_from_address: ExtractCityFromAddressFn,
        merge_listing_payloads: MergeListingPayloadsFn,
        scroll_interval_seconds: ScrollIntervalSecondsFn,
        resolve_first_visible_pattern: ResolveFirstVisiblePatternFn,
        search_google_maps_query: SearchGoogleMapsQueryFn,
        wait_for_results_feed: WaitForResultsFeedFn,
        wait_for_results_feed_growth: WaitForResultsFeedGrowthFn,
        collect_visible_results: CollectVisibleResultsFn,
        scroll_results: ScrollResultsFn,
        enrich_candidates: EnrichCandidatesFn,
    ) -> None:
        self._scraper_factory = scraper_factory
        self._normalize_text = normalize_text
        self._canonicalize_maps_url = canonicalize_maps_url
        self._parse_rating_text = parse_rating_text
        self._parse_reviews_count_text = parse_reviews_count_text
        self._sanitize_listing_categories = sanitize_listing_categories
        self._extract_city_from_address = extract_city_from_address
        self._merge_listing_payloads = merge_listing_payloads
        self._scroll_interval_seconds = scroll_interval_seconds
        self._resolve_first_visible_pattern = resolve_first_visible_pattern
        self._search_google_maps_query = search_google_maps_query
        self._wait_for_results_feed = wait_for_results_feed
        self._wait_for_results_feed_growth = wait_for_results_feed_growth
        self._collect_visible_results = collect_visible_results
        self._scroll_results = scroll_results
        self._enrich_candidates = enrich_candidates

    async def discover_candidates_live_google_maps(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or "").strip()
        city_text = str(task_payload.city or "").strip()
        search_query = query_text if not city_text else f"{query_text} {city_text}".strip()
        if not search_query:
            return []

        scraper = self._scraper_factory()
        max_scroll_rounds = min(180, max(20, int(safe_limit // 2) + 10))
        scroll_wait_ms = max(400, int(self._scroll_interval_seconds() * 1000))
        collected: dict[str, dict[str, Any]] = {}

        try:
            await scraper.start()
            await self._search_google_maps_query(scraper=scraper, query=search_query)
            feed_found = await self._wait_for_results_feed(scraper=scraper, timeout_ms=16_000)
            if not feed_found:
                listing_name = ""
                for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
                    locator = scraper.page.locator(selector).first
                    try:
                        if await locator.is_visible():
                            listing_name = str(await locator.inner_text()).strip()
                            break
                    except Exception:
                        continue

                current_url = str(scraper.page.url or "").strip()
                if listing_name and "/maps/place/" in current_url:
                    canonical_url = self._canonicalize_maps_url(current_url)
                    fallback_candidates = [
                        {
                            "business_name": listing_name,
                            "category": str(task_payload.category or "").strip() or None,
                            "address": None,
                            "city": str(task_payload.city or "").strip() or None,
                            "phone": None,
                            "email": None,
                            "website": None,
                            "source": "google_maps_live_discovery",
                            "source_ref": {
                                "maps_url": current_url,
                                "maps_url_canonical": canonical_url or current_url,
                                "discovery_query": search_query,
                                "source_card_label": None,
                                "discovery_mode": "live_google_maps_auto_scroll",
                            },
                            "rating": None,
                            "review_count": None,
                        }
                    ]
                    return await self._enrich_candidates(scraper=scraper, candidates=fallback_candidates)
                return []

            stable_rounds = 0
            for _ in range(max_scroll_rounds):
                before = len(collected)
                visible_items = await self._collect_visible_results(scraper=scraper)
                for item in visible_items:
                    name = str(item.get("name") or "").strip()
                    raw_url = str(item.get("maps_url") or "").strip()
                    canonical_url = self._canonicalize_maps_url(raw_url)
                    if not name or not canonical_url:
                        continue
                    key = f"{canonical_url}|{self._normalize_text(name)}"
                    if key in collected:
                        continue
                    source_card_label = str(item.get("source_card_label") or "").strip() or None

                    collected[key] = {
                        "business_name": name,
                        "category": str(task_payload.category or "").strip() or None,
                        "address": None,
                        "city": str(task_payload.city or "").strip() or None,
                        "phone": None,
                        "email": None,
                        "website": None,
                        "source": "google_maps_live_discovery",
                        "source_ref": {
                            "maps_url": raw_url,
                            "maps_url_canonical": canonical_url,
                            "discovery_query": search_query,
                            "source_card_label": source_card_label,
                            "discovery_mode": "live_google_maps_auto_scroll",
                        },
                        "rating": item.get("rating"),
                        "review_count": item.get("review_count"),
                    }

                if len(collected) >= safe_limit:
                    break

                if len(collected) == before:
                    growth_detected = await self._wait_for_results_feed_growth(
                        scraper=scraper,
                        min_wait_ms=900,
                        max_wait_ms=4_200,
                    )
                    if growth_detected:
                        stable_rounds = 0
                        continue
                    stable_rounds += 1
                else:
                    stable_rounds = 0

                if stable_rounds >= 5:
                    break

                await self._scroll_results(scraper=scraper)
                await scraper.page.wait_for_timeout(scroll_wait_ms)

            candidates = list(collected.values())
            candidates.sort(key=lambda item: self._normalize_text(str(item.get("business_name") or "")))
            if normalized_query:
                query_tokens = set(normalized_query.split())
                candidates.sort(
                    key=lambda item: len(
                        query_tokens & set(self._normalize_text(str(item.get("business_name") or "")).split())
                    ),
                    reverse=True,
                )
            top_candidates = candidates[:safe_limit]
            return await self._enrich_candidates(scraper=scraper, candidates=top_candidates)
        finally:
            await scraper.close()

    async def wait_for_results_feed(
        self,
        *,
        scraper: GoogleMapsScraper,
        timeout_ms: int = 15_000,
    ) -> bool:
        deadline = asyncio.get_running_loop().time() + (max(1, int(timeout_ms)) / 1000.0)
        while asyncio.get_running_loop().time() < deadline:
            for selector in SELECTOR_PATTERNS["RESULTS_FEED"]:
                locator = scraper.page.locator(selector).first
                try:
                    if await locator.is_visible():
                        return True
                except Exception:
                    continue
            await scraper.page.wait_for_timeout(220)
        return False

    async def first_visible_from_patterns(
        self,
        *,
        scraper: GoogleMapsScraper,
        key: str,
        timeout_ms: int = 1_200,
    ) -> Any | None:
        for selector in SELECTOR_PATTERNS[key]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return locator
            except Exception:
                continue
            try:
                await locator.wait_for(state="visible", timeout=max(1, timeout_ms))
                return locator
            except Exception:
                continue
        return None

    async def search_google_maps_query(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        await scraper._go_to_maps_home()
        await scraper._dismiss_google_consent_if_present()
        search_input = await self._resolve_first_visible_pattern(
            scraper=scraper,
            key="SEARCH_INPUT",
            timeout_ms=8_000,
        )
        if search_input is None:
            raise RuntimeError("No se encontró el input de búsqueda de Google Maps para discovery live.")

        await scraper._human_click(search_input)
        await scraper.page.keyboard.press("Control+A")
        await scraper.page.keyboard.press("Backspace")
        await scraper._human_type(search_input, query)
        await scraper.page.wait_for_timeout(300)

        search_button = await self._resolve_first_visible_pattern(
            scraper=scraper,
            key="SEARCH_BUTTON",
            timeout_ms=2_000,
        )
        if search_button is None:
            await scraper.page.keyboard.press("Enter")
        else:
            await scraper._human_click(search_button)

    async def search_google_maps_query_from_current_view(
        self,
        *,
        scraper: GoogleMapsScraper,
        query: str,
    ) -> None:
        await scraper._dismiss_google_consent_if_present()
        search_input = await self._resolve_first_visible_pattern(
            scraper=scraper,
            key="SEARCH_INPUT",
            timeout_ms=8_000,
        )
        if search_input is None:
            raise RuntimeError("No se encontró el input de búsqueda de Google Maps para GeoGrid.")

        await scraper._human_click(search_input)
        await scraper.page.keyboard.press("Control+A")
        await scraper.page.keyboard.press("Backspace")
        await scraper._human_type(search_input, query)
        await scraper.page.wait_for_timeout(250)

        search_button = await self._resolve_first_visible_pattern(
            scraper=scraper,
            key="SEARCH_BUTTON",
            timeout_ms=2_000,
        )
        if search_button is None:
            await scraper.page.keyboard.press("Enter")
        else:
            await scraper._human_click(search_button)

    async def read_results_feed_metrics(self, *, scraper: GoogleMapsScraper) -> dict[str, Any]:
        raw = await scraper.page.evaluate(
            """
            () => {
              const isVisible = (node) => {
                if (!(node instanceof HTMLElement)) return false;
                const style = window.getComputedStyle(node);
                if (!style) return false;
                return style.display !== "none" && style.visibility !== "hidden";
              };

              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );
              let bestFeed = null;
              let bestAnchors = [];
              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchors = Array.from(candidate.querySelectorAll("a[href*='/maps/place/']"));
                if (anchors.length > bestAnchors.length) {
                  bestAnchors = anchors;
                  bestFeed = candidate;
                }
              }
              if (!(bestFeed instanceof HTMLElement)) {
                return { found: false };
              }

              const anchorCount = bestAnchors.length;
              const loadingProgress = Array.from(
                document.querySelectorAll("[role='progressbar'], div[aria-busy='true'], div[aria-label*='cargando' i], div[aria-label*='loading' i]")
              ).some(isVisible);
              const loadingSkeleton = Array.from(
                bestFeed.querySelectorAll(".q0z1yb.CiOaN, .UJwFBf, .uQ4NLd")
              ).some(isVisible);
              const loading = loadingProgress || loadingSkeleton;

              const scrollHeight = Number(bestFeed.scrollHeight || 0);
              const scrollTop = Number(bestFeed.scrollTop || 0);
              const clientHeight = Number(bestFeed.clientHeight || 0);
              const atBottom = scrollTop + clientHeight >= scrollHeight - 6;

              return {
                found: true,
                anchor_count: anchorCount,
                loading: Boolean(loading),
                scroll_height: scrollHeight,
                scroll_top: scrollTop,
                client_height: clientHeight,
                at_bottom: Boolean(atBottom),
              };
            }
            """
        )
        if not isinstance(raw, dict):
            return {"found": False}
        return {
            "found": bool(raw.get("found")),
            "anchor_count": int(raw.get("anchor_count") or 0),
            "loading": bool(raw.get("loading")),
            "scroll_height": int(raw.get("scroll_height") or 0),
            "scroll_top": int(raw.get("scroll_top") or 0),
            "client_height": int(raw.get("client_height") or 0),
            "at_bottom": bool(raw.get("at_bottom")),
        }

    async def wait_for_results_feed_growth(
        self,
        *,
        scraper: GoogleMapsScraper,
        min_wait_ms: int,
        max_wait_ms: int,
    ) -> bool:
        min_wait_ms = max(300, int(min_wait_ms))
        max_wait_ms = max(min_wait_ms, int(max_wait_ms))
        baseline = await self.read_results_feed_metrics(scraper=scraper)
        if not baseline.get("found"):
            return False

        loop = asyncio.get_running_loop()
        started_at = loop.time()
        deadline = started_at + (max_wait_ms / 1000.0)
        stable_without_loading = 0
        poll_ms = 320

        while loop.time() < deadline:
            await scraper.page.wait_for_timeout(poll_ms)
            current = await self.read_results_feed_metrics(scraper=scraper)
            if not current.get("found"):
                return False

            anchor_grew = int(current.get("anchor_count") or 0) > int(baseline.get("anchor_count") or 0)
            geometry_grew = int(current.get("scroll_height") or 0) > int(baseline.get("scroll_height") or 0) + 8
            if anchor_grew or geometry_grew:
                return True

            elapsed_ms = int((loop.time() - started_at) * 1000)
            if current.get("loading"):
                stable_without_loading = 0
                continue
            if elapsed_ms < min_wait_ms:
                continue

            stable_without_loading += 1
            if stable_without_loading >= 2:
                return False

        return False

    async def collect_visible_google_maps_results(
        self,
        *,
        scraper: GoogleMapsScraper,
    ) -> list[dict[str, Any]]:
        raw = await scraper.page.evaluate(
            """
            () => {
              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );
              let feed = null;
              let bestAnchors = [];
              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchors = Array.from(candidate.querySelectorAll("a[href*='/maps/place/']"));
                if (anchors.length > bestAnchors.length) {
                  bestAnchors = anchors;
                  feed = candidate;
                }
              }
              if (!feed) {
                return { found: false, items: [] };
              }

              const readText = (node) => {
                if (!node || !node.textContent) return "";
                return String(node.textContent).trim();
              };

              const anchors = bestAnchors.length
                ? bestAnchors
                : Array.from(feed.querySelectorAll("a[href*='/maps/place/']"));
              const items = [];
              for (const anchor of anchors) {
                const article =
                  anchor.closest("div[role='article']") ||
                  anchor.closest("div.Nv2PK") ||
                  anchor.parentElement;
                const labelFromAnchor = String(anchor.getAttribute("aria-label") || "").trim();
                const heading =
                  article && article.querySelector
                    ? article.querySelector("h3, [role='heading'], .qBF1Pd, .fontHeadlineSmall")
                    : null;
                const labelFromHeading = readText(heading);
                const labelFromArticle = String(
                  article && article.getAttribute ? article.getAttribute("aria-label") || "" : ""
                ).trim();
                const fallbackText = readText(anchor).split("\\n")[0].trim();
                const name = labelFromHeading || labelFromAnchor || labelFromArticle || fallbackText;
                const ratingAria = String(
                  (
                    article &&
                    article.querySelector &&
                    article.querySelector("[role='img'][aria-label*='estrella' i], [role='img'][aria-label*='star' i]")
                  )?.getAttribute("aria-label") || ""
                ).trim();
                const ratingText = readText(
                  article && article.querySelector ? article.querySelector(".MW4etd") : null
                );
                const reviewsText = readText(
                  article && article.querySelector ? article.querySelector(".UY7F9") : null
                );
                const href = String(anchor.href || "").trim();
                if (!name || !href) continue;
                items.push({
                  name: name,
                  maps_url: href,
                  source_card_label: labelFromArticle || labelFromAnchor || null,
                  rating_label: ratingAria || ratingText || null,
                  reviews_label: reviewsText || ratingAria || null,
                });
              }
              return { found: true, items: items };
            }
            """
        )
        if not isinstance(raw, dict):
            return []
        items = raw.get("items")
        if not isinstance(items, list):
            return []

        cleaned: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            maps_url = str(item.get("maps_url") or "").strip()
            if not name or not maps_url:
                continue
            cleaned.append(
                {
                    "name": name,
                    "maps_url": maps_url,
                    "source_card_label": str(item.get("source_card_label") or "").strip() or None,
                    "rating": self._parse_rating_text(item.get("rating_label")),
                    "review_count": self._parse_reviews_count_text(item.get("reviews_label")),
                }
            )
        return cleaned

    async def scroll_google_maps_results(self, *, scraper: GoogleMapsScraper) -> None:
        await scraper.page.evaluate(
            """
            () => {
              const isScrollable = (el) => {
                if (!(el instanceof HTMLElement)) return false;
                const style = window.getComputedStyle(el);
                const overflowY = String(style.overflowY || "");
                const canScroll = el.scrollHeight > el.clientHeight + 20;
                return canScroll && ["auto", "scroll", "overlay"].includes(overflowY);
              };

              const feedCandidates = Array.from(
                document.querySelectorAll("div[role='feed'], div.m6QErb")
              );

              let best = null;
              let bestAnchorCount = -1;

              for (const candidate of feedCandidates) {
                if (!(candidate instanceof HTMLElement)) continue;
                const anchorCount = candidate.querySelectorAll("a[href*='/maps/place/']").length;
                if (!isScrollable(candidate)) continue;
                if (anchorCount > bestAnchorCount) {
                  best = candidate;
                  bestAnchorCount = anchorCount;
                }
              }

              if (!best) {
                const anchor = document.querySelector("a[href*='/maps/place/']");
                let parent = anchor ? anchor.parentElement : null;
                while (parent && parent !== document.body) {
                  if (isScrollable(parent)) {
                    best = parent;
                    break;
                  }
                  parent = parent.parentElement;
                }
              }

              if (!best) return;
              const step = Math.max(900, Math.floor(best.clientHeight * 0.9));
              const before = best.scrollTop;
              best.scrollBy({ top: step, left: 0, behavior: "auto" });
              if (best.scrollTop === before) {
                best.scrollTop = Math.min(best.scrollTop + step, best.scrollHeight);
              }
            }
            """
        )

    async def enrich_live_google_maps_candidates(
        self,
        *,
        scraper: GoogleMapsScraper,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not candidates:
            return []

        try:
            browser_context = scraper.page.context
        except Exception:
            return candidates
        if browser_context is None:
            return candidates

        try:
            detail_page = await browser_context.new_page()
        except Exception:
            return candidates

        detail_scraper = GoogleMapsScraper(page=detail_page)
        enriched_candidates: list[dict[str, Any]] = []
        try:
            for candidate in candidates:
                enriched_candidates.append(
                    await self.enrich_live_google_maps_candidate(
                        detail_scraper=detail_scraper,
                        candidate=candidate,
                    )
                )
        finally:
            try:
                await detail_page.close()
            except Exception:
                pass
        return enriched_candidates

    async def enrich_live_google_maps_candidate(
        self,
        *,
        detail_scraper: GoogleMapsScraper,
        candidate: dict[str, Any],
        timeout_ms: int = 11_000,
    ) -> dict[str, Any]:
        enriched = dict(candidate)
        source_ref = dict(enriched.get("source_ref") or {})
        raw_maps_url = str(source_ref.get("maps_url") or "").strip()
        canonical_maps_url = self._canonicalize_maps_url(
            raw_maps_url or str(source_ref.get("maps_url_canonical") or "").strip()
        )
        target_maps_url = raw_maps_url or canonical_maps_url
        if not target_maps_url:
            return enriched

        listing: dict[str, Any] = {}
        try:
            await detail_scraper.page.goto(target_maps_url, wait_until="domcontentloaded")
            await detail_scraper._dismiss_google_consent_if_present()
            await detail_scraper._wait_for_listing_ready(timeout_ms=max(4_000, int(timeout_ms)))
            listing = await detail_scraper.extract_listing()
        except Exception as exc:
            source_ref["maps_url"] = target_maps_url
            source_ref["maps_url_canonical"] = canonical_maps_url or target_maps_url
            source_ref["listing_primary_extract_error"] = str(exc)[:180]

        listing_fallback = await self.extract_listing_fallback_from_dom(detail_scraper=detail_scraper)
        listing = self._merge_listing_payloads(primary=listing, fallback=listing_fallback)

        listing_name = str(listing.get("business_name") or "").strip()
        listing_address = str(listing.get("address") or "").strip() or None
        listing_phone = str(listing.get("phone") or "").strip() or None
        listing_website = str(listing.get("website") or "").strip() or None
        listing_rating = listing.get("overall_rating")
        listing_review_count = listing.get("total_reviews")

        category_values_raw: list[str] = []
        raw_categories = listing.get("categories")
        if isinstance(raw_categories, list):
            for value in raw_categories:
                cleaned = str(value or "").strip()
                if cleaned:
                    category_values_raw.append(cleaned)
        category_values = self._sanitize_listing_categories(category_values_raw)
        listing_category = ", ".join(category_values) if category_values else None
        listing_primary_category = category_values[0] if category_values else None

        if listing_name:
            enriched["business_name"] = listing_name
        if listing_address:
            enriched["address"] = listing_address
            if not str(enriched.get("city") or "").strip():
                enriched["city"] = self._extract_city_from_address(listing_address)
        if listing_phone:
            enriched["phone"] = listing_phone
        if listing_website:
            enriched["website"] = listing_website
        if listing_rating is not None:
            enriched["rating"] = listing_rating
        if listing_review_count is not None:
            enriched["review_count"] = listing_review_count
        if listing_primary_category:
            enriched["category"] = listing_primary_category

        current_page_url = str(detail_scraper.page.url or "").strip()
        source_ref["maps_url"] = current_page_url or target_maps_url
        source_ref["maps_url_canonical"] = (
            self._canonicalize_maps_url(current_page_url) or canonical_maps_url or target_maps_url
        )
        source_ref["discovery_mode"] = "live_google_maps_auto_scroll_listing_extract"

        listing_details: dict[str, Any] = {}
        for key in ("service_options", "price_per_person", "description", "menu_url", "reservation_url"):
            value = listing.get(key)
            if isinstance(value, list):
                cleaned_items = [str(item or "").strip() for item in value if str(item or "").strip()]
                if cleaned_items:
                    listing_details[key] = cleaned_items
                continue
            cleaned_text = str(value or "").strip()
            if cleaned_text:
                listing_details[key] = cleaned_text
        if listing_category:
            listing_details["categories"] = category_values
        if listing_primary_category:
            listing_details["category"] = listing_primary_category
        if listing_details:
            source_ref["listing_details"] = listing_details

        listing_enriched = bool(
            listing_name
            or listing_address
            or listing_phone
            or listing_website
            or listing_rating is not None
            or listing_review_count is not None
        )
        source_ref["listing_enriched"] = listing_enriched
        source_ref.pop("listing_extract_error", None)
        primary_extract_error = str(source_ref.get("listing_primary_extract_error") or "").strip()
        if listing_enriched:
            source_ref.pop("listing_primary_extract_error", None)
        elif primary_extract_error:
            source_ref["listing_extract_error"] = primary_extract_error
        enriched["source_ref"] = source_ref
        return enriched

    async def extract_listing_fallback_from_dom(
        self,
        *,
        detail_scraper: GoogleMapsScraper,
    ) -> dict[str, Any]:
        try:
            raw = await detail_scraper.page.evaluate(
                """
                () => {
                  const clean = (value) => {
                    if (typeof value !== "string") return "";
                    return value.replace(/\\s+/g, " ").trim();
                  };
                  const text = (el) => clean(el && el.textContent ? String(el.textContent) : "");

                  const businessName =
                    text(document.querySelector("h1")) ||
                    text(document.querySelector("[role='main'] h1")) ||
                    "";
                  const address = text(
                    document.querySelector("[data-item-id='address'] .Io6YTe") ||
                    document.querySelector("[data-item-id='address']")
                  );
                  const phone = text(
                    document.querySelector("[data-item-id^='phone:'] .Io6YTe") ||
                    document.querySelector("[data-item-id^='phone:']")
                  );
                  const websiteText = text(
                    document.querySelector("[data-item-id='authority'] .Io6YTe") ||
                    document.querySelector("[data-item-id='authority']")
                  );
                  const pickHref = (selectors) => {
                    for (const selector of selectors) {
                      const node = document.querySelector(selector);
                      if (!node) continue;
                      const href = clean(String(node.getAttribute("href") || ""));
                      if (href) return href;
                    }
                    return "";
                  };
                  let websiteHref = "";
                  const websiteAnchor = document.querySelector("[data-item-id='authority'] a[href]");
                  if (websiteAnchor && websiteAnchor.getAttribute) {
                    websiteHref = clean(String(websiteAnchor.getAttribute("href") || ""));
                  }

                  const pickAriaLabel = (nodes, expectedKeyword) => {
                    let best = "";
                    for (const node of nodes) {
                      if (!node || !node.getAttribute) continue;
                      const rawValue = String(node.getAttribute("aria-label") || "").trim();
                      if (!rawValue) continue;
                      const hasDigit = /\\d/.test(rawValue);
                      const normalized = rawValue.toLowerCase();
                      if (hasDigit && normalized.includes(expectedKeyword)) {
                        return rawValue;
                      }
                      if (hasDigit && !best) {
                        best = rawValue;
                      } else if (!best) {
                        best = rawValue;
                      }
                    }
                    return best;
                  };

                  const ratingNodes = Array.from(
                    document.querySelectorAll("[aria-label*='estrella' i], [aria-label*='star' i], [role='img'][aria-label]")
                  );
                  const reviewsNodes = Array.from(
                    document.querySelectorAll("[aria-label*='rese' i], [aria-label*='review' i], button[jsaction*='reviewChart.moreReviews']")
                  );
                  const ratingLabel = pickAriaLabel(ratingNodes, "estrella");
                  const reviewsLabel = pickAriaLabel(reviewsNodes, "rese");
                  const ratingText =
                    text(document.querySelector(".F7nice .MW4etd")) ||
                    text(document.querySelector(".AJB7ye .MW4etd")) ||
                    text(document.querySelector(".ZkP5Je .MW4etd")) ||
                    "";
                  const reviewsText =
                    text(document.querySelector(".F7nice .UY7F9")) ||
                    text(document.querySelector(".AJB7ye .UY7F9")) ||
                    text(document.querySelector(".ZkP5Je .UY7F9")) ||
                    "";

                  const categoryButtons = Array.from(
                    document.querySelectorAll("button[jsaction*='.category'], div.LBgpqf button[jsaction*='.category'], div.LBgpqf .fontBodyMedium button")
                  );
                  const categories = [];
                  const seen = new Set();
                  for (const button of categoryButtons) {
                    const value = text(button);
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (seen.has(key)) continue;
                    seen.add(key);
                    categories.push(value);
                    if (categories.length >= 6) break;
                  }
                  if (!categories.length) {
                    const headerLine = text(document.querySelector("div.LBgpqf .fontBodyMedium"));
                    if (headerLine) {
                      categories.push(headerLine);
                    }
                  }

                  const serviceOptions = [];
                  const serviceSeen = new Set();
                  const serviceNodes = Array.from(document.querySelectorAll("div.y0K5Df .LTs0Rc"));
                  for (const node of serviceNodes) {
                    const aria = clean(String(node.getAttribute("aria-label") || ""));
                    const fallback = text(node.querySelector("div[aria-hidden='true']") || node);
                    const value = aria || fallback;
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (serviceSeen.has(key)) continue;
                    serviceSeen.add(key);
                    serviceOptions.push(value);
                  }

                  const priceText =
                    text(document.querySelector(".DfOCNb .MNVeJb > div")) ||
                    text(document.querySelector("[data-item-id='price'] .Io6YTe")) ||
                    null;
                  const descriptionText = text(document.querySelector("div.y0K5Df .PYvSYb")) || null;
                  const menuUrl = pickHref([
                    "a[data-item-id='menu'][href]",
                    "[data-item-id='menu'] a[href]",
                  ]);
                  const reservationUrl = pickHref([
                    "a[data-item-id^='action:'][href]",
                    "a[href*='/maps/reserve/']",
                    "a[href*='/reserve/']",
                  ]);
                  const websiteUrl = pickHref([
                    "a[data-item-id='authority'][href]",
                    "[data-item-id='authority'] a[href]",
                  ]);

                  return {
                    business_name: businessName || null,
                    address: address || null,
                    phone: phone || null,
                    website: websiteText || websiteHref || null,
                    website_url: websiteUrl || websiteHref || null,
                    rating_label: ratingLabel || ratingText || null,
                    reviews_label: reviewsLabel || reviewsText || null,
                    categories: categories,
                    service_options: serviceOptions,
                    price_per_person: priceText || null,
                    description: descriptionText || null,
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

        rating_value = self._parse_rating_text(raw.get("rating_label"))
        reviews_value = self._parse_reviews_count_text(raw.get("reviews_label"))

        categories: list[str] = []
        raw_categories = raw.get("categories")
        if isinstance(raw_categories, list):
            for item in raw_categories:
                cleaned = str(item or "").strip()
                if cleaned:
                    categories.append(cleaned)
        categories = self._sanitize_listing_categories(categories)

        service_options: list[str] = []
        raw_service_options = raw.get("service_options")
        if isinstance(raw_service_options, list):
            seen: set[str] = set()
            for item in raw_service_options:
                cleaned = str(item or "").strip()
                if not cleaned:
                    continue
                key = self._normalize_text(cleaned)
                if key in seen:
                    continue
                seen.add(key)
                service_options.append(cleaned)

        return {
            "business_name": str(raw.get("business_name") or "").strip() or None,
            "address": str(raw.get("address") or "").strip() or None,
            "phone": str(raw.get("phone") or "").strip() or None,
            "website": str(raw.get("website") or raw.get("website_url") or "").strip() or None,
            "overall_rating": rating_value,
            "total_reviews": reviews_value,
            "categories": categories,
            "category": categories[0] if categories else None,
            "service_options": service_options,
            "price_per_person": str(raw.get("price_per_person") or "").strip() or None,
            "description": str(raw.get("description") or "").strip() or None,
            "menu_url": str(raw.get("menu_url") or "").strip() or None,
            "reservation_url": str(raw.get("reservation_url") or "").strip() or None,
        }
