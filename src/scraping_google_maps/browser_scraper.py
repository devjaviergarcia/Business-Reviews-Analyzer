from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from playwright.async_api import Page

from src.browser_runtime.browser_profiles import (
    BrowserProfile,
    resolve_browser_profile,
    select_stable_browser_profile,
)
from src.scraping_google_maps.browser_scraper_facets import (
    GoogleMapsBrowserLifecycleFacet,
    GoogleMapsBrowserListingFacet,
    GoogleMapsBrowserNavigationFacet,
    GoogleMapsBrowserParsingFacet,
    GoogleMapsBrowserReviewCardFacet,
    GoogleMapsBrowserReviewsCollectionFacet,
    GoogleMapsBrowserReviewsFeedFacet,
    GoogleMapsBrowserReviewsOpenFacet,
)


class GoogleMapsScraper(
    GoogleMapsBrowserLifecycleFacet,
    GoogleMapsBrowserNavigationFacet,
    GoogleMapsBrowserListingFacet,
    GoogleMapsBrowserReviewCardFacet,
    GoogleMapsBrowserReviewsCollectionFacet,
    GoogleMapsBrowserReviewsFeedFacet,
    GoogleMapsBrowserReviewsOpenFacet,
    GoogleMapsBrowserParsingFacet,
):
    """Composition root for the Google Maps browser scraper.

    The stage pipeline is orchestrated from `GoogleMapsBusinessPageScraper`.
    This class only groups the facet modules that implement each browser step.
    """

    def __init__(
        self,
        page: Page | None = None,
        *,
        headless: bool = False,
        slow_mo_ms: int = 50,
        user_data_dir: str = "playwright-data",
        browser_channel: str | None = None,
        maps_url: str = "https://www.google.com/maps?hl=es",
        timeout_ms: int = 30000,
        min_click_delay_ms: int = 3100,
        max_click_delay_ms: int = 5200,
        min_key_delay_ms: int = 90,
        max_key_delay_ms: int = 260,
        stealth_mode: bool = True,
        harden_headless: bool = True,
        extra_chromium_args: list[str] | None = None,
        incognito: bool = False,
        reviews_strategy: str = "interactive",
        browser_profile_id: str | None = None,
        browser_profile_stable_key: str | None = None,
    ) -> None:
        self._page = page
        self._external_page = page is not None

        self._headless = headless
        self._slow_mo_ms = slow_mo_ms
        self._user_data_dir = user_data_dir
        self._browser_channel = (browser_channel or "").strip() or None
        self._maps_url = maps_url
        self._timeout_ms = timeout_ms
        self._min_click_delay_ms = max(3001, min_click_delay_ms)
        self._max_click_delay_ms = max(self._min_click_delay_ms, max_click_delay_ms)
        self._min_key_delay_ms = max(10, min_key_delay_ms)
        self._max_key_delay_ms = max(self._min_key_delay_ms, max_key_delay_ms)
        self._stealth_mode = stealth_mode
        self._harden_headless = harden_headless
        self._extra_chromium_args = list(extra_chromium_args or [])
        self._incognito = incognito
        self._project_root = Path(__file__).resolve().parents[2]
        self._reviews_strategy = self._resolve_reviews_strategy(reviews_strategy)
        self._browser_profile_id: str | None = None
        self._browser_profile: BrowserProfile = resolve_browser_profile()

        self._playwright = None
        self._browser = None
        self._context = None
        self._last_click_ts: float | None = None
        self._last_reviews_open_state: dict[str, Any] = {
            "status": "unknown",
            "section_variant": "none",
            "found": False,
            "panel_ready": False,
            "review_count": 0,
        }
        self._rng = random.Random()
        self.use_browser_profile(
            explicit_profile_id=browser_profile_id,
            stable_key=browser_profile_stable_key,
        )

    def use_browser_profile(
        self,
        *,
        explicit_profile_id: str | None = None,
        stable_key: str | None = None,
    ) -> str:
        profile = select_stable_browser_profile(
            source="google_maps",
            stable_key=stable_key,
            explicit_profile_id=explicit_profile_id,
        )
        self._browser_profile = profile
        self._browser_profile_id = profile.profile_id
        return profile.profile_id
