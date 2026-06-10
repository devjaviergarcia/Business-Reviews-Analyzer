from __future__ import annotations

import random
from pathlib import Path

from playwright.async_api import Page
from playwright_stealth import Stealth

from src.scraping_tripadvisor.browser_scraper_facets import (
    TripadvisorBrowserLifecycleFacet,
    TripadvisorBrowserListingFacet,
    TripadvisorBrowserPageSupportFacet,
    TripadvisorBrowserReviewCardFacet,
    TripadvisorBrowserReviewsCollectionFacet,
    TripadvisorBrowserReviewsPaginationFacet,
    TripadvisorBrowserSearchFlowFacet,
    TripadvisorBrowserSearchMatchingFacet,
    TripadvisorBrowserTextFacet,
)


class TripadvisorScraper(
    TripadvisorBrowserLifecycleFacet,
    TripadvisorBrowserSearchFlowFacet,
    TripadvisorBrowserSearchMatchingFacet,
    TripadvisorBrowserListingFacet,
    TripadvisorBrowserReviewsCollectionFacet,
    TripadvisorBrowserReviewsPaginationFacet,
    TripadvisorBrowserReviewCardFacet,
    TripadvisorBrowserPageSupportFacet,
    TripadvisorBrowserTextFacet,
):
    """Composition root for the Tripadvisor browser scraper.

    The stage pipeline is orchestrated from `TripadvisorBusinessPageScraper`.
    This class only groups the facet modules that implement each browser step.
    """

    def __init__(
        self,
        page: Page | None = None,
        *,
        headless: bool = False,
        slow_mo_ms: int = 50,
        user_data_dir: str = "playwright-data-tripadvisor",
        browser_channel: str | None = None,
        tripadvisor_url: str = "https://www.tripadvisor.es",
        timeout_ms: int = 30000,
        min_click_delay_ms: int = 700,
        max_click_delay_ms: int = 1500,
        min_key_delay_ms: int = 35,
        max_key_delay_ms: int = 95,
        max_reviews_open_seconds: float = 3.0,
        max_seconds_per_reviews_page: float = 10.0,
        stealth_mode: bool = True,
        harden_headless: bool = True,
        extra_chromium_args: list[str] | None = None,
        incognito: bool = False,
    ) -> None:
        self._page = page
        self._external_page = page is not None

        self._headless = False
        self._slow_mo_ms = slow_mo_ms
        self._user_data_dir = user_data_dir
        self._browser_channel = (browser_channel or "").strip() or None
        self._tripadvisor_url = tripadvisor_url
        self._timeout_ms = timeout_ms
        self._min_click_delay_ms = max(120, min(700, int(min_click_delay_ms)))
        self._max_click_delay_ms = max(self._min_click_delay_ms, min(1500, int(max_click_delay_ms)))
        self._min_key_delay_ms = max(5, min(60, int(min_key_delay_ms)))
        self._max_key_delay_ms = max(self._min_key_delay_ms, min(120, int(max_key_delay_ms)))
        self._max_reviews_open_seconds = max(0.8, float(max_reviews_open_seconds))
        self._max_seconds_per_reviews_page = max(2.0, float(max_seconds_per_reviews_page))
        self._stealth_mode = stealth_mode
        self._harden_headless = harden_headless
        self._extra_chromium_args = list(extra_chromium_args or [])
        self._incognito = incognito
        self._project_root = Path(__file__).resolve().parents[2]

        self._playwright = None
        self._browser = None
        self._context = None
        self._last_click_ts: float | None = None
        self._rng = random.Random()
        self._cookies_checked_once = False
        self._consent_checked_once = False
        self._location_prompt_checked_once = False
        self._stealth = Stealth(
            navigator_languages_override=("es-ES", "es"),
            navigator_platform_override="Win32",
        )
