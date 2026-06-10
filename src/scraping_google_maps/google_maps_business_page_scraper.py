from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.config import settings
from src.scraping_google_maps.browser_scraper import GoogleMapsScraper

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None] | None] | None


class GoogleMapsBusinessPageScraper:
    """Run the Google Maps scraping pipeline stage by stage.

    Happy path:
    start -> search_business -> extract_listing -> extract_reviews -> close
    """

    def __init__(
        self,
        *,
        scraper: GoogleMapsScraper,
        emit_progress: Callable[..., Awaitable[None]],
        resolve_optional_int_override: Callable[..., int],
    ) -> None:
        self._scraper = scraper
        self._emit_progress = emit_progress
        self._resolve_optional_int_override = resolve_optional_int_override

    async def scrape_business_page(
        self,
        business_name: str,
        *,
        strategy: str,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        progress_callback: ProgressCallback = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Execute the full Google Maps scrape for one business page."""
        effective_interactive_max_rounds = self._resolve_optional_int_override(
            value=interactive_max_rounds,
            fallback=max(1, settings.scraper_interactive_max_rounds),
            min_value=1,
            field_name="interactive_max_rounds",
        )
        effective_html_scroll_max_rounds = self._resolve_optional_int_override(
            value=html_scroll_max_rounds,
            fallback=max(0, settings.scraper_html_scroll_max_rounds),
            min_value=0,
            field_name="html_scroll_max_rounds",
        )
        effective_html_stable_rounds = self._resolve_optional_int_override(
            value=html_stable_rounds,
            fallback=max(2, settings.scraper_html_stable_rounds),
            min_value=2,
            field_name="html_stable_rounds",
        )

        async def _scraper_progress(event: dict[str, Any]) -> None:
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_progress",
                "Review scrolling in progress.",
                event,
            )

        await self._emit_progress(
            progress_callback,
            "scraper_starting",
            "Starting browser and scraper.",
            {
                "strategy": strategy,
                "browser_profile_id": getattr(self._scraper, "_browser_profile_id", None),
            },
        )
        await self._scraper.start()
        try:
            await self._emit_progress(
                progress_callback,
                "scraper_search_started",
                "Searching business on Google Maps.",
                {"query": business_name},
            )
            await self._scraper.search_business(business_name)
            await self._emit_progress(
                progress_callback,
                "scraper_search_completed",
                "Business page opened.",
                {"query": business_name},
            )

            listing = await self._scraper.extract_listing()
            await self._emit_progress(
                progress_callback,
                "scraper_listing_completed",
                "Listing extracted.",
                {
                    "business_name": listing.get("business_name"),
                    "total_reviews": listing.get("total_reviews"),
                },
            )

            await self._emit_progress(
                progress_callback,
                "scraper_reviews_started",
                "Starting reviews extraction.",
                {
                    "strategy": strategy,
                    "interactive_max_rounds": effective_interactive_max_rounds,
                    "html_scroll_max_rounds": effective_html_scroll_max_rounds,
                    "html_stable_rounds": effective_html_stable_rounds,
                },
            )
            reviews = await self._scraper.extract_reviews(
                strategy=strategy,
                max_rounds=effective_interactive_max_rounds,
                html_scroll_max_rounds=effective_html_scroll_max_rounds,
                html_stable_rounds=effective_html_stable_rounds,
                html_min_interval_s=max(0.1, settings.scraper_html_scroll_min_interval_s),
                html_max_interval_s=max(
                    max(0.1, settings.scraper_html_scroll_min_interval_s),
                    settings.scraper_html_scroll_max_interval_s,
                ),
                progress_callback=_scraper_progress,
            )
            await self._emit_progress(
                progress_callback,
                "scraper_reviews_completed",
                "Reviews extracted.",
                {"scraped_review_count": len(reviews)},
            )
            return listing, reviews
        finally:
            await self._scraper.close()
