import asyncio
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.google_maps import GoogleMapsScraper


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test for Google Maps search and review extraction.")
    parser.add_argument("query", nargs="*", help="Business search query.")
    parser.add_argument(
        "--strategy",
        choices=("interactive", "scroll_copy"),
        default=settings.scraper_reviews_strategy,
        help=f"Review extraction strategy (default: {settings.scraper_reviews_strategy}).",
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=10,
        help="Maximum number of reviews to print/process in this smoke test (default: 10).",
    )
    parser.add_argument(
        "--scroll-rounds",
        type=int,
        default=4,
        help="Scroll rounds for interactive strategy (default: 4).",
    )
    parser.add_argument(
        "--html-scroll-rounds",
        type=int,
        default=180,
        help="Scroll rounds for scroll_copy strategy (default: 180).",
    )
    parser.add_argument(
        "--html-stable-rounds",
        type=int,
        default=6,
        help="Stable rounds required to stop in scroll_copy strategy (default: 6).",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    query = " ".join(args.query).strip() or "Restaurante Casa Pepe Madrid"
    max_reviews = max(1, args.max_reviews)
    strategy = args.strategy

    scraper = GoogleMapsScraper(
        headless=settings.scraper_headless,
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=settings.scraper_user_data_dir,
        browser_channel=settings.scraper_browser_channel,
        maps_url=settings.scraper_maps_url,
        timeout_ms=settings.scraper_timeout_ms,
        min_click_delay_ms=settings.scraper_min_click_delay_ms,
        max_click_delay_ms=settings.scraper_max_click_delay_ms,
        min_key_delay_ms=settings.scraper_min_key_delay_ms,
        max_key_delay_ms=settings.scraper_max_key_delay_ms,
    )

    try:
        await scraper.start()
        await scraper.search_business(query)

        page = scraper.page
        limited_view = await scraper._is_limited_maps_view()
        name_locator = page.locator("h1.DUwDvf").first
        business_name = (await name_locator.inner_text()) if await name_locator.count() else "(name not found)"
        listing = await scraper.extract_listing()
        reviews = (
            await scraper.extract_reviews(
                strategy=strategy,
                max_rounds=max(0, args.scroll_rounds),
                html_scroll_max_rounds=max(1, args.html_scroll_rounds),
                html_stable_rounds=max(2, args.html_stable_rounds),
            )
        )[:max_reviews]

        print(f"OK - search completed for: {query}")
        print(f"Strategy: {strategy}")
        print(f"Business page: {business_name}")
        print(f"URL: {page.url}")
        print(f"Limited view detected: {limited_view}")
        print(f"Listing: {listing}")
        print(f"Reviews extracted: {len(reviews)} (limit={max_reviews})")
        if limited_view:
            print(
                "NOTE: Google Maps is in limited view; reviews panel may be unavailable. "
                "Open the persistent profile once and sign in to unblock full reviews."
            )
        if reviews:
            print(f"First review sample: {reviews[0]}")
            print(f"Other reviews samples: {reviews[1:3]}")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
