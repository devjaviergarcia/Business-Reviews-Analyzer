import argparse
import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.tripadvisor import TripadvisorScraper


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test for Tripadvisor search, selection and paginated review extraction."
    )
    parser.add_argument("query", nargs="*", help="Business query.")
    parser.add_argument("--max-pages", type=int, default=4, help="Maximum number of review pages to scrape.")
    parser.add_argument("--max-reviews", type=int, default=30, help="Maximum reviews to print.")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode. Default uses .env SCRAPER_HEADLESS.",
    )
    parser.add_argument(
        "--incognito",
        action="store_true",
        help="Use incognito context (no persistent profile).",
    )
    parser.add_argument(
        "--artifacts-dir",
        default="artifacts/tripadvisor_smoke",
        help="Directory where screenshot/html/json artifacts are saved.",
    )
    return parser.parse_args()


def _slugify(value: str) -> str:
    text = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE).strip().lower()
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:100] or "query"


async def main() -> None:
    args = _parse_args()
    query = " ".join(args.query).strip() or "Banos Arabes de Cordoba"
    max_pages = max(1, int(args.max_pages))
    max_reviews = max(1, int(args.max_reviews))

    use_headless = bool(args.headless) or bool(settings.scraper_headless)
    use_incognito = bool(args.incognito) or bool(settings.scraper_incognito)

    scraper = TripadvisorScraper(
        headless=use_headless,
        incognito=use_incognito,
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=settings.scraper_user_data_dir,
        browser_channel=settings.scraper_browser_channel,
        timeout_ms=settings.scraper_timeout_ms,
        min_click_delay_ms=settings.scraper_min_click_delay_ms,
        max_click_delay_ms=settings.scraper_max_click_delay_ms,
        min_key_delay_ms=settings.scraper_min_key_delay_ms,
        max_key_delay_ms=settings.scraper_max_key_delay_ms,
        stealth_mode=settings.scraper_stealth_mode,
        harden_headless=settings.scraper_harden_headless,
        extra_chromium_args=settings.scraper_extra_chromium_args,
    )

    def _progress_logger(payload: dict) -> None:
        event = str(payload.get("event", "unknown"))
        page_num = payload.get("page")
        total = payload.get("total_unique_reviews")
        if page_num is not None:
            print(f"[progress] event={event} page={page_num} total={total}")
        else:
            print(f"[progress] event={event} total={total}")

    try:
        await scraper.start()
        await scraper.search_business(query)
        listing = await scraper.extract_listing()
        reviews = await scraper.extract_reviews(
            max_pages=max_pages,
            html_min_interval_s=max(0.2, settings.scraper_html_scroll_min_interval_s),
            html_max_interval_s=max(
                max(0.2, settings.scraper_html_scroll_min_interval_s),
                settings.scraper_html_scroll_max_interval_s,
            ),
            progress_callback=_progress_logger,
        )

        artifacts_root = Path(args.artifacts_dir).resolve()
        run_dir = artifacts_root / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slugify(query)}"
        run_dir.mkdir(parents=True, exist_ok=True)

        page = scraper.page
        await page.screenshot(path=str(run_dir / "final_page.png"), full_page=True)
        html_content = await page.content()
        (run_dir / "final_page.html").write_text(html_content, encoding="utf-8")

        payload = {
            "query": query,
            "url": page.url,
            "headless": use_headless,
            "incognito": use_incognito,
            "listing": listing,
            "reviews_total": len(reviews),
            "reviews_sample": reviews[:max_reviews],
        }
        (run_dir / "result.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"OK - Tripadvisor scrape completed for query: {query}")
        print(f"URL: {page.url}")
        print(f"Listing: {listing}")
        print(f"Reviews extracted: {len(reviews)}")
        print(f"Artifacts: {run_dir}")
        if reviews:
            print("First review sample:")
            print(json.dumps(reviews[0], ensure_ascii=False, indent=2))
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
