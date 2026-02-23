import argparse
import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.google_maps import GoogleMapsScraper


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Open Google Maps in incognito, accept cookies, search a business, "
            "open reviews, scroll every 1s, and print all loaded reviews."
        )
    )
    parser.add_argument("query", nargs="+", help="Business search query.")
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=1000,
        help="Milliseconds between scroll steps (default: 1000).",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=0,
        help="Maximum scroll rounds. Use 0 for auto-until-end with safety cap (default: 0).",
    )
    parser.add_argument(
        "--stable-rounds",
        type=int,
        default=10,
        help="Rounds without growth/movement before stop (default: 10).",
    )
    parser.add_argument(
        "--step-px",
        type=int,
        default=900,
        help="Scroll step in pixels (default: 900).",
    )
    parser.add_argument(
        "--bottom-wait-ms",
        type=int,
        default=3000,
        help="Extra wait when feed seems at bottom to allow loading more reviews (default: 3000).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (default: headed).",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output JSON file path for all extracted reviews.",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    query = " ".join(args.query).strip()
    interval_ms = max(250, args.interval_ms)
    max_rounds = args.max_rounds if args.max_rounds > 0 else 6000
    stable_rounds = max(2, args.stable_rounds)
    step_px = max(250, args.step_px)
    bottom_wait_ms = max(800, args.bottom_wait_ms)

    scraper = GoogleMapsScraper(
        headless=bool(args.headless),
        incognito=True,
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=settings.scraper_user_data_dir,
        browser_channel=settings.scraper_browser_channel,
        maps_url=settings.scraper_maps_url,
        timeout_ms=settings.scraper_timeout_ms,
        min_click_delay_ms=settings.scraper_min_click_delay_ms,
        max_click_delay_ms=settings.scraper_max_click_delay_ms,
        min_key_delay_ms=settings.scraper_min_key_delay_ms,
        max_key_delay_ms=settings.scraper_max_key_delay_ms,
        stealth_mode=settings.scraper_stealth_mode,
        harden_headless=settings.scraper_harden_headless,
        extra_chromium_args=settings.scraper_extra_chromium_args,
        reviews_strategy="scroll_copy",
    )

    try:
        await scraper.start()
        await scraper.search_business(query)
        listing = await scraper.extract_listing()
        expected_total_reviews = listing.get("total_reviews")

        opened = await scraper._ensure_reviews_open()
        if not opened:
            print("Could not open reviews panel. Limited view or selector mismatch.")
            return

        print(f"Search OK: {query}")
        print(f"Listing: {listing.get('business_name')}")
        print("Starting deterministic scroll loop...")

        initial_state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
        last_count = int(initial_state.get("review_count", 0))
        unchanged = 0
        last_top = int(initial_state.get("scroll_top", -1))
        last_scroll_height = int(initial_state.get("scroll_height", -1))

        for round_idx in range(1, max_rounds + 1):
            metrics = await scraper._scroll_reviews_feed_step(step_px=step_px)
            await scraper.page.wait_for_timeout(interval_ms)

            current_state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
            current_count = int(current_state.get("review_count", 0))
            top = int(current_state.get("scroll_top", -1))
            scroll_height = int(current_state.get("scroll_height", -1))
            at_bottom = bool(current_state.get("at_bottom"))

            if at_bottom:
                await scraper.page.wait_for_timeout(bottom_wait_ms)
                settled_state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
                settled_count = int(settled_state.get("review_count", 0))
                settled_top = int(settled_state.get("scroll_top", -1))
                settled_scroll_height = int(settled_state.get("scroll_height", -1))
                if (
                    settled_count > current_count
                    or settled_scroll_height > scroll_height
                    or settled_top != top
                ):
                    current_state = settled_state
                    current_count = settled_count
                    top = settled_top
                    scroll_height = settled_scroll_height
                    at_bottom = bool(settled_state.get("at_bottom"))

            moved = bool(metrics.get("scrolled")) or (top != last_top)
            count_grew = current_count > last_count
            geometry_changed = (top != last_top) or (scroll_height != last_scroll_height)

            if count_grew:
                last_count = current_count

            if moved or count_grew or geometry_changed:
                unchanged = 0
            else:
                unchanged += 1

            print(
                f"round={round_idx} reviews={current_count} "
                f"moved={moved} at_bottom={at_bottom} unchanged={unchanged}"
            )

            last_top = top
            last_scroll_height = scroll_height

            if at_bottom and unchanged >= stable_rounds:
                break
            if not bool(current_state.get("found")) and unchanged >= stable_rounds:
                break

        reviews_html = await scraper.capture_reviews_container_html()
        dom_loaded_cards = await scraper._review_count()
        reviews = scraper.extract_reviews_from_html(reviews_html)
        unique_review_ids = {
            str(item.get("review_id", "") or "").strip()
            for item in reviews
            if str(item.get("review_id", "") or "").strip()
        }
        unique_reviews_count = len(unique_review_ids) if unique_review_ids else len(reviews)

        coverage_pct: float | None = None
        if isinstance(expected_total_reviews, int) and expected_total_reviews > 0:
            coverage_pct = round((unique_reviews_count / expected_total_reviews) * 100.0, 2)

        print("=== Reviews Count Summary ===")
        print(f"Expected total reviews (listing): {expected_total_reviews}")
        print(f"DOM loaded review cards: {dom_loaded_cards}")
        print(f"Extracted reviews (raw): {len(reviews)}")
        print(f"Extracted reviews (unique by review_id): {unique_reviews_count}")
        if coverage_pct is not None:
            print(f"Coverage vs listing total: {coverage_pct}%")

        if args.output:
            output_path = Path(args.output)
            if not output_path.is_absolute():
                output_path = (PROJECT_ROOT / output_path).resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "query": query,
                "summary": {
                    "expected_total_reviews": expected_total_reviews,
                    "dom_loaded_cards": dom_loaded_cards,
                    "extracted_reviews_raw": len(reviews),
                    "extracted_reviews_unique": unique_reviews_count,
                    "coverage_pct": coverage_pct,
                },
                "reviews": reviews,
            }
            output_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Saved all reviews to: {output_path}")

        print(json.dumps(reviews, ensure_ascii=False, indent=2))
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
