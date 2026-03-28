import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.google_maps import GoogleMapsScraper


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    return normalized or "query"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Local diagnostics for Google Maps reviews panel opening and scroll behavior. "
            "Saves screenshots, feed HTML, and a JSON timeline."
        )
    )
    parser.add_argument("query", nargs="+", help="Business search query.")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (default: headed).",
    )
    parser.add_argument(
        "--incognito",
        action="store_true",
        default=True,
        help="Run with incognito context (default: true).",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/diagnostics",
        help="Directory where diagnostics are saved (default: artifacts/diagnostics).",
    )
    parser.add_argument(
        "--open-attempts",
        type=int,
        default=3,
        help="Manual attempts to open reviews panel before fallback ensure (default: 3).",
    )
    parser.add_argument(
        "--ready-timeout-ms",
        type=int,
        default=3000,
        help="Timeout per ready check during open attempts (default: 3000).",
    )
    parser.add_argument(
        "--post-click-wait-ms",
        type=int,
        default=1200,
        help="Wait after each open click attempt (default: 1200).",
    )
    parser.add_argument(
        "--scroll-rounds",
        type=int,
        default=120,
        help="Max diagnostic scroll rounds after panel is open (default: 120).",
    )
    parser.add_argument(
        "--stable-rounds",
        type=int,
        default=10,
        help="Stop when feed is at bottom and unchanged reaches this number (default: 10).",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=1200,
        help="Wait between scroll rounds (default: 1200).",
    )
    parser.add_argument(
        "--step-px",
        type=int,
        default=900,
        help="Scroll step size (default: 900).",
    )
    parser.add_argument(
        "--snapshot-every",
        type=int,
        default=10,
        help="Take screenshot every N scroll rounds (default: 10).",
    )
    return parser.parse_args()


def _safe_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _safe_scalar(v) for k, v in list(value.items())[:30]}
    if isinstance(value, (list, tuple, set)):
        return [_safe_scalar(v) for v in list(value)[:30]]
    return str(value)


async def _take_screenshot(scraper: GoogleMapsScraper, path: Path) -> None:
    page = scraper.page
    if page is None:
        return
    await page.screenshot(path=str(path), full_page=True)


async def _safe_label(scraper: GoogleMapsScraper, locator) -> str | None:
    if locator is None:
        return None
    try:
        label = await scraper._candidate_label(locator)
        cleaned = str(label or "").strip()
        return cleaned or None
    except Exception:
        return None


async def _feed_snapshot(scraper: GoogleMapsScraper) -> dict[str, Any]:
    try:
        state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
    return {
        "panel_ready": bool(state.get("panel_ready")),
        "found": bool(state.get("found")),
        "section_variant": str(state.get("section_variant", "") or ""),
        "marker_count": int(state.get("marker_count", 0)),
        "search_cue": bool(state.get("search_cue")),
        "filter_cue": bool(state.get("filter_cue")),
        "review_count": int(state.get("review_count", 0)),
        "at_bottom": bool(state.get("at_bottom")),
        "scroll_top": int(state.get("scroll_top", 0)),
        "scroll_height": int(state.get("scroll_height", 0)),
        "client_height": int(state.get("client_height", 0)),
    }


async def main() -> None:
    args = _parse_args()
    query = " ".join(args.query).strip()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = Path(args.output_dir)
    if not output_root.is_absolute():
        output_root = (PROJECT_ROOT / output_root).resolve()
    run_dir = output_root / f"{timestamp}_{_slug(query)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "query": query,
        "created_at": datetime.now().isoformat(),
        "settings": {
            "headless": bool(args.headless),
            "incognito": bool(args.incognito),
            "open_attempts": int(args.open_attempts),
            "ready_timeout_ms": int(args.ready_timeout_ms),
            "post_click_wait_ms": int(args.post_click_wait_ms),
            "scroll_rounds": int(args.scroll_rounds),
            "stable_rounds": int(args.stable_rounds),
            "interval_ms": int(args.interval_ms),
            "step_px": int(args.step_px),
            "snapshot_every": int(args.snapshot_every),
        },
        "timeline": [],
        "artifacts_dir": str(run_dir),
    }

    started_at = time.monotonic()

    def log(event: str, **data: Any) -> None:
        elapsed_s = round(time.monotonic() - started_at, 2)
        payload = {"elapsed_s": elapsed_s, "event": event, **{k: _safe_scalar(v) for k, v in data.items()}}
        summary["timeline"].append(payload)
        print(json.dumps(payload, ensure_ascii=False))

    scraper = GoogleMapsScraper(
        headless=bool(args.headless),
        incognito=bool(args.incognito),
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

    pending_error: Exception | None = None
    try:
        log("start")
        await scraper.start()
        log("scraper_started")

        await scraper.search_business(query)
        log("search_completed", url=(scraper.page.url if scraper.page else None))
        await _take_screenshot(scraper, run_dir / "01_after_search.png")

        limited = await scraper._is_limited_maps_view()
        listing = await scraper.extract_listing()
        log(
            "listing_extracted",
            limited_view=limited,
            business_name=listing.get("business_name"),
            total_reviews=listing.get("total_reviews"),
        )

        tab_button = await scraper._find_valid_reviews_tab_from_tablist()
        more_reviews_button = await scraper._find_more_reviews_summary_button()
        any_review_button = await scraper._find_any_valid_review_button()
        has_entrypoint = await scraper._has_review_entrypoint()
        log(
            "entrypoint_scan",
            has_entrypoint=has_entrypoint,
            tab_button_found=tab_button is not None,
            tab_button_label=await _safe_label(scraper, tab_button),
            more_reviews_found=more_reviews_button is not None,
            more_reviews_label=await _safe_label(scraper, more_reviews_button),
            any_review_button_found=any_review_button is not None,
            any_review_button_label=await _safe_label(scraper, any_review_button),
        )

        opened = False
        for attempt in range(1, max(1, int(args.open_attempts)) + 1):
            ready_before = await scraper._wait_for_reviews_ready(timeout_ms=max(500, int(args.ready_timeout_ms)))
            feed_before = await _feed_snapshot(scraper)
            log("open_attempt_ready_check", attempt=attempt, ready=ready_before, feed=feed_before)
            if ready_before:
                opened = True
                break

            clicked_more = await scraper._click_more_reviews_summary_button()
            log("open_attempt_click_more_reviews", attempt=attempt, clicked=clicked_more)
            if clicked_more and scraper.page is not None:
                await scraper.page.wait_for_timeout(max(250, int(args.post_click_wait_ms)))

            ready_after_more = await scraper._wait_for_reviews_ready(timeout_ms=max(500, int(args.ready_timeout_ms)))
            if ready_after_more:
                opened = True
                break

            clicked_tab = await scraper._click_first_valid_review_button_in_group("REVIEWS_TAB")
            log("open_attempt_click_reviews_tab", attempt=attempt, clicked=clicked_tab)
            if clicked_tab and scraper.page is not None:
                await scraper.page.wait_for_timeout(max(250, int(args.post_click_wait_ms)))

            ready_after_tab = await scraper._wait_for_reviews_ready(timeout_ms=max(500, int(args.ready_timeout_ms)))
            if ready_after_tab:
                opened = True
                break

            clicked_button = await scraper._click_first_valid_review_button_in_group("REVIEWS_BUTTON")
            log("open_attempt_click_reviews_button", attempt=attempt, clicked=clicked_button)
            if clicked_button and scraper.page is not None:
                await scraper.page.wait_for_timeout(max(250, int(args.post_click_wait_ms)))

            ready_after_button = await scraper._wait_for_reviews_ready(timeout_ms=max(500, int(args.ready_timeout_ms)))
            if ready_after_button:
                opened = True
                break

            clicked_fallback = await scraper._click_review_entrypoint()
            log("open_attempt_click_fallback_entrypoint", attempt=attempt, clicked=clicked_fallback)
            if clicked_fallback and scraper.page is not None:
                await scraper.page.wait_for_timeout(max(250, int(args.post_click_wait_ms)))

            await _take_screenshot(scraper, run_dir / f"02_open_attempt_{attempt}.png")

        if not opened:
            ensured = await scraper._ensure_reviews_open()
            log("ensure_reviews_open_fallback", opened=ensured)
            opened = bool(ensured)

        await _take_screenshot(scraper, run_dir / "03_after_open_reviews_attempts.png")
        feed_after_open = await _feed_snapshot(scraper)
        log("feed_after_open_attempts", opened=opened, feed=feed_after_open)

        if not opened:
            log("stop_no_reviews_panel")
            summary["result"] = {
                "opened_reviews_panel": False,
                "reason": "Could not open reviews panel.",
            }
            return

        stable_rounds_target = max(2, int(args.stable_rounds))
        max_rounds = max(1, int(args.scroll_rounds))
        interval_ms = max(200, int(args.interval_ms))
        step_px = max(250, int(args.step_px))
        snapshot_every = max(1, int(args.snapshot_every))

        initial_state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
        last_count = int(initial_state.get("review_count", 0))
        last_top = int(initial_state.get("scroll_top", -1))
        last_scroll_height = int(initial_state.get("scroll_height", -1))
        unchanged_rounds = 0

        log(
            "scroll_started",
            initial_review_count=last_count,
            initial_at_bottom=bool(initial_state.get("at_bottom")),
            max_rounds=max_rounds,
            stable_rounds=stable_rounds_target,
            step_px=step_px,
            interval_ms=interval_ms,
        )

        for round_index in range(1, max_rounds + 1):
            metrics = await scraper._scroll_reviews_feed_step(step_px=step_px)
            if scraper.page is not None:
                await scraper.page.wait_for_timeout(interval_ms)
            state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
            current_count = int(state.get("review_count", 0))
            top = int(state.get("scroll_top", -1))
            scroll_height = int(state.get("scroll_height", -1))
            at_bottom = bool(state.get("at_bottom"))
            moved = bool(metrics.get("scrolled")) or top != last_top
            count_grew = current_count > last_count
            geometry_changed = top != last_top or scroll_height != last_scroll_height

            if count_grew:
                last_count = current_count
            if moved or count_grew or geometry_changed:
                unchanged_rounds = 0
            else:
                unchanged_rounds += 1

            log(
                "scroll_round",
                round=round_index,
                reviews_loaded=current_count,
                moved=moved,
                at_bottom=at_bottom,
                unchanged_rounds=unchanged_rounds,
                panel_ready=bool(state.get("panel_ready")),
                found=bool(state.get("found")),
                section_variant=str(state.get("section_variant", "") or ""),
                marker_count=int(state.get("marker_count", 0)),
                search_cue=bool(state.get("search_cue")),
                filter_cue=bool(state.get("filter_cue")),
                scroll_top=top,
                scroll_height=scroll_height,
            )

            if round_index % snapshot_every == 0:
                await _take_screenshot(scraper, run_dir / f"04_scroll_round_{round_index}.png")

            last_top = top
            last_scroll_height = scroll_height

            if at_bottom and unchanged_rounds >= stable_rounds_target:
                log("scroll_finished", reason="bottom_and_stable", round=round_index)
                break

            if not bool(state.get("found")) and unchanged_rounds >= stable_rounds_target:
                log("scroll_finished", reason="feed_not_found_stable", round=round_index)
                break

        reviews_html = await scraper._capture_reviews_feed_html()
        html_path = run_dir / "reviews_feed.html"
        html_path.write_text(reviews_html, encoding="utf-8")

        extracted_reviews = scraper.extract_reviews_from_html(reviews_html)
        unique_ids = {
            str(item.get("review_id", "") or "").strip()
            for item in extracted_reviews
            if str(item.get("review_id", "") or "").strip()
        }
        unique_count = len(unique_ids) if unique_ids else len(extracted_reviews)
        dom_count = await scraper._review_count()

        log(
            "extract_from_html",
            html_chars=len(reviews_html),
            extracted_count=len(extracted_reviews),
            unique_count=unique_count,
            dom_count=dom_count,
            listing_total_reviews=listing.get("total_reviews"),
            html_path=str(html_path),
        )
        await _take_screenshot(scraper, run_dir / "05_final.png")

        summary["result"] = {
            "opened_reviews_panel": True,
            "listing": listing,
            "dom_count": dom_count,
            "extracted_count": len(extracted_reviews),
            "unique_count": unique_count,
            "html_path": str(html_path),
        }
        reviews_json_path = run_dir / "reviews_extracted.json"
        reviews_json_path.write_text(json.dumps(extracted_reviews, ensure_ascii=False, indent=2), encoding="utf-8")
        summary["result"]["reviews_json_path"] = str(reviews_json_path)
    except Exception as exc:  # noqa: BLE001
        pending_error = exc
        log("error", error_type=type(exc).__name__, error=str(exc))
    finally:
        try:
            await scraper.close()
            log("scraper_closed")
        except Exception as close_exc:  # noqa: BLE001
            log("close_error", error_type=type(close_exc).__name__, error=str(close_exc))

        summary_path = run_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Diagnostics saved to: {run_dir}")
        print(f"Summary file: {summary_path}")

    if pending_error is not None:
        raise pending_error


if __name__ == "__main__":
    asyncio.run(main())
