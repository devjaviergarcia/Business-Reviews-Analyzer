import argparse
import asyncio
import json
import re
import sys
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
            "Run two local E2E scrapes, capture reviews-panel HTML for each, "
            "and compare structure differences."
        )
    )
    parser.add_argument(
        "--query-a",
        default="Baños Árabes de Córdoba",
        help="First query (default: Baños Árabes de Córdoba).",
    )
    parser.add_argument(
        "--query-b",
        default="Dobuss group",
        help="Second query (default: Dobuss group).",
    )
    parser.add_argument(
        "--scroll-attempts",
        type=int,
        default=4,
        help="Number of scroll attempts per query (default: 4).",
    )
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=1200,
        help="Wait between scroll attempts (default: 1200ms).",
    )
    parser.add_argument(
        "--step-px",
        type=int,
        default=900,
        help="Scroll pixels per attempt (default: 900).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode (default: headed).",
    )
    parser.add_argument(
        "--incognito",
        action="store_true",
        default=True,
        help="Run in incognito context (default: true).",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/compare_reviews_structure",
        help="Output directory (default: artifacts/compare_reviews_structure).",
    )
    return parser.parse_args()


def _safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_safe(v) for v in value[:30]]
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in list(value.items())[:40]}
    return str(value)


def _structure_metrics(html: str) -> dict[str, Any]:
    review_ids = re.findall(r"""data-review-id=['"]([^'"]+)['"]""", html, flags=re.IGNORECASE)
    unique_review_ids = sorted(set(review_ids))

    class_tokens = [
        "jftiEf",
        "d4r55",
        "kvMYJc",
        "rsqaWe",
        "MyEned",
        "wiI7pd",
        "CDe7pd",
        "m6QErb",
        "Hk4XGb",
        "QoaCgb",
        "wNNZR",
        "M77dve",
    ]
    class_counts = {token: len(re.findall(rf"""\b{re.escape(token)}\b""", html)) for token in class_tokens}

    marker_patterns = {
        "has_owner_reply_label_es": r"respuesta del propietario",
        "has_owner_reply_label_en": r"owner response",
        "has_more_reviews_label_es": r"m[aá]s rese",
        "has_more_reviews_label_en": r"more review",
        "has_sort_reviews_es": r"ordenar rese",
        "has_sort_reviews_en": r"sort review",
        "has_search_reviews_es": r"buscar rese",
        "has_search_reviews_en": r"search review",
    }
    marker_presence = {
        key: bool(re.search(pattern, html, flags=re.IGNORECASE))
        for key, pattern in marker_patterns.items()
    }

    return {
        "html_chars": len(html),
        "tag_counts": {
            "div": len(re.findall(r"<div\b", html, flags=re.IGNORECASE)),
            "button": len(re.findall(r"<button\b", html, flags=re.IGNORECASE)),
            "span": len(re.findall(r"<span\b", html, flags=re.IGNORECASE)),
            "img": len(re.findall(r"<img\b", html, flags=re.IGNORECASE)),
            "a": len(re.findall(r"<a\b", html, flags=re.IGNORECASE)),
        },
        "review_ids": {
            "occurrences": len(review_ids),
            "unique_count": len(unique_review_ids),
            "sample": unique_review_ids[:10],
        },
        "class_counts": class_counts,
        "markers": marker_presence,
    }


def _compare_case_metrics(case_a: dict[str, Any], case_b: dict[str, Any]) -> dict[str, Any]:
    metrics_a = case_a.get("structure_metrics", {})
    metrics_b = case_b.get("structure_metrics", {})

    numeric_diffs: dict[str, dict[str, Any]] = {}
    for key in ["html_chars"]:
        a_val = int(metrics_a.get(key, 0) or 0)
        b_val = int(metrics_b.get(key, 0) or 0)
        numeric_diffs[key] = {
            "a": a_val,
            "b": b_val,
            "delta_a_minus_b": a_val - b_val,
        }

    for tag in ["div", "button", "span", "img", "a"]:
        a_val = int(((metrics_a.get("tag_counts") or {}).get(tag, 0)) or 0)
        b_val = int(((metrics_b.get("tag_counts") or {}).get(tag, 0)) or 0)
        numeric_diffs[f"tag_count_{tag}"] = {
            "a": a_val,
            "b": b_val,
            "delta_a_minus_b": a_val - b_val,
        }

    a_unique_ids = int((((metrics_a.get("review_ids") or {}).get("unique_count")) or 0))
    b_unique_ids = int((((metrics_b.get("review_ids") or {}).get("unique_count")) or 0))
    numeric_diffs["unique_review_ids"] = {
        "a": a_unique_ids,
        "b": b_unique_ids,
        "delta_a_minus_b": a_unique_ids - b_unique_ids,
    }

    class_diffs: dict[str, dict[str, Any]] = {}
    all_class_tokens = sorted(
        set(((metrics_a.get("class_counts") or {}).keys()))
        | set(((metrics_b.get("class_counts") or {}).keys()))
    )
    for token in all_class_tokens:
        a_val = int(((metrics_a.get("class_counts") or {}).get(token, 0)) or 0)
        b_val = int(((metrics_b.get("class_counts") or {}).get(token, 0)) or 0)
        class_diffs[token] = {
            "a": a_val,
            "b": b_val,
            "delta_a_minus_b": a_val - b_val,
        }

    marker_diffs: dict[str, dict[str, Any]] = {}
    all_markers = sorted(
        set(((metrics_a.get("markers") or {}).keys()))
        | set(((metrics_b.get("markers") or {}).keys()))
    )
    for key in all_markers:
        a_val = bool((metrics_a.get("markers") or {}).get(key, False))
        b_val = bool((metrics_b.get("markers") or {}).get(key, False))
        marker_diffs[key] = {"a": a_val, "b": b_val, "different": a_val != b_val}

    compatibility_hints: list[str] = []
    if not bool(case_a.get("opened_reviews_panel")) and bool(case_b.get("opened_reviews_panel")):
        compatibility_hints.append(
            "Case A did not open reviews panel while Case B did; issue is likely in entrypoint/ready selectors."
        )
    if numeric_diffs["unique_review_ids"]["a"] == 0 and numeric_diffs["unique_review_ids"]["b"] > 0:
        compatibility_hints.append(
            "Case A reviews container produced 0 review IDs while Case B produced >0."
        )
    if (class_diffs.get("jftiEf") or {}).get("a", 0) == 0 and (class_diffs.get("jftiEf") or {}).get("b", 0) > 0:
        compatibility_hints.append(
            "Case A HTML lacks review-card class 'jftiEf' compared to Case B."
        )
    if bool((marker_diffs.get("has_more_reviews_label_es") or {}).get("different")):
        compatibility_hints.append(
            "Difference detected in presence of 'Más reseñas' marker between both cases."
        )

    return {
        "numeric_diffs": numeric_diffs,
        "class_diffs": class_diffs,
        "marker_diffs": marker_diffs,
        "compatibility_hints": compatibility_hints,
    }


async def _run_case(
    *,
    query: str,
    case_label: str,
    case_dir: Path,
    headless: bool,
    incognito: bool,
    scroll_attempts: int,
    interval_ms: int,
    step_px: int,
) -> dict[str, Any]:
    case_dir.mkdir(parents=True, exist_ok=True)
    timeline: list[dict[str, Any]] = []
    started_at = time_start = asyncio.get_event_loop().time()

    def add_timeline(event: str, **data: Any) -> None:
        elapsed_s = round(asyncio.get_event_loop().time() - started_at, 2)
        payload = {"elapsed_s": elapsed_s, "event": event, **{k: _safe(v) for k, v in data.items()}}
        timeline.append(payload)
        print(f"[{case_label}] {json.dumps(payload, ensure_ascii=False)}")

    scraper = GoogleMapsScraper(
        headless=headless,
        incognito=incognito,
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

    progress_events: list[dict[str, Any]] = []

    async def progress_cb(event: dict[str, Any]) -> None:
        progress_events.append({k: _safe(v) for k, v in event.items()})
        add_timeline("progress_event", **event)

    case_result: dict[str, Any] = {
        "query": query,
        "case_label": case_label,
        "opened_reviews_panel": False,
        "listing": {},
        "feed_state_before_scroll": {},
        "feed_state_after_scroll": {},
        "dom_review_count": 0,
        "reviews_feed_html_path": None,
        "progress_events_count": 0,
        "timeline": timeline,
    }

    try:
        add_timeline("scraper_start")
        await scraper.start()
        add_timeline("search_start", query=query)
        await scraper.search_business(query)
        add_timeline("search_done", url=(scraper.page.url if scraper.page else None))
        if scraper.page is not None:
            await scraper.page.screenshot(path=str(case_dir / "01_after_search.png"), full_page=True)

        listing = await scraper.extract_listing()
        limited_view = await scraper._is_limited_maps_view()
        case_result["listing"] = _safe(listing)
        add_timeline(
            "listing_done",
            business_name=listing.get("business_name"),
            total_reviews=listing.get("total_reviews"),
            limited_view=limited_view,
        )

        opened = await scraper._ensure_reviews_open(progress_callback=progress_cb)
        case_result["opened_reviews_panel"] = bool(opened)
        add_timeline("open_reviews_done", opened=opened)
        if scraper.page is not None:
            await scraper.page.screenshot(path=str(case_dir / "02_after_open_reviews.png"), full_page=True)

        feed_before = await scraper._reviews_feed_state(step_px=None, capture_html=False)
        case_result["feed_state_before_scroll"] = _safe(feed_before)

        if opened:
            for round_index in range(1, max(1, scroll_attempts) + 1):
                metrics = await scraper._scroll_reviews_feed_step(step_px=step_px)
                if scraper.page is not None:
                    await scraper.page.wait_for_timeout(interval_ms)
                state = await scraper._reviews_feed_state(step_px=None, capture_html=False)
                add_timeline(
                    "manual_scroll_round",
                    round=round_index,
                    metrics=metrics,
                    state={
                        "panel_ready": bool(state.get("panel_ready")),
                        "found": bool(state.get("found")),
                        "review_count": int(state.get("review_count", 0)),
                        "at_bottom": bool(state.get("at_bottom")),
                        "scroll_top": int(state.get("scroll_top", 0)),
                        "scroll_height": int(state.get("scroll_height", 0)),
                    },
                )
            if scraper.page is not None:
                await scraper.page.screenshot(path=str(case_dir / "03_after_scrolls.png"), full_page=True)

        feed_after = await scraper._reviews_feed_state(step_px=None, capture_html=False)
        case_result["feed_state_after_scroll"] = _safe(feed_after)
        case_result["dom_review_count"] = int(await scraper._review_count())

        reviews_html = await scraper._capture_reviews_feed_html()
        html_path = case_dir / "reviews_panel.html"
        html_path.write_text(reviews_html, encoding="utf-8")
        case_result["reviews_feed_html_path"] = str(html_path)
        case_result["reviews_feed_html_chars"] = len(reviews_html)
        case_result["structure_metrics"] = _structure_metrics(reviews_html)
        case_result["progress_events_count"] = len(progress_events)

        events_path = case_dir / "progress_events.json"
        events_path.write_text(json.dumps(progress_events, ensure_ascii=False, indent=2), encoding="utf-8")
        case_result["progress_events_path"] = str(events_path)
        add_timeline(
            "case_done",
            dom_review_count=case_result["dom_review_count"],
            html_chars=case_result["reviews_feed_html_chars"],
            progress_events=len(progress_events),
        )
    finally:
        await scraper.close()
        add_timeline("scraper_closed")

    summary_path = case_dir / "case_summary.json"
    summary_path.write_text(json.dumps(case_result, ensure_ascii=False, indent=2), encoding="utf-8")
    case_result["case_summary_path"] = str(summary_path)
    return case_result


async def main() -> None:
    args = _parse_args()
    output_root = Path(args.output_dir)
    if not output_root.is_absolute():
        output_root = (PROJECT_ROOT / output_root).resolve()
    run_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    case_a_dir = run_dir / f"case_a_{_slug(args.query_a)}"
    case_b_dir = run_dir / f"case_b_{_slug(args.query_b)}"

    print(f"Output run dir: {run_dir}")
    print(f"Case A query: {args.query_a}")
    print(f"Case B query: {args.query_b}")
    print(f"Scroll attempts per case: {args.scroll_attempts}")

    case_a = await _run_case(
        query=args.query_a,
        case_label="A",
        case_dir=case_a_dir,
        headless=bool(args.headless),
        incognito=bool(args.incognito),
        scroll_attempts=max(1, int(args.scroll_attempts)),
        interval_ms=max(200, int(args.interval_ms)),
        step_px=max(250, int(args.step_px)),
    )
    case_b = await _run_case(
        query=args.query_b,
        case_label="B",
        case_dir=case_b_dir,
        headless=bool(args.headless),
        incognito=bool(args.incognito),
        scroll_attempts=max(1, int(args.scroll_attempts)),
        interval_ms=max(200, int(args.interval_ms)),
        step_px=max(250, int(args.step_px)),
    )

    comparison = {
        "created_at": datetime.now().isoformat(),
        "queries": {
            "a": args.query_a,
            "b": args.query_b,
        },
        "scroll_attempts": int(args.scroll_attempts),
        "case_a": {
            "opened_reviews_panel": case_a.get("opened_reviews_panel"),
            "dom_review_count": case_a.get("dom_review_count"),
            "reviews_feed_html_chars": case_a.get("reviews_feed_html_chars"),
            "reviews_feed_html_path": case_a.get("reviews_feed_html_path"),
            "listing_total_reviews": (case_a.get("listing") or {}).get("total_reviews"),
            "structure_metrics": case_a.get("structure_metrics"),
        },
        "case_b": {
            "opened_reviews_panel": case_b.get("opened_reviews_panel"),
            "dom_review_count": case_b.get("dom_review_count"),
            "reviews_feed_html_chars": case_b.get("reviews_feed_html_chars"),
            "reviews_feed_html_path": case_b.get("reviews_feed_html_path"),
            "listing_total_reviews": (case_b.get("listing") or {}).get("total_reviews"),
            "structure_metrics": case_b.get("structure_metrics"),
        },
        "diff": _compare_case_metrics(case_a, case_b),
    }

    comparison_json = run_dir / "comparison.json"
    comparison_json.write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# Comparison Summary",
        "",
        f"- Query A: `{args.query_a}`",
        f"- Query B: `{args.query_b}`",
        f"- Scroll attempts per case: `{args.scroll_attempts}`",
        "",
        "## Key Results",
        "",
        f"- Case A opened reviews panel: `{comparison['case_a']['opened_reviews_panel']}`",
        f"- Case B opened reviews panel: `{comparison['case_b']['opened_reviews_panel']}`",
        f"- Case A dom_review_count: `{comparison['case_a']['dom_review_count']}`",
        f"- Case B dom_review_count: `{comparison['case_b']['dom_review_count']}`",
        f"- Case A HTML chars: `{comparison['case_a']['reviews_feed_html_chars']}`",
        f"- Case B HTML chars: `{comparison['case_b']['reviews_feed_html_chars']}`",
        "",
        "## Compatibility Hints",
        "",
    ]
    hints = comparison["diff"].get("compatibility_hints") or []
    if hints:
        for hint in hints:
            md_lines.append(f"- {hint}")
    else:
        md_lines.append("- No clear incompatibility hint detected from structural counters.")

    comparison_md = run_dir / "comparison.md"
    comparison_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"Comparison JSON: {comparison_json}")
    print(f"Comparison MD: {comparison_md}")
    print(f"Case A HTML: {comparison['case_a']['reviews_feed_html_path']}")
    print(f"Case B HTML: {comparison['case_b']['reviews_feed_html_path']}")


if __name__ == "__main__":
    asyncio.run(main())
