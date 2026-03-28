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
            "Open Google Maps business pages, open reviews panel, save reviews HTML for each case, "
            "and produce a comparison report."
        )
    )
    parser.add_argument("--query-a", default="Baños Árabes de Córdoba")
    parser.add_argument("--query-b", default="Dobuss group")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode.")
    parser.add_argument("--incognito", action="store_true", default=True, help="Run in incognito context.")
    parser.add_argument(
        "--output-dir",
        default="artifacts/reviews_html_compare",
        help="Directory where artifacts will be saved.",
    )
    parser.add_argument(
        "--scroll-after-open",
        type=int,
        default=0,
        help="Optional manual scroll attempts after opening reviews (default: 0).",
    )
    parser.add_argument(
        "--scroll-step-px",
        type=int,
        default=900,
        help="Scroll step pixels for optional post-open scrolls.",
    )
    parser.add_argument(
        "--scroll-interval-ms",
        type=int,
        default=1000,
        help="Wait between optional post-open scrolls.",
    )
    return parser.parse_args()


def _review_id_count(html: str) -> int:
    return len(set(re.findall(r"""data-review-id=['"]([^'"]+)['"]""", html, flags=re.IGNORECASE)))


def _contains_marker(html: str, pattern: str) -> bool:
    return bool(re.search(pattern, html, flags=re.IGNORECASE))


async def _fallback_reviews_section_html(scraper: GoogleMapsScraper) -> dict[str, Any]:
    page = scraper.page
    payload = await page.evaluate(
        """
        () => {
          const main = document.querySelector("div[role='main']") || document.body;
          const score = (el) => {
            const text = (el.innerText || "").toLowerCase();
            const html = el.outerHTML || "";
            const hasReviewWord = /rese|review/.test(text) ? 1 : 0;
            const reviewIds = (html.match(/data-review-id=/g) || []).length;
            return (hasReviewWord * 1000000) + (reviewIds * 10000) + html.length;
          };

          const candidates = [];
          const selectors = [
            "div[role='main'] div.m6QErb",
            "div[role='main'] [role='feed']",
            "div[role='main']",
            "div"
          ];
          for (const selector of selectors) {
            const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 300);
            for (let idx = 0; idx < nodes.length; idx++) {
              const node = nodes[idx];
              const html = node.outerHTML || "";
              if (!html) continue;
              const text = (node.innerText || "");
              if (!/rese|review/i.test(text) && !/data-review-id=/i.test(html)) continue;
              candidates.push({
                selector,
                index: idx,
                score: score(node),
                html,
                text_len: text.length,
                html_len: html.length,
                review_id_occurrences: (html.match(/data-review-id=/g) || []).length,
              });
            }
          }

          candidates.sort((a, b) => b.score - a.score);
          const best = candidates[0] || null;
          return {
            best_html: best ? best.html : "",
            best_meta: best
              ? {
                  selector: best.selector,
                  index: best.index,
                  text_len: best.text_len,
                  html_len: best.html_len,
                  review_id_occurrences: best.review_id_occurrences,
                  score: best.score,
                }
              : null,
            top_candidates: candidates.slice(0, 10).map(c => ({
              selector: c.selector,
              index: c.index,
              text_len: c.text_len,
              html_len: c.html_len,
              review_id_occurrences: c.review_id_occurrences,
              score: c.score,
            })),
          };
        }
        """
    )
    if not isinstance(payload, dict):
        return {"best_html": "", "best_meta": None, "top_candidates": []}
    return payload


async def _capture_case(
    *,
    label: str,
    query: str,
    case_dir: Path,
    headless: bool,
    incognito: bool,
    scroll_after_open: int,
    scroll_step_px: int,
    scroll_interval_ms: int,
) -> dict[str, Any]:
    case_dir.mkdir(parents=True, exist_ok=True)

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

    async def _on_progress(event: dict[str, Any]) -> None:
        progress_events.append(event)

    result: dict[str, Any] = {
        "label": label,
        "query": query,
        "opened_reviews_panel": False,
        "listing": {},
        "reviews_html_path": None,
        "reviews_html_len": 0,
        "review_id_unique_count": 0,
        "used_fallback_section_capture": False,
        "fallback_meta": None,
        "progress_events_count": 0,
    }

    try:
        print(f"[{label}] search -> {query}")
        await scraper.start()
        await scraper.search_business(query)
        if scraper.page is not None:
            await scraper.page.screenshot(path=str(case_dir / "01_after_search.png"), full_page=True)

        listing = await scraper.extract_listing()
        result["listing"] = listing
        print(f"[{label}] listing: {listing.get('business_name')} total_reviews={listing.get('total_reviews')}")

        opened = await scraper._ensure_reviews_open(progress_callback=_on_progress)
        result["opened_reviews_panel"] = bool(opened)
        print(f"[{label}] opened_reviews_panel={opened}")
        if scraper.page is not None:
            await scraper.page.screenshot(path=str(case_dir / "02_after_open_attempt.png"), full_page=True)

        if opened and scroll_after_open > 0:
            for idx in range(1, scroll_after_open + 1):
                _ = await scraper._scroll_reviews_feed_step(step_px=max(250, scroll_step_px))
                if scraper.page is not None:
                    await scraper.page.wait_for_timeout(max(250, scroll_interval_ms))
            if scraper.page is not None:
                await scraper.page.screenshot(path=str(case_dir / "03_after_scrolls.png"), full_page=True)

        reviews_html = ""
        if opened:
            reviews_html = await scraper._capture_reviews_feed_html()

        if not reviews_html:
            fallback = await _fallback_reviews_section_html(scraper)
            reviews_html = str(fallback.get("best_html", "") or "")
            result["used_fallback_section_capture"] = True
            result["fallback_meta"] = fallback.get("best_meta")
            fallback_meta_path = case_dir / "fallback_candidates.json"
            fallback_meta_path.write_text(
                json.dumps(
                    {
                        "best_meta": fallback.get("best_meta"),
                        "top_candidates": fallback.get("top_candidates"),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            result["fallback_candidates_path"] = str(fallback_meta_path)

        html_path = case_dir / "reviews_section.html"
        html_path.write_text(reviews_html, encoding="utf-8")
        result["reviews_html_path"] = str(html_path)
        result["reviews_html_len"] = len(reviews_html)
        result["review_id_unique_count"] = _review_id_count(reviews_html)
        result["progress_events_count"] = len(progress_events)

        progress_path = case_dir / "progress_events.json"
        progress_path.write_text(json.dumps(progress_events, ensure_ascii=False, indent=2), encoding="utf-8")
        result["progress_events_path"] = str(progress_path)
    finally:
        await scraper.close()

    summary_path = case_dir / "case_summary.json"
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    result["case_summary_path"] = str(summary_path)
    return result


def _build_comparison(case_a: dict[str, Any], case_b: dict[str, Any]) -> dict[str, Any]:
    html_a = Path(str(case_a.get("reviews_html_path"))).read_text(encoding="utf-8")
    html_b = Path(str(case_b.get("reviews_html_path"))).read_text(encoding="utf-8")

    diff = {
        "opened_reviews_panel": {
            "a": bool(case_a.get("opened_reviews_panel")),
            "b": bool(case_b.get("opened_reviews_panel")),
        },
        "reviews_html_len": {
            "a": int(case_a.get("reviews_html_len", 0) or 0),
            "b": int(case_b.get("reviews_html_len", 0) or 0),
            "delta_a_minus_b": int(case_a.get("reviews_html_len", 0) or 0) - int(case_b.get("reviews_html_len", 0) or 0),
        },
        "review_id_unique_count": {
            "a": int(case_a.get("review_id_unique_count", 0) or 0),
            "b": int(case_b.get("review_id_unique_count", 0) or 0),
            "delta_a_minus_b": int(case_a.get("review_id_unique_count", 0) or 0)
            - int(case_b.get("review_id_unique_count", 0) or 0),
        },
        "markers": {
            "a_has_reviews_word": _contains_marker(html_a, r"rese|review"),
            "b_has_reviews_word": _contains_marker(html_b, r"rese|review"),
            "a_has_data_review_id": _contains_marker(html_a, r"data-review-id"),
            "b_has_data_review_id": _contains_marker(html_b, r"data-review-id"),
            "a_has_sort_reviews": _contains_marker(html_a, r"ordenar rese|sort review"),
            "b_has_sort_reviews": _contains_marker(html_b, r"ordenar rese|sort review"),
        },
    }

    hints: list[str] = []
    if not case_a.get("opened_reviews_panel") and case_b.get("opened_reviews_panel"):
        hints.append("Caso A no abre el panel de reseñas; caso B sí.")
    if int(case_a.get("review_id_unique_count", 0) or 0) == 0 and int(case_b.get("review_id_unique_count", 0) or 0) > 0:
        hints.append("Caso A no contiene review_ids en HTML, caso B sí.")
    if bool(case_a.get("used_fallback_section_capture")):
        hints.append("Caso A necesitó captura fallback porque no hubo feed de reseñas listo.")

    return {"diff": diff, "hints": hints}


async def main() -> None:
    args = _parse_args()
    output_root = Path(args.output_dir)
    if not output_root.is_absolute():
        output_root = (PROJECT_ROOT / output_root).resolve()
    run_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    case_a_dir = run_dir / f"case_a_{_slug(args.query_a)}"
    case_b_dir = run_dir / f"case_b_{_slug(args.query_b)}"

    case_a = await _capture_case(
        label="A",
        query=args.query_a,
        case_dir=case_a_dir,
        headless=bool(args.headless),
        incognito=bool(args.incognito),
        scroll_after_open=max(0, int(args.scroll_after_open)),
        scroll_step_px=max(250, int(args.scroll_step_px)),
        scroll_interval_ms=max(250, int(args.scroll_interval_ms)),
    )
    case_b = await _capture_case(
        label="B",
        query=args.query_b,
        case_dir=case_b_dir,
        headless=bool(args.headless),
        incognito=bool(args.incognito),
        scroll_after_open=max(0, int(args.scroll_after_open)),
        scroll_step_px=max(250, int(args.scroll_step_px)),
        scroll_interval_ms=max(250, int(args.scroll_interval_ms)),
    )

    comparison = {
        "created_at": datetime.now().isoformat(),
        "queries": {"a": args.query_a, "b": args.query_b},
        "case_a": case_a,
        "case_b": case_b,
    }
    comparison.update(_build_comparison(case_a, case_b))

    comparison_json_path = run_dir / "comparison.json"
    comparison_json_path.write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")

    comparison_md_path = run_dir / "comparison.md"
    md_lines = [
        "# Reviews Section HTML Comparison",
        "",
        f"- Query A: `{args.query_a}`",
        f"- Query B: `{args.query_b}`",
        "",
        "## Result",
        "",
        f"- A opened reviews panel: `{case_a.get('opened_reviews_panel')}`",
        f"- B opened reviews panel: `{case_b.get('opened_reviews_panel')}`",
        f"- A reviews_html_len: `{case_a.get('reviews_html_len')}`",
        f"- B reviews_html_len: `{case_b.get('reviews_html_len')}`",
        f"- A review_id_unique_count: `{case_a.get('review_id_unique_count')}`",
        f"- B review_id_unique_count: `{case_b.get('review_id_unique_count')}`",
        "",
        "## Hints",
        "",
    ]
    hints = comparison.get("hints") or []
    if hints:
        for hint in hints:
            md_lines.append(f"- {hint}")
    else:
        md_lines.append("- No major incompatibility hint detected.")
    comparison_md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"Run directory: {run_dir}")
    print(f"Comparison JSON: {comparison_json_path}")
    print(f"Comparison MD: {comparison_md_path}")
    print(f"A HTML: {case_a.get('reviews_html_path')}")
    print(f"B HTML: {case_b.get('reviews_html_path')}")


if __name__ == "__main__":
    asyncio.run(main())
