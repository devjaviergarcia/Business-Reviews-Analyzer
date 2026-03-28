import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.tripadvisor import TripadvisorScraper


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    return normalized or "query"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Tripadvisor search flow and diagnose review-card/text selectors. "
            "Outputs JSON + HTML snapshots for each analyzed page."
        )
    )
    parser.add_argument(
        "--query",
        default="Hotel de los Faroles",
        help="Tripadvisor query/name to search.",
    )
    parser.add_argument(
        "--url",
        default="https://www.tripadvisor.es",
        help="Initial Tripadvisor URL.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
        help="Maximum review pages to diagnose (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/tripadvisor_review_dom_diagnose",
        help="Output directory for report and HTML snapshots.",
    )
    parser.add_argument(
        "--profile-dir",
        default=settings.scraper_tripadvisor_user_data_dir,
        help=(
            "Profile dir used by Playwright persistent context "
            f"(default: {settings.scraper_tripadvisor_user_data_dir})."
        ),
    )
    parser.add_argument(
        "--incognito",
        action="store_true",
        help="Use incognito context instead of persistent profile.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Force headless mode (default uses current scraper setting).",
    )
    parser.add_argument(
        "--save-screenshot",
        action="store_true",
        help="Save one screenshot per diagnosed page.",
    )
    return parser.parse_args()


async def _collect_review_diagnostics(page, page_index: int) -> dict[str, Any]:
    return await page.evaluate(
        """
        ({ pageIndex }) => {
          const clean = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
          const signature = (node) => {
            if (!node) return "";
            const tag = (node.tagName || "").toLowerCase();
            const dtt = node.getAttribute?.("data-test-target") || "";
            const da = node.getAttribute?.("data-automation") || "";
            const classes = clean(node.className || "").split(" ").filter(Boolean).slice(0, 3).join(".");
            const cls = classes ? `.${classes}` : "";
            const dttPart = dtt ? `[dtt=${dtt}]` : "";
            const daPart = da ? `[da=${da}]` : "";
            return `${tag}${dttPart}${daPart}${cls}`;
          };
          const nodeDepthWithin = (root, node) => {
            let depth = 0;
            let current = node;
            while (current && current !== root) {
              depth += 1;
              current = current.parentElement;
            }
            return current === root ? depth : -1;
          };
          const pathWithin = (root, node) => {
            const parts = [];
            let current = node;
            while (current && current !== root) {
              parts.push(signature(current));
              current = current.parentElement;
            }
            if (current === root) {
              parts.push(signature(root));
            }
            return parts.reverse().join(" > ");
          };
          const pickFirstNode = (scope, selectors) => {
            for (const selector of selectors) {
              const node = scope.querySelector(selector);
              if (node) return { selector, node };
            }
            return null;
          };
          const pickBestTextNode = (scope, selectors) => {
            let best = null;
            for (const selector of selectors) {
              const node = scope.querySelector(selector);
              const text = clean(node?.textContent);
              if (!text) continue;
              if (!best || text.length > best.text.length) {
                best = { selector, node, text };
              }
            }
            return best;
          };
          const sortFieldStats = (fieldMap) =>
            Object.entries(fieldMap || {})
              .map(([path, data]) => ({
                path,
                count: data.count,
                avg_depth: data.count > 0 ? Number((data.depth_sum / data.count).toFixed(2)) : 0,
                sample: (data.sample || "").slice(0, 220),
              }))
              .sort((a, b) => {
                if (b.count !== a.count) return b.count - a.count;
                return a.avg_depth - b.avg_depth;
              })
              .slice(0, 15);

          const registerFieldHit = (fieldStats, field, root, node, sampleText) => {
            if (!root || !node) return;
            const depth = nodeDepthWithin(root, node);
            if (depth < 0) return;
            const path = pathWithin(root, node);
            if (!path) return;
            if (!fieldStats[field]) fieldStats[field] = {};
            if (!fieldStats[field][path]) {
              fieldStats[field][path] = { count: 0, depth_sum: 0, sample: "" };
            }
            fieldStats[field][path].count += 1;
            fieldStats[field][path].depth_sum += depth;
            if (!fieldStats[field][path].sample && sampleText) {
              fieldStats[field][path].sample = clean(sampleText).slice(0, 320);
            }
          };

          const cardSelectors = [
            "div[data-automation='reviewCard']",
            "div[data-test-target='HR_CC_CARD']",
            "div.ryPjd.Gi.f.e",
            "div.bAlNy.Gi._c.f.e",
            "[data-test-target='review-title']",
            "a[href*='ShowUserReviews-']",
          ];
          const textSelectors = [
            "div[data-test-target='review-body'] span.JguWG div.biGQs._P.VImYz.AWdfh",
            "div[data-test-target='review-body'] span.JguWG",
            "div[data-test-target='review-body'] div.biGQs._P.VImYz.AWdfh",
            "div[data-test-target='review-body']",
            "div._c div._T.FKffI span.JguWG div.biGQs._P.VImYz.AWdfh",
            "div._c div._T.FKffI span.JguWG",
            "div._c div._T.FKffI",
            "div._T.FKffI span.JguWG",
            "div._T.FKffI",
          ];
          const ratingSelectors = [
            "svg[data-automation='bubbleRatingImage'] title",
            "title[id*='_lithium']",
            "svg[data-automation='bubbleRatingImage']",
            "svg.evwcZ title",
            "svg.evwcZ",
          ];
          const profileSelectors = [
            "a[href*='/Profile/'].ukgoS",
            "span.biGQs._P.ezezH a[href*='/Profile/']",
            "a[href*='/Profile/']",
          ];
          const relativeSelectors = [
            "div.VufqL.o.W",
            "div.VufqL",
            "div.biGQs._P.SewaP.AWdfh",
          ];
          const writtenDateSelectors = [
            "div.biGQs._P.VImYz.ncFvv.navcl",
            "div.biGQs._P.VImYz.navcl",
          ];
          const ownerReplyRootSelectors = [
            "div.mahws",
            "div[data-test-target='management-response']",
            "div[data-test-target='owner-response']",
          ];
          const ownerReplyTextSelectors = [
            "div._T.FKffI span.JguWG div.biGQs._P.VImYz.AWdfh",
            "div._T.FKffI span.JguWG",
            "div._T.FKffI",
            "span.JguWG",
          ];
          const ownerReplyAuthorSelectors = [
            "a[href*='/Profile/'].ukgoS",
            "span.biGQs._P.ezezH",
            "a[href*='/Profile/']",
          ];

          const inferCard = (anchor) =>
            anchor.closest("div[data-automation='reviewCard'], div[data-test-target='HR_CC_CARD'], div.ryPjd.Gi.f.e, div.bAlNy.Gi._c.f.e")
            || anchor.closest("div");

          const countBySelector = {};
          for (const selector of cardSelectors) {
            countBySelector[selector] = document.querySelectorAll(selector).length;
          }

          let anchors = Array.from(document.querySelectorAll("a[href*='ShowUserReviews-']"));
          if (!anchors.length) {
            anchors = Array.from(document.querySelectorAll("[data-test-target='review-title'] a[href], [data-test-target='review-title']"))
              .map((node) => (node.tagName.toLowerCase() === 'a' ? node : node.querySelector("a[href]")))
              .filter(Boolean);
          }
          anchors = anchors.slice(0, 60);

          const selectorStats = textSelectors.map((selector) => ({
            selector,
            hit_count: 0,
            total_length: 0,
          }));

          const rootPatterns = {};
          const fieldPathStats = {};
          const samples = [];

          for (let i = 0; i < anchors.length; i += 1) {
            const anchor = anchors[i];
            const card = inferCard(anchor);
            if (!card) continue;

            const cardPattern = [
              card.tagName?.toLowerCase() || "",
              card.getAttribute("data-test-target") || "",
              clean(card.className).slice(0, 80),
            ].join("|");
            rootPatterns[cardPattern] = (rootPatterns[cardPattern] || 0) + 1;
            registerFieldHit(fieldPathStats, "card_root", card, card, cardPattern);

            const textCandidates = [];
            for (let s = 0; s < textSelectors.length; s += 1) {
              const selector = textSelectors[s];
              const node = card.querySelector(selector);
              const text = clean(node?.textContent);
              if (text) {
                selectorStats[s].hit_count += 1;
                selectorStats[s].total_length += text.length;
                textCandidates.push({
                  selector,
                  length: text.length,
                  sample: text.slice(0, 220),
                });
              }
            }
            const bestText = pickBestTextNode(card, textSelectors);
            if (bestText) {
              registerFieldHit(fieldPathStats, "text", card, bestText.node, bestText.text);
            }

            const bestRatingNode = pickFirstNode(card, ratingSelectors);
            const ratingText = clean(bestRatingNode?.node?.textContent);
            if (bestRatingNode) {
              registerFieldHit(fieldPathStats, "rating", card, bestRatingNode.node, ratingText);
            }

            let author = "";
            let authorNode = null;
            for (const selector of profileSelectors) {
              const node = card.querySelector(selector);
              author = clean(node?.textContent);
              if (author) {
                authorNode = node;
                break;
              }
            }
            if (authorNode) {
              registerFieldHit(fieldPathStats, "author", card, authorNode, author);
            }

            let relativeTime = "";
            let relativeNode = null;
            for (const selector of relativeSelectors) {
              const node = card.querySelector(selector);
              relativeTime = clean(node?.textContent);
              if (relativeTime) {
                relativeNode = node;
                break;
              }
            }
            if (relativeNode) {
              registerFieldHit(fieldPathStats, "relative_time", card, relativeNode, relativeTime);
            }

            let writtenDate = "";
            let writtenDateNode = null;
            for (const selector of writtenDateSelectors) {
              const node = card.querySelector(selector);
              const text = clean(node?.textContent);
              if (!text) continue;
              if (/escrita el|escrito el|written/i.test(text)) {
                writtenDate = text;
                writtenDateNode = node;
                break;
              }
            }
            if (writtenDateNode) {
              registerFieldHit(fieldPathStats, "written_date", card, writtenDateNode, writtenDate);
            }

            let ownerReplyText = "";
            let ownerReplyNode = null;
            let ownerReplyAuthor = "";
            let ownerReplyAuthorNode = null;
            let ownerReplyWrittenDate = "";
            let ownerReplyWrittenDateNode = null;
            let ownerReplyRootNode = null;
            for (const rootSelector of ownerReplyRootSelectors) {
              const rootNode = card.querySelector(rootSelector);
              if (!rootNode) continue;
              ownerReplyRootNode = rootNode;

              const textNode = pickBestTextNode(rootNode, ownerReplyTextSelectors);
              if (textNode) {
                ownerReplyText = textNode.text;
                ownerReplyNode = textNode.node;
              }
              const authorEntry = pickFirstNode(rootNode, ownerReplyAuthorSelectors);
              if (authorEntry) {
                ownerReplyAuthor = clean(authorEntry.node?.textContent);
                ownerReplyAuthorNode = authorEntry.node;
              }
              for (const selector of writtenDateSelectors) {
                const node = rootNode.querySelector(selector);
                const text = clean(node?.textContent);
                if (!text) continue;
                if (/escrita el|escrito el|written|responded/i.test(text)) {
                  ownerReplyWrittenDate = text;
                  ownerReplyWrittenDateNode = node;
                  break;
                }
              }
              break;
            }
            if (ownerReplyRootNode) {
              registerFieldHit(fieldPathStats, "owner_reply_root", card, ownerReplyRootNode, "");
            }
            if (ownerReplyNode) {
              registerFieldHit(fieldPathStats, "owner_reply_text", card, ownerReplyNode, ownerReplyText);
            }
            if (ownerReplyAuthorNode) {
              registerFieldHit(fieldPathStats, "owner_reply_author", card, ownerReplyAuthorNode, ownerReplyAuthor);
            }
            if (ownerReplyWrittenDateNode) {
              registerFieldHit(
                fieldPathStats,
                "owner_reply_written_date",
                card,
                ownerReplyWrittenDateNode,
                ownerReplyWrittenDate
              );
            }

            samples.push({
              index: i + 1,
              href: clean(anchor.getAttribute("href")),
              title: clean(anchor.textContent).slice(0, 180),
              author,
              relative_time: relativeTime,
              written_date: writtenDate,
              rating_text: ratingText,
              owner_reply_text: ownerReplyText.slice(0, 180),
              owner_reply_author: ownerReplyAuthor,
              card_pattern: cardPattern,
              text_candidates: textCandidates.slice(0, 5),
            });
          }

          const sortedSelectorStats = selectorStats
            .map((item) => ({
              selector: item.selector,
              hit_count: item.hit_count,
              avg_length: item.hit_count > 0 ? Math.round(item.total_length / item.hit_count) : 0,
            }))
            .sort((a, b) => {
              if (b.hit_count !== a.hit_count) return b.hit_count - a.hit_count;
              return b.avg_length - a.avg_length;
            });

          const rootPatternList = Object.entries(rootPatterns)
            .map(([pattern, count]) => ({ pattern, count }))
            .sort((a, b) => b.count - a.count)
            .slice(0, 15);

          const fieldPaths = {};
          for (const [field, fieldMap] of Object.entries(fieldPathStats)) {
            fieldPaths[field] = sortFieldStats(fieldMap);
          }
          const recommendedFieldPaths = {};
          for (const [field, rows] of Object.entries(fieldPaths)) {
            recommendedFieldPaths[field] = (rows || []).slice(0, 3);
          }

          return {
            page_index: pageIndex,
            page_url: window.location.href,
            selectors_count: countBySelector,
            anchors_detected: anchors.length,
            text_selector_stats: sortedSelectorStats,
            recommended_text_selectors: sortedSelectorStats.filter((item) => item.hit_count > 0).slice(0, 5),
            root_patterns: rootPatternList,
            field_paths: fieldPaths,
            recommended_field_paths: recommendedFieldPaths,
            samples: samples.slice(0, 15),
          };
        }
        """,
        {"pageIndex": page_index},
    )


async def main() -> None:
    args = _parse_args()
    query = str(args.query or "").strip()
    if not query:
        raise ValueError("--query is required.")

    max_pages = max(1, int(args.max_pages))
    output_root = Path(args.output_dir).resolve()
    run_dir = output_root / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_slug(query)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    use_headless = bool(args.headless) or bool(settings.scraper_headless)
    scraper = TripadvisorScraper(
        headless=use_headless,
        incognito=bool(args.incognito),
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=str(args.profile_dir),
        browser_channel=settings.scraper_browser_channel,
        tripadvisor_url=str(args.url or "https://www.tripadvisor.es"),
        timeout_ms=settings.scraper_timeout_ms,
        min_click_delay_ms=settings.scraper_min_click_delay_ms,
        max_click_delay_ms=settings.scraper_max_click_delay_ms,
        min_key_delay_ms=settings.scraper_min_key_delay_ms,
        max_key_delay_ms=settings.scraper_max_key_delay_ms,
        stealth_mode=settings.scraper_stealth_mode,
        harden_headless=settings.scraper_harden_headless,
        extra_chromium_args=settings.scraper_extra_chromium_args,
    )

    report: dict[str, Any] = {
        "query": query,
        "max_pages": max_pages,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "run_dir": str(run_dir),
        "pages": [],
    }

    try:
        await scraper.start()
        await scraper.search_business(query)
        listing = await scraper.extract_listing()
        report["listing"] = listing
        await scraper._open_reviews_section()  # noqa: SLF001

        page = scraper.page
        for page_index in range(1, max_pages + 1):
            page_report = await _collect_review_diagnostics(page, page_index)
            report["pages"].append(page_report)

            html_path = run_dir / f"page_{page_index:02d}.html"
            html_path.write_text(await page.content(), encoding="utf-8")
            page_report["html_path"] = str(html_path)

            if args.save_screenshot:
                screenshot_path = run_dir / f"page_{page_index:02d}.png"
                await page.screenshot(path=str(screenshot_path), full_page=True)
                page_report["screenshot_path"] = str(screenshot_path)

            print(
                f"[page {page_index}] url={page_report.get('page_url')} "
                f"anchors={page_report.get('anchors_detected')} "
                f"best_text_selector={(page_report.get('recommended_text_selectors') or [{}])[0].get('selector', 'n/a')}"
            )

            if page_index >= max_pages:
                break
            moved = await scraper._go_next_reviews_page()  # noqa: SLF001
            if not moved:
                report["stopped_reason"] = f"pagination_end_at_page_{page_index}"
                break
            await page.wait_for_timeout(200)
            await scraper._open_reviews_section()  # noqa: SLF001
    finally:
        await scraper.close()

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report_path = run_dir / "diagnostic_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"Diagnostic report: {report_path}")
    print("Recommended selectors by page:")
    for page in report.get("pages", []):
        top = (page.get("recommended_text_selectors") or [])[:3]
        top_text = ", ".join(
            f"{entry.get('selector')} (hits={entry.get('hit_count')},avg={entry.get('avg_length')})"
            for entry in top
        )
        print(f"  page={page.get('page_index')}: {top_text or 'n/a'}")


if __name__ == "__main__":
    asyncio.run(main())
