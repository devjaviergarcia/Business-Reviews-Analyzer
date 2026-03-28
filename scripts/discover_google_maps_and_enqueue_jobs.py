#!/usr/bin/env python3
"""
Discover places in Google Maps (hotels/hostels/guesthouses by default) and enqueue
one scraping job per discovered place via the API.

Example:
  python scripts/discover_google_maps_and_enqueue_jobs.py \
    --area "Córdoba" \
    --max-results-per-query 25 \
    --api-base-url "http://localhost:8000" \
    --sources google_maps
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import unicodedata
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.google_maps import GoogleMapsScraper
from src.scraper.selectors import SELECTOR_PATTERNS


DEFAULT_CATEGORIES = ("hoteles", "hostales", "pensiones")


@dataclass
class DiscoveredPlace:
    query: str
    category: str
    name: str
    maps_url: str
    source_card_label: str | None = None


@dataclass
class EnqueueResult:
    place_name: str
    maps_url: str
    ok: bool
    status_code: int
    response: dict[str, Any] | str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Busca sitios en Google Maps por categorías y lanza jobs de scraping en la API."
        )
    )
    parser.add_argument(
        "--area",
        default="",
        help=(
            "Zona para concatenar en la búsqueda (ej: 'Córdoba'). "
            "Si no se indica, usará solo la categoría."
        ),
    )
    parser.add_argument(
        "--categories",
        default=",".join(DEFAULT_CATEGORIES),
        help=(
            "Categorías separadas por coma (default: hoteles,hostales,pensiones)."
        ),
    )
    parser.add_argument(
        "--queries",
        nargs="*",
        default=None,
        help=(
            "Consultas completas opcionales. Si se pasan, se ignoran --area y --categories."
        ),
    )
    parser.add_argument(
        "--max-results-per-query",
        type=int,
        default=20,
        help="Máximo de resultados a capturar por query (default: 20).",
    )
    parser.add_argument(
        "--max-total-results",
        type=int,
        default=100,
        help="Máximo total tras deduplicar (default: 100).",
    )
    parser.add_argument(
        "--max-scroll-rounds",
        type=int,
        default=40,
        help="Rondas máximas de scroll por query para cargar más resultados (default: 40).",
    )
    parser.add_argument(
        "--scroll-wait-ms",
        type=int,
        default=900,
        help="Espera entre scrolls en ms (default: 900).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Fuerza headless para esta ejecución (por defecto usa config del proyecto).",
    )
    parser.add_argument(
        "--incognito",
        action="store_true",
        help="Fuerza modo incógnito para esta ejecución.",
    )
    parser.add_argument(
        "--profile-dir",
        default=settings.scraper_user_data_dir,
        help=f"Perfil Playwright para Maps (default: {settings.scraper_user_data_dir}).",
    )
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("API_BASE_URL", "http://localhost:8000"),
        help="Base URL de la API (default: http://localhost:8000 o $API_BASE_URL).",
    )
    parser.add_argument(
        "--sources",
        default="google_maps",
        help=(
            "Fuentes para encolar: google_maps | tripadvisor | google_maps,tripadvisor "
            "(default: google_maps)."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Encola con force=true.",
    )
    parser.add_argument(
        "--strategy",
        default=None,
        help="Estrategia de scraping opcional (ej: scroll_copy).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No llama a la API; solo descubre y guarda lista.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta JSON de salida (default: artifacts/google_maps_discovery_<timestamp>.json).",
    )
    return parser.parse_args()


def _normalize_text(text: str) -> str:
    lowered = (text or "").strip().lower()
    normalized = unicodedata.normalize("NFKD", lowered)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _canonicalize_maps_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse.urlsplit(raw)
    except Exception:
        return raw

    # Keep only scheme/netloc/path to improve dedupe stability.
    return urlparse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _normalize_sources(raw: str) -> list[str]:
    items = [part.strip().lower() for part in (raw or "").split(",") if part.strip()]
    allowed = {"google_maps", "tripadvisor"}
    invalid = [item for item in items if item not in allowed]
    if invalid:
        raise ValueError(f"Sources no válidas: {invalid}. Permitidas: google_maps, tripadvisor")
    unique: list[str] = []
    for item in items:
        if item not in unique:
            unique.append(item)
    if not unique:
        raise ValueError("Debe indicar al menos una source en --sources")
    return unique


def _query_from_category(category: str, area: str) -> str:
    cat = category.strip()
    zone = area.strip()
    if not zone:
        return cat
    return f"{cat} en {zone}"


def _build_queries(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.queries:
        return [(query.strip(), "custom") for query in args.queries if query.strip()]

    categories = [item.strip() for item in str(args.categories).split(",") if item.strip()]
    if not categories:
        categories = list(DEFAULT_CATEGORIES)

    return [(_query_from_category(category, args.area), category) for category in categories]


async def _first_visible_from_patterns(scraper: GoogleMapsScraper, key: str):
    for selector in SELECTOR_PATTERNS[key]:
        locator = scraper.page.locator(selector).first
        try:
            if await locator.is_visible():
                return locator
        except Exception:
            continue
    return None


async def _wait_for_results_feed(scraper: GoogleMapsScraper, timeout_ms: int = 15000) -> bool:
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
    while asyncio.get_running_loop().time() < deadline:
        for selector in SELECTOR_PATTERNS["RESULTS_FEED"]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return True
            except Exception:
                continue
        await scraper.page.wait_for_timeout(220)
    return False


async def _search_query(scraper: GoogleMapsScraper, query: str) -> None:
    await scraper._dismiss_google_consent_if_present()
    search_input = await _first_visible_from_patterns(scraper, "SEARCH_INPUT")
    if search_input is None:
        raise RuntimeError("No se encontró el input de búsqueda de Google Maps.")

    await scraper._human_click(search_input)
    await scraper.page.keyboard.press("Control+A")
    await scraper.page.keyboard.press("Backspace")
    await scraper._human_type(search_input, query)
    await scraper.page.wait_for_timeout(300)

    search_button = await _first_visible_from_patterns(scraper, "SEARCH_BUTTON")
    if search_button is None:
        await scraper.page.keyboard.press("Enter")
    else:
        await scraper._human_click(search_button)


async def _collect_visible_results(scraper: GoogleMapsScraper) -> list[dict[str, str | None]]:
    # Extract from left results feed. Uses structural hooks (feed + maps/place anchors)
    # instead of volatile class names.
    raw = await scraper.page.evaluate(
        """
        () => {
          const feed = document.querySelector("div[role='feed']");
          if (!feed) {
            return { found: false, items: [], at_bottom: true };
          }

          const readText = (node) => {
            if (!node || !node.textContent) return "";
            return String(node.textContent).trim();
          };

          const anchors = Array.from(feed.querySelectorAll("a[href*='/maps/place/']"));
          const items = [];

          for (const anchor of anchors) {
            const article =
              anchor.closest("div[role='article']") ||
              anchor.closest("div.Nv2PK") ||
              anchor.parentElement;

            const labelFromAnchor = String(anchor.getAttribute("aria-label") || "").trim();
            const heading =
              article && article.querySelector
                ? article.querySelector("h3, [role='heading'], .qBF1Pd, .fontHeadlineSmall")
                : null;
            const labelFromHeading = readText(heading);
            const labelFromArticle = String(
              article && article.getAttribute ? article.getAttribute("aria-label") || "" : ""
            ).trim();
            const fallbackText = readText(anchor).split("\\n")[0].trim();

            const name = labelFromHeading || labelFromAnchor || labelFromArticle || fallbackText;
            const href = String(anchor.href || "").trim();
            if (!name || !href) continue;

            items.push({
              name: name,
              maps_url: href,
              source_card_label: labelFromArticle || labelFromAnchor || null,
            });
          }

          const atBottom = feed.scrollTop + feed.clientHeight >= feed.scrollHeight - 4;
          return { found: true, items: items, at_bottom: atBottom };
        }
        """
    )
    if not isinstance(raw, dict):
        return []
    items = raw.get("items") or []
    if not isinstance(items, list):
        return []
    out: list[dict[str, str | None]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        maps_url = str(item.get("maps_url") or "").strip()
        if not name or not maps_url:
            continue
        out.append(
            {
                "name": name,
                "maps_url": maps_url,
                "source_card_label": (
                    str(item.get("source_card_label")).strip()
                    if item.get("source_card_label") is not None
                    else None
                ),
            }
        )
    return out


async def _scroll_results_feed(scraper: GoogleMapsScraper) -> None:
    await scraper.page.evaluate(
        """
        () => {
          const feed = document.querySelector("div[role='feed']");
          if (!feed) return;
          const step = Math.max(900, Math.floor(feed.clientHeight * 0.9));
          feed.scrollBy({ top: step, left: 0, behavior: 'auto' });
        }
        """
    )


async def _discover_for_query(
    scraper: GoogleMapsScraper,
    query: str,
    category: str,
    max_results_per_query: int,
    max_scroll_rounds: int,
    scroll_wait_ms: int,
) -> list[DiscoveredPlace]:
    await _search_query(scraper, query)

    found_results = await _wait_for_results_feed(scraper, timeout_ms=16000)
    if not found_results:
        # Fallback: if Maps opened a specific listing directly, capture that one.
        listing_name = ""
        for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    listing_name = (await locator.inner_text()).strip()
                    break
            except Exception:
                continue

        current_url = scraper.page.url
        if listing_name and "/maps/place/" in current_url:
            return [
                DiscoveredPlace(
                    query=query,
                    category=category,
                    name=listing_name,
                    maps_url=current_url,
                    source_card_label=None,
                )
            ]
        return []

    collected: dict[str, DiscoveredPlace] = {}
    stable_rounds = 0

    for _ in range(max(1, max_scroll_rounds)):
        before = len(collected)
        visible_items = await _collect_visible_results(scraper)

        for item in visible_items:
            name = str(item["name"]).strip()
            maps_url = _canonicalize_maps_url(str(item["maps_url"]).strip())
            if not name or not maps_url:
                continue

            normalized_name = _normalize_text(name)
            key = f"{maps_url}|{normalized_name}"
            if key in collected:
                continue

            collected[key] = DiscoveredPlace(
                query=query,
                category=category,
                name=name,
                maps_url=maps_url,
                source_card_label=item.get("source_card_label"),
            )

        if len(collected) >= max_results_per_query:
            break

        if len(collected) == before:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 3:
            break

        await _scroll_results_feed(scraper)
        await scraper.page.wait_for_timeout(max(200, scroll_wait_ms))

    items = list(collected.values())
    return items[: max(1, max_results_per_query)]


def _http_json_request(method: str, url: str, payload: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(url=url, method=method.upper(), data=data, headers=headers)

    try:
        with urlrequest.urlopen(req, timeout=25) as resp:
            status_code = int(resp.status)
            body = resp.read().decode("utf-8", errors="replace")
    except urlerror.HTTPError as exc:
        status_code = int(exc.code)
        body = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return 0, {"error": str(exc)}

    body_str = (body or "").strip()
    if not body_str:
        return status_code, {}
    try:
        return status_code, json.loads(body_str)
    except Exception:
        return status_code, body_str


def _enqueue_one_job(
    api_base_url: str,
    place: DiscoveredPlace,
    sources: list[str],
    force: bool,
    strategy: str | None,
) -> EnqueueResult:
    endpoint = f"{api_base_url.rstrip('/')}/business/scrape/jobs"
    payload: dict[str, Any] = {
        "name": place.name,
        "sources": sources,
        "google_maps_name": place.name,
        "force": bool(force),
    }
    if "tripadvisor" in sources:
        payload["tripadvisor_name"] = place.name
    if strategy:
        payload["strategy"] = strategy

    status_code, body = _http_json_request("POST", endpoint, payload=payload)
    ok = 200 <= status_code < 300
    return EnqueueResult(
        place_name=place.name,
        maps_url=place.maps_url,
        ok=ok,
        status_code=status_code,
        response=body,
    )


async def _discover_places(args: argparse.Namespace) -> list[DiscoveredPlace]:
    queries = _build_queries(args)
    if not queries:
        raise RuntimeError("No hay queries para ejecutar.")

    scraper = GoogleMapsScraper(
        headless=bool(args.headless),
        incognito=bool(args.incognito),
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=args.profile_dir,
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
        reviews_strategy=settings.scraper_reviews_strategy,
    )

    all_places: dict[str, DiscoveredPlace] = {}

    try:
        await scraper.start()
        await scraper._dismiss_google_consent_if_present()

        for query, category in queries:
            print(f"[discover] Query: {query}")
            items = await _discover_for_query(
                scraper=scraper,
                query=query,
                category=category,
                max_results_per_query=max(1, int(args.max_results_per_query)),
                max_scroll_rounds=max(1, int(args.max_scroll_rounds)),
                scroll_wait_ms=max(200, int(args.scroll_wait_ms)),
            )
            print(f"[discover]   resultados capturados: {len(items)}")

            for place in items:
                key = f"{_canonicalize_maps_url(place.maps_url)}|{_normalize_text(place.name)}"
                if key not in all_places:
                    all_places[key] = place

            if len(all_places) >= max(1, int(args.max_total_results)):
                print("[discover] Alcanzado max-total-results. Cortando.")
                break

    finally:
        await scraper.close()

    discovered = list(all_places.values())
    discovered.sort(key=lambda item: (item.category, _normalize_text(item.name)))
    return discovered[: max(1, int(args.max_total_results))]


def _default_output_path() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "artifacts" / f"google_maps_discovery_{ts}.json"


async def _main() -> int:
    args = _parse_args()
    sources = _normalize_sources(args.sources)

    discovered = await _discover_places(args)
    print(f"[discover] Total deduplicado: {len(discovered)}")

    enqueue_results: list[EnqueueResult] = []
    if args.dry_run:
        print("[enqueue] Dry-run activo: no se encolan jobs.")
    else:
        print("[enqueue] Lanzando jobs en API...")
        for idx, place in enumerate(discovered, start=1):
            result = _enqueue_one_job(
                api_base_url=args.api_base_url,
                place=place,
                sources=sources,
                force=bool(args.force),
                strategy=args.strategy,
            )
            enqueue_results.append(result)
            status_word = "OK" if result.ok else "ERROR"
            print(
                f"[enqueue] {idx:03d}/{len(discovered):03d} {status_word} "
                f"{place.name} (status={result.status_code})"
            )

    ok_count = sum(1 for item in enqueue_results if item.ok)
    error_count = sum(1 for item in enqueue_results if not item.ok)

    output_path = Path(args.output).expanduser().resolve() if args.output else _default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    report = {
        "created_at": datetime.now().isoformat(),
        "api_base_url": args.api_base_url,
        "sources": sources,
        "force": bool(args.force),
        "strategy": args.strategy,
        "dry_run": bool(args.dry_run),
        "queries": [{"query": q, "category": c} for q, c in _build_queries(args)],
        "summary": {
            "discovered_count": len(discovered),
            "enqueue_ok": ok_count,
            "enqueue_error": error_count,
        },
        "discovered_places": [asdict(item) for item in discovered],
        "enqueue_results": [asdict(item) for item in enqueue_results],
    }

    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[output] {output_path}")

    if args.dry_run:
        return 0
    return 0 if error_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
