"""
capture_leads.py — Repiq Lead Scraper
======================================
Abre Google Maps con el término buscado, espera a que el usuario haga scroll
completo, extrae los listings mediante page.evaluate() y los guarda en MongoDB.

Uso:
    python scripts/capture_leads.py "restaurante cordoba"
    python scripts/capture_leads.py "hotel sevilla" --min-rating 3.8 --max-rating 4.3
"""

import argparse
import asyncio
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import Page, async_playwright
from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.models.research import ResearchLead, ResearchTerm

# ---------------------------------------------------------------------------
# JS snippet injected into the page via page.evaluate() to extract the feed
# ---------------------------------------------------------------------------
_JS_EXTRACT_FEED = """
() => {
    const feed = document.querySelector('div[role="feed"]');
    if (!feed) return { error: 'feed_not_found', items: [] };

    const articles = [...feed.querySelectorAll('div[role="article"]')];

    const items = articles.map(el => {
        // ── Name ──────────────────────────────────────────────────────────
        const nameEl =
            el.querySelector('.fontHeadlineSmall') ||
            el.querySelector('[aria-label]');
        const name = (
            nameEl?.textContent?.trim() ||
            nameEl?.getAttribute('aria-label') ||
            ''
        );

        // ── Rating ────────────────────────────────────────────────────────
        const ratingEl = el.querySelector('span[role="img"]');
        const ratingLabel = ratingEl?.getAttribute('aria-label') || '';
        const ratingMatch = ratingLabel.match(/[\\d][\\.,][\\d]/);
        const rating = ratingMatch
            ? parseFloat(ratingMatch[0].replace(',', '.'))
            : null;

        // ── Review count ──────────────────────────────────────────────────
        const reviewEl = el.querySelector(
            'span[aria-label*="reseñas"], span[aria-label*="opiniones"], ' +
            'span[aria-label*="reviews"], span[aria-label*="valoraciones"]'
        );
        const reviewLabel = reviewEl?.getAttribute('aria-label') || '';
        const reviewMatch = reviewLabel.match(/([\\d\\.]+)/);
        const review_count = reviewMatch
            ? parseInt(reviewMatch[1].replace(/\\./g, ''), 10)
            : null;

        // ── Category ──────────────────────────────────────────────────────
        // Usually a plain span with no children inside a fontBodyMedium div
        const allText = [...el.querySelectorAll(
            '.fontBodyMedium span, .fontBodySmall span'
        )];
        const category = allText
            .map(s => s.textContent?.trim())
            .filter(t => t && t.length > 2 && t.length < 60 && !/^\\d/.test(t))
            [0] || '';

        // ── Address  (may be visible in some cards) ───────────────────────
        const addrCandidates = [...el.querySelectorAll('span')].map(
            s => s.textContent?.trim()
        ).filter(t =>
            t &&
            (t.includes('Calle') || t.includes('Av') || t.includes('Plaza') ||
             t.includes('C/') || t.match(/^\\d{5}/))
        );
        const address = addrCandidates[0] || null;

        // ── Maps URL ──────────────────────────────────────────────────────
        const linkEl = el.querySelector('a[href*="/maps/place/"]');
        const maps_url = linkEl?.href || null;

        return {
            name,
            rating,
            review_count,
            category,
            address,
            maps_url,
            raw_html: el.outerHTML,
        };
    });

    return { error: null, items };
}
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(text: str) -> str:
    """Convert 'restaurante córdoba' → 'restaurante_cordoba'."""
    text = text.lower().strip()
    text = re.sub(r"[àáâãäå]", "a", text)
    text = re.sub(r"[èéêë]", "e", text)
    text = re.sub(r"[ìíîï]", "i", text)
    text = re.sub(r"[òóôõö]", "o", text)
    text = re.sub(r"[ùúûü]", "u", text)
    text = re.sub(r"ñ", "n", text)
    text = re.sub(r"ç", "c", text)
    text = re.sub(r"[^a-z0-9\s_]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text


def _parse_city_entity(term: str) -> tuple[str | None, str | None]:
    """
    Heuristic: 'restaurante cordoba' → entity='restaurante', city='cordoba'.
    Works with 1-3 word terms where the last word is the city.
    """
    parts = term.strip().lower().split()
    if len(parts) >= 2:
        return parts[-1].capitalize(), " ".join(parts[:-1]).capitalize()
    return None, None


def _process_item(
    raw: dict,
    term_id: str,
    term_key: str,
    min_rating: float,
    max_rating: float,
) -> ResearchLead:
    """Convert a raw JS-extracted dict into a normalised ResearchLead."""
    rating = raw.get("rating")
    in_range = (
        rating is not None and min_rating <= rating <= max_rating
    )

    return ResearchLead(
        listing_id=str(uuid.uuid4()),
        term_id=term_id,
        term_key=term_key,
        name=raw.get("name") or None,
        rating=rating,
        review_count=raw.get("review_count"),
        category=raw.get("category") or None,
        address=raw.get("address"),
        maps_url=raw.get("maps_url"),
        in_range=in_range,
        raw_html=raw.get("raw_html"),
        processed_at=datetime.now(timezone.utc),
    )


def _save_to_mongo(term: ResearchTerm, leads: list[ResearchLead]) -> None:
    """Persist research term + leads to MongoDB (sync)."""
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]

    term_doc = term.model_dump()
    term_doc["captured_at"] = term_doc["captured_at"].isoformat()
    db["research_terms"].insert_one(term_doc)

    if leads:
        lead_docs = []
        for lead in leads:
            doc = lead.model_dump()
            doc["processed_at"] = doc["processed_at"].isoformat()
            lead_docs.append(doc)
        db["research_leads"].insert_many(lead_docs)

    client.close()


# ---------------------------------------------------------------------------
# Browser interaction
# ---------------------------------------------------------------------------

async def _wait_for_enter(prompt: str) -> None:
    """Non-blocking async wait for the user to press Enter in the terminal."""
    loop = asyncio.get_event_loop()
    print(prompt, flush=True)
    await loop.run_in_executor(None, input)


async def _open_maps_and_capture(
    term: str,
    term_id: str,
    term_key: str,
    min_rating: float,
    max_rating: float,
) -> list[ResearchLead]:
    """
    Open Chromium, navigate to Google Maps search, wait for user to scroll,
    then extract listings via JS and return processed leads.
    """
    maps_url = f"https://www.google.com/maps/search/{quote_plus(term)}?hl=es"

    async with async_playwright() as pw:
        # Reuse the persistent profile so the user is already logged in
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=settings.scraper_user_data_dir,
            headless=False,
            slow_mo=settings.scraper_slow_mo_ms,
            channel=settings.scraper_browser_channel or None,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            viewport={"width": 1280, "height": 900},
        )

        page: Page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=30_000)

        print(f"\n  URL: {maps_url}")
        print("\n──────────────────────────────────────────────────")
        print("  Google Maps abierto. Haz scroll hasta cargar")
        print("  todos los resultados del panel lateral.")
        print("  Cuando hayas terminado, vuelve aquí y pulsa Enter.")
        print("──────────────────────────────────────────────────")

        await _wait_for_enter("")

        print("  Extrayendo listings…", end=" ", flush=True)
        result = await page.evaluate(_JS_EXTRACT_FEED)

        if result.get("error") == "feed_not_found":
            print("ERROR")
            print(
                "\n  No se encontró el feed de resultados.\n"
                "  Asegúrate de que la búsqueda ha cargado el panel lateral con resultados."
            )
            await context.close()
            return []

        raw_items: list[dict] = result.get("items", [])
        print(f"encontrados {len(raw_items)} listings.")

        leads = [
            _process_item(item, term_id, term_key, min_rating, max_rating)
            for item in raw_items
            if item.get("name")  # skip blank cards
        ]

        await context.close()
        return leads


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Captura listings de Google Maps y los guarda en MongoDB."
    )
    parser.add_argument(
        "term",
        help='Término de búsqueda, e.g. "restaurante cordoba"',
    )
    parser.add_argument(
        "--min-rating",
        type=float,
        default=3.6,
        help="Rating mínimo para marcar un lead como in_range (default: 3.6)",
    )
    parser.add_argument(
        "--max-rating",
        type=float,
        default=4.2,
        help="Rating máximo para marcar un lead como in_range (default: 4.2)",
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    term = args.term.strip()
    term_id = str(uuid.uuid4())
    term_key = _slugify(term)
    city, entity_type = _parse_city_entity(term)

    print(f"\n  Término    : {term}")
    print(f"  term_key   : {term_key}")
    print(f"  Ciudad     : {city or '(no detectada)'}")
    print(f"  Tipo       : {entity_type or '(no detectado)'}")
    print(f"  Rango      : {args.min_rating} ≤ rating ≤ {args.max_rating}")
    print(f"  MongoDB    : {settings.mongo_uri} / {settings.db_name}")

    leads = await _open_maps_and_capture(
        term=term,
        term_id=term_id,
        term_key=term_key,
        min_rating=args.min_rating,
        max_rating=args.max_rating,
    )

    if not leads:
        print("\n  No se capturaron leads. Saliendo sin guardar.\n")
        return

    in_range_count = sum(1 for l in leads if l.in_range)
    out_of_range = len(leads) - in_range_count

    term_record = ResearchTerm(
        term_id=term_id,
        term_key=term_key,
        original_term=term,
        city=city,
        entity_type=entity_type,
        captured_at=datetime.now(timezone.utc),
        listing_count=len(leads),
    )

    print(f"\n  Guardando en MongoDB…", end=" ", flush=True)
    _save_to_mongo(term_record, leads)
    print("OK")

    print("\n──────────────────────────────────────────────────")
    print(f"  Total capturados : {len(leads)}")
    print(f"  In range         : {in_range_count}  ✓")
    print(f"  Fuera de rango   : {out_of_range}")
    print(f"  Colecciones      : research_terms, research_leads")
    print("──────────────────────────────────────────────────\n")


if __name__ == "__main__":
    asyncio.run(_main())
