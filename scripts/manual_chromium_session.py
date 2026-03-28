import argparse
import asyncio
import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

if sys.platform == "win32":
    import msvcrt
else:
    msvcrt = None  # type: ignore[assignment]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.tripadvisor import TripadvisorScraper


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open a manual Chromium session and keep it alive until you close the browser."
    )
    parser.add_argument(
        "--url",
        default=settings.scraper_maps_url,
        help=f"Initial URL to open (default: {settings.scraper_maps_url}).",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--persistent",
        action="store_true",
        help="Use persistent profile.",
    )
    mode_group.add_argument(
        "--incognito",
        action="store_true",
        help="Force incognito context.",
    )
    parser.add_argument(
        "--profile-dir",
        default=settings.scraper_user_data_dir,
        help=f"Profile directory for persistent mode (default: {settings.scraper_user_data_dir}).",
    )
    parser.add_argument(
        "--channel",
        default=settings.scraper_browser_channel,
        help="Browser channel override: msedge | chrome | chromium (empty = bundled Chromium).",
    )
    parser.add_argument(
        "--executable-path",
        default="",
        help="Explicit browser executable path (if set, channel is ignored).",
    )
    parser.add_argument(
        "--no-stealth",
        action="store_true",
        help="Disable stealth init script.",
    )
    parser.add_argument(
        "--user-agent",
        default="",
        help="Optional explicit user-agent. Empty means use browser default.",
    )
    parser.add_argument(
        "--tripadvisor-query",
        default="banos arabes de cordoba",
        help=(
            "If URL is Tripadvisor, run search flow with this query "
            "(default: banos arabes de cordoba)."
        ),
    )
    parser.add_argument(
        "--tripadvisor-start-delay-seconds",
        type=float,
        default=0.0,
        help="Delay in seconds before running Tripadvisor flow (default: 0).",
    )
    parser.add_argument(
        "--tripadvisor-start-delay-min-seconds",
        type=float,
        default=None,
        help="Optional minimum delay (seconds) for randomized flow start.",
    )
    parser.add_argument(
        "--tripadvisor-start-delay-max-seconds",
        type=float,
        default=None,
        help="Optional maximum delay (seconds) for randomized flow start.",
    )
    parser.add_argument(
        "--no-tripadvisor-flow",
        action="store_true",
        help="Disable automatic Tripadvisor search flow when opening a Tripadvisor URL.",
    )
    parser.add_argument(
        "--tripadvisor-trigger",
        choices=("auto", "manual"),
        default="auto",
        help=(
            "Tripadvisor flow trigger mode: auto (starts on Tripadvisor URL) "
            "or manual (press 's' in terminal to trigger)."
        ),
    )
    parser.add_argument(
        "--max-pages",
        "--tripadvisor-reviews-pages",
        dest="tripadvisor_reviews_pages",
        type=int,
        default=1,
        help=(
            "Maximum Tripadvisor review pages to process after opening listing "
            "(default: 1, set 0 = all pages)."
        ),
    )
    parser.add_argument(
        "--tripadvisor-pages-percent",
        type=float,
        default=None,
        help=(
            "Optional reviews percentage cap based on detected total pages "
            "(0 < value <= 100)."
        ),
    )
    parser.add_argument(
        "--skip-tripadvisor-reviews",
        action="store_true",
        help="Skip Tripadvisor reviews extraction after reaching listing.",
    )
    parser.add_argument(
        "--exit-after-tripadvisor-flow",
        action="store_true",
        help="Exit session automatically once Tripadvisor flow finishes (success or failure).",
    )
    parser.add_argument(
        "--tripadvisor-output-json",
        default="",
        help=(
            "Optional path to write extracted Tripadvisor listing/reviews as JSON. "
            "Useful for live commit flows."
        ),
    )
    return parser.parse_args()


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if "://" in cleaned:
        return cleaned
    return f"https://{cleaned}"


def _looks_like_tripadvisor_url(url: str) -> bool:
    hostname = (urlparse(_normalize_url(url)).hostname or "").lower()
    return "tripadvisor." in hostname


def _is_tripadvisor_context(*urls: str) -> bool:
    return any(_looks_like_tripadvisor_url(item) for item in urls if str(item or "").strip())


def _resolve_effective_start_delay_seconds(
    *,
    fixed_seconds: float,
    min_seconds: float | None,
    max_seconds: float | None,
) -> float:
    fixed = max(0.0, float(fixed_seconds))
    if min_seconds is None and max_seconds is None:
        return fixed

    lower = fixed if min_seconds is None else max(0.0, float(min_seconds))
    upper = fixed if max_seconds is None else max(0.0, float(max_seconds))
    if upper < lower:
        lower, upper = upper, lower
    if abs(upper - lower) < 1e-9:
        return lower
    return random.uniform(lower, upper)


def _tripadvisor_base_url(url: str) -> str:
    parsed = urlparse(_normalize_url(url))
    scheme = parsed.scheme or "https"
    return f"{scheme}://{parsed.netloc}"


def _poll_manual_trigger_keys() -> tuple[bool, bool]:
    """
    Poll terminal keys in Windows without blocking.
    Returns (start_requested, quit_requested).
    """
    if msvcrt is None:
        return False, False

    start_requested = False
    quit_requested = False
    while msvcrt.kbhit():
        key = msvcrt.getwch().lower()
        if key == "s":
            start_requested = True
        elif key == "q":
            quit_requested = True
    return start_requested, quit_requested


def _pick_runtime_page(*, context: BrowserContext, fallback_page: Page) -> Page:
    pages = [item for item in context.pages if not item.is_closed()]
    if not pages:
        return fallback_page

    # Prefer any tab already on Tripadvisor.
    for candidate in reversed(pages):
        if _looks_like_tripadvisor_url(candidate.url):
            return candidate

    if fallback_page in pages:
        return fallback_page
    return pages[-1]


def _resolve_user_data_dir(user_data_dir: str) -> Path:
    path = Path(user_data_dir).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _build_chromium_args(headless: bool) -> list[str]:
    args = [
        "--disable-blink-features=AutomationControlled",
        "--deny-permission-prompts",
        "--disable-geolocation",
        "--window-size=1920,1080",
        "--lang=es-ES",
    ]
    if headless and settings.scraper_harden_headless:
        args.extend(
            [
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]
        )
    args.extend(settings.scraper_extra_chromium_args)

    deduped: list[str] = []
    seen: set[str] = set()
    for arg in args:
        cleaned = str(arg or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def _stealth_init_script() -> str:
    return """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'languages', { get: () => ['es-ES', 'es', 'en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4] });
        Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
        window.chrome = window.chrome || { runtime: {} };
    """


def _block_geolocation_init_script() -> str:
    return """
        (() => {
            const deniedError = {
                code: 1,
                message: 'User denied Geolocation',
                PERMISSION_DENIED: 1,
                POSITION_UNAVAILABLE: 2,
                TIMEOUT: 3
            };

            if (navigator.geolocation) {
                navigator.geolocation.getCurrentPosition = (_, error) => {
                    if (typeof error === 'function') error(deniedError);
                };
                navigator.geolocation.watchPosition = (_, error) => {
                    if (typeof error === 'function') error(deniedError);
                    return -1;
                };
                navigator.geolocation.clearWatch = () => {};
            }

            if (navigator.permissions && navigator.permissions.query) {
                const originalQuery = navigator.permissions.query.bind(navigator.permissions);
                navigator.permissions.query = (params) => {
                    if (params && params.name === 'geolocation') {
                        return Promise.resolve({ state: 'denied', onchange: null });
                    }
                    return originalQuery(params);
                };
            }
        })();
    """


async def _launch_incognito_context(
    playwright: Playwright,
    *,
    channel: str | None,
    executable_path: str | None,
    user_agent: str | None,
) -> tuple[Browser, BrowserContext]:
    launch_options = {
        "headless": False,
        "slow_mo": settings.scraper_slow_mo_ms,
        "args": _build_chromium_args(headless=False),
    }
    if executable_path:
        launch_options["executable_path"] = executable_path
    elif channel:
        launch_options["channel"] = channel

    try:
        browser = await playwright.chromium.launch(**launch_options)
    except Exception:
        if not channel or executable_path:
            raise
        launch_options.pop("channel", None)
        browser = await playwright.chromium.launch(**launch_options)

    context_options: dict[str, object] = {
        "viewport": {"width": 1366, "height": 900},
        "locale": "es-ES",
        "timezone_id": "Europe/Madrid",
    }
    if user_agent:
        context_options["user_agent"] = user_agent
    context = await browser.new_context(**context_options)
    return browser, context


async def _launch_persistent_context(
    playwright: Playwright,
    *,
    channel: str | None,
    executable_path: str | None,
    profile_dir: Path,
    user_agent: str | None,
) -> tuple[Browser | None, BrowserContext]:
    launch_options: dict[str, object] = {
        "user_data_dir": str(profile_dir),
        "headless": False,
        "slow_mo": settings.scraper_slow_mo_ms,
        "viewport": {"width": 1366, "height": 900},
        "locale": "es-ES",
        "timezone_id": "Europe/Madrid",
        "args": _build_chromium_args(headless=False),
    }
    if user_agent:
        launch_options["user_agent"] = user_agent
    if executable_path:
        launch_options["executable_path"] = executable_path
    elif channel:
        launch_options["channel"] = channel

    try:
        context = await playwright.chromium.launch_persistent_context(**launch_options)
    except Exception:
        if not channel or executable_path:
            raise
        launch_options.pop("channel", None)
        context = await playwright.chromium.launch_persistent_context(**launch_options)

    return context.browser, context


async def _run_tripadvisor_search_flow(
    page: Page,
    query: str,
    source_url: str,
    *,
    reviews_pages: int,
    reviews_pages_percent: float | None,
    skip_reviews: bool,
    start_delay_seconds: float,
) -> dict[str, object]:
    flow_started_at = monotonic()
    current_stage = "init"
    stage_durations: dict[str, float] = {}
    listing_payload: dict[str, object] = {}
    reviews_payload: list[dict[str, object]] = []

    def _log_stage_done(stage_key: str, label: str, started_at: float) -> None:
        duration_s = max(0.0, monotonic() - started_at)
        stage_durations[stage_key] = duration_s
        total_s = max(0.0, monotonic() - flow_started_at)
        print(
            f"[tripadvisor-timing] etapa={label} "
            f"duracion={duration_s:.2f}s total={total_s:.2f}s"
        )

    print(f"Tripadvisor flow: searching '{query}'")
    try:
        delay_s = max(0.0, float(start_delay_seconds))
        if delay_s > 0:
            current_stage = "start_delay"
            stage_started_at = monotonic()
            print(f"Tripadvisor flow: waiting {delay_s:.1f}s before starting...")
            await asyncio.sleep(delay_s)
            _log_stage_done("start_delay", "espera_inicial", stage_started_at)

        tripadvisor_scraper = TripadvisorScraper(
            page=page,
            tripadvisor_url=_tripadvisor_base_url(source_url),
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
        )

        current_stage = "search_business"
        search_started_at = monotonic()

        def _search_progress_logger(payload: dict[str, object]) -> None:
            event = str(payload.get("event", "unknown"))
            step = str(payload.get("step", "") or "-")
            elapsed_step_s = float(payload.get("elapsed_step_s") or 0.0)
            elapsed_total_s = float(payload.get("elapsed_total_s") or 0.0)
            if event.startswith("tripadvisor_search_"):
                print(
                    "[tripadvisor-timing] "
                    f"etapa=buscar step={step} event={event} "
                    f"paso={elapsed_step_s:.2f}s total_busqueda={elapsed_total_s:.2f}s"
                )

        await tripadvisor_scraper.search_business(query, progress_callback=_search_progress_logger)
        _log_stage_done("search_business", "escribir_busqueda_y_abrir_ficha", search_started_at)

        current_stage = "extract_listing"
        listing_started_at = monotonic()
        listing = await tripadvisor_scraper.extract_listing()
        listing_payload = dict(listing)
        _log_stage_done("extract_listing", "extraer_listing", listing_started_at)
        print(
            "Tripadvisor flow: listing detectado "
            f"nombre='{listing.get('business_name') or ''}' "
            f"total_reviews={listing.get('total_reviews')}"
        )

        if not skip_reviews:
            current_stage = "extract_reviews"
            reviews_started_at = monotonic()
            last_reviews_progress_at = reviews_started_at

            def _progress_logger(payload: dict[str, object]) -> None:
                nonlocal last_reviews_progress_at
                event = str(payload.get("event", "unknown"))
                total_unique = payload.get("total_unique_reviews")
                now = monotonic()
                elapsed_reviews_s = max(0.0, now - reviews_started_at)
                delta_s = max(0.0, now - last_reviews_progress_at)

                if event == "tripadvisor_reviews_started":
                    current = payload.get("current_page")
                    total_pages = payload.get("total_pages")
                    range_start = payload.get("range_start")
                    range_end = payload.get("range_end")
                    total_results = payload.get("total_results")
                    print(
                        "[tripadvisor-progress] start "
                        f"pagina={current or '?'}"
                        f"/{total_pages or '?'} "
                        f"rango={range_start or '?'}-{range_end or '?'} "
                        f"de {total_results or '?'} "
                        f"t_total={elapsed_reviews_s:.2f}s t_delta={delta_s:.2f}s"
                    )
                    last_reviews_progress_at = now
                    return

                if event == "tripadvisor_reviews_page_collected":
                    loop_page = payload.get("page")
                    current = payload.get("current_page")
                    total_pages = payload.get("total_pages")
                    remaining = payload.get("remaining_pages")
                    items_in_page = payload.get("items_in_page")
                    added = payload.get("added_to_total")
                    print(
                        "[tripadvisor-progress] "
                        f"pagina_loop={loop_page} "
                        f"pagina_real={current or '?'}"
                        f"/{total_pages or '?'} "
                        f"restantes={remaining if remaining is not None else '?'} "
                        f"items_pagina={items_in_page} "
                        f"nuevas={added} "
                        f"total_unicas={total_unique} "
                        f"t_total={elapsed_reviews_s:.2f}s t_delta={delta_s:.2f}s"
                    )
                    last_reviews_progress_at = now
                    return

                if event == "tripadvisor_reviews_end_of_pagination":
                    current = payload.get("current_page")
                    total_pages = payload.get("total_pages")
                    print(
                        "[tripadvisor-progress] fin_paginacion "
                        f"pagina={current or '?'}"
                        f"/{total_pages or '?'} "
                        f"total_unicas={total_unique} "
                        f"t_total={elapsed_reviews_s:.2f}s t_delta={delta_s:.2f}s"
                    )
                    last_reviews_progress_at = now
                    return

                if event == "tripadvisor_reviews_completed":
                    print(
                        "[tripadvisor-progress] completado "
                        f"total_unicas={total_unique} "
                        f"t_total={elapsed_reviews_s:.2f}s t_delta={delta_s:.2f}s"
                    )
                    last_reviews_progress_at = now
                    return

                print(
                    "[tripadvisor-progress] "
                    f"event={event} total_unicas={total_unique} "
                    f"t_total={elapsed_reviews_s:.2f}s t_delta={delta_s:.2f}s"
                )
                last_reviews_progress_at = now

            if reviews_pages > 0:
                max_pages = max(1, int(reviews_pages))
                max_rounds = max_pages
                pages_label = str(max_pages)
            else:
                max_pages = None
                max_rounds = 1000
                pages_label = "all"
            if reviews_pages_percent is not None:
                pages_label = f"{pages_label} ({float(reviews_pages_percent):.1f}% max)"

            print(f"Tripadvisor flow: opening reviews (pages={pages_label})...")
            reviews = await tripadvisor_scraper.extract_reviews(
                max_pages=max_pages,
                max_rounds=max_rounds,
                max_pages_percent=reviews_pages_percent,
                progress_callback=_progress_logger,
            )
            reviews_payload = [dict(item) for item in reviews if isinstance(item, dict)]
            _log_stage_done("extract_reviews", "extraer_reviews", reviews_started_at)
            _print_reviews(reviews)
        else:
            print("Tripadvisor flow: skip reviews enabled.")

        total_flow_s = max(0.0, monotonic() - flow_started_at)
        print("[tripadvisor-timing] resumen_etapas")
        for key in ("start_delay", "search_business", "extract_listing", "extract_reviews"):
            if key in stage_durations:
                print(f"[tripadvisor-timing] {key}={stage_durations[key]:.2f}s")
        print(f"[tripadvisor-timing] total={total_flow_s:.2f}s")
        print("Tripadvisor flow completed.")
        return {
            "success": True,
            "listing": listing_payload,
            "reviews": reviews_payload,
            "stage_durations": stage_durations,
            "error": None,
        }
    except Exception as exc:
        total_flow_s = max(0.0, monotonic() - flow_started_at)
        print(
            "Tripadvisor flow failed: "
            f"stage={current_stage} elapsed={total_flow_s:.2f}s error={exc!r}"
        )
        if stage_durations:
            for key, value in stage_durations.items():
                print(f"[tripadvisor-timing] parcial {key}={value:.2f}s")
        return {
            "success": False,
            "listing": listing_payload,
            "reviews": reviews_payload,
            "stage_durations": stage_durations,
            "error": str(exc),
            "failed_stage": current_stage,
        }


def _write_tripadvisor_capture_json(
    *,
    output_path: str,
    query: str,
    success: bool,
    listing: dict[str, object],
    reviews: list[dict[str, object]],
    stage_durations: dict[str, float] | None = None,
    error: str | None = None,
) -> None:
    destination = Path(output_path).expanduser()
    if not destination.is_absolute():
        destination = (PROJECT_ROOT / destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "query": str(query or "").strip(),
        "success": bool(success),
        "listing": listing if isinstance(listing, dict) else {},
        "reviews": reviews if isinstance(reviews, list) else [],
        "review_count": len(reviews) if isinstance(reviews, list) else 0,
        "stage_durations": stage_durations if isinstance(stage_durations, dict) else {},
        "error": str(error or "").strip() or None,
    }
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Tripadvisor flow: capture JSON saved at {destination}")


def _print_reviews(reviews: list[dict[str, object]]) -> None:
    total = len(reviews)
    print(f"Tripadvisor flow: reseñas obtenidas={total}")
    if total == 0:
        return

    for idx, review in enumerate(reviews, start=1):
        review_id = str(review.get("review_id") or "")
        author = str(review.get("author_name") or "")
        rating = str(review.get("rating") or "")
        relative_time = str(review.get("relative_time") or "")
        written_date = str(review.get("written_date") or "")
        title = str(review.get("review_title") or "")
        text = str(review.get("text") or "").replace("\n", " ").strip()
        owner_reply = review.get("owner_reply")
        owner_reply_text = ""
        owner_reply_date = ""
        owner_reply_author = str(review.get("owner_reply_author_name") or "")
        if isinstance(owner_reply, dict):
            owner_reply_text = str(owner_reply.get("text") or "").replace("\n", " ").strip()
            owner_reply_date = str(
                owner_reply.get("relative_time")
                or review.get("owner_reply_written_date")
                or ""
            ).strip()
        elif isinstance(owner_reply, str):
            owner_reply_text = owner_reply.replace("\n", " ").strip()
            owner_reply_date = str(review.get("owner_reply_written_date") or "").strip()

        print(
            f"[review {idx}/{total}] "
            f"id={review_id} author={author} rating={rating} "
            f"relative_time={relative_time} written_date={written_date}"
        )
        if title:
            print(f"  titulo: {title}")
        if text:
            print(f"  texto: {text}")
        if owner_reply_text:
            print(
                "  respuesta_dueno: "
                f"{owner_reply_text}"
            )
            if owner_reply_author:
                print(f"  respuesta_dueno_autor: {owner_reply_author}")
            if owner_reply_date:
                print(f"  respuesta_dueno_fecha: {owner_reply_date}")
        print("-" * 100)


async def main() -> None:
    args = _parse_args()
    channel = (args.channel or "").strip() or None
    executable_path = (args.executable_path or "").strip() or None
    user_agent = (args.user_agent or "").strip() or None
    use_stealth = not args.no_stealth
    target_url = _normalize_url(args.url)
    use_persistent = bool(args.persistent) and not bool(args.incognito)

    playwright: Playwright | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None
    tripadvisor_flow_started = False
    tripadvisor_flow_finished = False
    tripadvisor_flow_completed = False
    live_exit_reason: str | None = None
    manual_trigger_requested = False
    manual_input_task: asyncio.Task[str] | None = None
    auto_start_due_at: float | None = None
    auto_start_delay_seconds = 0.0
    tripadvisor_flow_capture: dict[str, object] | None = None
    capture_json_written = False
    output_capture_path = str(args.tripadvisor_output_json or "").strip()

    try:
        playwright = await async_playwright().start()

        if use_persistent:
            profile_dir = _resolve_user_data_dir(args.profile_dir)
            browser, context = await _launch_persistent_context(
                playwright,
                channel=channel,
                executable_path=executable_path,
                profile_dir=profile_dir,
                user_agent=user_agent,
            )
            print(f"Mode: persistent profile ({profile_dir})")
        else:
            browser, context = await _launch_incognito_context(
                playwright,
                channel=channel,
                executable_path=executable_path,
                user_agent=user_agent,
            )
            print("Mode: incognito (fresh context, no saved cookies).")

        if use_stealth:
            await context.add_init_script(_stealth_init_script())
        await context.add_init_script(_block_geolocation_init_script())

        context.set_default_timeout(settings.scraper_timeout_ms)
        page = context.pages[0] if context.pages else await context.new_page()
        print(f"Opening URL: {target_url}")
        await page.goto(target_url, wait_until="domcontentloaded")

        tripadvisor_enabled = not args.no_tripadvisor_flow
        manual_trigger_mode = args.tripadvisor_trigger == "manual"

        if tripadvisor_enabled:
            if manual_trigger_mode:
                print("Tripadvisor manual-flow is enabled.")
                print("Manual trigger: start Tripadvisor flow from terminal.")
                if msvcrt is None:
                    print("Linux/macOS mode: press Enter to trigger, or type 'q' + Enter to quit.")
                    manual_input_task = asyncio.create_task(asyncio.to_thread(input, ""))
                else:
                    print("Windows mode: press 's' to trigger, press 'q' to quit.")
            else:
                print("Tripadvisor auto-flow is enabled.")
                query = (args.tripadvisor_query or "").strip()
                if query:
                    auto_start_delay_seconds = _resolve_effective_start_delay_seconds(
                        fixed_seconds=args.tripadvisor_start_delay_seconds,
                        min_seconds=args.tripadvisor_start_delay_min_seconds,
                        max_seconds=args.tripadvisor_start_delay_max_seconds,
                    )
                    auto_start_due_at = monotonic() + auto_start_delay_seconds
                    print(
                        "Auto-flow armed. "
                        f"It will start in {auto_start_delay_seconds:.1f}s (independent of URL condition)."
                    )
                else:
                    print("Tripadvisor auto-flow disabled because --tripadvisor-query is empty.")

        print(f"Opened: {page.url}")
        print("Interact manually. This script will exit only when you close the browser window.")

        if browser is None:
            browser = context.browser

        if browser is not None:
            last_status_log = monotonic()
            while browser.is_connected():
                runtime_pages = [item for item in context.pages if not item.is_closed()]
                if not runtime_pages:
                    live_exit_reason = "window-close"
                    break
                page = _pick_runtime_page(context=context, fallback_page=page)

                if tripadvisor_enabled and manual_trigger_mode and not tripadvisor_flow_started:
                    if msvcrt is None:
                        if manual_input_task is not None and manual_input_task.done():
                            try:
                                manual_text = str(manual_input_task.result()).strip().lower()
                            except Exception:
                                manual_text = ""
                            manual_input_task = asyncio.create_task(asyncio.to_thread(input, ""))
                            if manual_text == "q":
                                live_exit_reason = "manual-quit"
                                print("Manual quit requested from terminal input.")
                                break
                            manual_trigger_requested = True
                            print("Manual trigger received. Starting Tripadvisor flow when URL is Tripadvisor.")
                    else:
                        start_requested, quit_requested = _poll_manual_trigger_keys()
                        if quit_requested:
                            live_exit_reason = "manual-quit"
                            print("Manual quit requested from terminal key 'q'.")
                            break
                        if start_requested:
                            manual_trigger_requested = True
                            print("Manual trigger received. Starting Tripadvisor flow when URL is Tripadvisor.")

                if (
                    tripadvisor_enabled
                    and manual_trigger_mode
                    and manual_trigger_requested
                    and not tripadvisor_flow_started
                    and _is_tripadvisor_context(page.url, target_url)
                ):
                    query = (args.tripadvisor_query or "").strip()
                    if query:
                        tripadvisor_flow_started = True
                        print("Tripadvisor detected after manual trigger. Starting flow now...")
                        tripadvisor_flow_capture = await _run_tripadvisor_search_flow(
                            page,
                            query,
                            page.url,
                            reviews_pages=args.tripadvisor_reviews_pages,
                            reviews_pages_percent=args.tripadvisor_pages_percent,
                            skip_reviews=args.skip_tripadvisor_reviews,
                            start_delay_seconds=args.tripadvisor_start_delay_seconds,
                        )
                        tripadvisor_flow_completed = bool(tripadvisor_flow_capture.get("success"))
                        tripadvisor_flow_finished = True

                # Auto mode: start on timer, independent of active URL.
                if (
                    tripadvisor_enabled
                    and not manual_trigger_mode
                    and not tripadvisor_flow_started
                    and auto_start_due_at is not None
                    and monotonic() >= auto_start_due_at
                ):
                    query = (args.tripadvisor_query or "").strip()
                    if query:
                        tripadvisor_flow_started = True
                        print("Tripadvisor auto timer reached. Starting flow now...")
                        tripadvisor_flow_capture = await _run_tripadvisor_search_flow(
                            page,
                            query,
                            page.url if str(page.url or "").strip() else target_url,
                            reviews_pages=args.tripadvisor_reviews_pages,
                            reviews_pages_percent=args.tripadvisor_pages_percent,
                            skip_reviews=args.skip_tripadvisor_reviews,
                            start_delay_seconds=0.0,
                        )
                        tripadvisor_flow_completed = bool(tripadvisor_flow_capture.get("success"))
                        tripadvisor_flow_finished = True

                if (
                    output_capture_path
                    and tripadvisor_flow_finished
                    and not capture_json_written
                    and isinstance(tripadvisor_flow_capture, dict)
                ):
                    _write_tripadvisor_capture_json(
                        output_path=output_capture_path,
                        query=str(args.tripadvisor_query or "").strip(),
                        success=bool(tripadvisor_flow_capture.get("success")),
                        listing=(
                            tripadvisor_flow_capture.get("listing")
                            if isinstance(tripadvisor_flow_capture.get("listing"), dict)
                            else {}
                        ),
                        reviews=(
                            tripadvisor_flow_capture.get("reviews")
                            if isinstance(tripadvisor_flow_capture.get("reviews"), list)
                            else []
                        ),
                        stage_durations=(
                            tripadvisor_flow_capture.get("stage_durations")
                            if isinstance(tripadvisor_flow_capture.get("stage_durations"), dict)
                            else {}
                        ),
                        error=(
                            str(tripadvisor_flow_capture.get("error") or "").strip() or None
                        ),
                    )
                    capture_json_written = True

                if args.exit_after_tripadvisor_flow and tripadvisor_flow_finished:
                    if tripadvisor_flow_completed:
                        live_exit_reason = "tripadvisor-flow-complete"
                        print("Tripadvisor flow completed. Closing live session automatically.")
                    else:
                        live_exit_reason = "tripadvisor-flow-failed"
                        print("Tripadvisor flow failed. Closing live session automatically.")
                    break
                now = monotonic()
                if now - last_status_log >= 10:
                    if tripadvisor_enabled and not tripadvisor_flow_started and manual_trigger_mode:
                        if manual_trigger_requested:
                            print("Manual trigger is armed. Navigate to Tripadvisor URL to run flow...")
                        else:
                            if msvcrt is None:
                                print("Waiting manual trigger in terminal (Enter to start, q to quit)...")
                            else:
                                print("Waiting manual trigger key 's' in terminal...")
                    elif tripadvisor_enabled and not tripadvisor_flow_started and not manual_trigger_mode:
                        if auto_start_due_at is None:
                            print("Auto-flow is enabled but not armed (empty query).")
                        else:
                            remaining = max(0.0, auto_start_due_at - now)
                            print(
                                "Waiting auto timer to start Tripadvisor flow... "
                                f"(remaining={remaining:.1f}s current={page.url})"
                            )
                    elif tripadvisor_flow_started and not tripadvisor_flow_finished:
                        print("Tripadvisor flow started. Waiting for completion...")
                    elif tripadvisor_flow_finished and not tripadvisor_flow_completed:
                        print("Tripadvisor flow ended with failure. Close browser to finish session.")
                    elif tripadvisor_flow_finished and tripadvisor_flow_completed:
                        print("Tripadvisor flow completed. Close browser to finish session.")
                    last_status_log = now
                await asyncio.sleep(0.75)
            if live_exit_reason is None and not browser.is_connected():
                live_exit_reason = "window-close"
        else:
            # Fallback: keep process alive until interrupted.
            while True:
                await asyncio.sleep(1.0)
    finally:
        if manual_input_task is not None:
            manual_input_task.cancel()
            try:
                await manual_input_task
            except Exception:
                pass
        try:
            if context is not None:
                await context.close()
        except Exception:
            pass
        if playwright is not None:
            await playwright.stop()
        if live_exit_reason:
            print(f"[live-session-exit] reason={live_exit_reason}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[live-session-exit] reason=keyboard-interrupt")
        print("Session interrupted by user.")

