import argparse
import asyncio
import sys
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
    parser.add_argument(
        "--persistent",
        action="store_true",
        help="Use persistent profile (default is incognito context).",
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
        "--tripadvisor-query",
        default="banos arabes de cordoba",
        help=(
            "If URL is Tripadvisor, run search flow with this query "
            "(default: banos arabes de cordoba)."
        ),
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
        "--skip-tripadvisor-reviews",
        action="store_true",
        help="Skip Tripadvisor reviews extraction after reaching listing.",
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
    playwright: Playwright, *, channel: str | None, executable_path: str | None
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

    context = await browser.new_context(
        viewport={"width": 1366, "height": 900},
        locale="es-ES",
        timezone_id="Europe/Madrid",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        ),
    )
    return browser, context


async def _launch_persistent_context(
    playwright: Playwright, *, channel: str | None, executable_path: str | None, profile_dir: Path
) -> tuple[Browser | None, BrowserContext]:
    launch_options = {
        "user_data_dir": str(profile_dir),
        "headless": False,
        "slow_mo": settings.scraper_slow_mo_ms,
        "viewport": {"width": 1366, "height": 900},
        "locale": "es-ES",
        "timezone_id": "Europe/Madrid",
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        ),
        "args": _build_chromium_args(headless=False),
    }
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
    skip_reviews: bool,
) -> bool:
    print(f"Tripadvisor flow: searching '{query}'")
    try:
        tripadvisor_scraper = TripadvisorScraper(
            page=page,
            tripadvisor_url=_tripadvisor_base_url(source_url),
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
        )
        await tripadvisor_scraper.search_business(query)
        if not skip_reviews:
            def _progress_logger(payload: dict[str, object]) -> None:
                event = str(payload.get("event", "unknown"))
                total_unique = payload.get("total_unique_reviews")

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
                        f"de {total_results or '?'}"
                    )
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
                        f"total_unicas={total_unique}"
                    )
                    return

                if event == "tripadvisor_reviews_end_of_pagination":
                    current = payload.get("current_page")
                    total_pages = payload.get("total_pages")
                    print(
                        "[tripadvisor-progress] fin_paginacion "
                        f"pagina={current or '?'}"
                        f"/{total_pages or '?'} "
                        f"total_unicas={total_unique}"
                    )
                    return

                if event == "tripadvisor_reviews_completed":
                    print(f"[tripadvisor-progress] completado total_unicas={total_unique}")
                    return

                print(f"[tripadvisor-progress] event={event} total_unicas={total_unique}")

            if reviews_pages > 0:
                max_pages = max(1, int(reviews_pages))
                max_rounds = max_pages
                pages_label = str(max_pages)
            else:
                max_pages = None
                max_rounds = 1000
                pages_label = "all"

            print(f"Tripadvisor flow: opening reviews (pages={pages_label})...")
            reviews = await tripadvisor_scraper.extract_reviews(
                max_pages=max_pages,
                max_rounds=max_rounds,
                progress_callback=_progress_logger,
            )
            _print_reviews(reviews)
        print("Tripadvisor flow completed.")
        return True
    except Exception as exc:
        print(f"Tripadvisor flow failed: {exc!r}")
        return False


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
    use_stealth = not args.no_stealth
    target_url = _normalize_url(args.url)

    playwright: Playwright | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None
    tripadvisor_flow_started = False
    tripadvisor_flow_completed = False
    manual_trigger_requested = False

    try:
        playwright = await async_playwright().start()

        if args.persistent:
            profile_dir = _resolve_user_data_dir(args.profile_dir)
            browser, context = await _launch_persistent_context(
                playwright,
                channel=channel,
                executable_path=executable_path,
                profile_dir=profile_dir,
            )
            print(f"Mode: persistent profile ({profile_dir})")
        else:
            browser, context = await _launch_incognito_context(
                playwright, channel=channel, executable_path=executable_path
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
                print("Press 's' in this terminal to start flow. Press 'q' to quit script.")
                if msvcrt is None:
                    print(
                        "Manual key trigger is only available on Windows terminals. "
                        "Use --tripadvisor-trigger auto on this platform."
                    )
            else:
                print("Tripadvisor auto-flow is enabled.")
                print("It will start automatically when current tab URL is Tripadvisor.")

        if _looks_like_tripadvisor_url(target_url) and tripadvisor_enabled and not manual_trigger_mode:
            query = (args.tripadvisor_query or "").strip()
            if query:
                tripadvisor_flow_started = True
                print("Tripadvisor detected from initial URL. Starting flow now...")
                tripadvisor_flow_completed = await _run_tripadvisor_search_flow(
                    page,
                    query,
                    target_url,
                    reviews_pages=args.tripadvisor_reviews_pages,
                    skip_reviews=args.skip_tripadvisor_reviews,
                )

        print(f"Opened: {page.url}")
        print("Interact manually. This script will exit only when you close the browser window.")

        if browser is None:
            browser = context.browser

        if browser is not None:
            last_status_log = monotonic()
            while browser.is_connected():
                if tripadvisor_enabled and manual_trigger_mode and not tripadvisor_flow_started:
                    start_requested, quit_requested = _poll_manual_trigger_keys()
                    if quit_requested:
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
                    and _looks_like_tripadvisor_url(page.url)
                ):
                    query = (args.tripadvisor_query or "").strip()
                    if query:
                        tripadvisor_flow_started = True
                        print("Tripadvisor detected after manual trigger. Starting flow now...")
                        tripadvisor_flow_completed = await _run_tripadvisor_search_flow(
                            page,
                            query,
                            page.url,
                            reviews_pages=args.tripadvisor_reviews_pages,
                            skip_reviews=args.skip_tripadvisor_reviews,
                        )

                # Auto mode: if user navigates to Tripadvisor later, auto-start once.
                if (
                    tripadvisor_enabled
                    and not manual_trigger_mode
                    and not tripadvisor_flow_started
                    and _looks_like_tripadvisor_url(page.url)
                ):
                    query = (args.tripadvisor_query or "").strip()
                    if query:
                        tripadvisor_flow_started = True
                        print("Tripadvisor detected from current tab. Starting flow now...")
                        tripadvisor_flow_completed = await _run_tripadvisor_search_flow(
                            page,
                            query,
                            page.url,
                            reviews_pages=args.tripadvisor_reviews_pages,
                            skip_reviews=args.skip_tripadvisor_reviews,
                        )
                now = monotonic()
                if now - last_status_log >= 10:
                    if tripadvisor_enabled and not tripadvisor_flow_started and manual_trigger_mode:
                        if manual_trigger_requested:
                            print("Manual trigger is armed. Navigate to Tripadvisor URL to run flow...")
                        else:
                            print("Waiting manual trigger key 's' in terminal...")
                    elif tripadvisor_enabled and not tripadvisor_flow_started and not manual_trigger_mode:
                        print("Waiting for Tripadvisor URL in current tab to auto-start flow...")
                    elif tripadvisor_flow_started and not tripadvisor_flow_completed:
                        print("Tripadvisor flow started. Waiting for completion...")
                    last_status_log = now
                await asyncio.sleep(0.75)
        else:
            # Fallback: keep process alive until interrupted.
            while True:
                await asyncio.sleep(1.0)
    finally:
        try:
            if context is not None:
                await context.close()
        except Exception:
            pass
        if playwright is not None:
            await playwright.stop()


if __name__ == "__main__":
    asyncio.run(main())

