import argparse
import asyncio
import sys
from pathlib import Path

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings


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
    return parser.parse_args()


def _resolve_user_data_dir(user_data_dir: str) -> Path:
    path = Path(user_data_dir).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _build_chromium_args(headless: bool) -> list[str]:
    args = [
        "--disable-blink-features=AutomationControlled",
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


async def main() -> None:
    args = _parse_args()
    channel = (args.channel or "").strip() or None
    executable_path = (args.executable_path or "").strip() or None
    use_stealth = not args.no_stealth

    playwright: Playwright | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None

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

        context.set_default_timeout(settings.scraper_timeout_ms)
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(args.url, wait_until="domcontentloaded")

        print(f"Opened: {page.url}")
        print("Interact manually. This script will exit only when you close the browser window.")

        if browser is None:
            browser = context.browser

        if browser is not None:
            while browser.is_connected():
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
