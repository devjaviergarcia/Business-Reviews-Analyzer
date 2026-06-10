from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from time import monotonic
from typing import Any, Awaitable, Callable

from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Playwright,
    async_playwright,
)


class GoogleMapsBrowserLifecycleFacet:

    def bind_page(self, page: Page) -> None:
        self._page = page
        self._external_page = True

    async def __aenter__(self) -> GoogleMapsScraper:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    async def start(self) -> Page:
        if self._page is not None:
            return self._page

        self._assert_event_loop_compatible_for_playwright()
        self._playwright = await async_playwright().start()
        if self._incognito:
            launch_options: dict[str, Any] = {
                "headless": self._headless,
                "slow_mo": self._slow_mo_ms,
                "args": self._build_chromium_args(),
            }
            if self._browser_channel:
                launch_options["channel"] = self._browser_channel

            try:
                self._browser = await self._playwright.chromium.launch(**launch_options)
            except Exception:
                if not self._browser_channel:
                    raise
                launch_options.pop("channel", None)
                self._browser = await self._playwright.chromium.launch(**launch_options)

            self._context = await self._browser.new_context(
                viewport={"width": 1366, "height": 900},
                locale="es-ES",
                timezone_id="Europe/Madrid",
                user_agent=self._default_user_agent,
            )
        else:
            user_data_dir = self._resolve_user_data_dir()
            launch_options = {
                "user_data_dir": str(user_data_dir),
                "headless": self._headless,
                "slow_mo": self._slow_mo_ms,
                "viewport": {"width": 1366, "height": 900},
                "locale": "es-ES",
                "timezone_id": "Europe/Madrid",
                "user_agent": self._default_user_agent,
                "args": self._build_chromium_args(),
            }
            if self._browser_channel:
                launch_options["channel"] = self._browser_channel

            try:
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
            except Exception:
                if not self._browser_channel:
                    raise
                # Fallback to bundled Chromium if requested browser channel is unavailable.
                launch_options.pop("channel", None)
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
        if self._stealth_mode:
            await self._context.add_init_script(self._stealth_init_script())
        self._context.set_default_timeout(self._timeout_ms)

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await self._go_to_maps_home()
        return self._page

    async def close(self) -> None:
        if not self._external_page and self._context is not None:
            await self._context.close()

        if not self._external_page and self._browser is not None:
            await self._browser.close()

        if not self._external_page and self._playwright is not None:
            await self._playwright.stop()

        self._context = None
        self._browser = None
        self._playwright = None
        self._page = None
        self._external_page = False
        self._last_click_ts = None
        self._last_reviews_open_state = {
            "status": "unknown",
            "section_variant": "none",
            "found": False,
            "panel_ready": False,
            "review_count": 0,
        }

    @property

    def page(self) -> Page:
        return self._require_page()

    def get_last_reviews_open_state(self) -> dict[str, Any]:
        return dict(self._last_reviews_open_state)

    def _resolve_reviews_strategy(self, strategy: str | None) -> str:
        raw_value = strategy or self._reviews_strategy
        normalized = self._normalize_text(raw_value).replace("-", "_").replace(" ", "_")

        interactive_aliases = {"interactive", "current", "legacy", "expand_click"}
        scroll_copy_aliases = {"scroll_copy", "scroll_and_copy", "html_snapshot", "snapshot"}

        if normalized in interactive_aliases:
            return "interactive"
        if normalized in scroll_copy_aliases:
            return "scroll_copy"

        raise ValueError(
            f"Unknown reviews strategy '{raw_value}'. "
            "Supported: interactive | scroll_copy"
        )

    async def _emit_progress(
        self,
        callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None,
        payload: dict[str, Any],
    ) -> None:
        if callback is None:
            return
        try:
            maybe_awaitable = callback(payload)
            if asyncio.iscoroutine(maybe_awaitable):
                await maybe_awaitable
        except Exception:
            # Progress updates must never break scraping flow.
            return

    async def _go_to_maps_home(self) -> None:
        page = self._require_page()
        await page.goto(self._maps_url, wait_until="domcontentloaded")
        await self._dismiss_google_consent_if_present()

        search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=8000)
        if search_input is not None:
            return

        await self._dismiss_google_consent_if_present()
        search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=9000)
        if search_input is None:
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
            await self._dismiss_google_consent_if_present()
            search_input = await self._first_optional_visible_from_patterns("SEARCH_INPUT", timeout_ms=9000)

        if search_input is None:
            raise RuntimeError("Google Maps search input was not found after consent fallback.")

    def _require_page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Playwright page is not configured. Call start() or bind_page(page).")
        return self._page

    def _assert_event_loop_compatible_for_playwright(self) -> None:
        # On Windows, uvicorn --reload switches to SelectorEventLoopPolicy.
        # Playwright needs subprocess support, which SelectorEventLoop lacks.
        if sys.platform != "win32":
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop_name = loop.__class__.__name__.lower()
        if "selector" in loop_name:
            raise RuntimeError(
                "Playwright is not compatible with Windows SelectorEventLoop "
                "(common with 'uvicorn --reload'). Start API without --reload, "
                "or run in Docker/WSL."
            )

    def _resolve_user_data_dir(self) -> Path:
        path = Path(self._user_data_dir).expanduser()
        if not path.is_absolute():
            path = self._project_root / path
        return path.resolve()

    def _build_chromium_args(self) -> list[str]:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--window-size=1920,1080",
            "--lang=es-ES",
        ]

        if self._headless and self._harden_headless:
            # Hardened headless mode: closer to headed runtime in server environments.
            args.extend(
                [
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ]
            )

        args.extend(self._extra_chromium_args)

        deduped: list[str] = []
        seen: set[str] = set()
        for arg in args:
            cleaned = str(arg or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    def _stealth_init_script(self) -> str:
        return """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['es-ES', 'es', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4] });
            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
            window.chrome = window.chrome || { runtime: {} };
        """

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(max(0, delay_ms) / 1000)

    async def _enforce_click_gap(self) -> None:
        target_gap_ms = self._rng.randint(self._min_click_delay_ms, self._max_click_delay_ms)
        if self._last_click_ts is None:
            await self._sleep_ms(self._rng.randint(450, 1100))
            return

        elapsed_ms = int((monotonic() - self._last_click_ts) * 1000)
        remaining = target_gap_ms - elapsed_ms
        if remaining > 0:
            await self._sleep_ms(remaining)

    async def _human_click(self, locator: Locator) -> None:
        await self._enforce_click_gap()
        try:
            await locator.scroll_into_view_if_needed()
        except Exception:
            pass
        await locator.click()
        self._last_click_ts = monotonic()

    async def _human_type(self, locator: Locator, text: str) -> None:
        await locator.fill("")
        for char in text:
            await locator.type(char, delay=self._rng.randint(self._min_key_delay_ms, self._max_key_delay_ms))
            if self._rng.random() < 0.1:
                await self._sleep_ms(self._rng.randint(220, 700))
