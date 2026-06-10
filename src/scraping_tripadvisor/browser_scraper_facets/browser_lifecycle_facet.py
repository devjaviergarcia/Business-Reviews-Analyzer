from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from time import monotonic
from typing import Any

from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


class TripadvisorBrowserLifecycleFacet:

    def bind_page(self, page: Page) -> None:
        self._page = page
        self._external_page = True

    async def __aenter__(self) -> TripadvisorScraper:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        await self.close()

    @property

    def page(self) -> Page:
        return self._require_page()

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
            )
        else:
            user_data_dir = self._resolve_user_data_dir()
            launch_options: dict[str, Any] = {
                "user_data_dir": str(user_data_dir),
                "headless": self._headless,
                "slow_mo": self._slow_mo_ms,
                "viewport": {"width": 1366, "height": 900},
                "locale": "es-ES",
                "timezone_id": "Europe/Madrid",
                "args": self._build_chromium_args(),
            }
            if self._browser_channel:
                launch_options["channel"] = self._browser_channel
            try:
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)
            except Exception:
                if not self._browser_channel:
                    raise
                launch_options.pop("channel", None)
                self._context = await self._playwright.chromium.launch_persistent_context(**launch_options)

        if self._stealth_mode and self._context is not None:
            await self._stealth.apply_stealth_async(self._context)
        if self._context is not None:
            await self._context.add_init_script(self._block_geolocation_init_script())
        if self._context is not None:
            self._context.set_default_timeout(self._timeout_ms)

        if self._context is None:
            raise RuntimeError("Playwright context was not initialized.")

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()
        await self._go_to_home()
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

    async def _wait_after_navigation(self) -> None:
        page = self._require_page()
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except PlaywrightTimeoutError:
            pass
        await page.wait_for_timeout(self._rng.randint(280, 760))

    def _require_page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Playwright page is not configured. Call start() or bind_page(page).")
        return self._page

    def _assert_event_loop_compatible_for_playwright(self) -> None:
        if sys.platform != "win32":
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if "selector" in loop.__class__.__name__.lower():
            raise RuntimeError(
                "Playwright is not compatible with Windows SelectorEventLoop "
                "(common with uvicorn --reload). Run without --reload or in Docker/WSL."
            )

    def _resolve_user_data_dir(self) -> Path:
        path = Path(self._user_data_dir).expanduser()
        if not path.is_absolute():
            path = self._project_root / path
        return path.resolve()

    def _build_chromium_args(self) -> list[str]:
        args = [
            "--disable-blink-features=AutomationControlled",
            "--deny-permission-prompts",
            "--disable-geolocation",
            "--window-size=1920,1080",
            "--lang=es-ES",
        ]
        if self._headless and self._harden_headless:
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

    def _block_geolocation_init_script(self) -> str:
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

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(max(0, delay_ms) / 1000.0)

    async def _enforce_click_gap(self) -> None:
        target_gap_ms = self._rng.randint(self._min_click_delay_ms, self._max_click_delay_ms)
        if self._last_click_ts is None:
            await self._sleep_ms(self._rng.randint(120, 320))
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
            if self._rng.random() < 0.04:
                await self._sleep_ms(self._rng.randint(80, 220))
