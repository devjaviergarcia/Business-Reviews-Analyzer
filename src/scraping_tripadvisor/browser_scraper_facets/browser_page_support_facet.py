from __future__ import annotations

import asyncio
from time import monotonic
from typing import Any, Awaitable, Callable

from playwright.async_api import Locator


class TripadvisorBrowserPageSupportFacet:

    async def _accept_cookies_if_present(self, *, timeout_seconds: float = 10.0, force_check: bool = False) -> None:
        if self._cookies_checked_once and not force_check:
            return
        page = self._require_page()
        accept_selectors = (
            "#onetrust-accept-btn-handler",
            "button#onetrust-accept-btn-handler",
            "#accept-recommended-btn-handler",
            "button#accept-recommended-btn-handler",
            "button:has-text('Permitirlas todas')",
            "button:has-text('Aceptar todo')",
            "button:has-text('Accept recommended')",
            "button:has-text('Allow all')",
            "button:has-text('Acepto')",
            "button:has-text('Aceptar')",
            "button:has-text('Accept all')",
        )
        open_panel_selectors = (
            "#onetrust-cookie-btn",
            "button#onetrust-cookie-btn",
            "button[aria-label='Cookies']",
        )
        panel_presence_selectors = (
            "#onetrust-banner-sdk",
            "#onetrust-accept-btn-handler",
            "#onetrust-cookie-btn",
            "#accept-recommended-btn-handler",
            "#ot-pc-content",
            "#ot-pc-title",
            "div[role='dialog'][aria-label*='privacidad' i]",
        )

        start = monotonic()
        deadline = start + max(0.4, float(timeout_seconds))
        clicked_any = False
        while monotonic() < deadline:
            scopes: list[Any] = [page, *page.frames]
            saw_cookie_ui = False
            opened_panel = False

            for scope in scopes:
                for selector in panel_presence_selectors:
                    candidates = scope.locator(selector)
                    try:
                        if await candidates.count() > 0:
                            saw_cookie_ui = True
                            break
                    except Exception:
                        continue

                for selector in accept_selectors:
                    candidates = scope.locator(selector)
                    try:
                        total = await candidates.count()
                    except Exception:
                        continue
                    for idx in range(min(total, 6)):
                        candidate = candidates.nth(idx)
                        try:
                            if not await candidate.is_visible():
                                continue
                            await candidate.click(timeout=1200, force=True)
                            clicked_any = True
                            await page.wait_for_timeout(self._rng.randint(260, 520))
                        except Exception:
                            continue

                # Some OneTrust configurations start with a floating cookie icon.
                for selector in open_panel_selectors:
                    candidates = scope.locator(selector)
                    try:
                        total = await candidates.count()
                    except Exception:
                        continue
                    for idx in range(min(total, 3)):
                        candidate = candidates.nth(idx)
                        try:
                            if not await candidate.is_visible():
                                continue
                            await candidate.click(timeout=700, force=True)
                            await page.wait_for_timeout(220)
                            opened_panel = True
                        except Exception:
                            continue

            # Fallback: JS click by id in top document.
            try:
                clicked = await page.evaluate(
                    """
                    () => {
                        const ids = [
                            '#accept-recommended-btn-handler',
                            '#onetrust-accept-btn-handler',
                        ];
                        for (const id of ids) {
                            const btn = document.querySelector(id);
                            if (!btn || btn.disabled) continue;
                            btn.click();
                            return true;
                        }
                        const byText = [...document.querySelectorAll('button')].find((btn) => {
                            const text = (btn.textContent || '').toLowerCase();
                            if (btn.disabled) return false;
                            return (
                                text.includes('permitirlas todas') ||
                                text.includes('accept all') ||
                                text.includes('allow all')
                            );
                        });
                        if (byText) {
                            byText.click();
                            return true;
                        }
                        return false;
                    }
                    """
                )
                if clicked:
                    clicked_any = True
                    await page.wait_for_timeout(self._rng.randint(240, 460))
            except Exception:
                pass

            # Exit once cookie UI is gone. If we clicked something, give UI a short grace period
            # in case a second-step modal appears and requires "Permitirlas todas".
            if not saw_cookie_ui and monotonic() - start >= (1.2 if clicked_any else 0.9):
                self._cookies_checked_once = True
                return
            if opened_panel:
                await page.wait_for_timeout(120)
            await page.wait_for_timeout(180)
        self._cookies_checked_once = True

    async def _dismiss_consent_if_present(self, *, force_check: bool = False) -> None:
        if self._consent_checked_once and not force_check:
            return
        terms = ("aceptar", "accept", "consentir", "agree")
        page = self._require_page()
        scopes: list[Any] = [page, *page.frames]
        selectors = (
            "button[aria-label]",
            "button",
            "[role='button'][aria-label]",
        )

        for scope in scopes:
            for selector in selectors:
                candidates = scope.locator(selector)
                try:
                    total = await candidates.count()
                except Exception:
                    continue
                for idx in range(min(total, 20)):
                    candidate = candidates.nth(idx)
                    try:
                        if not await candidate.is_visible():
                            continue
                        label = self._clean_text(await candidate.get_attribute("aria-label"))
                        if not label:
                            label = await self._safe_locator_inner_text(candidate)
                        normalized = self._normalize_text(label)
                        if not any(term in normalized for term in terms):
                            continue
                        if "acepto" in normalized or "accept all" in normalized:
                            await self._human_click(candidate)
                            await page.wait_for_timeout(self._rng.randint(220, 450))
                            self._consent_checked_once = True
                            return
                        if "cookies" not in normalized and "todo" not in normalized and "all" not in normalized:
                            continue
                        await self._human_click(candidate)
                        await page.wait_for_timeout(self._rng.randint(280, 520))
                        self._consent_checked_once = True
                        return
                    except Exception:
                        continue
        self._consent_checked_once = True

    async def _dismiss_location_prompt_if_present(self, *, force_check: bool = False) -> None:
        if self._location_prompt_checked_once and not force_check:
            return
        negative_terms = (
            "no gracias",
            "ahora no",
            "no permitir",
            "bloquear",
            "rechazar",
            "not now",
            "no thanks",
            "deny",
            "block",
            "dont allow",
            "do not allow",
        )
        page = self._require_page()
        scopes: list[Any] = [page, *page.frames]
        selectors = (
            "button[aria-label]",
            "button",
            "[role='button'][aria-label]",
            "[role='button']",
        )

        for scope in scopes:
            for selector in selectors:
                candidates = scope.locator(selector)
                try:
                    total = await candidates.count()
                except Exception:
                    continue
                for idx in range(min(total, 40)):
                    candidate = candidates.nth(idx)
                    try:
                        if not await candidate.is_visible():
                            continue
                        label = self._clean_text(await candidate.get_attribute("aria-label"))
                        if not label:
                            label = await self._safe_locator_inner_text(candidate)
                        normalized = self._normalize_text(label)
                        if not normalized:
                            continue
                        if not any(term in normalized for term in negative_terms):
                            continue
                        await self._human_click(candidate)
                        await page.wait_for_timeout(self._rng.randint(220, 480))
                        self._location_prompt_checked_once = True
                        return
                    except Exception:
                        continue
        self._location_prompt_checked_once = True

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
            return

    async def _find_first_visible(
        self,
        selectors: tuple[str, ...],
        *,
        timeout_ms: int,
    ) -> Locator:
        optional = await self._find_first_optional_visible(selectors, timeout_ms=timeout_ms)
        if optional is None:
            raise RuntimeError(f"No visible element found for selectors: {'; '.join(selectors)}")
        return optional

    async def _find_first_optional_visible(
        self,
        selectors: tuple[str, ...],
        *,
        timeout_ms: int,
    ) -> Locator | None:
        page = self._require_page()
        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)
        while monotonic() < deadline:
            for selector in selectors:
                try:
                    candidates = page.locator(selector)
                    total = await candidates.count()
                except Exception:
                    continue
                if total == 0:
                    continue
                for idx in range(min(total, 12)):
                    locator = candidates.nth(idx)
                    try:
                        if await locator.is_visible():
                            return locator
                    except Exception:
                        continue
            await page.wait_for_timeout(120)
        return None

    async def _safe_locator_inner_text(self, locator: Locator) -> str:
        try:
            if await locator.count() == 0:
                return ""
            text = await locator.inner_text()
            return self._clean_text(text)
        except Exception:
            return ""

    async def _safe_locator_attribute(self, locator: Locator, attribute: str) -> str:
        try:
            if await locator.count() == 0:
                return ""
            value = await locator.get_attribute(attribute)
            return self._clean_text(value)
        except Exception:
            return ""

    async def _first_non_empty_text(
        self,
        scope: Locator,
        *,
        selectors: tuple[str, ...],
        max_candidates_per_selector: int = 6,
    ) -> str:
        for selector in selectors:
            candidates = scope.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, max_candidates_per_selector)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if text:
                    return text
        return ""
