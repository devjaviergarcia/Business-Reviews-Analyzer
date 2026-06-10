from __future__ import annotations

from typing import Any, Awaitable, Callable


class GoogleMapsBrowserReviewsOpenFacet:

    async def _ensure_reviews_open(
        self,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> bool:
        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_panel_open_started",
            },
        )
        await self._dismiss_google_consent_if_present()
        if await self._wait_for_reviews_ready(timeout_ms=2200):
            open_state = self.get_last_reviews_open_state()
            await self._emit_progress(
                progress_callback,
                {
                    "event": "reviews_panel_open_ready_already",
                    "open_status": open_state.get("status"),
                    "section_variant": open_state.get("section_variant"),
                    "found_scrollable_feed": bool(open_state.get("found")),
                    "review_count": int(open_state.get("review_count", 0)),
                },
            )
            return True

        if await self._is_limited_maps_view():
            await self._emit_progress(
                progress_callback,
                {
                    "event": "reviews_panel_open_blocked_limited_view",
                },
            )
            return False

        if not await self._has_review_entrypoint():
            await self._emit_progress(
                progress_callback,
                {
                    "event": "reviews_panel_open_no_entrypoint",
                },
            )
            return False

        page = self._require_page()

        for attempt in range(1, 4):
            await self._emit_progress(
                progress_callback,
                {
                    "event": "reviews_panel_open_attempt",
                    "attempt": attempt,
                },
            )
            clicked_more_reviews = await self._click_more_reviews_summary_button()
            if clicked_more_reviews:
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_click",
                        "attempt": attempt,
                        "action": "more_reviews_button",
                        "clicked": True,
                    },
                )
                await page.wait_for_timeout(self._rng.randint(900, 1700))

            if await self._wait_for_reviews_ready(timeout_ms=5500):
                open_state = self.get_last_reviews_open_state()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_succeeded",
                        "attempt": attempt,
                        "action": "more_reviews_or_ready_check",
                        "open_status": open_state.get("status"),
                        "section_variant": open_state.get("section_variant"),
                        "found_scrollable_feed": bool(open_state.get("found")),
                        "review_count": int(open_state.get("review_count", 0)),
                    },
                )
                return True

            clicked_tab = await self._click_first_valid_review_button_in_group("REVIEWS_TAB")
            if clicked_tab:
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_click",
                        "attempt": attempt,
                        "action": "reviews_tab_button",
                        "clicked": True,
                    },
                )
                await page.wait_for_timeout(self._rng.randint(900, 1700))

            if await self._wait_for_reviews_ready(timeout_ms=4500):
                open_state = self.get_last_reviews_open_state()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_succeeded",
                        "attempt": attempt,
                        "action": "reviews_tab_button",
                        "open_status": open_state.get("status"),
                        "section_variant": open_state.get("section_variant"),
                        "found_scrollable_feed": bool(open_state.get("found")),
                        "review_count": int(open_state.get("review_count", 0)),
                    },
                )
                return True

            clicked_button = await self._click_first_valid_review_button_in_group("REVIEWS_BUTTON")
            if clicked_button:
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_click",
                        "attempt": attempt,
                        "action": "reviews_button_group",
                        "clicked": True,
                    },
                )
                await page.wait_for_timeout(self._rng.randint(900, 1700))

            if await self._wait_for_reviews_ready(timeout_ms=5500):
                open_state = self.get_last_reviews_open_state()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_succeeded",
                        "attempt": attempt,
                        "action": "reviews_button_group",
                        "open_status": open_state.get("status"),
                        "section_variant": open_state.get("section_variant"),
                        "found_scrollable_feed": bool(open_state.get("found")),
                        "review_count": int(open_state.get("review_count", 0)),
                    },
                )
                return True

            # Final fallback: strict button-only scan with nested div text.
            if await self._click_review_entrypoint():
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_panel_open_click",
                        "attempt": attempt,
                        "action": "fallback_review_entrypoint",
                        "clicked": True,
                    },
                )
                if await self._wait_for_reviews_ready(timeout_ms=5000):
                    open_state = self.get_last_reviews_open_state()
                    await self._emit_progress(
                        progress_callback,
                        {
                            "event": "reviews_panel_open_succeeded",
                            "attempt": attempt,
                            "action": "fallback_review_entrypoint",
                            "open_status": open_state.get("status"),
                            "section_variant": open_state.get("section_variant"),
                            "found_scrollable_feed": bool(open_state.get("found")),
                            "review_count": int(open_state.get("review_count", 0)),
                        },
                    )
                    return True

        final_ready = await self._wait_for_reviews_ready(timeout_ms=2500)
        final_state = self.get_last_reviews_open_state()
        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_panel_open_finished",
                "opened": bool(final_ready),
                "open_status": final_state.get("status"),
                "section_variant": final_state.get("section_variant"),
                "found_scrollable_feed": bool(final_state.get("found")),
                "review_count": int(final_state.get("review_count", 0)),
            },
        )
        return final_ready

    async def _wait_for_reviews_ready(self, timeout_ms: int = 8000) -> bool:
        page = self._require_page()
        deadline = monotonic() + (timeout_ms / 1000)
        classic_phase_deadline = monotonic() + min(3.2, (timeout_ms / 1000) * 0.6)
        self._last_reviews_open_state = {
            "status": "not_open",
            "section_variant": "none",
            "found": False,
            "panel_ready": False,
            "review_count": 0,
        }

        while monotonic() < deadline:
            # If "Más reseñas (N)" is still visible, we are not in the final full feed yet.
            if await self._has_more_reviews_summary_button_visible():
                await page.wait_for_timeout(220)
                continue

            feed_state = await self._reviews_feed_state(step_px=None, capture_html=False)
            panel_ready = bool(feed_state.get("panel_ready"))
            section_variant = str(feed_state.get("section_variant", "") or "")
            now = monotonic()
            in_classic_phase = now <= classic_phase_deadline
            variant_accepted = (
                section_variant == "classic_controls"
                if in_classic_phase
                else section_variant in {"classic_controls", "search_filter_controls"}
            )

            if panel_ready and variant_accepted:
                if section_variant == "search_filter_controls" and not bool(feed_state.get("found")):
                    # Some profiles open a search/filter reviews section without a scrollable feed.
                    # Try a few extra clicks to promote it to classic controls.
                    for _ in range(4):
                        clicked = await self._click_more_reviews_summary_button()
                        if not clicked:
                            clicked = await self._click_first_valid_review_button_in_group("REVIEWS_TAB")
                        if not clicked:
                            clicked = await self._click_first_valid_review_button_in_group("REVIEWS_BUTTON")
                        if clicked:
                            await page.wait_for_timeout(self._rng.randint(850, 1600))
                        candidate = await self._reviews_feed_state(step_px=None, capture_html=False)
                        candidate_variant = str(candidate.get("section_variant", "") or "")
                        if bool(candidate.get("found")) or candidate_variant == "classic_controls":
                            feed_state = candidate
                            break

                # Full reviews panel is open.
                # Fallback order:
                # 1) classic_controls
                # 2) search_filter_controls
                await self._scroll_reviews_feed_once()
                await page.wait_for_timeout(700)
                final_state = await self._reviews_feed_state(step_px=None, capture_html=False)
                final_variant = str(final_state.get("section_variant", "") or section_variant)
                final_found = bool(final_state.get("found"))
                open_status = "open_scrollable"
                if final_variant == "search_filter_controls" and not final_found:
                    open_status = "open_non_scrollable"
                self._last_reviews_open_state = {
                    "status": open_status,
                    "section_variant": final_variant or "none",
                    "found": final_found,
                    "panel_ready": bool(final_state.get("panel_ready")),
                    "review_count": int(final_state.get("review_count", 0)),
                }
                return True

            await page.wait_for_timeout(220)

        self._last_reviews_open_state = {
            "status": "not_open",
            "section_variant": "none",
            "found": False,
            "panel_ready": False,
            "review_count": 0,
        }
        return False

    async def _is_reviews_tab_selected(self) -> bool:
        page = self._require_page()
        selected_tabs = page.locator("[role='tablist'] button[role='tab'][aria-selected='true']")
        try:
            total = await selected_tabs.count()
        except Exception:
            return False

        for idx in range(min(total, 6)):
            tab = selected_tabs.nth(idx)
            try:
                label = await self._candidate_label(tab)
                if self._is_review_entrypoint_text(label):
                    return True
            except Exception:
                continue

        return False

    async def _has_review_entrypoint(self) -> bool:
        if await self._find_more_reviews_summary_button() is not None:
            return True
        if await self._find_first_valid_review_button_in_group("REVIEWS_TAB") is not None:
            return True
        if await self._find_first_valid_review_button_in_group("REVIEWS_BUTTON") is not None:
            return True
        return await self._find_any_valid_review_button() is not None

    async def _review_count(self) -> int:
        feed_state = await self._reviews_feed_state(step_px=None, capture_html=False)
        if bool(feed_state.get("panel_ready")):
            return int(feed_state.get("review_count", 0))

        panel_cards = await self._panel_scoped_collection("REVIEW_CARDS")
        if panel_cards is not None:
            try:
                return await panel_cards.count()
            except Exception:
                return 0

        cards = await self._first_available_collection("REVIEW_CARDS")
        if cards is None:
            return 0
        return await cards.count()

    async def _click_expand_buttons(self, max_clicks: int = 30) -> int:
        clicks = 0

        for selector in SELECTOR_PATTERNS["REVIEW_EXPAND"]:
            buttons = self._require_page().locator(selector)
            total = await buttons.count()
            if total == 0:
                continue

            for idx in range(total):
                if clicks >= max_clicks:
                    return clicks

                button = buttons.nth(idx)
                try:
                    if not await button.is_visible():
                        continue
                    await self._human_click(button)
                    clicks += 1
                    await self._require_page().wait_for_timeout(self._rng.randint(300, 900))
                except Exception:
                    continue

        return clicks

    async def _scroll_reviews_feed_once(self) -> bool:
        metrics = await self._scroll_reviews_feed_step()
        return bool(metrics.get("found")) and bool(metrics.get("scrolled"))
