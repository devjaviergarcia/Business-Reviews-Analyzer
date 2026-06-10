from __future__ import annotations

from typing import Any, Awaitable, Callable


class GoogleMapsBrowserReviewsCollectionFacet:

    async def scroll_reviews(self, max_rounds: int = 10) -> None:
        if max_rounds <= 0:
            return

        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return

        last_count = await self._review_count()
        stale_rounds = 0
        page = self._require_page()

        for _ in range(max_rounds):
            await self._click_expand_buttons(max_clicks=4)
            scrolled = await self._scroll_reviews_feed_once()
            await page.wait_for_timeout(700)

            current_count = await self._review_count()
            if current_count > last_count:
                last_count = current_count
                stale_rounds = 0
            else:
                stale_rounds += 1

            if stale_rounds >= 2 or not scrolled:
                break

    async def collect_reviews_html_snapshot(
        self,
        *,
        max_rounds: int = 0,
        stable_rounds: int = 8,
        min_pause_s: float = 1.0,
        max_pause_s: float = 2.0,
        bottom_wait_min_ms: int = 2200,
        bottom_wait_max_ms: int = 3600,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> str:
        reviews_open = await self._ensure_reviews_open(progress_callback=progress_callback)
        if not reviews_open:
            return ""

        effective_max_rounds = max_rounds if max_rounds > 0 else 6000
        stable_rounds = max(2, stable_rounds)
        min_pause_s = max(0.15, float(min_pause_s))
        max_pause_s = max(min_pause_s, float(max_pause_s))
        bottom_wait_min_ms = max(400, bottom_wait_min_ms)
        bottom_wait_max_ms = max(bottom_wait_min_ms, bottom_wait_max_ms)

        initial_state = await self._reviews_feed_state(step_px=None, capture_html=False)
        last_count = int(initial_state.get("review_count", 0))
        unchanged_rounds = 0
        last_top = int(initial_state.get("scroll_top", -1))
        last_scroll_height = int(initial_state.get("scroll_height", -1))
        page = self._require_page()

        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_scroll_started",
                "effective_max_rounds": effective_max_rounds,
                "stable_rounds": stable_rounds,
                "initial_review_count": last_count,
                "interval_s": {
                    "min": min_pause_s,
                    "max": max_pause_s,
                },
            },
        )

        for round_index in range(1, effective_max_rounds + 1):
            metrics = await self._scroll_reviews_feed_step(step_px=self._rng.randint(380, 980))
            await page.wait_for_timeout(self._rng.uniform(min_pause_s * 1000.0, max_pause_s * 1000.0))

            current_state = await self._reviews_feed_state(step_px=None, capture_html=False)
            current_count = int(current_state.get("review_count", 0))
            top = int(current_state.get("scroll_top", -1))
            scroll_height = int(current_state.get("scroll_height", -1))
            at_bottom = bool(current_state.get("at_bottom"))

            if at_bottom:
                await page.wait_for_timeout(self._rng.randint(bottom_wait_min_ms, bottom_wait_max_ms))
                settled_state = await self._reviews_feed_state(step_px=None, capture_html=False)
                settled_count = int(settled_state.get("review_count", 0))
                settled_top = int(settled_state.get("scroll_top", -1))
                settled_scroll_height = int(settled_state.get("scroll_height", -1))
                if (
                    settled_count > current_count
                    or settled_scroll_height > scroll_height
                    or settled_top != top
                ):
                    current_state = settled_state
                    current_count = settled_count
                    top = settled_top
                    scroll_height = settled_scroll_height
                    at_bottom = bool(settled_state.get("at_bottom"))

            moved = bool(metrics.get("scrolled")) or top != last_top
            count_grew = current_count > last_count
            geometry_changed = top != last_top or scroll_height != last_scroll_height

            if count_grew:
                last_count = current_count

            if moved or count_grew or geometry_changed:
                unchanged_rounds = 0
            else:
                unchanged_rounds += 1

            last_top = top
            last_scroll_height = scroll_height

            if (
                round_index == 1
                or count_grew
                or round_index % 5 == 0
                or (at_bottom and unchanged_rounds >= stable_rounds)
            ):
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "reviews_scroll_round",
                        "round": round_index,
                        "reviews_loaded": current_count,
                        "at_bottom": at_bottom,
                        "moved": moved,
                        "unchanged_rounds": unchanged_rounds,
                        "effective_max_rounds": effective_max_rounds,
                    },
                )

            if at_bottom and unchanged_rounds >= stable_rounds:
                break

            if not current_state.get("found") and unchanged_rounds >= stable_rounds:
                break

        await page.wait_for_timeout(self._rng.randint(500, 1100))
        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_scroll_finished",
                "reviews_loaded": last_count,
                "unchanged_rounds": unchanged_rounds,
            },
        )
        return await self._capture_reviews_feed_html()

    async def capture_reviews_container_html(self) -> str:
        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return ""
        return await self._capture_reviews_feed_html()

    async def extract_reviews(
        self,
        *,
        strategy: str | None = None,
        max_rounds: int = 10,
        html_scroll_max_rounds: int = 180,
        html_stable_rounds: int = 6,
        html_min_interval_s: float = 1.0,
        html_max_interval_s: float = 2.0,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> list[dict]:
        selected_strategy = self._resolve_reviews_strategy(strategy)

        async def _run_strategy(name: str) -> list[dict]:
            if name == "scroll_copy":
                reviews_html = await self.collect_reviews_html_snapshot(
                    max_rounds=html_scroll_max_rounds,
                    stable_rounds=html_stable_rounds,
                    min_pause_s=html_min_interval_s,
                    max_pause_s=html_max_interval_s,
                    progress_callback=progress_callback,
                )
                return self.extract_reviews_from_html(reviews_html)

            if max_rounds > 0:
                await self.scroll_reviews(max_rounds=max_rounds)
            return await self._extract_reviews_interactive()

        primary_items = await _run_strategy(selected_strategy)
        if primary_items:
            return primary_items

        fallback_strategy = "interactive" if selected_strategy == "scroll_copy" else "scroll_copy"
        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_strategy_fallback_triggered",
                "primary_strategy": selected_strategy,
                "fallback_strategy": fallback_strategy,
                "reason": "primary_strategy_returned_zero_reviews",
            },
        )
        fallback_items = await _run_strategy(fallback_strategy)
        await self._emit_progress(
            progress_callback,
            {
                "event": "reviews_strategy_fallback_finished",
                "primary_strategy": selected_strategy,
                "fallback_strategy": fallback_strategy,
                "fallback_review_count": len(fallback_items),
            },
        )
        return fallback_items

    async def _extract_reviews_interactive(self) -> list[dict]:
        reviews_open = await self._ensure_reviews_open()
        if not reviews_open:
            return []
        await self._click_expand_buttons(max_clicks=8)

        cards = await self._panel_scoped_collection("REVIEW_CARDS")
        if cards is None:
            cards = await self._first_available_collection("REVIEW_CARDS")
        if cards is None:
            return []

        total_cards = await cards.count()
        items: list[dict[str, Any]] = []

        for idx in range(total_cards):
            card = cards.nth(idx)

            review_id = await card.get_attribute("data-review-id")
            author_name = await self._text_from_locator(card.locator("div.d4r55").first)
            if not author_name:
                author_name = self._clean_text(await card.get_attribute("aria-label"))

            rating_label = await self._attribute_from_descendant_patterns(card, "RATING_LABEL", "aria-label")
            if not rating_label:
                rating_label = await self._text_from_descendant_patterns(card, "RATING_TEXT")
            rating = self._parse_rating(rating_label)
            relative_time = await self._text_from_locator(card.locator("span.rsqaWe").first)
            review_text = await self._text_from_locator(card.locator(".MyEned .wiI7pd").first)
            image_urls = await self._extract_review_photo_urls(card)
            raw_card_html = await self._outer_html_from_locator(card)

            review_payload: dict[str, Any] = {
                "source": "google_maps",
                "review_id": review_id,
                "author_name": author_name or "",
                "rating": rating if rating is not None else 0.0,
                "relative_time": relative_time or "",
                "text": review_text or "",
                "image_urls": image_urls,
            }
            if raw_card_html:
                review_payload["raw_card_html"] = raw_card_html[:50_000]

            owner_reply = await self._extract_owner_reply(card)
            if owner_reply is not None:
                review_payload["owner_reply"] = owner_reply

            items.append(review_payload)

        return items
