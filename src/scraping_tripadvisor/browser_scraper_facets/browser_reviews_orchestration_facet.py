from __future__ import annotations

import math
from time import monotonic
from typing import Any, Awaitable, Callable


class TripadvisorBrowserReviewsOrchestrationFacet:
    async def extract_reviews(
        self,
        *,
        strategy: str | None = None,
        max_rounds: int = 10,
        html_scroll_max_rounds: int = 180,
        html_stable_rounds: int = 6,
        html_min_interval_s: float = 1.0,
        html_max_interval_s: float = 2.0,
        max_pages: int | None = None,
        max_pages_percent: float | None = None,
        max_duration_seconds: float | None = None,
        include_owner_reply: bool = False,
        include_image_urls: bool = False,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> list[dict[str, Any]]:
        del strategy, html_stable_rounds
        page = self._require_page()
        await self._accept_cookies_if_present()
        await self._open_reviews_section()
        reviews_started_at = monotonic()

        effective_pages = self._resolve_effective_pages(
            max_pages=max_pages,
            max_rounds=max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
        )
        min_pause_s = max(0.2, float(html_min_interval_s))
        max_pause_s = max(min_pause_s, float(html_max_interval_s))
        pagination = await self._reviews_pagination_snapshot()
        known_total_pages = pagination.get("total_pages")
        if isinstance(known_total_pages, int) and known_total_pages > 0:
            effective_pages = min(effective_pages, known_total_pages)
            if max_pages_percent is not None:
                percent_value = float(max_pages_percent)
                percent_pages = max(1, int(math.ceil((known_total_pages * percent_value) / 100.0)))
                effective_pages = min(effective_pages, percent_pages)

        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_started",
                "max_pages": effective_pages,
                "max_pages_percent": max_pages_percent,
                "max_duration_seconds": max_duration_seconds,
                "pause_interval_s": {"min": min_pause_s, "max": max_pause_s},
                "range_start": pagination.get("range_start"),
                "range_end": pagination.get("range_end"),
                "total_results": pagination.get("total_results"),
                "current_page": pagination.get("current_page"),
                "total_pages": pagination.get("total_pages"),
            },
        )

        all_items: list[dict[str, Any]] = []
        seen: set[str] = set()
        effective_duration_limit: float | None = None
        if max_duration_seconds is not None:
            try:
                parsed_duration = float(max_duration_seconds)
            except (TypeError, ValueError):
                parsed_duration = 0.0
            if parsed_duration > 0:
                effective_duration_limit = parsed_duration

        for page_index in range(1, effective_pages + 1):
            page_collect_started_at = monotonic()
            if effective_duration_limit is not None:
                elapsed_seconds = max(0.0, monotonic() - reviews_started_at)
                if elapsed_seconds >= effective_duration_limit:
                    await self._emit_progress(
                        progress_callback,
                        {
                            "event": "tripadvisor_reviews_time_limit_reached",
                            "reason": "before_page_collection",
                            "elapsed_seconds": round(elapsed_seconds, 3),
                            "max_duration_seconds": round(effective_duration_limit, 3),
                            "last_page_index": page_index - 1,
                            "pages_target": effective_pages,
                            "total_unique_reviews": len(all_items),
                        },
                    )
                    break

            await self._expand_reviews(max_clicks=0)
            current_items = await self._extract_reviews_from_current_page(
                max_collection_seconds=self._max_seconds_per_reviews_page,
                include_owner_reply=include_owner_reply,
                include_image_urls=include_image_urls,
                ensure_reviews_open=(page_index == 1),
            )

            added_count = 0
            for item_index, item in enumerate(current_items):
                identity = self._review_identity(item)
                if not identity:
                    identity = self._review_identity_fallback(
                        review=item,
                        page_index=page_index,
                        item_index=item_index,
                    )
                if identity in seen:
                    continue
                seen.add(identity)
                all_items.append(item)
                added_count += 1

            pagination = await self._reviews_pagination_snapshot()
            current_page_num = pagination.get("current_page")
            total_pages_num = pagination.get("total_pages")
            remaining_pages: int | None = None
            if isinstance(current_page_num, int) and isinstance(total_pages_num, int):
                remaining_pages = max(0, total_pages_num - current_page_num)

            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_reviews_page_collected",
                    "page": page_index,
                    "page_elapsed_seconds": round(max(0.0, monotonic() - page_collect_started_at), 3),
                    "page_budget_seconds": round(self._max_seconds_per_reviews_page, 3),
                    "page_url": page.url,
                    "items_in_page": len(current_items),
                    "added_to_total": added_count,
                    "total_unique_reviews": len(all_items),
                    "range_start": pagination.get("range_start"),
                    "range_end": pagination.get("range_end"),
                    "total_results": pagination.get("total_results"),
                    "current_page": current_page_num,
                    "total_pages": total_pages_num,
                    "remaining_pages": remaining_pages,
                    "page_source": getattr(self, "_last_tripadvisor_reviews_page_source", None),
                    "graphql_expected_offset": getattr(self, "_last_tripadvisor_graphql_expected_offset", None),
                    "graphql_captured_offset": getattr(self, "_last_tripadvisor_graphql_reviews_offset", None),
                },
            )

            if effective_duration_limit is not None:
                elapsed_seconds = max(0.0, monotonic() - reviews_started_at)
                if elapsed_seconds >= effective_duration_limit:
                    await self._emit_progress(
                        progress_callback,
                        {
                            "event": "tripadvisor_reviews_time_limit_reached",
                            "reason": "after_page_collection",
                            "elapsed_seconds": round(elapsed_seconds, 3),
                            "max_duration_seconds": round(effective_duration_limit, 3),
                            "last_page_index": page_index,
                            "pages_target": effective_pages,
                            "total_unique_reviews": len(all_items),
                        },
                    )
                    break

            if page_index >= effective_pages:
                break

            try:
                moved = await self._go_next_reviews_page()
            except Exception as exc:
                moved = False
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_reviews_next_page_error",
                        "page": page_index,
                        "error": self._clean_text(str(exc)),
                        "total_unique_reviews": len(all_items),
                    },
                )
            if not moved:
                recovered = False
                if page_index < effective_pages:
                    recovered = await self._recover_reviews_and_retry_pagination(
                        page_index=page_index,
                        progress_callback=progress_callback,
                    )
                if recovered:
                    await page.wait_for_timeout(self._rng.randint(120, 260))
                    continue
                pagination = await self._reviews_pagination_snapshot()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_reviews_end_of_pagination",
                        "page": page_index,
                        "total_unique_reviews": len(all_items),
                        "current_page": pagination.get("current_page"),
                        "total_pages": pagination.get("total_pages"),
                    },
                )
                break

            # Keep transition pause short for predictable throughput between pages.
            page_pause_min_s = min(max(0.05, min_pause_s), 0.25)
            page_pause_max_s = min(max(page_pause_min_s, max_pause_s), 0.45)
            await page.wait_for_timeout(self._rng.uniform(page_pause_min_s * 1000.0, page_pause_max_s * 1000.0))

        await self._emit_progress(
            progress_callback,
            {
                "event": "tripadvisor_reviews_completed",
                "total_unique_reviews": len(all_items),
            },
        )
        return all_items

    def _resolve_effective_pages(
        self,
        *,
        max_pages: int | None,
        max_rounds: int,
        html_scroll_max_rounds: int,
    ) -> int:
        if max_pages is not None:
            return max(1, int(max_pages))
        if max_rounds > 0:
            return max(1, int(max_rounds))
        if html_scroll_max_rounds > 0:
            return max(1, min(100, int(html_scroll_max_rounds)))
        return 25
