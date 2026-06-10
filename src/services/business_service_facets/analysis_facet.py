from __future__ import annotations

from typing import Any, Awaitable, Callable


class BusinessServiceAnalysisFacet:

    async def analyze_business(
        self,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> dict:
        del tripadvisor_max_pages, tripadvisor_pages_percent
        selected_force_mode = self._resolve_force_mode(force_mode)
        if selected_force_mode != "fallback_existing":
            raise ValueError(
                "force_mode is supported only in queued pipeline mode. "
                "Use POST /business/scrape/jobs for strict rescrape behavior."
            )
        return await self.analyze_use_case.execute(
            name=name,
            force=force,
            strategy=strategy,
            interactive_max_rounds=interactive_max_rounds,
            html_scroll_max_rounds=html_scroll_max_rounds,
            html_stable_rounds=html_stable_rounds,
            progress_callback=progress_callback,
        )

    async def reanalyze_business_from_stored_reviews(
        self,
        business_id: str,
        *,
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
        source_mode: str | None = None,
        selected_source: str | None = None,
    ) -> dict:
        return await self.reanalyze_use_case.execute(
            business_id=business_id,
            dataset_id=dataset_id,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
            source_mode=source_mode,
            selected_source=selected_source,
        )
