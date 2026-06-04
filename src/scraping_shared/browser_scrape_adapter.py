from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol

from src.workers.contracts import AnalyzeBusinessTaskPayload

ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


class BrowserScrapeAdapter(Protocol):
    source: str

    async def run_scrape(
        self,
        *,
        task_payload: AnalyzeBusinessTaskPayload,
        job_id: str,
        progress_callback: ProgressCallback,
    ) -> dict[str, Any]:
        """Execute a browser-driven scrape for a single source."""

