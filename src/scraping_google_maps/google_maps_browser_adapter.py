from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.scraping_shared.browser_scrape_adapter import ProgressCallback
from src.workers.contracts import AnalyzeBusinessTaskPayload

if TYPE_CHECKING:
    from src.services.business_service import BusinessService


class GoogleMapsBrowserAdapter:
    source = "google_maps"

    def __init__(self, *, business_service: "BusinessService") -> None:
        self._business_service = business_service

    async def run_scrape(
        self,
        *,
        task_payload: AnalyzeBusinessTaskPayload,
        job_id: str,
        progress_callback: ProgressCallback,
    ) -> dict[str, Any]:
        return await self._business_service.scrape_business_for_analysis_pipeline(
            name=task_payload.name,
            canonical_name=task_payload.canonical_name,
            source_name=task_payload.source_name,
            root_business_id=task_payload.root_business_id,
            force=bool(task_payload.force),
            strategy=task_payload.strategy,
            force_mode=task_payload.force_mode,
            interactive_max_rounds=task_payload.interactive_max_rounds,
            html_scroll_max_rounds=task_payload.html_scroll_max_rounds,
            html_stable_rounds=task_payload.html_stable_rounds,
            tripadvisor_max_pages=task_payload.tripadvisor_max_pages,
            tripadvisor_pages_percent=task_payload.tripadvisor_pages_percent,
            sources=("google_maps",),
            source_job_id=str(job_id),
            progress_callback=progress_callback,
        )
