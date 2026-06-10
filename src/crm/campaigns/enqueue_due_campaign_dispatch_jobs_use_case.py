from __future__ import annotations

from typing import Awaitable, Callable

from src.crm.campaigns.campaign_workflow_runtime import CampaignWorkflowRuntime


class EnqueueDueCampaignDispatchJobsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        campaign_workflow_runtime: CampaignWorkflowRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._campaign_workflow_runtime = campaign_workflow_runtime

    async def execute(self, *, campaign_id: str | None = None, limit: int = 200) -> int:
        await self._ensure_indexes()
        return await self._campaign_workflow_runtime.enqueue_due_campaign_dispatch_jobs(
            campaign_id=campaign_id,
            limit=limit,
        )
