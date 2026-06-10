from __future__ import annotations

from typing import Awaitable, Callable

from src.crm.campaigns.campaign_workflow_runtime import CampaignWorkflowRuntime


class LaunchCRMCampaignUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        campaign_workflow_runtime: CampaignWorkflowRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._campaign_workflow_runtime = campaign_workflow_runtime

    async def execute(self, *, campaign_id: str) -> dict[str, object]:
        await self._ensure_indexes()
        return await self._campaign_workflow_runtime.launch_campaign(campaign_id=campaign_id)
