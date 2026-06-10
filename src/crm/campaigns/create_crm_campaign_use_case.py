from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.campaigns.campaign_workflow_runtime import CampaignWorkflowRuntime


class CreateCRMCampaignUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        campaign_workflow_runtime: CampaignWorkflowRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._campaign_workflow_runtime = campaign_workflow_runtime

    async def execute(
        self,
        *,
        name: str,
        description: str | None = None,
        audience_filter: dict[str, Any] | None = None,
        source_mode: str = "auto",
        selected_source: str | None = None,
        cadence_template_id: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._campaign_workflow_runtime.create_campaign(
            name=name,
            description=description,
            audience_filter=audience_filter,
            source_mode=source_mode,
            selected_source=selected_source,
            cadence_template_id=cadence_template_id,
        )
