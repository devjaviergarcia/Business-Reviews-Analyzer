from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.workers.contracts import CRMCampaignDispatchTaskPayload


class EnqueueDueCampaignDispatchJobsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        job_service: Any,
        messages_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._job_service = job_service
        self._messages_collection_name = messages_collection_name

    async def execute(self, *, campaign_id: str | None = None, limit: int = 200) -> int:
        await self._ensure_indexes()
        messages = get_database()[self._messages_collection_name]
        now = self._now_utc()
        safe_limit = max(1, min(int(limit), 2000))

        query: dict[str, Any] = {
            "status": "queued",
            "scheduled_at": {"$lte": now},
            "dispatch_job_id": None,
        }
        if campaign_id:
            query["campaign_id"] = str(campaign_id)

        docs = (
            await messages.find(query)
            .sort([("scheduled_at", 1), ("_id", 1)])
            .limit(safe_limit)
            .to_list(length=safe_limit)
        )
        queued_jobs = 0
        for doc in docs:
            message_id = str(doc.get("_id"))
            current_campaign_id = str(doc.get("campaign_id") or "").strip()
            if not current_campaign_id:
                continue
            payload = CRMCampaignDispatchTaskPayload(
                campaign_id=current_campaign_id,
                message_id=message_id,
            )
            enqueue_result = await self._job_service.enqueue_job(
                task_payload=payload,
                queue_name="crm",
                job_type="crm_campaign_dispatch",
            )
            dispatch_job_id = str(enqueue_result.get("job_id") or "").strip() or None
            await messages.update_one(
                {"_id": doc.get("_id")},
                {
                    "$set": {
                        "dispatch_job_id": dispatch_job_id,
                        "updated_at": now,
                    }
                },
            )
            queued_jobs += 1
        return queued_jobs
