from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMCampaign, CRMCampaignStatus


class CreateCRMCampaignUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        resolve_cadence_template: Callable[[str | None], Awaitable[dict[str, Any]]],
        now_utc: Callable[[], datetime],
        record_event: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        campaigns_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._resolve_cadence_template = resolve_cadence_template
        self._now_utc = now_utc
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._campaigns_collection_name = campaigns_collection_name

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
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("Campaign name cannot be empty.")
        cadence_doc = await self._resolve_cadence_template(cadence_template_id)
        now = self._now_utc()

        campaign = CRMCampaign(
            name=clean_name,
            description=str(description or "").strip() or None,
            status=CRMCampaignStatus.DRAFT,
            source_mode=str(source_mode or "auto").strip().lower() or "auto",
            selected_source=(str(selected_source).strip().lower() if selected_source else None),
            cadence_template_id=str(cadence_doc.get("_id")),
            audience_filter=dict(audience_filter or {}),
            metrics={
                "targeted_leads": 0,
                "messages_created": 0,
                "messages_sent": 0,
                "messages_delivered": 0,
                "messages_opened": 0,
                "messages_clicked": 0,
                "messages_replied": 0,
                "messages_bounced": 0,
                "messages_unsubscribed": 0,
                "messages_failed": 0,
            },
            created_at=now,
            updated_at=now,
        )
        campaigns = get_database()[self._campaigns_collection_name]
        inserted = await campaigns.insert_one(campaign.model_dump(mode="python"))
        created_doc = await campaigns.find_one({"_id": inserted.inserted_id})
        if created_doc is None:
            raise RuntimeError("Campaign could not be loaded after insert.")

        campaign_id = str(inserted.inserted_id)
        await self._record_event(
            event_type="campaign_created",
            campaign_id=campaign_id,
            data={"name": campaign.name, "cadence_template_id": campaign.cadence_template_id},
        )
        return self._sanitize_payload(self._serialize_mongo_doc(created_doc, id_key="campaign_id"))
