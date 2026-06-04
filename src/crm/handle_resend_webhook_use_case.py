from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMMessageStatus


class HandleResendWebhookUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        parse_object_id: Callable[..., Any],
        block_lead_contact: Callable[..., Awaitable[None]],
        upsert_suppression: Callable[..., Awaitable[None]],
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        messages_collection_name: str,
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._parse_object_id = parse_object_id
        self._block_lead_contact = block_lead_contact
        self._upsert_suppression = upsert_suppression
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._messages_collection_name = messages_collection_name
        self._leads_collection_name = leads_collection_name

    async def execute(self, *, payload: dict[str, Any]) -> dict[str, Any]:
        await self._ensure_indexes()
        event_type = str(payload.get("type") or "").strip().lower()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        provider_message_id = str(data.get("email_id") or data.get("id") or payload.get("email_id") or "").strip()
        if not provider_message_id:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": "missing_provider_message_id"})

        database = get_database()
        messages = database[self._messages_collection_name]
        message_doc = await messages.find_one({"provider_message_id": provider_message_id})
        if message_doc is None:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": "message_not_found"})

        message_id = str(message_doc.get("_id"))
        lead_id = str(message_doc.get("lead_id") or "").strip() or None
        campaign_id = str(message_doc.get("campaign_id") or "").strip() or None
        now = self._now_utc()

        status_map: dict[str, tuple[str, str | None]] = {
            "email.sent": (CRMMessageStatus.SENT.value, "sent_at"),
            "email.delivered": (CRMMessageStatus.DELIVERED.value, "delivered_at"),
            "email.opened": (CRMMessageStatus.OPEN.value, "opened_at"),
            "email.clicked": (CRMMessageStatus.CLICK.value, "clicked_at"),
            "email.bounced": (CRMMessageStatus.BOUNCED.value, "bounced_at"),
            "email.complained": (CRMMessageStatus.BOUNCED.value, "bounced_at"),
        }

        set_fields: dict[str, Any] = {"updated_at": now}
        mapped = status_map.get(event_type)
        if mapped:
            set_fields["status"] = mapped[0]
            if mapped[1]:
                set_fields[mapped[1]] = now
        elif event_type in {"email.unsubscribed", "email.suppressed"}:
            set_fields["status"] = CRMMessageStatus.UNSUBSCRIBED.value
            set_fields["unsubscribed_at"] = now
        elif event_type in {"email.replied", "email.reply"}:
            set_fields["status"] = CRMMessageStatus.REPLIED.value
            set_fields["replied_at"] = now
        else:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": f"unsupported_event_type:{event_type}"})

        await messages.update_one({"_id": message_doc.get("_id")}, {"$set": set_fields})

        if lead_id and event_type in {
            "email.unsubscribed",
            "email.suppressed",
            "email.bounced",
            "email.complained",
            "email.replied",
            "email.reply",
        }:
            reason = (
                "unsubscribed"
                if event_type in {"email.unsubscribed", "email.suppressed"}
                else "bounced"
                if event_type in {"email.bounced", "email.complained"}
                else "replied"
            )
            await self._block_lead_contact(lead_id=lead_id, reason=reason)
            if reason in {"unsubscribed", "bounced"}:
                lead_doc = await database[self._leads_collection_name].find_one(
                    {"_id": self._parse_object_id(lead_id, field_name="lead_id")}
                )
                if isinstance(lead_doc, dict):
                    email = str(lead_doc.get("email") or "").strip()
                    if email:
                        await self._upsert_suppression(email=email, reason=reason, source="resend_webhook")

        await self._record_event(
            event_type="email_webhook_processed",
            lead_id=lead_id,
            campaign_id=campaign_id,
            message_id=message_id,
            data={"provider_message_id": provider_message_id, "event_type": event_type},
        )
        return self._sanitize_payload({"ok": True, "message_id": message_id, "event_type": event_type})
