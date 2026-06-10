from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Awaitable, Callable

from src.models.crm import CRMMessageStatus
from src.workers.contracts import CRMCampaignDispatchTaskPayload


DatabaseFactory = Callable[[], Any]
ParseObjectIdFn = Callable[..., Any]
NowUtcFn = Callable[[], datetime]
SanitizePayloadFn = Callable[[Any], Any]
RecordEventFn = Callable[..., Awaitable[None]]
CanSendToLeadFn = Callable[..., Awaitable[tuple[bool, str]]]
SendResendEmailFn = Callable[..., dict[str, Any]]


class LegacyCampaignDispatchRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        parse_object_id: ParseObjectIdFn,
        now_utc: NowUtcFn,
        sanitize_payload: SanitizePayloadFn,
        record_event: RecordEventFn,
        can_send_to_lead: CanSendToLeadFn,
        send_resend_email: SendResendEmailFn,
        campaigns_collection_name: str,
        messages_collection_name: str,
        leads_collection_name: str,
    ) -> None:
        self._database_factory = database_factory
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._sanitize_payload = sanitize_payload
        self._record_event = record_event
        self._can_send_to_lead = can_send_to_lead
        self._send_resend_email = send_resend_email
        self._campaigns_collection_name = campaigns_collection_name
        self._messages_collection_name = messages_collection_name
        self._leads_collection_name = leads_collection_name

    async def process_campaign_dispatch_task(
        self,
        *,
        task_payload: CRMCampaignDispatchTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        database = self._database_factory()
        campaigns = database[self._campaigns_collection_name]
        messages = database[self._messages_collection_name]
        leads = database[self._leads_collection_name]

        message_id = self._parse_object_id(task_payload.message_id, field_name="message_id")
        campaign_id = self._parse_object_id(task_payload.campaign_id, field_name="campaign_id")

        message_doc = await messages.find_one({"_id": message_id, "campaign_id": task_payload.campaign_id})
        if message_doc is None:
            raise LookupError(f"Campaign message '{task_payload.message_id}' not found.")

        campaign_doc = await campaigns.find_one({"_id": campaign_id})
        if campaign_doc is None:
            raise LookupError(f"Campaign '{task_payload.campaign_id}' not found.")

        current_status = str(message_doc.get("status") or "").strip().lower()
        if current_status not in {CRMMessageStatus.QUEUED.value, CRMMessageStatus.FAILED.value}:
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": current_status,
                    "skipped": True,
                    "reason": "message_not_dispatchable",
                }
            )

        scheduled_at = (
            message_doc.get("scheduled_at")
            if isinstance(message_doc.get("scheduled_at"), datetime)
            else None
        )
        now = self._now_utc()
        if scheduled_at is not None and scheduled_at > now:
            await messages.update_one(
                {"_id": message_id},
                {"$set": {"dispatch_job_id": None, "updated_at": now}},
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.QUEUED.value,
                    "skipped": True,
                    "reason": "not_due_yet",
                }
            )

        lead_id = str(message_doc.get("lead_id") or "").strip()
        lead_doc = await leads.find_one({"_id": self._parse_object_id(lead_id, field_name="lead_id")})
        if lead_doc is None:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "status": CRMMessageStatus.FAILED.value,
                        "error": "lead_not_found",
                        "failed_at": now,
                        "updated_at": now,
                    }
                },
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.FAILED.value,
                    "reason": "lead_not_found",
                }
            )

        allowed, reason = await self._can_send_to_lead(lead_doc=lead_doc)
        if not allowed:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "status": CRMMessageStatus.SKIPPED.value,
                        "error": reason,
                        "updated_at": now,
                        "dispatch_job_id": None,
                    }
                },
            )
            await self._record_event(
                event_type="campaign_dispatch_skipped",
                campaign_id=task_payload.campaign_id,
                lead_id=lead_id,
                message_id=task_payload.message_id,
                data={"reason": reason},
            )
            return self._sanitize_payload(
                {
                    "campaign_id": task_payload.campaign_id,
                    "message_id": task_payload.message_id,
                    "status": CRMMessageStatus.SKIPPED.value,
                    "reason": reason,
                }
            )

        to_email = str(message_doc.get("to_email") or "").strip()
        subject = str(message_doc.get("subject") or "").strip()
        body = str(message_doc.get("body") or "").strip()

        send_result = await asyncio.to_thread(
            self._send_resend_email,
            to_email=to_email,
            subject=subject,
            html_body=body,
        )
        provider_message_id = str(send_result.get("id") or "").strip() or None

        await messages.update_one(
            {"_id": message_id},
            {
                "$set": {
                    "status": CRMMessageStatus.SENT.value,
                    "sent_at": now,
                    "provider_message_id": provider_message_id,
                    "provider_payload": send_result,
                    "dispatch_job_id": None,
                    "updated_at": now,
                    "error": None,
                }
            },
        )

        await campaigns.update_one(
            {"_id": campaign_id},
            {"$inc": {"metrics.messages_sent": 1}, "$set": {"updated_at": now}},
        )

        await self._record_event(
            event_type="campaign_message_sent",
            campaign_id=task_payload.campaign_id,
            lead_id=lead_id,
            message_id=task_payload.message_id,
            data={
                "provider": "resend",
                "provider_message_id": provider_message_id,
                "job_id": str(job_id) if job_id is not None else None,
            },
        )

        return self._sanitize_payload(
            {
                "campaign_id": task_payload.campaign_id,
                "message_id": task_payload.message_id,
                "status": CRMMessageStatus.SENT.value,
                "provider_message_id": provider_message_id,
                "provider_response": send_result,
            }
        )
