from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMCadenceStep, CRMCampaignStatus, CRMMessage, CRMMessageStatus


class LaunchCRMCampaignUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        parse_object_id: Callable[..., Any],
        resolve_cadence_template: Callable[[str | None], Awaitable[dict[str, Any]]],
        build_campaign_lead_query: Callable[[Any], dict[str, Any]],
        load_suppressed_emails: Callable[[], Awaitable[set[str]]],
        now_utc: Callable[[], datetime],
        normalize_email: Callable[[Any], str | None],
        build_mini_report_for_lead: Callable[..., Awaitable[str]],
        render_cadence_step: Callable[..., tuple[str, str]],
        record_event: Callable[..., Awaitable[None]],
        enqueue_due_campaign_dispatch_jobs: Callable[..., Awaitable[int]],
        campaigns_collection_name: str,
        leads_collection_name: str,
        messages_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._parse_object_id = parse_object_id
        self._resolve_cadence_template = resolve_cadence_template
        self._build_campaign_lead_query = build_campaign_lead_query
        self._load_suppressed_emails = load_suppressed_emails
        self._now_utc = now_utc
        self._normalize_email = normalize_email
        self._build_mini_report_for_lead = build_mini_report_for_lead
        self._render_cadence_step = render_cadence_step
        self._record_event = record_event
        self._enqueue_due_campaign_dispatch_jobs = enqueue_due_campaign_dispatch_jobs
        self._campaigns_collection_name = campaigns_collection_name
        self._leads_collection_name = leads_collection_name
        self._messages_collection_name = messages_collection_name

    async def execute(self, *, campaign_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        parsed_campaign_id = self._parse_object_id(campaign_id, field_name="campaign_id")
        database = get_database()
        campaigns = database[self._campaigns_collection_name]
        leads_collection = database[self._leads_collection_name]
        messages_collection = database[self._messages_collection_name]

        campaign = await campaigns.find_one({"_id": parsed_campaign_id})
        if campaign is None:
            raise LookupError(f"Campaign '{campaign_id}' not found.")

        status_value = str(campaign.get("status") or "").strip().lower()
        if status_value not in {CRMCampaignStatus.DRAFT.value, CRMCampaignStatus.PAUSED.value}:
            raise ValueError("Only draft or paused campaigns can be launched.")

        cadence_doc = await self._resolve_cadence_template(str(campaign.get("cadence_template_id") or ""))
        steps_raw = cadence_doc.get("steps") if isinstance(cadence_doc.get("steps"), list) else []
        cadence_steps = [CRMCadenceStep.model_validate(item) for item in steps_raw if isinstance(item, dict)]
        if not cadence_steps:
            raise ValueError("Campaign cadence has no valid steps.")

        lead_query = self._build_campaign_lead_query(campaign.get("audience_filter"))
        leads = await leads_collection.find(lead_query).sort([("updated_at", -1), ("_id", -1)]).to_list(length=2000)
        suppressed_emails = await self._load_suppressed_emails()

        created_messages = 0
        targeted_leads = 0
        now = self._now_utc()

        message_docs: list[dict[str, Any]] = []
        for lead in leads:
            email = str(lead.get("email") or "").strip()
            email_normalized = self._normalize_email(email)
            if not email or not email_normalized:
                continue
            if email_normalized in suppressed_emails:
                continue

            lead_id = str(lead.get("_id"))
            mini_report = await self._build_mini_report_for_lead(lead_doc=lead)
            targeted_leads += 1
            for step in cadence_steps:
                scheduled_at = now + timedelta(days=int(step.delay_days))
                rendered_subject, rendered_body = self._render_cadence_step(
                    step=step,
                    lead_doc=lead,
                    mini_report=mini_report,
                )
                message = CRMMessage(
                    campaign_id=campaign_id,
                    lead_id=lead_id,
                    step_order=int(step.step_order),
                    step_key=str(step.step_key),
                    scheduled_at=scheduled_at,
                    status=CRMMessageStatus.QUEUED,
                    to_email=email,
                    subject=rendered_subject,
                    body=rendered_body,
                    provider="resend",
                    created_at=now,
                    updated_at=now,
                )
                message_docs.append(message.model_dump(mode="python"))
                created_messages += 1

        if message_docs:
            await messages_collection.insert_many(message_docs)

        await campaigns.update_one(
            {"_id": parsed_campaign_id},
            {
                "$set": {
                    "status": CRMCampaignStatus.ACTIVE.value,
                    "launched_at": now,
                    "updated_at": now,
                    "metrics.targeted_leads": targeted_leads,
                    "metrics.messages_created": created_messages,
                }
            },
        )

        await self._record_event(
            event_type="campaign_launched",
            campaign_id=campaign_id,
            data={
                "targeted_leads": targeted_leads,
                "messages_created": created_messages,
                "cadence_template_id": str(cadence_doc.get("_id")),
            },
        )

        queued_dispatch_jobs = await self._enqueue_due_campaign_dispatch_jobs(campaign_id=campaign_id, limit=500)
        return {
            "campaign_id": campaign_id,
            "status": CRMCampaignStatus.ACTIVE.value,
            "targeted_leads": targeted_leads,
            "messages_created": created_messages,
            "dispatch_jobs_queued": queued_dispatch_jobs,
        }
