from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMCadenceStep, CRMCampaign, CRMCampaignStatus, CRMMessage, CRMMessageStatus
from src.workers.contracts import CRMCampaignDispatchTaskPayload


ResolveCadenceTemplateFn = Callable[[str | None], Awaitable[dict[str, Any]]]
NowUtcFn = Callable[[], datetime]
RecordEventFn = Callable[..., Awaitable[None]]
SerializeMongoDocFn = Callable[[dict[str, Any], str], dict[str, Any]]
SanitizePayloadFn = Callable[[Any], Any]
ParseObjectIdFn = Callable[..., Any]
BuildCampaignLeadQueryFn = Callable[[Any], dict[str, Any]]
LoadSuppressedEmailsFn = Callable[[], Awaitable[set[str]]]
NormalizeEmailFn = Callable[[Any], str | None]
BuildMiniReportForLeadFn = Callable[..., Awaitable[str]]
RenderCadenceStepFn = Callable[..., tuple[str, str]]
EnqueueJobFn = Callable[..., Awaitable[dict[str, Any]]]


class CampaignWorkflowRuntime:
    def __init__(
        self,
        *,
        resolve_cadence_template: ResolveCadenceTemplateFn,
        now_utc: NowUtcFn,
        record_event: RecordEventFn,
        serialize_mongo_doc: SerializeMongoDocFn,
        sanitize_payload: SanitizePayloadFn,
        parse_object_id: ParseObjectIdFn,
        build_campaign_lead_query: BuildCampaignLeadQueryFn,
        load_suppressed_emails: LoadSuppressedEmailsFn,
        normalize_email: NormalizeEmailFn,
        build_mini_report_for_lead: BuildMiniReportForLeadFn,
        render_cadence_step: RenderCadenceStepFn,
        enqueue_job: EnqueueJobFn,
        campaigns_collection_name: str,
        leads_collection_name: str,
        messages_collection_name: str,
    ) -> None:
        self._resolve_cadence_template = resolve_cadence_template
        self._now_utc = now_utc
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._parse_object_id = parse_object_id
        self._build_campaign_lead_query = build_campaign_lead_query
        self._load_suppressed_emails = load_suppressed_emails
        self._normalize_email = normalize_email
        self._build_mini_report_for_lead = build_mini_report_for_lead
        self._render_cadence_step = render_cadence_step
        self._enqueue_job = enqueue_job
        self._campaigns_collection_name = campaigns_collection_name
        self._leads_collection_name = leads_collection_name
        self._messages_collection_name = messages_collection_name

    async def create_campaign(
        self,
        *,
        name: str,
        description: str | None = None,
        audience_filter: dict[str, Any] | None = None,
        source_mode: str = "auto",
        selected_source: str | None = None,
        cadence_template_id: str | None = None,
    ) -> dict[str, Any]:
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

    async def launch_campaign(self, *, campaign_id: str) -> dict[str, Any]:
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
            if not email or not email_normalized or email_normalized in suppressed_emails:
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

        queued_dispatch_jobs = await self.enqueue_due_campaign_dispatch_jobs(campaign_id=campaign_id, limit=500)
        return self._sanitize_payload(
            {
                "campaign_id": campaign_id,
                "status": CRMCampaignStatus.ACTIVE.value,
                "targeted_leads": targeted_leads,
                "messages_created": created_messages,
                "dispatch_jobs_queued": queued_dispatch_jobs,
            }
        )

    async def enqueue_due_campaign_dispatch_jobs(self, *, campaign_id: str | None = None, limit: int = 200) -> int:
        messages = get_database()[self._messages_collection_name]
        now = self._now_utc()
        safe_limit = max(1, min(int(limit), 2000))

        query: dict[str, Any] = {
            "status": CRMMessageStatus.QUEUED.value,
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
            enqueue_result = await self._enqueue_job(
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
