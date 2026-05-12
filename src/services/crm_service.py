from __future__ import annotations

import asyncio
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from email.utils import formataddr
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.config import settings
from src.database import get_database
from src.scraper.google_maps import GoogleMapsScraper
from src.scraper.selectors import SELECTOR_PATTERNS
from src.models.crm import (
    CRMCampaign,
    CRMCampaignStatus,
    CRMCadenceStep,
    CRMCadenceTemplate,
    CRMConsentProof,
    CRMConsentStatus,
    CRMEvent,
    CRMLead,
    CRMLeadLegalBlock,
    CRMLeadPipelineRefs,
    CRMLeadStatus,
    CRMMessage,
    CRMMessageStatus,
    CRMSuppression,
)
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_service import BusinessService
from src.services.pagination import build_pagination_payload, coerce_pagination
from src.workers.contracts import (
    CRMCampaignDispatchTaskPayload,
    CRMLeadDiscoveryTaskPayload,
    CRMLeadPipelineTaskPayload,
)


class CRMService:
    _LEADS_COLLECTION = "crm_leads"
    _CAMPAIGNS_COLLECTION = "crm_campaigns"
    _CADENCE_COLLECTION = "crm_cadence_templates"
    _MESSAGES_COLLECTION = "crm_messages"
    _EVENTS_COLLECTION = "crm_events"
    _SUPPRESSIONS_COLLECTION = "crm_suppressions"
    _RESEARCH_LEADS_COLLECTION = "research_leads"
    _BUSINESSES_COLLECTION = "businesses"
    _ANALYSES_COLLECTION = "analyses"
    _JOBS_COLLECTION = "analysis_jobs"

    _DEFAULT_CADENCE_KEY = "default_optin_3touch"
    _ALLOWED_SOURCES = ("google_maps", "tripadvisor")
    _LIVE_GOOGLE_DISCOVERY_SOURCES = (
        "live_google_maps",
        "google_maps_live",
        "auto_live_google_maps",
        "live_auto_google_maps",
    )

    def __init__(
        self,
        *,
        job_service: AnalysisJobService | None = None,
        business_service: BusinessService | None = None,
    ) -> None:
        self.job_service = job_service or AnalysisJobService()
        self.business_service = business_service or BusinessService(job_service=self.job_service)
        self._indexes_ensured = False
        self._indexes_lock = asyncio.Lock()

    async def ensure_indexes(self) -> None:
        if self._indexes_ensured:
            return
        async with self._indexes_lock:
            if self._indexes_ensured:
                return
            database = get_database()
            leads = database[self._LEADS_COLLECTION]
            campaigns = database[self._CAMPAIGNS_COLLECTION]
            cadence = database[self._CADENCE_COLLECTION]
            messages = database[self._MESSAGES_COLLECTION]
            events = database[self._EVENTS_COLLECTION]
            suppressions = database[self._SUPPRESSIONS_COLLECTION]

            await leads.create_index(
                [("email_normalized", 1)],
                name="idx_crm_leads_email_partial_unique",
                unique=True,
                partialFilterExpression={"email_normalized": {"$type": "string"}},
            )
            await leads.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_leads_status_updated",
            )
            await leads.create_index(
                [("business_name_normalized", 1), ("address", 1)],
                name="idx_crm_leads_name_address",
            )
            await leads.create_index(
                [("legal.consent_status", 1), ("legal.do_not_contact", 1)],
                name="idx_crm_leads_legal",
            )

            await campaigns.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_campaign_status_updated",
            )

            await cadence.create_index(
                [("key", 1)],
                name="idx_crm_cadence_key_unique",
                unique=True,
            )

            await messages.create_index(
                [("campaign_id", 1), ("scheduled_at", 1), ("status", 1)],
                name="idx_crm_messages_campaign_schedule_status",
            )
            await messages.create_index(
                [("provider_message_id", 1)],
                name="idx_crm_messages_provider_id",
                sparse=True,
            )
            await messages.create_index(
                [("lead_id", 1), ("status", 1)],
                name="idx_crm_messages_lead_status",
            )

            await events.create_index(
                [("lead_id", 1), ("created_at", -1)],
                name="idx_crm_events_lead_created",
            )
            await events.create_index(
                [("campaign_id", 1), ("created_at", -1)],
                name="idx_crm_events_campaign_created",
            )

            await suppressions.create_index(
                [("email_normalized", 1)],
                name="idx_crm_suppressions_email_unique",
                unique=True,
            )

            self._indexes_ensured = True

    async def enqueue_lead_discovery_job(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
    ) -> dict[str, Any]:
        payload = CRMLeadDiscoveryTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=source,
        )
        return await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="crm_lead_discovery",
        )

    async def enqueue_lead_pipeline_job(
        self,
        *,
        lead_id: str,
        force: bool = False,
        sources: list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        payload = CRMLeadPipelineTaskPayload(
            lead_id=lead_id,
            force=force,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )
        queued_job = await self.job_service.enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="crm_lead_pipeline",
        )
        now = self._now_utc()
        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_QUEUED.value,
                    "updated_at": now,
                }
            },
        )
        await self._record_event(
            event_type="lead_pipeline_job_queued",
            lead_id=lead_id,
            data={
                "crm_job_id": queued_job.get("job_id"),
                "sources": payload.sources,
                "force": payload.force,
            },
        )
        return self._sanitize_payload(queued_job)

    async def list_leads(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        consent_filter: str | None = None,
        source_filter: str | None = None,
        q: str | None = None,
        sort_by: str = "updated_at",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        leads = get_database()[self._LEADS_COLLECTION]
        query = self._build_leads_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )
        sort_spec = self._resolve_leads_sort(sort_by=sort_by, sort_dir=sort_dir)

        total = await leads.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await leads.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="lead_id") for doc in docs]
        payload = build_pagination_payload(
            items=items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        return self._sanitize_payload(payload)

    async def bulk_delete_leads(
        self,
        *,
        lead_ids: list[str] | None = None,
        delete_all_matching: bool = False,
        exclude_lead_ids: list[str] | None = None,
        status_filter: str | None = None,
        consent_filter: str | None = None,
        source_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        leads = get_database()[self._LEADS_COLLECTION]

        normalized_ids: list[ObjectId] = []
        raw_ids = list(lead_ids or [])
        seen_ids: set[str] = set()
        for raw_id in raw_ids:
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(self._parse_object_id(normalized, field_name="lead_id"))

        excluded_ids: list[ObjectId] = []
        raw_excluded_ids = list(exclude_lead_ids or [])
        seen_excluded_ids: set[str] = set()
        for raw_id in raw_excluded_ids:
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_excluded_ids:
                continue
            seen_excluded_ids.add(normalized)
            excluded_ids.append(self._parse_object_id(normalized, field_name="exclude_lead_id"))

        if not normalized_ids and not bool(delete_all_matching):
            raise ValueError("Specify lead_ids or set delete_all_matching=true.")

        if normalized_ids:
            query: dict[str, Any] = {"_id": {"$in": normalized_ids}}
        else:
            query = self._build_leads_query(
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
            if excluded_ids:
                query["_id"] = {"$nin": excluded_ids}

        matched_count = await leads.count_documents(query)
        deleted_result = await leads.delete_many(query)
        deleted_count = int(deleted_result.deleted_count)
        await self._record_event(
            event_type="leads_bulk_deleted",
            data={
                "delete_all_matching": bool(delete_all_matching),
                "requested_ids": len(normalized_ids),
                "excluded_ids": len(excluded_ids),
                "matched_count": int(matched_count),
                "deleted_count": deleted_count,
                "filters": {
                    "status": str(status_filter or "").strip() or None,
                    "consent_status": str(consent_filter or "").strip() or None,
                    "source": str(source_filter or "").strip() or None,
                    "q": str(q or "").strip() or None,
                },
            },
        )
        return self._sanitize_payload(
            {
                "deleted_count": deleted_count,
                "matched_count": int(matched_count),
                "delete_all_matching": bool(delete_all_matching),
                "requested_ids": len(normalized_ids),
                "excluded_ids": len(excluded_ids),
            }
        )

    async def get_lead(self, *, lead_id: str, sync_pipeline_refs: bool = True) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        if sync_pipeline_refs:
            await self.sync_lead_pipeline_refs(lead_id=lead_id)
            lead_doc = await leads.find_one({"_id": parsed_lead_id})
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

    def _build_leads_query(
        self,
        *,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        normalized_consent = str(consent_filter or "").strip().lower()
        if normalized_consent:
            query["legal.consent_status"] = normalized_consent

        normalized_source = str(source_filter or "").strip().lower()
        if normalized_source:
            query["source"] = normalized_source

        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"website": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]
        return query

    def _resolve_leads_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        normalized_sort_by = str(sort_by or "updated_at").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("Invalid sort_dir. Use 'asc' or 'desc'.")

        field_map = {
            "updated_at": "updated_at",
            "business_name": "business_name_normalized",
            "score": "score",
        }
        field_name = field_map.get(normalized_sort_by)
        if field_name is None:
            raise ValueError("Invalid sort_by. Use 'updated_at', 'business_name' or 'score'.")

        direction = -1 if normalized_sort_dir == "desc" else 1
        return [(field_name, direction), ("_id", direction)]

    async def update_lead(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]

        set_fields: dict[str, Any] = {}
        now = self._now_utc()

        if "status" in updates and updates.get("status") is not None:
            set_fields["status"] = str(updates.get("status")).strip().lower()

        for text_field in ("business_name", "email", "phone", "website", "category", "city", "address"):
            if text_field in updates:
                value = updates.get(text_field)
                if value is None:
                    continue
                cleaned = str(value).strip()
                set_fields[text_field] = cleaned or None

        if "email" in set_fields:
            email_norm = self._normalize_email(set_fields.get("email"))
            set_fields["email_normalized"] = email_norm
            set_fields["domain_normalized"] = self._domain_from_email_or_website(
                email=set_fields.get("email"), website=set_fields.get("website")
            )

        if "business_name" in set_fields and set_fields.get("business_name"):
            set_fields["business_name_normalized"] = self._normalize_text(str(set_fields["business_name"]))

        if "do_not_contact" in updates:
            set_fields["legal.do_not_contact"] = bool(updates.get("do_not_contact"))

        if "consent_status" in updates and updates.get("consent_status") is not None:
            set_fields["legal.consent_status"] = str(updates.get("consent_status")).strip().lower()

        if "suppressed_reason" in updates:
            reason = str(updates.get("suppressed_reason") or "").strip()
            set_fields["legal.suppressed_reason"] = reason or None

        consent_proof_payload = updates.get("consent_proof")
        if isinstance(consent_proof_payload, dict):
            proof = CRMConsentProof.model_validate(consent_proof_payload)
            set_fields["legal.consent_proof"] = proof.model_dump(mode="python")
            set_fields["legal.consent_status"] = CRMConsentStatus.GRANTED.value

        if "unsubscribed" in updates:
            unsubscribed = bool(updates.get("unsubscribed"))
            set_fields["legal.unsubscribed_at"] = now if unsubscribed else None
            if unsubscribed:
                set_fields["legal.do_not_contact"] = True
                set_fields["legal.suppressed_reason"] = "unsubscribed"

        if not set_fields:
            return await self.get_lead(lead_id=lead_id, sync_pipeline_refs=False)

        set_fields["updated_at"] = now
        updated = await leads.find_one_and_update(
            {"_id": parsed_lead_id},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        await self._record_event(
            event_type="lead_updated",
            lead_id=lead_id,
            data={"fields": sorted(set_fields.keys())},
        )

        if set_fields.get("legal.suppressed_reason") or set_fields.get("legal.do_not_contact"):
            email_norm = self._normalize_email(updated.get("email"))
            email_value = str(updated.get("email") or "").strip()
            if email_norm and email_value:
                await self._upsert_suppression(
                    email=email_value,
                    reason=str(set_fields.get("legal.suppressed_reason") or "manual"),
                    source="manual",
                )

        return self._sanitize_payload(self._serialize_mongo_doc(updated, id_key="lead_id"))

    async def list_campaigns(
        self,
        *,
        page: int = 1,
        page_size: int = 30,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        campaigns = get_database()[self._CAMPAIGNS_COLLECTION]

        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        total = await campaigns.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await campaigns.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="campaign_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

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
        await self.ensure_indexes()
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
        campaigns = get_database()[self._CAMPAIGNS_COLLECTION]
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
        await self.ensure_indexes()
        parsed_campaign_id = self._parse_object_id(campaign_id, field_name="campaign_id")
        database = get_database()
        campaigns = database[self._CAMPAIGNS_COLLECTION]
        leads_collection = database[self._LEADS_COLLECTION]
        messages_collection = database[self._MESSAGES_COLLECTION]

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
        await self.ensure_indexes()
        database = get_database()
        messages = database[self._MESSAGES_COLLECTION]
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
            enqueue_result = await self.job_service.enqueue_job(
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

    async def list_messages(
        self,
        *,
        campaign_id: str | None = None,
        lead_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        messages = get_database()[self._MESSAGES_COLLECTION]
        query: dict[str, Any] = {}
        if campaign_id:
            query["campaign_id"] = str(campaign_id)
        if lead_id:
            query["lead_id"] = str(lead_id)

        total = await messages.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await messages.find(query)
            .sort([("scheduled_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="message_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def list_events(
        self,
        *,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        events = get_database()[self._EVENTS_COLLECTION]
        query: dict[str, Any] = {}
        if lead_id:
            query["lead_id"] = str(lead_id)
        if campaign_id:
            query["campaign_id"] = str(campaign_id)

        total = await events.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await events.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="event_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def handle_resend_webhook(self, *, payload: dict[str, Any]) -> dict[str, Any]:
        await self.ensure_indexes()
        event_type = str(payload.get("type") or "").strip().lower()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        provider_message_id = str(
            data.get("email_id")
            or data.get("id")
            or payload.get("email_id")
            or ""
        ).strip()
        if not provider_message_id:
            return self._sanitize_payload({"ok": True, "ignored": True, "reason": "missing_provider_message_id"})

        messages = get_database()[self._MESSAGES_COLLECTION]
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

        if lead_id and event_type in {"email.unsubscribed", "email.suppressed", "email.bounced", "email.complained", "email.replied", "email.reply"}:
            reason = (
                "unsubscribed"
                if event_type in {"email.unsubscribed", "email.suppressed"}
                else "bounced"
                if event_type in {"email.bounced", "email.complained"}
                else "replied"
            )
            await self._block_lead_contact(lead_id=lead_id, reason=reason)
            if reason in {"unsubscribed", "bounced"}:
                lead_doc = await get_database()[self._LEADS_COLLECTION].find_one({"_id": self._parse_object_id(lead_id, field_name="lead_id")})
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

    async def process_discovery_task(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        candidates = await self._discover_candidates(task_payload=task_payload)

        inserted = 0
        updated = 0
        skipped = 0
        for candidate in candidates:
            action = await self._upsert_lead_candidate(candidate)
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
            else:
                skipped += 1

        await self._record_event(
            event_type="lead_discovery_processed",
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            },
        )

        return self._sanitize_payload(
            {
                "query": task_payload.query,
                "city": task_payload.city,
                "category": task_payload.category,
                "limit": task_payload.limit,
                "source": task_payload.source,
                "candidates": len(candidates),
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
            }
        )

    async def process_lead_pipeline_task(
        self,
        *,
        task_payload: CRMLeadPipelineTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(task_payload.lead_id, field_name="lead_id")
        leads = get_database()[self._LEADS_COLLECTION]
        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{task_payload.lead_id}' not found.")

        business_name = str(lead_doc.get("business_name") or "").strip()
        if not business_name:
            raise ValueError("Lead has no business_name to run pipeline.")

        requested_sources = tuple(task_payload.sources)
        used_sources = requested_sources
        fallback_reason: str | None = None

        try:
            queue_result = await self.business_service.enqueue_business_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=requested_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
            )
        except RuntimeError as exc:
            error_text = str(exc)
            can_fallback_to_google = (
                "tripadvisor" in requested_sources
                and "google_maps" in requested_sources
                and (
                    "Tripadvisor local worker bridge is unreachable" in error_text
                    or "TRIPADVISOR_LOCAL_WORKER_BRIDGE_ENABLED" in error_text
                )
            )
            if not can_fallback_to_google:
                raise
            used_sources = ("google_maps",)
            fallback_reason = error_text
            queue_result = await self.business_service.enqueue_business_scrape_jobs(
                name=business_name,
                force=bool(task_payload.force),
                sources=used_sources,
                google_maps_name=task_payload.google_maps_name,
                tripadvisor_name=task_payload.tripadvisor_name,
            )

        jobs_by_source = queue_result.get("jobs_by_source") if isinstance(queue_result.get("jobs_by_source"), dict) else {}
        source_job_ids: list[str] = []
        for source_name in self._ALLOWED_SOURCES:
            source_job = jobs_by_source.get(source_name) if isinstance(jobs_by_source, dict) else None
            if isinstance(source_job, dict):
                source_job_id = str(source_job.get("job_id") or "").strip()
                if source_job_id:
                    source_job_ids.append(source_job_id)

        root_business_id = str(queue_result.get("business_id") or "").strip() or None
        now = self._now_utc()

        await leads.update_one(
            {"_id": parsed_lead_id},
            {
                "$set": {
                    "status": CRMLeadStatus.PIPELINE_RUNNING.value,
                    "pipeline.business_id": root_business_id,
                    "pipeline.source_job_ids": source_job_ids,
                    "updated_at": now,
                }
            },
        )

        await self._record_event(
            event_type="lead_pipeline_started",
            lead_id=task_payload.lead_id,
            data={
                "job_id": str(job_id) if job_id is not None else None,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            },
        )

        return self._sanitize_payload(
            {
                "lead_id": task_payload.lead_id,
                "business_name": business_name,
                "pipeline_root_business_id": root_business_id,
                "source_job_ids": source_job_ids,
                "requested_sources": list(requested_sources),
                "used_sources": list(used_sources),
                "fallback_reason": fallback_reason,
                "jobs_by_source": jobs_by_source,
            }
        )

    async def process_campaign_dispatch_task(
        self,
        *,
        task_payload: CRMCampaignDispatchTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        database = get_database()
        campaigns = database[self._CAMPAIGNS_COLLECTION]
        messages = database[self._MESSAGES_COLLECTION]
        leads = database[self._LEADS_COLLECTION]

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

        scheduled_at = message_doc.get("scheduled_at") if isinstance(message_doc.get("scheduled_at"), datetime) else None
        now = self._now_utc()
        if scheduled_at is not None and scheduled_at > now:
            await messages.update_one(
                {"_id": message_id},
                {
                    "$set": {
                        "dispatch_job_id": None,
                        "updated_at": now,
                    }
                },
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
            {
                "$inc": {"metrics.messages_sent": 1},
                "$set": {"updated_at": now},
            },
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

    async def sync_lead_pipeline_refs(self, *, lead_id: str) -> dict[str, Any]:
        await self.ensure_indexes()
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        database = get_database()
        leads = database[self._LEADS_COLLECTION]
        jobs = database[self._JOBS_COLLECTION]

        lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        source_job_ids = pipeline.get("source_job_ids") if isinstance(pipeline.get("source_job_ids"), list) else []
        source_job_ids = [str(item).strip() for item in source_job_ids if str(item).strip()]
        if not source_job_ids:
            return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

        analysis_job_doc = (
            await jobs.find(
                {
                    "queue_name": "analysis",
                    "job_type": "analysis_generate",
                    "payload.source_job_id": {"$in": source_job_ids},
                }
            )
            .sort([("updated_at", -1), ("_id", -1)])
            .limit(1)
            .to_list(length=1)
        )
        latest_analysis_job = analysis_job_doc[0] if analysis_job_doc else None

        report_job_doc: dict[str, Any] | None = None
        if latest_analysis_job is not None:
            analysis_job_id = str(latest_analysis_job.get("_id"))
            report_docs = (
                await jobs.find(
                    {
                        "queue_name": "report",
                        "job_type": "report_generate",
                        "payload.source_job_id": analysis_job_id,
                    }
                )
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(1)
                .to_list(length=1)
            )
            report_job_doc = report_docs[0] if report_docs else None

        update_fields: dict[str, Any] = {}
        if latest_analysis_job is not None:
            update_fields["pipeline.analysis_job_id"] = str(latest_analysis_job.get("_id"))
            update_fields["status"] = CRMLeadStatus.PIPELINE_DONE.value

        if report_job_doc is not None:
            update_fields["pipeline.report_job_id"] = str(report_job_doc.get("_id"))
            report_result = report_job_doc.get("result") if isinstance(report_job_doc.get("result"), dict) else {}
            artifacts = report_result.get("artifacts") if isinstance(report_result.get("artifacts"), dict) else {}
            update_fields["pipeline.latest_report_artifacts"] = artifacts

        if update_fields:
            update_fields["updated_at"] = self._now_utc()
            updated = await leads.find_one_and_update(
                {"_id": parsed_lead_id},
                {"$set": update_fields},
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                lead_doc = updated

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

    async def _resolve_cadence_template(self, cadence_template_id: str | None) -> dict[str, Any]:
        await self._ensure_default_cadence_template()
        cadence = get_database()[self._CADENCE_COLLECTION]

        normalized_id = str(cadence_template_id or "").strip()
        if normalized_id:
            try:
                doc = await cadence.find_one({"_id": ObjectId(normalized_id)})
            except InvalidId:
                doc = await cadence.find_one({"key": normalized_id})
            if doc is not None:
                return doc

        fallback = await cadence.find_one({"key": self._DEFAULT_CADENCE_KEY})
        if fallback is None:
            raise RuntimeError("Default cadence template is missing.")
        return fallback

    async def _ensure_default_cadence_template(self) -> None:
        cadence = get_database()[self._CADENCE_COLLECTION]
        now = self._now_utc()

        default_steps = [
            CRMCadenceStep(
                step_order=1,
                step_key="d0_intro",
                delay_days=0,
                subject_template="{business_name}: te comparto un mini informe de reputación",
                body_template=(
                    "Hola,\n\n"
                    "Hemos revisado la reputación online de {business_name}.\n"
                    "Resumen rápido:\n"
                    "{mini_report}\n\n"
                    "Si te encaja, te enseño en 15 minutos cómo mejorar estos puntos.\n"
                    "{cta_url}\n\n"
                    "Si no quieres recibir más mensajes, puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=2,
                step_key="d3_recordatorio",
                delay_days=3,
                subject_template="{business_name}: un dato clave para mejorar tu reputación",
                body_template=(
                    "Hola de nuevo,\n\n"
                    "Te comparto un insight adicional de {business_name}:\n"
                    "{mini_report}\n\n"
                    "Si quieres, te lo explico en una demo corta:\n"
                    "{cta_url}\n\n"
                    "Baja automática: {unsubscribe_url}\n"
                ),
            ),
            CRMCadenceStep(
                step_order=3,
                step_key="d7_cierre",
                delay_days=7,
                subject_template="Cierro hilo: {business_name}",
                body_template=(
                    "Último mensaje por aquí, prometido.\n\n"
                    "Si en otro momento quieres revisar el informe de {business_name},"
                    " aquí tienes acceso directo:\n"
                    "{cta_url}\n\n"
                    "Puedes darte de baja aquí: {unsubscribe_url}\n"
                ),
            ),
        ]

        template = CRMCadenceTemplate(
            key=self._DEFAULT_CADENCE_KEY,
            name="Cadencia opt-in 3 toques (D0/D+3/D+7)",
            locale="es-ES",
            is_default=True,
            steps=default_steps,
            created_at=now,
            updated_at=now,
        )
        payload = template.model_dump(mode="python")
        await cadence.update_one(
            {"key": self._DEFAULT_CADENCE_KEY},
            {
                "$set": {
                    "name": payload["name"],
                    "locale": payload["locale"],
                    "is_default": True,
                    "steps": payload["steps"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "key": self._DEFAULT_CADENCE_KEY,
                },
            },
            upsert=True,
        )

    def _build_campaign_lead_query(self, audience_filter: Any) -> dict[str, Any]:
        filters = dict(audience_filter or {}) if isinstance(audience_filter, dict) else {}

        query: dict[str, Any] = {
            "legal.consent_status": CRMConsentStatus.GRANTED.value,
            "legal.do_not_contact": {"$ne": True},
            "legal.unsubscribed_at": None,
        }

        lead_statuses = filters.get("lead_statuses")
        if isinstance(lead_statuses, list):
            normalized_statuses = [str(item).strip().lower() for item in lead_statuses if str(item).strip()]
            if normalized_statuses:
                query["status"] = {"$in": normalized_statuses}

        city = str(filters.get("city") or "").strip()
        if city:
            query["city"] = {"$regex": re.escape(city), "$options": "i"}

        category = str(filters.get("category") or "").strip()
        if category:
            query["category"] = {"$regex": re.escape(category), "$options": "i"}

        lead_ids = filters.get("lead_ids")
        if isinstance(lead_ids, list):
            parsed_ids: list[ObjectId] = []
            for raw_id in lead_ids:
                raw = str(raw_id or "").strip()
                if not raw:
                    continue
                try:
                    parsed_ids.append(ObjectId(raw))
                except InvalidId:
                    continue
            if parsed_ids:
                query["_id"] = {"$in": parsed_ids}

        return query

    async def _load_suppressed_emails(self) -> set[str]:
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        docs = await suppressions.find({}, projection={"email_normalized": 1}).to_list(length=50000)
        values: set[str] = set()
        for doc in docs:
            normalized = self._normalize_email(doc.get("email_normalized"))
            if normalized:
                values.add(normalized)
        return values

    async def _discover_candidates(self, *, task_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or "").strip()
        normalized_query = self._normalize_text(query_text)
        normalized_city = self._normalize_text(task_payload.city) if task_payload.city else None
        normalized_category = self._normalize_text(task_payload.category) if task_payload.category else None
        safe_limit = max(1, min(int(task_payload.limit), 5000))

        normalized_source = str(task_payload.source or "").strip().lower()
        if normalized_source in self._LIVE_GOOGLE_DISCOVERY_SOURCES:
            live_candidates = await self._discover_candidates_live_google_maps(
                task_payload=task_payload,
                normalized_query=normalized_query,
                normalized_city=normalized_city,
                normalized_category=normalized_category,
                safe_limit=safe_limit,
            )
            if normalized_source == "live_google_maps":
                return live_candidates[:safe_limit]

            # "auto_live_*": if live returns little/no data, top-up from stored sources.
            if len(live_candidates) >= safe_limit:
                return live_candidates[:safe_limit]
            remaining = safe_limit - len(live_candidates)
            fallback_candidates = await self._discover_candidates_from_stored_sources(
                task_payload=task_payload,
                normalized_query=normalized_query,
                normalized_city=normalized_city,
                normalized_category=normalized_category,
                safe_limit=remaining,
            )
            merged = live_candidates + fallback_candidates
            return merged[:safe_limit]

        return await self._discover_candidates_from_stored_sources(
            task_payload=task_payload,
            normalized_query=normalized_query,
            normalized_city=normalized_city,
            normalized_category=normalized_category,
            safe_limit=safe_limit,
        )

    async def _discover_candidates_from_stored_sources(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        normalized_city: str | None,
        normalized_category: str | None,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        database = get_database()
        candidates: list[dict[str, Any]] = []

        sources_to_scan: list[str]
        normalized_source = str(task_payload.source or "").strip().lower()
        if normalized_source in {"research_google_maps", "research"}:
            sources_to_scan = ["research"]
        elif normalized_source in {"businesses", "existing_businesses"}:
            sources_to_scan = ["businesses"]
        elif normalized_source in {"auto", "all", ""}:
            sources_to_scan = ["research", "businesses"]
        else:
            sources_to_scan = ["research", "businesses"]

        if "research" in sources_to_scan:
            research = database[self._RESEARCH_LEADS_COLLECTION]
            research_query: dict[str, Any] = {}
            if normalized_category:
                research_query["category"] = {"$regex": re.escape(task_payload.category or ""), "$options": "i"}
            if normalized_city:
                research_query["$or"] = [
                    {"address": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                    {"term_key": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                ]

            raw_docs = (
                await research.find(research_query)
                .sort([("processed_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in raw_docs:
                name = str(doc.get("name") or "").strip()
                if not name:
                    continue
                searchable = self._normalize_text(" ".join([
                    name,
                    str(doc.get("address") or ""),
                    str(doc.get("category") or ""),
                    str(doc.get("term_key") or ""),
                ]))
                if normalized_query and normalized_query not in searchable:
                    continue

                candidates.append(
                    {
                        "business_name": name,
                        "category": str(doc.get("category") or "").strip() or None,
                        "address": str(doc.get("address") or "").strip() or None,
                        "city": self._extract_city_from_address(str(doc.get("address") or "").strip() or None),
                        "phone": str(doc.get("phone") or "").strip() or None,
                        "email": str(doc.get("email") or "").strip() or None,
                        "website": str(doc.get("website") or "").strip() or None,
                        "source": "research_google_maps",
                        "source_ref": {
                            "listing_id": str(doc.get("listing_id") or "").strip() or None,
                            "term_id": str(doc.get("term_id") or "").strip() or None,
                            "term_key": str(doc.get("term_key") or "").strip() or None,
                            "maps_url": str(doc.get("maps_url") or "").strip() or None,
                        },
                        "rating": doc.get("rating"),
                        "review_count": doc.get("review_count"),
                    }
                )
                if len(candidates) >= safe_limit:
                    return candidates

        if "businesses" in sources_to_scan and len(candidates) < safe_limit:
            businesses = database[self._BUSINESSES_COLLECTION]
            business_query: dict[str, Any] = {}
            if normalized_category:
                business_query["listing.categories"] = {"$regex": re.escape(task_payload.category or ""), "$options": "i"}

            business_docs = (
                await businesses.find(business_query)
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in business_docs:
                name = str(doc.get("name") or "").strip()
                if not name:
                    continue
                listing = doc.get("listing") if isinstance(doc.get("listing"), dict) else {}
                address = str(listing.get("address") or "").strip() or None
                searchable = self._normalize_text(" ".join([
                    name,
                    str(address or ""),
                    " ".join([str(item) for item in (listing.get("categories") or []) if str(item).strip()]),
                ]))
                if normalized_query and normalized_query not in searchable:
                    continue
                if normalized_city and normalized_city not in self._normalize_text(address):
                    continue

                candidates.append(
                    {
                        "business_name": name,
                        "category": ", ".join([str(item) for item in (listing.get("categories") or []) if str(item).strip()]) or None,
                        "address": address,
                        "city": self._extract_city_from_address(address),
                        "phone": str(listing.get("phone") or "").strip() or None,
                        "email": None,
                        "website": str(listing.get("website") or "").strip() or None,
                        "source": str(doc.get("source") or "google_maps"),
                        "source_ref": {
                            "business_id": str(doc.get("_id")),
                            "name_normalized": str(doc.get("name_normalized") or "").strip() or None,
                        },
                        "rating": listing.get("overall_rating"),
                        "review_count": listing.get("total_reviews"),
                    }
                )
                if len(candidates) >= safe_limit:
                    return candidates

        return candidates[:safe_limit]

    async def _discover_candidates_live_google_maps(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        normalized_city: str | None,
        normalized_category: str | None,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        query_text = str(task_payload.query or "").strip()
        city_text = str(task_payload.city or "").strip()
        search_query = query_text if not city_text else f"{query_text} {city_text}".strip()
        if not search_query:
            return []

        scraper = BusinessService.build_default_scraper()
        max_scroll_rounds = min(180, max(20, int(safe_limit // 2) + 10))
        scroll_wait_ms = max(400, int(settings.scraper_html_scroll_min_interval_s * 1000))
        collected: dict[str, dict[str, Any]] = {}

        try:
            await scraper.start()
            await self._search_google_maps_query(scraper=scraper, query=search_query)
            feed_found = await self._wait_for_results_feed(scraper=scraper, timeout_ms=16_000)
            if not feed_found:
                listing_name = ""
                for selector in SELECTOR_PATTERNS["BUSINESS_NAME"]:
                    locator = scraper.page.locator(selector).first
                    try:
                        if await locator.is_visible():
                            listing_name = str(await locator.inner_text()).strip()
                            break
                    except Exception:
                        continue

                current_url = str(scraper.page.url or "").strip()
                if listing_name and "/maps/place/" in current_url:
                    canonical_url = self._canonicalize_maps_url(current_url)
                    fallback_candidates = [
                        {
                            "business_name": listing_name,
                            "category": str(task_payload.category or "").strip() or None,
                            "address": None,
                            "city": str(task_payload.city or "").strip() or None,
                            "phone": None,
                            "email": None,
                            "website": None,
                            "source": "google_maps_live_discovery",
                            "source_ref": {
                                "maps_url": current_url,
                                "maps_url_canonical": canonical_url or current_url,
                                "discovery_query": search_query,
                                "source_card_label": None,
                                "discovery_mode": "live_google_maps_auto_scroll",
                            },
                            "rating": None,
                            "review_count": None,
                        }
                    ]
                    return await self._enrich_live_google_maps_candidates(
                        scraper=scraper,
                        candidates=fallback_candidates,
                    )
                return []

            stable_rounds = 0
            for _ in range(max_scroll_rounds):
                before = len(collected)
                visible_items = await self._collect_visible_google_maps_results(scraper=scraper)
                for item in visible_items:
                    name = str(item.get("name") or "").strip()
                    raw_url = str(item.get("maps_url") or "").strip()
                    canonical_url = self._canonicalize_maps_url(raw_url)
                    if not name or not canonical_url:
                        continue
                    key = f"{canonical_url}|{self._normalize_text(name)}"
                    if key in collected:
                        continue
                    source_card_label = str(item.get("source_card_label") or "").strip() or None
                    searchable = self._normalize_text(" ".join([name, source_card_label or ""]))
                    if normalized_city and normalized_city not in searchable:
                        # City filter is soft in live mode; only enforce when card text has location context.
                        location_hint = self._normalize_text(source_card_label or "")
                        if location_hint and normalized_city not in location_hint:
                            continue
                    if normalized_category and normalized_category not in searchable:
                        # Category filter is also soft; keep result when the card has no obvious category text.
                        category_hint = self._normalize_text(source_card_label or "")
                        if category_hint and normalized_category not in category_hint:
                            continue

                    collected[key] = {
                        "business_name": name,
                        "category": str(task_payload.category or "").strip() or None,
                        "address": None,
                        "city": str(task_payload.city or "").strip() or None,
                        "phone": None,
                        "email": None,
                        "website": None,
                        "source": "google_maps_live_discovery",
                        "source_ref": {
                            "maps_url": raw_url,
                            "maps_url_canonical": canonical_url,
                            "discovery_query": search_query,
                            "source_card_label": source_card_label,
                            "discovery_mode": "live_google_maps_auto_scroll",
                        },
                        "rating": None,
                        "review_count": None,
                    }

                if len(collected) >= safe_limit:
                    break

                if len(collected) == before:
                    stable_rounds += 1
                else:
                    stable_rounds = 0
                if stable_rounds >= 3:
                    break

                await self._scroll_google_maps_results(scraper=scraper)
                await scraper.page.wait_for_timeout(scroll_wait_ms)
        finally:
            await scraper.close()

        candidates = list(collected.values())
        candidates.sort(key=lambda item: self._normalize_text(str(item.get("business_name") or "")))
        if normalized_query:
            # Keep cards that better match user query tokens first.
            query_tokens = set(normalized_query.split())
            candidates.sort(
                key=lambda item: len(
                    query_tokens
                    & set(self._normalize_text(str(item.get("business_name") or "")).split())
                ),
                reverse=True,
            )
        top_candidates = candidates[:safe_limit]
        return await self._enrich_live_google_maps_candidates(
            scraper=scraper,
            candidates=top_candidates,
        )

    async def _wait_for_results_feed(self, *, scraper: GoogleMapsScraper, timeout_ms: int = 15_000) -> bool:
        deadline = asyncio.get_running_loop().time() + (max(1, int(timeout_ms)) / 1000.0)
        while asyncio.get_running_loop().time() < deadline:
            for selector in SELECTOR_PATTERNS["RESULTS_FEED"]:
                locator = scraper.page.locator(selector).first
                try:
                    if await locator.is_visible():
                        return True
                except Exception:
                    continue
            await scraper.page.wait_for_timeout(220)
        return False

    async def _first_visible_from_patterns(
        self,
        *,
        scraper: GoogleMapsScraper,
        key: str,
        timeout_ms: int = 1_200,
    ) -> Any | None:
        for selector in SELECTOR_PATTERNS[key]:
            locator = scraper.page.locator(selector).first
            try:
                if await locator.is_visible():
                    return locator
            except Exception:
                continue
            try:
                await locator.wait_for(state="visible", timeout=max(1, timeout_ms))
                return locator
            except Exception:
                continue
        return None

    async def _search_google_maps_query(self, *, scraper: GoogleMapsScraper, query: str) -> None:
        await scraper._dismiss_google_consent_if_present()
        search_input = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_INPUT", timeout_ms=8_000)
        if search_input is None:
            raise RuntimeError("No se encontró el input de búsqueda de Google Maps para discovery live.")

        await scraper._human_click(search_input)
        await scraper.page.keyboard.press("Control+A")
        await scraper.page.keyboard.press("Backspace")
        await scraper._human_type(search_input, query)
        await scraper.page.wait_for_timeout(300)

        search_button = await self._first_visible_from_patterns(scraper=scraper, key="SEARCH_BUTTON", timeout_ms=2_000)
        if search_button is None:
            await scraper.page.keyboard.press("Enter")
        else:
            await scraper._human_click(search_button)

    async def _collect_visible_google_maps_results(self, *, scraper: GoogleMapsScraper) -> list[dict[str, Any]]:
        raw = await scraper.page.evaluate(
            """
            () => {
              const feed = document.querySelector("div[role='feed']");
              if (!feed) return { found: false, items: [] };

              const readText = (node) => {
                if (!node || !node.textContent) return "";
                return String(node.textContent).trim();
              };

              const anchors = Array.from(feed.querySelectorAll("a[href*='/maps/place/']"));
              const items = [];
              for (const anchor of anchors) {
                const article =
                  anchor.closest("div[role='article']") ||
                  anchor.closest("div.Nv2PK") ||
                  anchor.parentElement;
                const labelFromAnchor = String(anchor.getAttribute("aria-label") || "").trim();
                const heading =
                  article && article.querySelector
                    ? article.querySelector("h3, [role='heading'], .qBF1Pd, .fontHeadlineSmall")
                    : null;
                const labelFromHeading = readText(heading);
                const labelFromArticle = String(
                  article && article.getAttribute ? article.getAttribute("aria-label") || "" : ""
                ).trim();
                const fallbackText = readText(anchor).split("\\n")[0].trim();
                const name = labelFromHeading || labelFromAnchor || labelFromArticle || fallbackText;
                const href = String(anchor.href || "").trim();
                if (!name || !href) continue;
                items.push({
                  name: name,
                  maps_url: href,
                  source_card_label: labelFromArticle || labelFromAnchor || null,
                });
              }
              return { found: true, items: items };
            }
            """
        )
        if not isinstance(raw, dict):
            return []
        items = raw.get("items")
        if not isinstance(items, list):
            return []
        cleaned: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            maps_url = str(item.get("maps_url") or "").strip()
            if not name or not maps_url:
                continue
            cleaned.append(
                {
                    "name": name,
                    "maps_url": maps_url,
                    "source_card_label": str(item.get("source_card_label") or "").strip() or None,
                }
            )
        return cleaned

    async def _scroll_google_maps_results(self, *, scraper: GoogleMapsScraper) -> None:
        await scraper.page.evaluate(
            """
            () => {
              const feed = document.querySelector("div[role='feed']");
              if (!feed) return;
              const step = Math.max(900, Math.floor(feed.clientHeight * 0.9));
              feed.scrollBy({ top: step, left: 0, behavior: 'auto' });
            }
            """
        )

    async def _enrich_live_google_maps_candidates(
        self,
        *,
        scraper: GoogleMapsScraper,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not candidates:
            return []

        try:
            browser_context = scraper.page.context
        except Exception:
            return candidates

        if browser_context is None:
            return candidates

        try:
            detail_page = await browser_context.new_page()
        except Exception:
            return candidates

        detail_scraper = GoogleMapsScraper(page=detail_page)
        enriched_candidates: list[dict[str, Any]] = []
        try:
            for candidate in candidates:
                enriched_candidates.append(
                    await self._enrich_live_google_maps_candidate(
                        detail_scraper=detail_scraper,
                        candidate=candidate,
                    )
                )
        finally:
            try:
                await detail_page.close()
            except Exception:
                pass
        return enriched_candidates

    async def _enrich_live_google_maps_candidate(
        self,
        *,
        detail_scraper: GoogleMapsScraper,
        candidate: dict[str, Any],
        timeout_ms: int = 11_000,
    ) -> dict[str, Any]:
        enriched = dict(candidate)
        source_ref = dict(enriched.get("source_ref") or {})
        raw_maps_url = str(source_ref.get("maps_url") or "").strip()
        canonical_maps_url = self._canonicalize_maps_url(
            raw_maps_url or str(source_ref.get("maps_url_canonical") or "").strip()
        )
        target_maps_url = raw_maps_url or canonical_maps_url
        if not target_maps_url:
            return enriched

        listing: dict[str, Any] = {}
        try:
            await detail_scraper.page.goto(target_maps_url, wait_until="domcontentloaded")
            await detail_scraper._dismiss_google_consent_if_present()
            await detail_scraper._wait_for_listing_ready(timeout_ms=max(4_000, int(timeout_ms)))
            listing = await detail_scraper.extract_listing()
        except Exception as exc:
            source_ref["maps_url"] = target_maps_url
            source_ref["maps_url_canonical"] = canonical_maps_url or target_maps_url
            source_ref["listing_primary_extract_error"] = str(exc)[:180]

        listing_fallback = await self._extract_listing_fallback_from_dom(detail_scraper=detail_scraper)
        listing = self._merge_listing_payloads(primary=listing, fallback=listing_fallback)

        listing_name = str(listing.get("business_name") or "").strip()
        listing_address = str(listing.get("address") or "").strip() or None
        listing_phone = str(listing.get("phone") or "").strip() or None
        listing_website = str(listing.get("website") or "").strip() or None
        listing_rating = listing.get("overall_rating")
        listing_review_count = listing.get("total_reviews")

        category_values: list[str] = []
        raw_categories = listing.get("categories")
        if isinstance(raw_categories, list):
            for value in raw_categories:
                cleaned = str(value or "").strip()
                if cleaned:
                    category_values.append(cleaned)
        listing_category = ", ".join(category_values) if category_values else None

        if listing_name:
            enriched["business_name"] = listing_name
        if listing_address:
            enriched["address"] = listing_address
            if not str(enriched.get("city") or "").strip():
                enriched["city"] = self._extract_city_from_address(listing_address)
        if listing_phone:
            enriched["phone"] = listing_phone
        if listing_website:
            enriched["website"] = listing_website
        if listing_rating is not None:
            enriched["rating"] = listing_rating
        if listing_review_count is not None:
            enriched["review_count"] = listing_review_count
        if not str(enriched.get("category") or "").strip() and listing_category:
            enriched["category"] = listing_category

        current_page_url = str(detail_scraper.page.url or "").strip()
        source_ref["maps_url"] = current_page_url or target_maps_url
        source_ref["maps_url_canonical"] = self._canonicalize_maps_url(current_page_url) or canonical_maps_url or target_maps_url
        source_ref["discovery_mode"] = "live_google_maps_auto_scroll_listing_extract"
        listing_enriched = bool(
            listing_name
            or listing_address
            or listing_phone
            or listing_website
            or listing_rating is not None
            or listing_review_count is not None
        )
        source_ref["listing_enriched"] = listing_enriched
        source_ref.pop("listing_extract_error", None)
        primary_extract_error = str(source_ref.get("listing_primary_extract_error") or "").strip()
        if listing_enriched:
            source_ref.pop("listing_primary_extract_error", None)
        elif primary_extract_error:
            source_ref["listing_extract_error"] = primary_extract_error
        enriched["source_ref"] = source_ref
        return enriched

    async def _extract_listing_fallback_from_dom(self, *, detail_scraper: GoogleMapsScraper) -> dict[str, Any]:
        try:
            raw = await detail_scraper.page.evaluate(
                """
                () => {
                  const clean = (value) => {
                    if (typeof value !== "string") return "";
                    return value.replace(/\\s+/g, " ").trim();
                  };
                  const text = (el) => clean(el && el.textContent ? String(el.textContent) : "");

                  const businessName =
                    text(document.querySelector("h1")) ||
                    text(document.querySelector("[role='main'] h1")) ||
                    "";
                  const address = text(
                    document.querySelector("[data-item-id='address'] .Io6YTe") ||
                    document.querySelector("[data-item-id='address']")
                  );
                  const phone = text(
                    document.querySelector("[data-item-id^='phone:'] .Io6YTe") ||
                    document.querySelector("[data-item-id^='phone:']")
                  );
                  const websiteText = text(
                    document.querySelector("[data-item-id='authority'] .Io6YTe") ||
                    document.querySelector("[data-item-id='authority']")
                  );
                  let websiteHref = "";
                  const websiteAnchor = document.querySelector("[data-item-id='authority'] a[href]");
                  if (websiteAnchor && websiteAnchor.getAttribute) {
                    websiteHref = clean(String(websiteAnchor.getAttribute("href") || ""));
                  }

                  const pickAriaLabel = (nodes, expectedKeyword) => {
                    let best = "";
                    for (const node of nodes) {
                      if (!node || !node.getAttribute) continue;
                      const rawValue = String(node.getAttribute("aria-label") || "").trim();
                      if (!rawValue) continue;
                      const hasDigit = /\\d/.test(rawValue);
                      const normalized = rawValue.toLowerCase();
                      if (hasDigit && normalized.includes(expectedKeyword)) {
                        return rawValue;
                      }
                      if (hasDigit && !best) {
                        best = rawValue;
                      } else if (!best) {
                        best = rawValue;
                      }
                    }
                    return best;
                  };

                  const ratingNodes = Array.from(
                    document.querySelectorAll("[aria-label*='estrella' i], [aria-label*='star' i], [role='img'][aria-label]")
                  );
                  const reviewsNodes = Array.from(
                    document.querySelectorAll("[aria-label*='rese' i], [aria-label*='review' i], button[jsaction*='reviewChart.moreReviews']")
                  );
                  const ratingLabel = pickAriaLabel(ratingNodes, "estrella");
                  const reviewsLabel = pickAriaLabel(reviewsNodes, "rese");

                  const categoryButtons = Array.from(
                    document.querySelectorAll("button[jsaction*='.category'], button[jsaction*='pane.wfvdle'][aria-label], div.fontBodyMedium button")
                  );
                  const categories = [];
                  const seen = new Set();
                  for (const button of categoryButtons) {
                    const value = text(button);
                    if (!value) continue;
                    const key = value.toLowerCase();
                    if (seen.has(key)) continue;
                    seen.add(key);
                    categories.push(value);
                    if (categories.length >= 6) break;
                  }

                  return {
                    business_name: businessName || null,
                    address: address || null,
                    phone: phone || null,
                    website: websiteText || websiteHref || null,
                    rating_label: ratingLabel || null,
                    reviews_label: reviewsLabel || null,
                    categories: categories,
                  };
                }
                """
            )
        except Exception:
            return {}

        if not isinstance(raw, dict):
            return {}

        rating_value = self._parse_rating_text(raw.get("rating_label"))
        reviews_value = self._parse_reviews_count_text(raw.get("reviews_label"))

        categories: list[str] = []
        raw_categories = raw.get("categories")
        if isinstance(raw_categories, list):
            for item in raw_categories:
                cleaned = str(item or "").strip()
                if cleaned:
                    categories.append(cleaned)

        return {
            "business_name": str(raw.get("business_name") or "").strip() or None,
            "address": str(raw.get("address") or "").strip() or None,
            "phone": str(raw.get("phone") or "").strip() or None,
            "website": str(raw.get("website") or "").strip() or None,
            "overall_rating": rating_value,
            "total_reviews": reviews_value,
            "categories": categories,
        }

    def _merge_listing_payloads(self, *, primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = dict(primary or {})
        for key in ("business_name", "address", "phone", "website"):
            if not str(merged.get(key) or "").strip():
                value = str(fallback.get(key) or "").strip()
                if value:
                    merged[key] = value

        if merged.get("overall_rating") is None and fallback.get("overall_rating") is not None:
            merged["overall_rating"] = fallback.get("overall_rating")
        if merged.get("total_reviews") is None and fallback.get("total_reviews") is not None:
            merged["total_reviews"] = fallback.get("total_reviews")

        primary_categories = merged.get("categories") if isinstance(merged.get("categories"), list) else []
        fallback_categories = fallback.get("categories") if isinstance(fallback.get("categories"), list) else []
        if not primary_categories and fallback_categories:
            merged["categories"] = fallback_categories
        return merged

    def _parse_rating_text(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.search(r"([0-5](?:[.,]\d)?)", text)
        if not match:
            return None
        raw_value = str(match.group(1) or "").replace(",", ".")
        try:
            rating = float(raw_value)
        except ValueError:
            return None
        if rating < 0.0 or rating > 5.0:
            return None
        return rating

    def _parse_reviews_count_text(self, value: Any) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        text_no_spaces = re.sub(r"\s+", "", text)
        candidates = re.findall(r"\d[\d\.,]*", text_no_spaces)
        if not candidates:
            return None

        parsed_values: list[int] = []
        for candidate in candidates:
            digits = re.sub(r"[^0-9]", "", candidate)
            if not digits:
                continue
            try:
                parsed_values.append(int(digits))
            except ValueError:
                continue
        if not parsed_values:
            return None
        return max(parsed_values)

    def _canonicalize_maps_url(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
        except Exception:
            return raw
        path = str(parsed.path or "").strip()
        if not path:
            return raw
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    async def _upsert_lead_candidate(self, candidate: dict[str, Any]) -> str:
        leads = get_database()[self._LEADS_COLLECTION]
        business_name = str(candidate.get("business_name") or "").strip()
        if not business_name:
            return "skipped"

        business_name_normalized = self._normalize_text(business_name)
        address = str(candidate.get("address") or "").strip() or None
        email = str(candidate.get("email") or "").strip() or None
        website = str(candidate.get("website") or "").strip() or None
        email_normalized = self._normalize_email(email)
        domain_normalized = self._domain_from_email_or_website(email=email, website=website)

        lead_query: dict[str, Any] | None = None
        if email_normalized:
            lead_query = {"email_normalized": email_normalized}
        else:
            lookup_clauses: list[dict[str, Any]] = [{"business_name_normalized": business_name_normalized}]
            if address:
                lookup_clauses.append({"address": address})
            if domain_normalized:
                lookup_clauses.append({"domain_normalized": domain_normalized})
            lead_query = {"$and": lookup_clauses}

        existing = await leads.find_one(lead_query) if lead_query else None

        rating_value = candidate.get("rating")
        review_count_value = candidate.get("review_count")
        score = self._build_lead_score(
            rating=rating_value,
            review_count=review_count_value,
            has_email=bool(email_normalized),
            has_website=bool(website),
        )
        now = self._now_utc()

        if existing is None:
            legal = CRMLeadLegalBlock(
                consent_status=CRMConsentStatus.MISSING,
                consent_proof=None,
                do_not_contact=False,
                unsubscribed_at=None,
                suppressed_reason=None,
            )
            pipeline = CRMLeadPipelineRefs(
                business_id=None,
                source_job_ids=[],
                analysis_job_id=None,
                report_job_id=None,
                latest_report_artifacts={},
            )
            lead = CRMLead(
                business_name=business_name,
                business_name_normalized=business_name_normalized,
                email=email,
                email_normalized=email_normalized,
                domain_normalized=domain_normalized,
                phone=str(candidate.get("phone") or "").strip() or None,
                website=website,
                category=str(candidate.get("category") or "").strip() or None,
                city=str(candidate.get("city") or "").strip() or None,
                address=address,
                source=str(candidate.get("source") or "unknown"),
                source_ref=dict(candidate.get("source_ref") or {}),
                status=CRMLeadStatus.ENRICHING if not email_normalized else CRMLeadStatus.READY,
                score=score,
                legal=legal,
                pipeline=pipeline,
                notes=[],
                tags=[],
                created_at=now,
                updated_at=now,
            )
            await leads.insert_one(lead.model_dump(mode="python"))
            return "inserted"

        set_fields: dict[str, Any] = {"updated_at": now}
        if not str(existing.get("phone") or "").strip() and str(candidate.get("phone") or "").strip():
            set_fields["phone"] = str(candidate.get("phone") or "").strip()
        if not str(existing.get("website") or "").strip() and website:
            set_fields["website"] = website
        if not str(existing.get("email") or "").strip() and email:
            set_fields["email"] = email
            set_fields["email_normalized"] = email_normalized
        if not str(existing.get("domain_normalized") or "").strip() and domain_normalized:
            set_fields["domain_normalized"] = domain_normalized
        if not str(existing.get("address") or "").strip() and address:
            set_fields["address"] = address
        if not str(existing.get("city") or "").strip() and str(candidate.get("city") or "").strip():
            set_fields["city"] = str(candidate.get("city") or "").strip()
        if not str(existing.get("category") or "").strip() and str(candidate.get("category") or "").strip():
            set_fields["category"] = str(candidate.get("category") or "").strip()

        existing_score = float(existing.get("score") or 0.0)
        if score > existing_score:
            set_fields["score"] = score

        source_ref = existing.get("source_ref") if isinstance(existing.get("source_ref"), dict) else {}
        merged_source_ref = {**source_ref, **dict(candidate.get("source_ref") or {})}
        set_fields["source_ref"] = merged_source_ref

        status_value = str(existing.get("status") or "").strip().lower()
        if status_value in {CRMLeadStatus.NEW.value, CRMLeadStatus.ENRICHING.value} and email_normalized:
            set_fields["status"] = CRMLeadStatus.READY.value

        if len(set_fields.keys()) <= 2:
            return "skipped"

        await leads.update_one({"_id": existing.get("_id")}, {"$set": set_fields})
        return "updated"

    async def _can_send_to_lead(self, *, lead_doc: dict[str, Any]) -> tuple[bool, str]:
        legal = lead_doc.get("legal") if isinstance(lead_doc.get("legal"), dict) else {}
        consent_status = str(legal.get("consent_status") or "").strip().lower()
        consent_proof = legal.get("consent_proof") if isinstance(legal.get("consent_proof"), dict) else None
        do_not_contact = bool(legal.get("do_not_contact"))
        unsubscribed_at = legal.get("unsubscribed_at")

        if do_not_contact:
            return False, "do_not_contact"
        if unsubscribed_at is not None:
            return False, "unsubscribed"
        if consent_status != CRMConsentStatus.GRANTED.value:
            return False, "consent_not_granted"
        if not consent_proof:
            return False, "consent_proof_missing"

        email = str(lead_doc.get("email") or "").strip()
        email_normalized = self._normalize_email(email)
        if not email or not email_normalized:
            return False, "email_missing"

        suppressed = await self._is_email_suppressed(email_normalized)
        if suppressed:
            return False, "suppressed"

        return True, "ok"

    async def _is_email_suppressed(self, email_normalized: str) -> bool:
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        doc = await suppressions.find_one({"email_normalized": email_normalized}, projection={"_id": 1})
        return doc is not None

    async def _block_lead_contact(self, *, lead_id: str, reason: str) -> None:
        leads = get_database()[self._LEADS_COLLECTION]
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        now = self._now_utc()

        set_fields: dict[str, Any] = {
            "legal.do_not_contact": True,
            "legal.suppressed_reason": reason,
            "updated_at": now,
        }
        if reason == "unsubscribed":
            set_fields["legal.unsubscribed_at"] = now

        await leads.update_one({"_id": parsed_lead_id}, {"$set": set_fields})
        await self._stop_pending_messages_for_lead(lead_id=lead_id, reason=reason)

    async def _stop_pending_messages_for_lead(self, *, lead_id: str, reason: str) -> None:
        messages = get_database()[self._MESSAGES_COLLECTION]
        now = self._now_utc()
        await messages.update_many(
            {
                "lead_id": lead_id,
                "status": CRMMessageStatus.QUEUED.value,
            },
            {
                "$set": {
                    "status": CRMMessageStatus.SKIPPED.value,
                    "error": f"stopped:{reason}",
                    "updated_at": now,
                    "dispatch_job_id": None,
                }
            },
        )

    async def _upsert_suppression(self, *, email: str, reason: str, source: str) -> None:
        normalized = self._normalize_email(email)
        if not normalized:
            return
        suppressions = get_database()[self._SUPPRESSIONS_COLLECTION]
        now = self._now_utc()

        suppression = CRMSuppression(
            email=email,
            email_normalized=normalized,
            reason=str(reason or "manual"),
            source=str(source or "system"),
            created_at=now,
            updated_at=now,
        )
        payload = suppression.model_dump(mode="python")
        await suppressions.update_one(
            {"email_normalized": normalized},
            {
                "$set": {
                    "email": payload["email"],
                    "reason": payload["reason"],
                    "source": payload["source"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "email_normalized": normalized,
                },
            },
            upsert=True,
        )

    async def _record_event(
        self,
        *,
        event_type: str,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        message_id: str | None = None,
        data: dict[str, Any] | None = None,
        actor: str = "system",
    ) -> None:
        events = get_database()[self._EVENTS_COLLECTION]
        event = CRMEvent(
            event_type=str(event_type or "event").strip(),
            lead_id=str(lead_id).strip() if lead_id else None,
            campaign_id=str(campaign_id).strip() if campaign_id else None,
            message_id=str(message_id).strip() if message_id else None,
            actor=str(actor or "system"),
            data=dict(data or {}),
            created_at=self._now_utc(),
        )
        await events.insert_one(event.model_dump(mode="python"))

    async def _build_mini_report_for_lead(self, *, lead_doc: dict[str, Any]) -> str:
        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        business_id = str(pipeline.get("business_id") or "").strip()
        if not business_id:
            return "Todavía no hay informe completo, pero podemos activarlo en tu ficha en cuanto lo prefieras."

        try:
            parsed_business_id = ObjectId(business_id)
        except InvalidId:
            return "Tenemos señales de mejora en reseñas recientes y podemos enseñártelas en una demo corta."

        analyses = get_database()[self._ANALYSES_COLLECTION]
        analysis_docs = (
            await analyses.find({"business_id": str(parsed_business_id)}).sort([("created_at", -1), ("_id", -1)]).limit(1).to_list(length=1)
        )
        if not analysis_docs:
            return "Hemos detectado oportunidades claras en servicio y reputación digital que te enseñamos en 15 minutos."

        analysis_doc = analysis_docs[0]
        stats = analysis_doc.get("stats") if isinstance(analysis_doc.get("stats"), dict) else {}
        avg_rating = stats.get("avg_rating")
        response_rate = stats.get("response_rate")
        rating_text = f"valoración media {float(avg_rating):.2f}/5" if isinstance(avg_rating, (int, float)) else "valoración media disponible"
        if isinstance(response_rate, (int, float)):
            response_pct = float(response_rate) * 100 if float(response_rate) <= 1.0 else float(response_rate)
            response_text = f"tasa de respuesta {response_pct:.0f}%"
        else:
            response_text = "tasa de respuesta mejorable"
        return f"Resumen actual: {rating_text}, {response_text}."

    def _render_cadence_step(
        self,
        *,
        step: CRMCadenceStep,
        lead_doc: dict[str, Any],
        mini_report: str,
    ) -> tuple[str, str]:
        business_name = str(lead_doc.get("business_name") or "tu negocio").strip()
        lead_id = str(lead_doc.get("_id") or "").strip()

        cta_url = str(settings.crm_cta_url or "").strip() or "https://www.repiq.ai/demo"
        unsubscribe_base = str(settings.crm_unsubscribe_url or "").strip() or cta_url
        unsubscribe_token = self._unsubscribe_token(lead_id=lead_id, email=str(lead_doc.get("email") or ""))
        sep = "&" if "?" in unsubscribe_base else "?"
        unsubscribe_url = f"{unsubscribe_base}{sep}lead={lead_id}&token={unsubscribe_token}"

        template_context = {
            "business_name": business_name,
            "mini_report": mini_report,
            "cta_url": cta_url,
            "unsubscribe_url": unsubscribe_url,
        }
        subject = str(step.subject_template).format(**template_context)
        body_text = str(step.body_template).format(**template_context)
        body_html = self._text_to_html(body_text)
        return subject, body_html

    def _send_resend_email(self, *, to_email: str, subject: str, html_body: str) -> dict[str, Any]:
        api_key = str(settings.crm_resend_api_key or "").strip()
        from_email = str(settings.crm_resend_from_email or "").strip()
        sender_name = str(settings.crm_sender_name or "Repiq").strip() or "Repiq"
        reply_to = str(settings.crm_resend_reply_to or "").strip() or None

        if not api_key or not from_email:
            return {
                "id": f"dryrun-{hashlib.sha1(f'{to_email}-{subject}'.encode('utf-8')).hexdigest()[:16]}",
                "dry_run": True,
                "reason": "missing_resend_config",
            }

        from_header = formataddr((sender_name, from_email))
        payload: dict[str, Any] = {
            "from": from_header,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        }
        if reply_to:
            payload["reply_to"] = reply_to

        body_bytes = json.dumps(payload).encode("utf-8")
        request = Request(
            url="https://api.resend.com/emails",
            data=body_bytes,
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        with urlopen(request, timeout=20) as response:  # noqa: S310 - endpoint fixed
            raw_response = response.read().decode("utf-8", errors="ignore")
            if not raw_response.strip():
                return {"id": None, "provider_status": response.status}
            parsed = json.loads(raw_response)
            if isinstance(parsed, dict):
                return parsed
            return {"id": None, "provider_status": response.status, "raw": parsed}

    def _text_to_html(self, text: str) -> str:
        lines = [line.strip() for line in str(text or "").splitlines()]
        html_lines: list[str] = []
        for line in lines:
            if not line:
                html_lines.append("<p>&nbsp;</p>")
                continue
            safe_line = (
                line.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            html_lines.append(f"<p>{safe_line}</p>")
        return "".join(html_lines)

    def _unsubscribe_token(self, *, lead_id: str, email: str) -> str:
        secret = str(settings.crm_unsubscribe_secret or "").strip() or "crm-unsubscribe-secret"
        payload = f"{lead_id}|{email}|{secret}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    def _extract_city_from_address(self, value: str | None) -> str | None:
        cleaned = str(value or "").strip()
        if not cleaned:
            return None
        parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        if not parts:
            return None
        return parts[-1]

    def _normalize_text(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        normalized = unicodedata.normalize("NFKD", raw)
        ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
        collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_value)
        return re.sub(r"\s+", " ", collapsed).strip()

    def _normalize_email(self, value: Any) -> str | None:
        raw = str(value or "").strip().lower()
        if not raw or "@" not in raw:
            return None
        return raw

    def _domain_from_email_or_website(self, *, email: str | None, website: str | None) -> str | None:
        email_norm = self._normalize_email(email)
        if email_norm and "@" in email_norm:
            return email_norm.split("@", 1)[1].strip() or None

        website_raw = str(website or "").strip().lower()
        if not website_raw:
            return None
        if not website_raw.startswith("http://") and not website_raw.startswith("https://"):
            website_raw = f"https://{website_raw}"
        parsed = urlparse(website_raw)
        host = str(parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None

    def _build_lead_score(
        self,
        *,
        rating: Any,
        review_count: Any,
        has_email: bool,
        has_website: bool,
    ) -> float:
        score = 0.0
        if isinstance(rating, (int, float)):
            rating_value = max(0.0, min(5.0, float(rating)))
            # Prefer leads with room to improve (3.2 - 4.4 window)
            if rating_value < 2.8:
                score += 20
            elif rating_value <= 4.4:
                score += 45
            else:
                score += 30
        if isinstance(review_count, (int, float)):
            reviews = max(0, int(review_count))
            if reviews >= 500:
                score += 25
            elif reviews >= 200:
                score += 20
            elif reviews >= 50:
                score += 14
            elif reviews >= 10:
                score += 8
            else:
                score += 4
        if has_email:
            score += 18
        if has_website:
            score += 12
        return round(min(100.0, score), 2)

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _serialize_mongo_doc(self, doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop("_id"))
        return payload

    def _sanitize_payload(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, list):
            return [self._sanitize_payload(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self._sanitize_payload(item) for key, item in value.items()}
        return value

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
