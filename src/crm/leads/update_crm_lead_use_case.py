from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from bson import ObjectId
from pymongo import ReturnDocument

from src.database import get_database
from src.models.crm import CRMConsentProof, CRMConsentStatus


class UpdateCRMLeadUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_repo_v2: bool,
        update_lead_v2: Callable[..., Awaitable[dict[str, Any]]],
        parse_object_id: Callable[..., ObjectId],
        now_utc: Callable[[], datetime],
        normalize_email: Callable[[Any], str | None],
        normalize_text: Callable[[Any], str],
        domain_from_email_or_website: Callable[..., str | None],
        get_lead: Callable[..., Awaitable[dict[str, Any]]],
        record_event: Callable[..., Awaitable[None]],
        upsert_suppression: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_repo_v2 = use_repo_v2
        self._update_lead_v2 = update_lead_v2
        self._parse_object_id = parse_object_id
        self._now_utc = now_utc
        self._normalize_email = normalize_email
        self._normalize_text = normalize_text
        self._domain_from_email_or_website = domain_from_email_or_website
        self._get_lead = get_lead
        self._record_event = record_event
        self._upsert_suppression = upsert_suppression
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name

    async def execute(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        if self._use_repo_v2:
            return await self._update_lead_v2(lead_id=lead_id, updates=updates)

        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = get_database()[self._leads_collection_name]

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
                email=set_fields.get("email"),
                website=set_fields.get("website"),
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
            return await self._get_lead(lead_id=lead_id, sync_pipeline_refs=False)

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
