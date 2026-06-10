from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.models.crm import CRMConsentStatus, CRMLeadStatus


class CreateCRMLeadUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        normalize_email: Callable[[Any], str | None],
        normalize_text: Callable[[Any], str],
        domain_from_email_or_website: Callable[..., str | None],
        record_event: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._normalize_email = normalize_email
        self._normalize_text = normalize_text
        self._domain_from_email_or_website = domain_from_email_or_website
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name

    async def execute(
        self,
        *,
        business_name: str,
        contact_name: str | None = None,
        email: str | None = None,
        phone: str | None = None,
        website: str | None = None,
        category: str | None = None,
        city: str | None = None,
        address: str | None = None,
        source: str | None = None,
        status: str | None = None,
        notes: list[str] | None = None,
        tags: list[str] | None = None,
        do_not_contact: bool | None = None,
        consent_status: str | None = None,
        suppressed_reason: str | None = None,
        unsubscribed: bool | None = None,
        consent_proof: dict[str, Any] | None = None,
        source_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()

        normalized_business_name = str(business_name or "").strip()
        if not normalized_business_name:
            raise ValueError("business_name is required.")

        normalized_status = str(status or CRMLeadStatus.NEW.value).strip().lower() or CRMLeadStatus.NEW.value
        normalized_consent = (
            str(consent_status or CRMConsentStatus.MISSING.value).strip().lower() or CRMConsentStatus.MISSING.value
        )
        allowed_consents = {
            CRMConsentStatus.MISSING.value,
            CRMConsentStatus.GRANTED.value,
            CRMConsentStatus.REVOKED.value,
            CRMConsentStatus.DENIED.value,
        }
        if normalized_consent not in allowed_consents:
            raise ValueError("Invalid consent_status.")

        normalized_email = str(email or "").strip() or None
        normalized_phone = str(phone or "").strip() or None
        normalized_website = str(website or "").strip() or None
        normalized_category = str(category or "").strip() or None
        normalized_city = str(city or "").strip() or None
        normalized_address = str(address or "").strip() or None
        normalized_source = str(source or "manual").strip().lower() or "manual"
        normalized_notes = [str(item or "").strip() for item in list(notes or []) if str(item or "").strip()]
        normalized_tags = [str(item or "").strip().lower() for item in list(tags or []) if str(item or "").strip()]
        normalized_contact_name = str(contact_name or "").strip() or None

        email_normalized = self._normalize_email(normalized_email)
        domain_normalized = self._domain_from_email_or_website(email=normalized_email, website=normalized_website)

        now = self._now_utc()
        doc: dict[str, Any] = {
            "business_name": normalized_business_name,
            "business_name_normalized": self._normalize_text(normalized_business_name),
            "email": normalized_email,
            "email_normalized": email_normalized,
            "domain_normalized": domain_normalized,
            "phone": normalized_phone,
            "website": normalized_website,
            "category": normalized_category,
            "city": normalized_city,
            "address": normalized_address,
            "source": normalized_source,
            "source_ref": source_ref or {},
            "rating": None,
            "review_count": None,
            "status": normalized_status,
            "score": 0.0,
            "legal": {
                "consent_status": normalized_consent,
                "consent_proof": consent_proof,
                "do_not_contact": bool(do_not_contact),
                "unsubscribed_at": now if bool(unsubscribed) else None,
                "suppressed_reason": str(suppressed_reason or "").strip() or None,
            },
            "pipeline": {
                "business_id": None,
                "source_job_ids": [],
                "analysis_job_id": None,
                "report_job_id": None,
                "latest_report_artifacts": {},
            },
            "notes": normalized_notes,
            "tags": normalized_tags,
            "created_at": now,
            "updated_at": now,
        }
        if normalized_contact_name:
            doc["source_ref"]["contact_name"] = normalized_contact_name

        leads = get_database()[self._leads_collection_name]
        inserted = await leads.insert_one(doc)
        doc["_id"] = inserted.inserted_id

        await self._record_event(
            event_type="lead_created_manual",
            lead_id=str(inserted.inserted_id),
            data={
                "source": normalized_source,
                "status": normalized_status,
                "consent_status": normalized_consent,
                "contact_name": normalized_contact_name,
            },
        )

        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="lead_id"))
