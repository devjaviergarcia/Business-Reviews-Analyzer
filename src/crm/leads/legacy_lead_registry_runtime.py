from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

from bson import ObjectId
from pymongo import ReturnDocument

from src.models.crm import (
    CRMConsentProof,
    CRMConsentStatus,
    CRMLead,
    CRMLeadLegalBlock,
    CRMLeadPipelineRefs,
    CRMLeadStatus,
)
from src.services.pagination import build_pagination_payload, coerce_pagination


DatabaseFactory = Callable[[], Any]
NowUtcFn = Callable[[], Any]
NormalizeTextFn = Callable[[Any], str]
NormalizeEmailFn = Callable[[Any], str | None]
DomainFromEmailOrWebsiteFn = Callable[..., str | None]
ParseObjectIdFn = Callable[..., ObjectId]
SerializeMongoDocFn = Callable[..., dict[str, Any]]
SanitizePayloadFn = Callable[[Any], Any]
RecordEventFn = Callable[..., Awaitable[None]]
SyncLeadPipelineRefsFn = Callable[..., Awaitable[dict[str, Any]]]
UpsertSuppressionFn = Callable[..., Awaitable[None]]
ParseRatingTextFn = Callable[[Any], float | None]
ParseReviewsCountTextFn = Callable[[Any], int | None]
BuildLeadScoreFn = Callable[..., float]


class LegacyLeadRegistryRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        lead_repository: Any,
        leads_collection_name: str,
        now_utc: NowUtcFn,
        normalize_text: NormalizeTextFn,
        normalize_email: NormalizeEmailFn,
        domain_from_email_or_website: DomainFromEmailOrWebsiteFn,
        parse_object_id: ParseObjectIdFn,
        serialize_mongo_doc: SerializeMongoDocFn,
        sanitize_payload: SanitizePayloadFn,
        record_event: RecordEventFn,
        sync_lead_pipeline_refs: SyncLeadPipelineRefsFn,
        upsert_suppression: UpsertSuppressionFn,
        parse_rating_text: ParseRatingTextFn,
        parse_reviews_count_text: ParseReviewsCountTextFn,
        build_lead_score: BuildLeadScoreFn,
    ) -> None:
        self._database_factory = database_factory
        self._lead_repository = lead_repository
        self._leads_collection_name = leads_collection_name
        self._now_utc = now_utc
        self._normalize_text = normalize_text
        self._normalize_email = normalize_email
        self._domain_from_email_or_website = domain_from_email_or_website
        self._parse_object_id = parse_object_id
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._record_event = record_event
        self._sync_lead_pipeline_refs = sync_lead_pipeline_refs
        self._upsert_suppression = upsert_suppression
        self._parse_rating_text = parse_rating_text
        self._parse_reviews_count_text = parse_reviews_count_text
        self._build_lead_score = build_lead_score

    async def list_leads(
        self,
        *,
        use_repo_v2: bool,
        page: int,
        page_size: int,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
        sort_by: str,
        sort_dir: str,
    ) -> dict[str, Any]:
        if use_repo_v2:
            payload = await self._lead_repository.list(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            return self._sanitize_payload(payload)

        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        leads = self._database_factory()[self._leads_collection_name]
        query = self.build_leads_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )
        sort_spec = self.resolve_leads_sort(sort_by=sort_by, sort_dir=sort_dir)

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

    async def create_lead(
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
        normalized_business_name = str(business_name or "").strip()
        if not normalized_business_name:
            raise ValueError("business_name is required.")

        normalized_status = str(status or CRMLeadStatus.NEW.value).strip().lower() or CRMLeadStatus.NEW.value
        normalized_consent = (
            str(consent_status or CRMConsentStatus.MISSING.value).strip().lower()
            or CRMConsentStatus.MISSING.value
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
        domain_normalized = self._domain_from_email_or_website(
            email=normalized_email,
            website=normalized_website,
        )

        now = self._now_utc()
        legal = CRMLeadLegalBlock(
            consent_status=normalized_consent,
            consent_proof=consent_proof,
            do_not_contact=bool(do_not_contact),
            unsubscribed_at=now if bool(unsubscribed) else None,
            suppressed_reason=str(suppressed_reason or "").strip() or None,
        )
        pipeline = CRMLeadPipelineRefs(
            business_id=None,
            source_job_ids=[],
            analysis_job_id=None,
            report_job_id=None,
            latest_report_artifacts={},
        )
        lead = CRMLead(
            business_name=normalized_business_name,
            business_name_normalized=self._normalize_text(normalized_business_name),
            email=normalized_email,
            email_normalized=email_normalized,
            domain_normalized=domain_normalized,
            phone=normalized_phone,
            website=normalized_website,
            category=normalized_category,
            city=normalized_city,
            address=normalized_address,
            source=normalized_source,
            source_ref=source_ref or {},
            rating=None,
            review_count=None,
            status=normalized_status,
            score=0.0,
            legal=legal,
            pipeline=pipeline,
            notes=normalized_notes,
            tags=normalized_tags,
            created_at=now,
            updated_at=now,
        )
        doc = lead.model_dump(mode="python")
        if normalized_contact_name:
            source_ref_doc = doc.get("source_ref") if isinstance(doc.get("source_ref"), dict) else {}
            source_ref_doc["contact_name"] = normalized_contact_name
            doc["source_ref"] = source_ref_doc

        leads = self._database_factory()[self._leads_collection_name]
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

    async def bulk_delete_leads(
        self,
        *,
        use_repo_v2: bool,
        lead_ids: list[str] | None,
        delete_all_matching: bool,
        exclude_lead_ids: list[str] | None,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        if use_repo_v2:
            result = await self._lead_repository.bulk_delete(
                lead_ids=lead_ids,
                delete_all_matching=delete_all_matching,
                exclude_lead_ids=exclude_lead_ids,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
            await self._record_event(event_type="leads_bulk_deleted", data=result)
            return self._sanitize_payload(
                {
                    "deleted_count": int(result.get("deleted_count") or 0),
                    "matched_count": int(result.get("matched_count") or 0),
                    "delete_all_matching": bool(result.get("delete_all_matching")),
                    "requested_ids": int(result.get("requested_ids") or 0),
                    "excluded_ids": int(result.get("excluded_ids") or 0),
                }
            )

        leads = self._database_factory()[self._leads_collection_name]

        normalized_ids: list[ObjectId] = []
        seen_ids: set[str] = set()
        for raw_id in list(lead_ids or []):
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(self._parse_object_id(normalized, field_name="lead_id"))

        excluded_ids: list[ObjectId] = []
        seen_excluded_ids: set[str] = set()
        for raw_id in list(exclude_lead_ids or []):
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
            query = self.build_leads_query(
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

    async def get_lead(
        self,
        *,
        use_repo_v2: bool,
        lead_id: str,
        sync_pipeline_refs: bool,
    ) -> dict[str, Any]:
        if use_repo_v2:
            lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            parsed_lead_id = None
            leads = None
        else:
            parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
            leads = self._database_factory()[self._leads_collection_name]
            lead_doc = await leads.find_one({"_id": parsed_lead_id})

        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        if sync_pipeline_refs:
            await self._sync_lead_pipeline_refs(lead_id=lead_id)
            if use_repo_v2:
                lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            else:
                lead_doc = await leads.find_one({"_id": parsed_lead_id})
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

    async def update_lead(
        self,
        *,
        use_repo_v2: bool,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        if use_repo_v2:
            return await self.update_lead_v2(lead_id=lead_id, updates=updates)

        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        leads = self._database_factory()[self._leads_collection_name]
        set_fields, now = self._build_update_fields(updates=updates)

        if not set_fields:
            return await self.get_lead(use_repo_v2=False, lead_id=lead_id, sync_pipeline_refs=False)

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
        await self._sync_suppression_if_needed(updated=updated, set_fields=set_fields)
        return self._sanitize_payload(self._serialize_mongo_doc(updated, id_key="lead_id"))

    async def update_lead_v2(self, *, lead_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
        set_fields, now = self._build_update_fields(updates=updates)

        if not set_fields:
            lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")
            return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))

        set_fields["updated_at"] = now
        updated = await self._lead_repository.find_one_and_update(
            {"_id": parsed_lead_id},
            {"$set": set_fields},
        )
        if updated is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        await self._record_event(
            event_type="lead_updated",
            lead_id=lead_id,
            data={"fields": sorted(set_fields.keys())},
        )
        await self._sync_suppression_if_needed(updated=updated, set_fields=set_fields)
        return self._sanitize_payload(self._serialize_mongo_doc(updated, id_key="lead_id"))

    async def upsert_lead_candidate(self, candidate: dict[str, Any]) -> str:
        leads = self._database_factory()[self._leads_collection_name]
        business_name = str(candidate.get("business_name") or "").strip()
        if not business_name:
            return "skipped"

        business_name_normalized = self._normalize_text(business_name)
        address = str(candidate.get("address") or "").strip() or None
        email = str(candidate.get("email") or "").strip() or None
        website = str(candidate.get("website") or "").strip() or None
        email_normalized = self._normalize_email(email)
        domain_normalized = self._domain_from_email_or_website(email=email, website=website)

        if email_normalized:
            lead_query: dict[str, Any] | None = {"email_normalized": email_normalized}
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
        parsed_rating = self._parse_rating_text(rating_value)
        parsed_review_count = self._parse_reviews_count_text(review_count_value)
        score = self._build_lead_score(
            rating=parsed_rating,
            review_count=parsed_review_count,
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
                rating=parsed_rating,
                review_count=parsed_review_count,
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

        existing_rating = self._parse_rating_text(existing.get("rating"))
        if parsed_rating is not None and (existing_rating is None or abs(parsed_rating - existing_rating) > 1e-9):
            set_fields["rating"] = parsed_rating

        existing_review_count = self._parse_reviews_count_text(existing.get("review_count"))
        if parsed_review_count is not None and (
            existing_review_count is None or parsed_review_count > existing_review_count
        ):
            set_fields["review_count"] = parsed_review_count

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

    def build_leads_query(
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

    def resolve_leads_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        normalized_sort_by = str(sort_by or "updated_at").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("Invalid sort_dir. Use 'asc' or 'desc'.")

        field_map = {
            "updated_at": "updated_at",
            "business_name": "business_name_normalized",
            "score": "score",
            "status": "status",
            "consent_status": "legal.consent_status",
            "source": "source",
        }
        field_name = field_map.get(normalized_sort_by)
        if field_name is None:
            raise ValueError(
                "Invalid sort_by. Use 'updated_at', 'business_name', 'score', 'status', 'consent_status' or 'source'."
            )

        direction = -1 if normalized_sort_dir == "desc" else 1
        return [(field_name, direction), ("_id", direction)]

    def _build_update_fields(self, *, updates: dict[str, Any]) -> tuple[dict[str, Any], Any]:
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

        return set_fields, now

    async def _sync_suppression_if_needed(
        self,
        *,
        updated: dict[str, Any],
        set_fields: dict[str, Any],
    ) -> None:
        if not (set_fields.get("legal.suppressed_reason") or set_fields.get("legal.do_not_contact")):
            return
        email_norm = self._normalize_email(updated.get("email"))
        email_value = str(updated.get("email") or "").strip()
        if email_norm and email_value:
            await self._upsert_suppression(
                email=email_value,
                reason=str(set_fields.get("legal.suppressed_reason") or "manual"),
                source="manual",
            )
