from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Awaitable, Callable

from src.services.pagination import build_pagination_payload, coerce_pagination


DatabaseFactory = Callable[[], Any]
NowUtcFn = Callable[[], datetime]
NormalizeTextFn = Callable[[Any], str]
NormalizeEmailFn = Callable[[Any], str | None]
NormalizeUtmFn = Callable[[dict[str, Any]], dict[str, str | None]]
ParseObjectIdFn = Callable[..., Any]
EnqueueReportRequestDocFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
RecordEventFn = Callable[..., Awaitable[None]]
SerializeMongoDocFn = Callable[..., dict[str, Any]]
SanitizePayloadFn = Callable[[Any], Any]


class LegacyReportRequestRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        report_requests_collection_name: str,
        report_feedback_collection_name: str,
        leads_collection_name: str,
        lead_reports_collection_name: str = "lead_reports",
        now_utc: NowUtcFn,
        normalize_text: NormalizeTextFn,
        normalize_email: NormalizeEmailFn,
        normalize_utm: NormalizeUtmFn,
        parse_object_id: ParseObjectIdFn,
        enqueue_report_request_doc: EnqueueReportRequestDocFn,
        record_event: RecordEventFn,
        serialize_mongo_doc: SerializeMongoDocFn,
        sanitize_payload: SanitizePayloadFn,
    ) -> None:
        self._database_factory = database_factory
        self._report_requests_collection_name = report_requests_collection_name
        self._report_feedback_collection_name = report_feedback_collection_name
        self._leads_collection_name = leads_collection_name
        self._lead_reports_collection_name = lead_reports_collection_name
        self._now_utc = now_utc
        self._normalize_text = normalize_text
        self._normalize_email = normalize_email
        self._normalize_utm = normalize_utm
        self._parse_object_id = parse_object_id
        self._enqueue_report_request_doc = enqueue_report_request_doc
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload

    async def create_report_request(
        self,
        *,
        business_name: str,
        city: str | None,
        category: str | None = None,
        contact_name: str | None = None,
        email: str,
        phone: str | None = None,
        website: str | None = None,
        message: str | None = None,
        consent_report: bool,
        consent_marketing: bool = False,
        utm: dict[str, Any] | None = None,
        source_page: str | None = None,
    ) -> dict[str, Any]:
        normalized_business_name = str(business_name or "").strip()
        normalized_email = str(email or "").strip()
        if not normalized_business_name:
            raise ValueError("business_name is required.")

        normalized_email_address = self._normalize_email(normalized_email)
        if not normalized_email_address:
            raise ValueError("valid email is required.")
        if not consent_report:
            raise ValueError("consent_report is required to send the requested report.")

        now = self._now_utc()
        query = " ".join(item for item in (normalized_business_name, str(city or "").strip()) if item)
        doc: dict[str, Any] = {
            "business_name": normalized_business_name,
            "business_name_normalized": self._normalize_text(normalized_business_name),
            "city": str(city or "").strip() or None,
            "category": str(category or "").strip() or None,
            "contact_name": str(contact_name or "").strip() or None,
            "email": normalized_email,
            "email_normalized": normalized_email_address,
            "phone": str(phone or "").strip() or None,
            "website": str(website or "").strip() or None,
            "message": str(message or "").strip() or None,
            "query": query,
            "status": "queued",
            "source": "landing_report_request",
            "source_page": str(source_page or "").strip() or None,
            "utm": self._normalize_utm(utm or {}),
            "consents": {
                "report_delivery": {
                    "granted": True,
                    "granted_at": now,
                    "text": "Acepto recibir por email el informe solicitado y comunicaciones necesarias para entregarlo.",
                },
                "marketing": {
                    "granted": bool(consent_marketing),
                    "granted_at": now if consent_marketing else None,
                    "text": "Acepto recibir contenido comercial y seguimiento opcional.",
                },
            },
            "created_at": now,
            "updated_at": now,
            "benchmark_run_id": None,
            "job_id": None,
            "failure_reason": None,
        }

        collection = self._database_factory()[self._report_requests_collection_name]
        inserted = await collection.insert_one(doc)
        doc["_id"] = inserted.inserted_id
        report_request_id = str(inserted.inserted_id)

        try:
            update_fields = await self._enqueue_report_request_doc(doc)
            await collection.update_one({"_id": inserted.inserted_id}, {"$set": update_fields})
            doc.update(update_fields)
        except Exception as exc:
            update_fields = {
                "status": "failed_to_queue",
                "failure_reason": str(exc),
                "updated_at": self._now_utc(),
            }
            await collection.update_one({"_id": inserted.inserted_id}, {"$set": update_fields})
            doc.update(update_fields)

        await self._record_event(
            event_type="report_request_created",
            data={
                "report_request_id": report_request_id,
                "business_name": normalized_business_name,
                "city": doc.get("city"),
                "email": normalized_email_address,
                "consent_marketing": bool(consent_marketing),
                "status": doc.get("status"),
                "benchmark_run_id": doc.get("benchmark_run_id"),
                "job_id": doc.get("job_id"),
                "failure_reason": doc.get("failure_reason"),
                "utm": doc.get("utm"),
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_request_id"))

    async def list_report_requests(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query: dict[str, Any] = {}
        if str(status_filter or "").strip():
            query["status"] = str(status_filter).strip()

        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]

        collection = self._database_factory()[self._report_requests_collection_name]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key="report_request_id") for doc in docs]
        payload = build_pagination_payload(
            items=items,
            page=page_value,
            page_size=page_size_value,
            total=total,
        )
        return self._sanitize_payload(payload)

    async def retry_report_request(self, *, report_request_id: str) -> dict[str, Any]:
        parsed_id = self._parse_object_id(report_request_id, field_name="report_request_id")
        collection = self._database_factory()[self._report_requests_collection_name]
        doc = await collection.find_one({"_id": parsed_id})
        if doc is None:
            raise LookupError(f"Report request '{report_request_id}' not found.")

        update_fields = await self._enqueue_report_request_doc(doc)
        await collection.update_one({"_id": parsed_id}, {"$set": update_fields})
        doc.update(update_fields)
        await self._record_event(
            event_type="report_request_retried",
            data={
                "report_request_id": report_request_id,
                "job_id": doc.get("job_id"),
                "benchmark_run_id": doc.get("benchmark_run_id"),
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_request_id"))

    async def process_pending_report_requests(self, *, limit: int = 50) -> dict[str, Any]:
        limit_value = max(1, min(int(limit or 50), 200))
        collection = self._database_factory()[self._report_requests_collection_name]
        query = {
            "$or": [
                {"status": {"$in": ["pending", "failed_to_queue"]}},
                {"job_id": None},
            ]
        }
        docs = (
            await collection.find(query)
            .sort([("created_at", 1), ("_id", 1)])
            .limit(limit_value)
            .to_list(length=limit_value)
        )

        processed = 0
        retried = 0
        failed = 0
        errors: list[dict[str, Any]] = []
        for doc in docs:
            processed += 1
            report_request_id = str(doc.get("_id"))
            try:
                update_fields = await self._enqueue_report_request_doc(doc)
                await collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})
                retried += 1
            except Exception as exc:
                failed += 1
                errors.append({"report_request_id": report_request_id, "error": str(exc)})
                await collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "status": "failed_to_queue",
                            "failure_reason": str(exc),
                            "updated_at": self._now_utc(),
                        }
                    },
                )

        if processed > 0:
            await self._record_event(
                event_type="report_requests_pending_processed",
                data={"processed": processed, "retried": retried, "failed": failed, "limit": limit_value},
            )
        return self._sanitize_payload(
            {
                "processed": processed,
                "retried": retried,
                "failed": failed,
                "errors": errors,
            }
        )

    async def create_report_feedback(
        self,
        *,
        branch: str,
        answers: dict[str, Any] | None = None,
        lead_id: str | None = None,
        report_request_id: str | None = None,
        lead_report_id: str | None = None,
        benchmark_business_id: str | None = None,
        report_kind: str | None = None,
        source_page: str | None = None,
        referrer: str | None = None,
        user_agent: str | None = None,
        ip_hash: str | None = None,
    ) -> dict[str, Any]:
        normalized_branch = str(branch or "").strip().upper()
        if normalized_branch not in {"A", "B", "C"}:
            raise ValueError("branch must be one of A, B or C.")

        normalized_lead_id = str(lead_id or "").strip() or None
        normalized_report_request_id = str(report_request_id or "").strip() or None
        normalized_lead_report_id = str(lead_report_id or "").strip() or None
        normalized_benchmark_business_id = str(benchmark_business_id or "").strip() or None
        normalized_report_kind = str(report_kind or "").strip().lower() or "lead"
        normalized_source_page = str(source_page or "").strip() or None
        normalized_referrer = str(referrer or "").strip() or None
        normalized_user_agent = str(user_agent or "").strip() or None
        normalized_ip_hash = str(ip_hash or "").strip() or None
        payload_answers = dict(answers or {})

        if not any(
            (
                normalized_lead_id,
                normalized_report_request_id,
                normalized_lead_report_id,
                normalized_benchmark_business_id,
            )
        ):
            raise ValueError(
                "At least one identifier is required (lead_id, report_request_id, lead_report_id or benchmark_business_id)."
            )

        label = "warm_lead"
        if normalized_branch == "A":
            label = "hot_lead"
        elif normalized_branch == "C":
            reasons = payload_answers.get("c1_reasons")
            reason_values = (
                [str(item).strip().lower() for item in reasons]
                if isinstance(reasons, list)
                else [str(reasons or "").strip().lower()]
            )
            label = "recoverable" if "ia_gratis" in reason_values else "cold_lead"

        now = self._now_utc()
        doc: dict[str, Any] = {
            "branch": normalized_branch,
            "label": label,
            "report_kind": normalized_report_kind,
            "lead_id": normalized_lead_id,
            "report_request_id": normalized_report_request_id,
            "lead_report_id": normalized_lead_report_id,
            "benchmark_business_id": normalized_benchmark_business_id,
            "answers": payload_answers,
            "source_page": normalized_source_page,
            "referrer": normalized_referrer,
            "user_agent": normalized_user_agent,
            "ip_hash": normalized_ip_hash,
            "created_at": now,
            "updated_at": now,
        }

        database = self._database_factory()
        inserted = await database[self._report_feedback_collection_name].insert_one(doc)
        doc["_id"] = inserted.inserted_id
        feedback_id = str(inserted.inserted_id)

        if normalized_report_request_id:
            parsed_request_id = self._parse_object_id(
                normalized_report_request_id,
                field_name="report_request_id",
            )
            await database[self._report_requests_collection_name].update_one(
                {"_id": parsed_request_id},
                {
                    "$set": {
                        "feedback.latest_feedback_id": feedback_id,
                        "feedback.branch": normalized_branch,
                        "feedback.label": label,
                        "feedback.answers": payload_answers,
                        "feedback.updated_at": now,
                        "updated_at": now,
                    }
                },
            )

        if normalized_lead_report_id:
            parsed_lead_report_id = self._parse_object_id(
                normalized_lead_report_id,
                field_name="lead_report_id",
            )
            await database[self._lead_reports_collection_name].update_one(
                {"_id": parsed_lead_report_id},
                {
                    "$set": {
                        "feedback.latest_feedback_id": feedback_id,
                        "feedback.branch": normalized_branch,
                        "feedback.label": label,
                        "feedback.answers": payload_answers,
                        "feedback.updated_at": now,
                        "updated_at": now,
                    }
                },
            )

        if normalized_lead_id:
            parsed_lead_id = self._parse_object_id(normalized_lead_id, field_name="lead_id")
            await database[self._leads_collection_name].update_one(
                {"_id": parsed_lead_id},
                {
                    "$set": {
                        "status": "form_2_done",
                        "updated_at": now,
                        "source_ref.last_feedback_id": feedback_id,
                    },
                    "$addToSet": {
                        "tags": label,
                        "notes": f"Feedback formulario final rama {normalized_branch} ({label}) · {now.isoformat()}",
                    },
                },
            )

        await self._record_event(
            event_type="report_feedback_submitted",
            lead_id=normalized_lead_id,
            data={
                "report_feedback_id": feedback_id,
                "branch": normalized_branch,
                "label": label,
                "lead_id": normalized_lead_id,
                "report_request_id": normalized_report_request_id,
                "lead_report_id": normalized_lead_report_id,
                "benchmark_business_id": normalized_benchmark_business_id,
                "report_kind": normalized_report_kind,
            },
        )
        return self._sanitize_payload(self._serialize_mongo_doc(doc, id_key="report_feedback_id"))
