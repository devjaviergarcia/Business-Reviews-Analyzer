from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database


class CreateCRMReportRequestUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        normalize_email: Callable[[Any], str | None],
        normalize_text: Callable[[Any], str],
        normalize_utm: Callable[[dict[str, Any]], dict[str, str | None]],
        enqueue_benchmark_study_job: Callable[..., Awaitable[dict[str, Any]]],
        record_event: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        report_requests_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._normalize_email = normalize_email
        self._normalize_text = normalize_text
        self._normalize_utm = normalize_utm
        self._enqueue_benchmark_study_job = enqueue_benchmark_study_job
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._report_requests_collection_name = report_requests_collection_name

    async def execute(
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
        await self._ensure_indexes()
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
        collection = get_database()[self._report_requests_collection_name]
        inserted = await collection.insert_one(doc)
        doc["_id"] = inserted.inserted_id
        report_request_id = str(inserted.inserted_id)

        try:
            update_fields = await self._enqueue_report_request_doc(doc)
            await collection.update_one({"_id": inserted.inserted_id}, {"$set": update_fields})
            doc.update(update_fields)
        except Exception as exc:  # noqa: BLE001
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
        serialized = self._serialize_mongo_doc(doc, id_key="report_request_id")
        return self._sanitize_payload(serialized)

    async def _enqueue_report_request_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        business_name = str(doc.get("business_name") or "").strip()
        query = str(doc.get("query") or "").strip()
        if not query:
            query = " ".join(item for item in (business_name, str(doc.get("city") or "").strip()) if item)
        if not query:
            raise ValueError("Report request has no query to enqueue.")
        queued = await self._enqueue_benchmark_study_job(
            query=query,
            city=str(doc.get("city") or "").strip() or None,
            category=str(doc.get("category") or "").strip() or None,
            limit=30,
            source="auto_live_google_maps",
            title=f"Solicitud informe: {business_name or query}",
        )
        return {
            "status": "queued",
            "job_id": str(queued.get("job_id") or "").strip() or None,
            "benchmark_run_id": str(queued.get("benchmark_run_id") or "").strip() or None,
            "failure_reason": None,
            "updated_at": self._now_utc(),
        }
