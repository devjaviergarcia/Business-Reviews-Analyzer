from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.database import get_database


class CreateCRMReportFeedbackUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        now_utc: Callable[[], datetime],
        parse_object_id: Callable[..., Any],
        record_event: Callable[..., Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        report_feedback_collection_name: str,
        report_requests_collection_name: str,
        leads_collection_name: str,
        lead_reports_collection_name: str = "lead_reports",
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._now_utc = now_utc
        self._parse_object_id = parse_object_id
        self._record_event = record_event
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._report_feedback_collection_name = report_feedback_collection_name
        self._report_requests_collection_name = report_requests_collection_name
        self._leads_collection_name = leads_collection_name
        self._lead_reports_collection_name = lead_reports_collection_name

    async def execute(
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
        await self._ensure_indexes()
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
        database = get_database()
        inserted = await database[self._report_feedback_collection_name].insert_one(doc)
        doc["_id"] = inserted.inserted_id
        feedback_id = str(inserted.inserted_id)

        if normalized_report_request_id:
            parsed_request_id = self._parse_object_id(normalized_report_request_id, field_name="report_request_id")
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
            parsed_lead_report_id = self._parse_object_id(normalized_lead_report_id, field_name="lead_report_id")
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
        serialized = self._serialize_mongo_doc(doc, id_key="report_feedback_id")
        return self._sanitize_payload(serialized)
