from __future__ import annotations

import hashlib
import json
import re
from email.utils import formataddr
from typing import Any, Awaitable, Callable
from urllib.request import Request, urlopen

from bson.errors import InvalidId

from src.config import settings
from src.models.crm import CRMConsentStatus, CRMEvent, CRMMessageStatus, CRMSuppression


DatabaseFactory = Callable[[], Any]
NowUtcFn = Callable[[], Any]
NormalizeEmailFn = Callable[[Any], str | None]
ParseObjectIdFn = Callable[..., Any]
RecordEventFn = Callable[..., Awaitable[None]]


class CampaignDeliveryRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        now_utc: NowUtcFn,
        normalize_email: NormalizeEmailFn,
        parse_object_id: ParseObjectIdFn,
        record_event: RecordEventFn,
        leads_collection_name: str,
        analyses_collection_name: str,
        messages_collection_name: str,
        suppressions_collection_name: str,
        jobs_collection_name: str,
        events_collection_insert: Callable[[dict[str, Any]], Awaitable[Any]],
    ) -> None:
        self._database_factory = database_factory
        self._now_utc = now_utc
        self._normalize_email = normalize_email
        self._parse_object_id = parse_object_id
        self._record_event = record_event
        self._leads_collection_name = leads_collection_name
        self._analyses_collection_name = analyses_collection_name
        self._messages_collection_name = messages_collection_name
        self._suppressions_collection_name = suppressions_collection_name
        self._jobs_collection_name = jobs_collection_name
        self._events_collection_insert = events_collection_insert

    async def load_suppressed_emails(self) -> set[str]:
        suppressions = self._database_factory()[self._suppressions_collection_name]
        docs = await suppressions.find({}, projection={"email_normalized": 1}).to_list(length=50000)
        values: set[str] = set()
        for doc in docs:
            normalized = self._normalize_email(doc.get("email_normalized"))
            if normalized:
                values.add(normalized)
        return values

    def build_campaign_lead_query(self, audience_filter: Any) -> dict[str, Any]:
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
            parsed_ids: list[Any] = []
            for raw_id in lead_ids:
                raw = str(raw_id or "").strip()
                if not raw:
                    continue
                try:
                    parsed_ids.append(self._parse_object_id(raw, field_name="lead_id"))
                except ValueError:
                    continue
            if parsed_ids:
                query["_id"] = {"$in": parsed_ids}
        return query

    async def is_email_suppressed(self, email_normalized: str) -> bool:
        suppressions = self._database_factory()[self._suppressions_collection_name]
        doc = await suppressions.find_one({"email_normalized": email_normalized}, projection={"_id": 1})
        return doc is not None

    async def can_send_to_lead(
        self,
        *,
        lead_doc: dict[str, Any],
        is_email_suppressed: Callable[[str], Awaitable[bool]] | None = None,
    ) -> tuple[bool, str]:
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

        suppressed_checker = is_email_suppressed or self.is_email_suppressed
        if await suppressed_checker(email_normalized):
            return False, "suppressed"
        return True, "ok"

    async def block_lead_contact(self, *, lead_id: str, reason: str) -> None:
        leads = self._database_factory()[self._leads_collection_name]
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
        await self.stop_pending_messages_for_lead(lead_id=lead_id, reason=reason)

    async def stop_pending_messages_for_lead(self, *, lead_id: str, reason: str) -> None:
        messages = self._database_factory()[self._messages_collection_name]
        now = self._now_utc()
        await messages.update_many(
            {"lead_id": lead_id, "status": CRMMessageStatus.QUEUED.value},
            {"$set": {"status": CRMMessageStatus.SKIPPED.value, "error": f"stopped:{reason}", "updated_at": now, "dispatch_job_id": None}},
        )

    async def upsert_suppression(self, *, email: str, reason: str, source: str) -> None:
        normalized = self._normalize_email(email)
        if not normalized:
            return
        suppressions = self._database_factory()[self._suppressions_collection_name]
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
                "$setOnInsert": {"created_at": now, "email_normalized": normalized},
            },
            upsert=True,
        )

    async def build_mini_report_for_lead(self, *, lead_doc: dict[str, Any]) -> str:
        pipeline = lead_doc.get("pipeline") if isinstance(lead_doc.get("pipeline"), dict) else {}
        business_id = str(pipeline.get("business_id") or "").strip()
        if not business_id:
            return "Todavía no hay informe completo, pero podemos activarlo en tu ficha en cuanto lo prefieras."
        try:
            parsed_business_id = self._parse_object_id(business_id, field_name="business_id")
        except ValueError:
            return "Tenemos señales de mejora en reseñas recientes y podemos enseñártelas en una demo corta."

        analyses = self._database_factory()[self._analyses_collection_name]
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

    def render_cadence_step(
        self,
        *,
        step: Any,
        lead_doc: dict[str, Any],
        mini_report: str,
    ) -> tuple[str, str]:
        business_name = str(lead_doc.get("business_name") or "tu negocio").strip()
        lead_id = str(lead_doc.get("_id") or "").strip()
        cta_url = str(settings.crm_cta_url or "").strip() or "https://repiq.es/#pre-report-form"
        unsubscribe_base = str(settings.crm_unsubscribe_url or "").strip() or cta_url
        unsubscribe_token = self.unsubscribe_token(lead_id=lead_id, email=str(lead_doc.get("email") or ""))
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
        body_html = self.text_to_html(body_text)
        return subject, body_html

    def send_resend_email(self, *, to_email: str, subject: str, html_body: str) -> dict[str, Any]:
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
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urlopen(request, timeout=20) as response:  # noqa: S310
            raw_response = response.read().decode("utf-8", errors="ignore")
            if not raw_response.strip():
                return {"id": None, "provider_status": response.status}
            parsed = json.loads(raw_response)
            if isinstance(parsed, dict):
                return parsed
            return {"id": None, "provider_status": response.status, "raw": parsed}

    def text_to_html(self, text: str) -> str:
        lines = [line.strip() for line in str(text or "").splitlines()]
        html_lines: list[str] = []
        for line in lines:
            if not line:
                html_lines.append("<p>&nbsp;</p>")
                continue
            safe_line = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append(f"<p>{safe_line}</p>")
        return "".join(html_lines)

    def unsubscribe_token(self, *, lead_id: str, email: str) -> str:
        secret = str(settings.crm_unsubscribe_secret or "").strip() or "crm-unsubscribe-secret"
        payload = f"{lead_id}|{email}|{secret}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    async def insert_event(self, event: CRMEvent) -> None:
        await self._events_collection_insert(event.model_dump(mode="python"))

    async def handle_resend_webhook(
        self,
        *,
        payload: dict[str, Any],
        analyses_collection_name: str,
        block_lead_contact: Callable[..., Awaitable[None]],
        upsert_suppression: Callable[..., Awaitable[None]],
    ) -> dict[str, Any]:
        del analyses_collection_name
        event_type = str(payload.get("type") or "").strip().lower()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        provider_message_id = str(data.get("email_id") or data.get("id") or payload.get("email_id") or "").strip()
        if not provider_message_id:
            return {"ok": True, "ignored": True, "reason": "missing_provider_message_id"}

        messages = self._database_factory()[self._messages_collection_name]
        message_doc = await messages.find_one({"provider_message_id": provider_message_id})
        if message_doc is None:
            return {"ok": True, "ignored": True, "reason": "message_not_found"}

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
            return {"ok": True, "ignored": True, "reason": f"unsupported_event_type:{event_type}"}

        await messages.update_one({"_id": message_doc.get("_id")}, {"$set": set_fields})

        if lead_id and event_type in {
            "email.unsubscribed",
            "email.suppressed",
            "email.bounced",
            "email.complained",
            "email.replied",
            "email.reply",
        }:
            reason = (
                "unsubscribed"
                if event_type in {"email.unsubscribed", "email.suppressed"}
                else "bounced"
                if event_type in {"email.bounced", "email.complained"}
                else "replied"
            )
            await block_lead_contact(lead_id=lead_id, reason=reason)
            if reason in {"unsubscribed", "bounced"}:
                leads = self._database_factory()[self._leads_collection_name]
                try:
                    parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
                except ValueError:
                    parsed_lead_id = None
                if parsed_lead_id is not None:
                    lead_doc = await leads.find_one({"_id": parsed_lead_id})
                    if isinstance(lead_doc, dict):
                        email = str(lead_doc.get("email") or "").strip()
                        if email:
                            await upsert_suppression(
                                email=email,
                                reason=reason,
                                source="resend_webhook",
                            )

        await self._record_event(
            event_type="email_webhook_processed",
            lead_id=lead_id,
            campaign_id=campaign_id,
            message_id=message_id,
            data={"provider_message_id": provider_message_id, "event_type": event_type},
        )
        return {"ok": True, "message_id": message_id, "event_type": event_type}
