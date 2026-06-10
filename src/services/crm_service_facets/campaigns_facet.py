from __future__ import annotations

from typing import Any

from src.models.crm import CRMCadenceStep
from src.workers.contracts import CRMCampaignDispatchTaskPayload


class CRMServiceCampaignsFacet:

    async def list_campaigns(
        self,
        *,
        page: int = 1,
        page_size: int = 30,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        if self._list_crm_campaigns_use_case is not None:
            return await self._list_crm_campaigns_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
            )
        await self.ensure_indexes()
        return await self._campaign_query_runtime.list_campaigns(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
        )

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
        if self._create_crm_campaign_use_case is not None:
            return await self._create_crm_campaign_use_case.execute(
                name=name,
                description=description,
                audience_filter=audience_filter,
                source_mode=source_mode,
                selected_source=selected_source,
                cadence_template_id=cadence_template_id,
            )
        await self.ensure_indexes()
        return await self._campaign_workflow_runtime.create_campaign(
            name=name,
            description=description,
            audience_filter=audience_filter,
            source_mode=source_mode,
            selected_source=selected_source,
            cadence_template_id=cadence_template_id,
        )

    async def launch_campaign(self, *, campaign_id: str) -> dict[str, Any]:
        if self._launch_crm_campaign_use_case is not None:
            return await self._launch_crm_campaign_use_case.execute(campaign_id=campaign_id)
        await self.ensure_indexes()
        return await self._campaign_workflow_runtime.launch_campaign(campaign_id=campaign_id)

    async def enqueue_due_campaign_dispatch_jobs(self, *, campaign_id: str | None = None, limit: int = 200) -> int:
        if self._enqueue_due_campaign_dispatch_jobs_use_case is not None:
            return await self._enqueue_due_campaign_dispatch_jobs_use_case.execute(
                campaign_id=campaign_id,
                limit=limit,
            )
        await self.ensure_indexes()
        return await self._campaign_workflow_runtime.enqueue_due_campaign_dispatch_jobs(
            campaign_id=campaign_id,
            limit=limit,
        )

    async def list_messages(
        self,
        *,
        campaign_id: str | None = None,
        lead_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._list_crm_messages_use_case is not None:
            return await self._list_crm_messages_use_case.execute(
                campaign_id=campaign_id,
                lead_id=lead_id,
                page=page,
                page_size=page_size,
            )
        await self.ensure_indexes()
        return await self._campaign_query_runtime.list_messages(
            campaign_id=campaign_id,
            lead_id=lead_id,
            page=page,
            page_size=page_size,
        )

    async def list_events(
        self,
        *,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._list_crm_events_use_case is not None:
            return await self._list_crm_events_use_case.execute(
                lead_id=lead_id,
                campaign_id=campaign_id,
                page=page,
                page_size=page_size,
            )
        await self.ensure_indexes()
        return await self._campaign_query_runtime.list_events(
            lead_id=lead_id,
            campaign_id=campaign_id,
            page=page,
            page_size=page_size,
        )

    async def handle_resend_webhook(self, *, payload: dict[str, Any]) -> dict[str, Any]:
        if self._handle_resend_webhook_use_case is not None:
            return await self._handle_resend_webhook_use_case.execute(payload=payload)
        await self.ensure_indexes()
        return self._sanitize_payload(
            await self._campaign_delivery_runtime.handle_resend_webhook(
                payload=payload,
                analyses_collection_name=self._ANALYSES_COLLECTION,
                block_lead_contact=self._block_lead_contact,
                upsert_suppression=self._upsert_suppression,
            )
        )

    async def process_campaign_dispatch_task(
        self,
        *,
        task_payload: CRMCampaignDispatchTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_campaign_dispatch_task_use_case is not None:
            return await self._process_campaign_dispatch_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        return await self._legacy_campaign_dispatch_runtime.process_campaign_dispatch_task(
            task_payload=task_payload,
            job_id=job_id,
        )

    async def _resolve_cadence_template(self, cadence_template_id: str | None) -> dict[str, Any]:
        return await self._campaign_cadence_runtime.resolve_cadence_template(cadence_template_id)

    async def _ensure_default_cadence_template(self) -> None:
        await self._campaign_cadence_runtime.ensure_default_cadence_template()

    def _build_campaign_lead_query(self, audience_filter: Any) -> dict[str, Any]:
        return self._campaign_delivery_runtime.build_campaign_lead_query(audience_filter)

    async def _load_suppressed_emails(self) -> set[str]:
        return await self._campaign_delivery_runtime.load_suppressed_emails()

    async def _can_send_to_lead(self, *, lead_doc: dict[str, Any]) -> tuple[bool, str]:
        return await self._campaign_delivery_runtime.can_send_to_lead(
            lead_doc=lead_doc,
            is_email_suppressed=self._is_email_suppressed,
        )

    async def _is_email_suppressed(self, email_normalized: str) -> bool:
        return await self._campaign_delivery_runtime.is_email_suppressed(email_normalized)

    async def _block_lead_contact(self, *, lead_id: str, reason: str) -> None:
        await self._campaign_delivery_runtime.block_lead_contact(lead_id=lead_id, reason=reason)

    async def _stop_pending_messages_for_lead(self, *, lead_id: str, reason: str) -> None:
        await self._campaign_delivery_runtime.stop_pending_messages_for_lead(lead_id=lead_id, reason=reason)

    async def _upsert_suppression(self, *, email: str, reason: str, source: str) -> None:
        await self._campaign_delivery_runtime.upsert_suppression(email=email, reason=reason, source=source)

    async def _build_mini_report_for_lead(self, *, lead_doc: dict[str, Any]) -> str:
        return await self._campaign_delivery_runtime.build_mini_report_for_lead(lead_doc=lead_doc)

    def _render_cadence_step(
        self,
        *,
        step: CRMCadenceStep,
        lead_doc: dict[str, Any],
        mini_report: str,
    ) -> tuple[str, str]:
        return self._campaign_delivery_runtime.render_cadence_step(
            step=step,
            lead_doc=lead_doc,
            mini_report=mini_report,
        )

    def _send_resend_email(self, *, to_email: str, subject: str, html_body: str) -> dict[str, Any]:
        return self._campaign_delivery_runtime.send_resend_email(
            to_email=to_email,
            subject=subject,
            html_body=html_body,
        )

    def _text_to_html(self, text: str) -> str:
        return self._campaign_delivery_runtime.text_to_html(text)

    def _unsubscribe_token(self, *, lead_id: str, email: str) -> str:
        return self._campaign_delivery_runtime.unsubscribe_token(lead_id=lead_id, email=email)
