from __future__ import annotations

from typing import Any


class CRMServiceReportRequestsFacet:

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
        if self._create_crm_report_request_use_case is not None:
            return await self._create_crm_report_request_use_case.execute(
                business_name=business_name,
                city=city,
                category=category,
                contact_name=contact_name,
                email=email,
                phone=phone,
                website=website,
                message=message,
                consent_report=consent_report,
                consent_marketing=consent_marketing,
                utm=utm,
                source_page=source_page,
            )
        await self.ensure_indexes()
        return await self._legacy_report_request_runtime.create_report_request(
            business_name=business_name,
            city=city,
            category=category,
            contact_name=contact_name,
            email=email,
            phone=phone,
            website=website,
            message=message,
            consent_report=consent_report,
            consent_marketing=consent_marketing,
            utm=utm,
            source_page=source_page,
        )

    async def list_report_requests(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        if self._list_crm_report_requests_use_case is not None:
            return await self._list_crm_report_requests_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                q=q,
            )
        await self.ensure_indexes()
        return await self._legacy_report_request_runtime.list_report_requests(
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            q=q,
        )

    async def retry_report_request(self, *, report_request_id: str) -> dict[str, Any]:
        if self._retry_crm_report_request_use_case is not None:
            return await self._retry_crm_report_request_use_case.execute(
                report_request_id=report_request_id,
            )
        await self.ensure_indexes()
        return await self._legacy_report_request_runtime.retry_report_request(
            report_request_id=report_request_id,
        )

    async def process_pending_report_requests(self, *, limit: int = 50) -> dict[str, Any]:
        if self._process_pending_crm_report_requests_use_case is not None:
            return await self._process_pending_crm_report_requests_use_case.execute(limit=limit)
        await self.ensure_indexes()
        return await self._legacy_report_request_runtime.process_pending_report_requests(limit=limit)

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
        if self._create_crm_report_feedback_use_case is not None:
            return await self._create_crm_report_feedback_use_case.execute(
                branch=branch,
                answers=answers,
                lead_id=lead_id,
                report_request_id=report_request_id,
                lead_report_id=lead_report_id,
                benchmark_business_id=benchmark_business_id,
                report_kind=report_kind,
                source_page=source_page,
                referrer=referrer,
                user_agent=user_agent,
                ip_hash=ip_hash,
            )
        await self.ensure_indexes()
        return await self._legacy_report_request_runtime.create_report_feedback(
            branch=branch,
            answers=answers,
            lead_id=lead_id,
            report_request_id=report_request_id,
            lead_report_id=lead_report_id,
            benchmark_business_id=benchmark_business_id,
            report_kind=report_kind,
            source_page=source_page,
            referrer=referrer,
            user_agent=user_agent,
            ip_hash=ip_hash,
        )

    async def _enqueue_report_request_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        business_name = str(doc.get("business_name") or "").strip()
        query = str(doc.get("query") or "").strip()
        if not query:
            query = " ".join(item for item in (business_name, str(doc.get("city") or "").strip()) if item)
        if not query:
            raise ValueError("Report request has no query to enqueue.")
        queued = await self.enqueue_benchmark_study_job(
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
