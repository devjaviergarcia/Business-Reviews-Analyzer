from __future__ import annotations

from typing import Any

from src.models.crm import CRMLeadStatus
from src.workers.contracts import CRMLeadPipelineTaskPayload


class CRMServiceLeadsFacet:

    async def enqueue_lead_pipeline_job(
        self,
        *,
        lead_id: str,
        force: bool = False,
        sources: list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
    ) -> dict[str, Any]:
        if self._enqueue_crm_lead_pipeline_job_use_case is not None:
            return await self._enqueue_crm_lead_pipeline_job_use_case.execute(
                lead_id=lead_id,
                force=force,
                sources=sources,
                google_maps_name=google_maps_name,
                tripadvisor_name=tripadvisor_name,
            )
        await self.ensure_indexes()
        return await self._lead_job_enqueue_runtime.enqueue_lead_pipeline_job(
            lead_id=lead_id,
            force=force,
            sources=sources,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )

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
        if self._list_crm_leads_use_case is not None:
            return await self._list_crm_leads_use_case.execute(
                page=page,
                page_size=page_size,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
        await self.ensure_indexes()
        return await self._legacy_lead_registry_runtime.list_leads(
            use_repo_v2=self._use_repo_v2,
            page=page,
            page_size=page_size,
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

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
        if self._create_crm_lead_use_case is not None:
            return await self._create_crm_lead_use_case.execute(
                business_name=business_name,
                contact_name=contact_name,
                email=email,
                phone=phone,
                website=website,
                category=category,
                city=city,
                address=address,
                source=source,
                status=status,
                notes=notes,
                tags=tags,
                do_not_contact=do_not_contact,
                consent_status=consent_status,
                suppressed_reason=suppressed_reason,
                unsubscribed=unsubscribed,
                consent_proof=consent_proof,
                source_ref=source_ref,
            )
        await self.ensure_indexes()
        return await self._legacy_lead_registry_runtime.create_lead(
            business_name=business_name,
            contact_name=contact_name,
            email=email,
            phone=phone,
            website=website,
            category=category,
            city=city,
            address=address,
            source=source,
            status=status,
            notes=notes,
            tags=tags,
            do_not_contact=do_not_contact,
            consent_status=consent_status,
            suppressed_reason=suppressed_reason,
            unsubscribed=unsubscribed,
            consent_proof=consent_proof,
            source_ref=source_ref,
        )

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
        if self._bulk_delete_crm_leads_use_case is not None:
            return await self._bulk_delete_crm_leads_use_case.execute(
                lead_ids=lead_ids,
                delete_all_matching=delete_all_matching,
                exclude_lead_ids=exclude_lead_ids,
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
        await self.ensure_indexes()
        return await self._legacy_lead_registry_runtime.bulk_delete_leads(
            use_repo_v2=self._use_repo_v2,
            lead_ids=lead_ids,
            delete_all_matching=delete_all_matching,
            exclude_lead_ids=exclude_lead_ids,
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )

    async def get_lead(self, *, lead_id: str, sync_pipeline_refs: bool = True) -> dict[str, Any]:
        if self._get_crm_lead_use_case is not None:
            return await self._get_crm_lead_use_case.execute(
                lead_id=lead_id,
                sync_pipeline_refs=sync_pipeline_refs,
            )
        await self.ensure_indexes()
        return await self._legacy_lead_registry_runtime.get_lead(
            use_repo_v2=self._use_repo_v2,
            lead_id=lead_id,
            sync_pipeline_refs=sync_pipeline_refs,
        )

    def _build_leads_query(
        self,
        *,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        return self._legacy_lead_registry_runtime.build_leads_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )

    def _resolve_leads_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        return self._legacy_lead_registry_runtime.resolve_leads_sort(
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    async def update_lead(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        if self._update_crm_lead_use_case is not None:
            return await self._update_crm_lead_use_case.execute(lead_id=lead_id, updates=updates)
        await self.ensure_indexes()
        return await self._legacy_lead_registry_runtime.update_lead(
            use_repo_v2=self._use_repo_v2,
            lead_id=lead_id,
            updates=updates,
        )

    async def _update_lead_v2(
        self,
        *,
        lead_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._legacy_lead_registry_runtime.update_lead_v2(
            lead_id=lead_id,
            updates=updates,
        )

    async def process_lead_pipeline_task(
        self,
        *,
        task_payload: CRMLeadPipelineTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_crm_lead_pipeline_task_use_case is not None:
            return await self._process_crm_lead_pipeline_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        return await self._legacy_lead_pipeline_runtime.process_lead_pipeline_task(
            task_payload=task_payload,
            job_id=job_id,
        )

    async def sync_lead_pipeline_refs(self, *, lead_id: str) -> dict[str, Any]:
        if self._sync_crm_lead_pipeline_refs_use_case is not None:
            return await self._sync_crm_lead_pipeline_refs_use_case.execute(lead_id=lead_id)
        await self.ensure_indexes()
        return await self._lead_pipeline_sync_runtime.sync_lead_pipeline_refs(
            lead_id=lead_id,
            pipeline_done_status=CRMLeadStatus.PIPELINE_DONE.value,
        )
