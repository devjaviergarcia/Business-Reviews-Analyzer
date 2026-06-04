from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.database import get_database


class GetCRMLeadUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_repo_v2: bool,
        lead_repository: Any,
        parse_object_id: Callable[..., Any],
        sync_lead_pipeline_refs: Callable[..., Awaitable[dict[str, Any]]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_repo_v2 = use_repo_v2
        self._lead_repository = lead_repository
        self._parse_object_id = parse_object_id
        self._sync_lead_pipeline_refs = sync_lead_pipeline_refs
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name

    async def execute(self, *, lead_id: str, sync_pipeline_refs: bool = True) -> dict[str, Any]:
        await self._ensure_indexes()
        if self._use_repo_v2:
            lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
        else:
            parsed_lead_id = self._parse_object_id(lead_id, field_name="lead_id")
            leads = get_database()[self._leads_collection_name]
            lead_doc = await leads.find_one({"_id": parsed_lead_id})
        if lead_doc is None:
            raise LookupError(f"Lead '{lead_id}' not found.")

        if sync_pipeline_refs:
            await self._sync_lead_pipeline_refs(lead_id=lead_id)
            if self._use_repo_v2:
                lead_doc = await self._lead_repository.get_by_id(lead_id=lead_id)
            else:
                lead_doc = await leads.find_one({"_id": parsed_lead_id})
            if lead_doc is None:
                raise LookupError(f"Lead '{lead_id}' not found.")

        return self._sanitize_payload(self._serialize_mongo_doc(lead_doc, id_key="lead_id"))
