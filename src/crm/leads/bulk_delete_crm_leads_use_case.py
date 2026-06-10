from __future__ import annotations

from typing import Any, Awaitable, Callable

from bson import ObjectId

from src.database import get_database


class BulkDeleteCRMLeadsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_repo_v2: bool,
        lead_repository: Any,
        parse_object_id: Callable[..., ObjectId],
        build_leads_query: Callable[..., dict[str, Any]],
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_repo_v2 = use_repo_v2
        self._lead_repository = lead_repository
        self._parse_object_id = parse_object_id
        self._build_leads_query = build_leads_query
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name

    async def execute(
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
        await self._ensure_indexes()
        if self._use_repo_v2:
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

        leads = get_database()[self._leads_collection_name]

        normalized_ids: list[ObjectId] = []
        raw_ids = list(lead_ids or [])
        seen_ids: set[str] = set()
        for raw_id in raw_ids:
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(self._parse_object_id(normalized, field_name="lead_id"))

        excluded_ids: list[ObjectId] = []
        raw_excluded_ids = list(exclude_lead_ids or [])
        seen_excluded_ids: set[str] = set()
        for raw_id in raw_excluded_ids:
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
            query = self._build_leads_query(
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
