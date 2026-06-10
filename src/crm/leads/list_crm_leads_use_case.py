from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination


class ListCRMLeadsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_repo_v2: bool,
        lead_repository: Any,
        build_leads_query: Callable[..., dict[str, Any]],
        resolve_leads_sort: Callable[..., list[tuple[str, int]]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        leads_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_repo_v2 = use_repo_v2
        self._lead_repository = lead_repository
        self._build_leads_query = build_leads_query
        self._resolve_leads_sort = resolve_leads_sort
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._leads_collection_name = leads_collection_name

    async def execute(
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
        await self._ensure_indexes()
        if self._use_repo_v2:
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
        leads = get_database()[self._leads_collection_name]
        query = self._build_leads_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )
        sort_spec = self._resolve_leads_sort(sort_by=sort_by, sort_dir=sort_dir)

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
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)
