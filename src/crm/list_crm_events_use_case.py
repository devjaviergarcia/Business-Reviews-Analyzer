from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination


class ListCRMEventsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        use_repo_v2: bool,
        event_repository: Any,
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        events_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._use_repo_v2 = use_repo_v2
        self._event_repository = event_repository
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._events_collection_name = events_collection_name

    async def execute(
        self,
        *,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        if self._use_repo_v2:
            payload = await self._event_repository.list(
                page=page,
                page_size=page_size,
                lead_id=lead_id,
                campaign_id=campaign_id,
            )
            return self._sanitize_payload(payload)

        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        events = get_database()[self._events_collection_name]
        query: dict[str, Any] = {}
        if lead_id:
            query["lead_id"] = str(lead_id)
        if campaign_id:
            query["campaign_id"] = str(campaign_id)

        total = await events.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await events.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items = [self._serialize_mongo_doc(doc, id_key="event_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)
