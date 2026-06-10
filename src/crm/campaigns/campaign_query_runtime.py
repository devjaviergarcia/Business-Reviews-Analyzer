from __future__ import annotations

from typing import Any, Callable


DatabaseFactory = Callable[[], Any]
CoercePaginationFn = Callable[..., tuple[int, int]]
BuildPaginationPayloadFn = Callable[..., dict[str, Any]]
SanitizePayloadFn = Callable[[Any], Any]


class CampaignQueryRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        coerce_pagination: CoercePaginationFn,
        build_pagination_payload: BuildPaginationPayloadFn,
        sanitize_payload: SanitizePayloadFn,
        event_repository: Any,
        use_repo_v2: bool,
        campaigns_collection_name: str,
        messages_collection_name: str,
        events_collection_name: str,
    ) -> None:
        self._database_factory = database_factory
        self._coerce_pagination = coerce_pagination
        self._build_pagination_payload = build_pagination_payload
        self._sanitize_payload = sanitize_payload
        self._event_repository = event_repository
        self._use_repo_v2 = use_repo_v2
        self._campaigns_collection_name = campaigns_collection_name
        self._messages_collection_name = messages_collection_name
        self._events_collection_name = events_collection_name

    async def list_campaigns(self, *, page: int = 1, page_size: int = 30, status_filter: str | None = None) -> dict[str, Any]:
        page_value, page_size_value = self._coerce_pagination(page=page, page_size=page_size, max_page_size=100)
        campaigns = self._database_factory()[self._campaigns_collection_name]
        query: dict[str, Any] = {}
        normalized_status = str(status_filter or '').strip().lower()
        if normalized_status:
            query['status'] = normalized_status
        total = await campaigns.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await campaigns.find(query)
            .sort([('updated_at', -1), ('_id', -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key='campaign_id') for doc in docs]
        payload = self._build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def list_messages(
        self,
        *,
        campaign_id: str | None = None,
        lead_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        page_value, page_size_value = self._coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        messages = self._database_factory()[self._messages_collection_name]
        query: dict[str, Any] = {}
        if campaign_id:
            query['campaign_id'] = str(campaign_id)
        if lead_id:
            query['lead_id'] = str(lead_id)
        total = await messages.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await messages.find(query)
            .sort([('scheduled_at', -1), ('_id', -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key='message_id') for doc in docs]
        payload = self._build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    async def list_events(
        self,
        *,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        if self._use_repo_v2:
            return self._sanitize_payload(
                await self._event_repository.list(
                    page=page,
                    page_size=page_size,
                    lead_id=lead_id,
                    campaign_id=campaign_id,
                )
            )

        page_value, page_size_value = self._coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        events = self._database_factory()[self._events_collection_name]
        query: dict[str, Any] = {}
        if lead_id:
            query['lead_id'] = str(lead_id)
        if campaign_id:
            query['campaign_id'] = str(campaign_id)
        total = await events.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await events.find(query)
            .sort([('created_at', -1), ('_id', -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key='event_id') for doc in docs]
        payload = self._build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)

    @staticmethod
    def _serialize_mongo_doc(doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop('_id'))
        return payload
