from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

from src.database import get_database
from src.services.pagination import build_pagination_payload, coerce_pagination


class ListCRMReportRequestsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        serialize_mongo_doc: Callable[[dict[str, Any], str], dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
        report_requests_collection_name: str,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._serialize_mongo_doc = serialize_mongo_doc
        self._sanitize_payload = sanitize_payload
        self._report_requests_collection_name = report_requests_collection_name

    async def execute(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        status_filter: str | None = None,
        q: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query: dict[str, Any] = {}
        if str(status_filter or "").strip():
            query["status"] = str(status_filter).strip()
        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]

        collection = get_database()[self._report_requests_collection_name]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_mongo_doc(doc, id_key="report_request_id") for doc in docs]
        payload = build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)
        return self._sanitize_payload(payload)
