from __future__ import annotations

import asyncio
from typing import Any

import pytest
from bson import ObjectId

from src.services import business_query_service as business_query_module
from src.services.business_query_service import BusinessQueryService


class _FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = list(docs)
        self._skip = 0
        self._limit: int | None = None

    def sort(self, sort_by: list[tuple[str, int]]) -> "_FakeCursor":
        for key, direction in reversed(sort_by):
            reverse = direction < 0
            self._docs.sort(
                key=lambda item: (item.get(key) is None, item.get(key)),
                reverse=reverse,
            )
        return self

    def skip(self, value: int) -> "_FakeCursor":
        self._skip = max(0, int(value))
        return self

    def limit(self, value: int) -> "_FakeCursor":
        self._limit = max(0, int(value))
        return self

    async def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        docs = self._docs[self._skip :]
        effective_limit = self._limit if self._limit is not None else None
        if length is not None:
            effective_limit = min(effective_limit, length) if effective_limit is not None else length
        if effective_limit is not None:
            docs = docs[:effective_limit]
        return [dict(item) for item in docs]


class _FakeCommentsCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = [dict(item) for item in docs]

    async def count_documents(self, query: dict[str, Any]) -> int:
        return sum(1 for item in self._docs if _matches_query(item, query))

    def find(self, query: dict[str, Any]) -> _FakeCursor:
        filtered = [item for item in self._docs if _matches_query(item, query)]
        return _FakeCursor(filtered)

    def aggregate(self, pipeline: list[dict[str, Any]]) -> _FakeCursor:
        if not pipeline:
            return _FakeCursor([])
        match_stage = pipeline[0].get("$match", {}) if isinstance(pipeline[0], dict) else {}
        filtered = [item for item in self._docs if _matches_query(item, match_stage)]
        grouped: dict[str, int] = {}
        for item in filtered:
            key = str(item.get("source") or "")
            grouped[key] = grouped.get(key, 0) + 1
        results = [{"_id": key, "count": count} for key, count in grouped.items()]
        return _FakeCursor(results)


class _FakeDatabase:
    def __init__(self, comments_collection: _FakeCommentsCollection) -> None:
        self._comments = comments_collection

    def __getitem__(self, name: str) -> Any:
        if name == "comments":
            return self._comments
        raise KeyError(name)


def _matches_query(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    for key, expected in query.items():
        if doc.get(key) != expected:
            return False
    return True


def test_list_job_comments_returns_paginated_comments_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = str(ObjectId())
    docs = [
        {
            "_id": ObjectId(),
            "source_job_id": job_id,
            "source": "tripadvisor",
            "review_fingerprint": "fp-1",
            "rating": 5.0,
            "author_name": "A",
            "text": "Great",
        },
        {
            "_id": ObjectId(),
            "source_job_id": job_id,
            "source": "tripadvisor",
            "review_fingerprint": "fp-2",
            "rating": 1.0,
            "author_name": "B",
            "text": "Bad",
        },
        {
            "_id": ObjectId(),
            "source_job_id": job_id,
            "source": "google_maps",
            "review_fingerprint": "fp-3",
            "rating": 4.0,
            "author_name": "C",
            "text": "Nice",
        },
        {
            "_id": ObjectId(),
            "source_job_id": str(ObjectId()),
            "source": "tripadvisor",
            "review_fingerprint": "other-job",
            "rating": 3.0,
            "author_name": "D",
            "text": "Other",
        },
    ]
    fake_db = _FakeDatabase(_FakeCommentsCollection(docs))
    monkeypatch.setattr(business_query_module, "get_database", lambda: fake_db)

    service = BusinessQueryService()
    result = asyncio.run(
        service.list_job_comments(
            job_id=job_id,
            source="tripadvisor",
            page=1,
            page_size=10,
        )
    )

    assert result["job_id"] == job_id
    assert result["source"] == "tripadvisor"
    assert result["total"] == 2
    assert result["total_comments"] == 3
    assert result["source_counts"] == {"google_maps": 1, "tripadvisor": 2}
    assert len(result["items"]) == 2
    assert all(item["source"] == "tripadvisor" for item in result["items"])
    assert all("review_fingerprint" not in item for item in result["items"])
    assert all("id" in item for item in result["items"])


def test_list_job_comments_rejects_invalid_source() -> None:
    service = BusinessQueryService()
    with pytest.raises(ValueError):
        asyncio.run(
            service.list_job_comments(
                job_id=str(ObjectId()),
                source="unknown",
                page=1,
                page_size=10,
            )
        )


def test_list_job_comments_accepts_scrape_type_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = str(ObjectId())
    docs = [
        {
            "_id": ObjectId(),
            "source_job_id": job_id,
            "source": "tripadvisor",
            "review_fingerprint": "fp-1",
            "rating": 5.0,
            "author_name": "A",
            "text": "Great",
        },
        {
            "_id": ObjectId(),
            "source_job_id": job_id,
            "source": "google_maps",
            "review_fingerprint": "fp-2",
            "rating": 4.0,
            "author_name": "B",
            "text": "Nice",
        },
    ]
    fake_db = _FakeDatabase(_FakeCommentsCollection(docs))
    monkeypatch.setattr(business_query_module, "get_database", lambda: fake_db)

    service = BusinessQueryService()
    result = asyncio.run(
        service.list_job_comments(
            job_id=job_id,
            scrape_type="tripadvisor",
            page=1,
            page_size=10,
        )
    )

    assert result["source"] == "tripadvisor"
    assert result["scrape_type"] == "tripadvisor"
    assert result["pagination_scope"] == "source"
    assert set(result["source_pagination"].keys()) == {"tripadvisor"}
    assert len(result["items"]) == 1
    assert result["items"][0]["scrape_type"] == "tripadvisor"


def test_list_job_comments_rejects_conflicting_source_and_scrape_type() -> None:
    service = BusinessQueryService()
    with pytest.raises(ValueError):
        asyncio.run(
            service.list_job_comments(
                job_id=str(ObjectId()),
                source="google_maps",
                scrape_type="tripadvisor",
                page=1,
                page_size=10,
            )
        )
