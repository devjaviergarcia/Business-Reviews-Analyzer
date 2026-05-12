from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from src.workers.report_worker import ReportWorker


class _FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = list(docs)
        self._limit: int | None = None

    def sort(self, fields: list[tuple[str, int]]) -> "_FakeCursor":
        sorted_docs = list(self._docs)
        for field, direction in reversed(fields):
            reverse = int(direction) < 0
            if field == "scraped_at":
                sorted_docs.sort(
                    key=lambda doc: doc.get(field) if isinstance(doc.get(field), datetime) else datetime.min.replace(tzinfo=timezone.utc),
                    reverse=reverse,
                )
            else:
                sorted_docs.sort(key=lambda doc: str(doc.get(field) or ""), reverse=reverse)
        self._docs = sorted_docs
        return self

    def limit(self, value: int) -> "_FakeCursor":
        self._limit = max(0, int(value))
        return self

    async def to_list(self, *, length: int) -> list[dict[str, Any]]:
        max_len = self._limit if self._limit is not None else int(length)
        return list(self._docs[: max(0, int(max_len))])


class _FakeReviewsCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = list(docs)

    async def count_documents(self, query: dict[str, Any]) -> int:
        return len(self._apply_query(query))

    def find(self, query: dict[str, Any]) -> _FakeCursor:
        return _FakeCursor(self._apply_query(query))

    def _apply_query(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        for doc in self._docs:
            if not isinstance(doc, dict):
                continue
            if any(doc.get(field) != value for field, value in query.items()):
                continue
            filtered.append(doc)
        return filtered


def _build_reviews(*, business_id: str, source: str, count: int, start_at: datetime) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(count):
        rows.append(
            {
                "_id": f"{source}-{index:04d}",
                "business_id": business_id,
                "source": source,
                "scraped_at": start_at - timedelta(minutes=index),
                "rating": 5,
                "text": f"Review {index}",
            }
        )
    return rows


def test_report_worker_single_source_selection_google_maps() -> None:
    worker = ReportWorker.__new__(ReportWorker)
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    business_id = "b-1"
    reviews = (
        _build_reviews(business_id=business_id, source="google_maps", count=25, start_at=now)
        + _build_reviews(business_id=business_id, source="tripadvisor", count=30, start_at=now)
    )
    collection = _FakeReviewsCollection(reviews)

    docs, mode, included_sources, source_counts = asyncio.run(
        worker._load_report_review_docs(
            reviews_collection=collection,
            business_id=business_id,
            source_mode="single",
            selected_source="google_maps",
            limit=800,
        )
    )

    assert mode == "single"
    assert len(docs) == 25
    assert set(included_sources) == {"google_maps"}
    assert source_counts == {"google_maps": 25}
    assert all(str(doc.get("source")) == "google_maps" for doc in docs)


def test_report_worker_auto_mode_balances_when_both_sources_exist() -> None:
    worker = ReportWorker.__new__(ReportWorker)
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    business_id = "b-2"
    reviews = (
        _build_reviews(business_id=business_id, source="google_maps", count=600, start_at=now)
        + _build_reviews(business_id=business_id, source="tripadvisor", count=300, start_at=now)
    )
    collection = _FakeReviewsCollection(reviews)

    docs, mode, included_sources, source_counts = asyncio.run(
        worker._load_report_review_docs(
            reviews_collection=collection,
            business_id=business_id,
            source_mode="auto",
            selected_source=None,
            limit=800,
        )
    )

    assert mode == "auto"
    assert len(docs) == 800
    assert included_sources == ["google_maps", "tripadvisor"]
    assert source_counts == {"google_maps": 500, "tripadvisor": 300}


def test_report_worker_single_without_selected_source_falls_back_to_auto() -> None:
    worker = ReportWorker.__new__(ReportWorker)
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    business_id = "b-3"
    reviews = (
        _build_reviews(business_id=business_id, source="google_maps", count=10, start_at=now)
        + _build_reviews(business_id=business_id, source="tripadvisor", count=12, start_at=now)
    )
    collection = _FakeReviewsCollection(reviews)

    docs, mode, included_sources, source_counts = asyncio.run(
        worker._load_report_review_docs(
            reviews_collection=collection,
            business_id=business_id,
            source_mode="single",
            selected_source=None,
            limit=800,
        )
    )

    assert mode == "auto"
    assert len(docs) == 22
    assert included_sources == ["google_maps", "tripadvisor"]
    assert source_counts == {"google_maps": 10, "tripadvisor": 12}
