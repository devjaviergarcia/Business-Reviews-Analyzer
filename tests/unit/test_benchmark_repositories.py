from __future__ import annotations

import asyncio
import re
from typing import Any

from bson import ObjectId

import src.crm.repositories.bootstrap as bootstrap_module
import src.crm.repositories.mongo as mongo_module
from src.crm.repositories.bootstrap import CRMRepositoryBootstrap
from src.crm.repositories.mongo import (
    MongoBenchmarkBusinessRepository,
    MongoBenchmarkRunRepository,
    MongoCompetitorSetRepository,
    MongoLeadReportRepository,
    MongoPaidReportRepository,
)
from src.models.benchmark import BenchmarkBusiness, BenchmarkRun, CompetitorSet, LeadReport, PaidReport


class _InsertOneResult:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = [dict(doc) for doc in docs]
        self._skip = 0
        self._limit = len(self._docs)

    def sort(self, spec: list[tuple[str, int]]) -> "_FakeCursor":
        for field, direction in reversed(spec):
            reverse = int(direction) < 0
            self._docs.sort(key=lambda doc: _sort_value(_get_nested(doc, field)), reverse=reverse)
        return self

    def skip(self, value: int) -> "_FakeCursor":
        self._skip = max(0, int(value))
        return self

    def limit(self, value: int) -> "_FakeCursor":
        self._limit = max(0, int(value))
        return self

    async def to_list(self, *, length: int | None = None) -> list[dict[str, Any]]:
        limit = self._limit if length is None else min(self._limit, int(length))
        return [dict(doc) for doc in self._docs[self._skip : self._skip + limit]]


class _FakeCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []
        self.indexes: list[dict[str, Any]] = []

    async def create_index(self, keys: list[tuple[str, int]], **kwargs: Any) -> str:
        self.indexes.append({"keys": keys, **kwargs})
        return str(kwargs.get("name") or keys)

    async def insert_one(self, doc: dict[str, Any]) -> _InsertOneResult:
        payload = dict(doc)
        payload.setdefault("_id", ObjectId())
        self.docs.append(payload)
        return _InsertOneResult(payload["_id"])

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self.docs:
            if _matches(doc, query):
                return dict(doc)
        return None

    def find(self, query: dict[str, Any]) -> _FakeCursor:
        return _FakeCursor([doc for doc in self.docs if _matches(doc, query)])

    async def count_documents(self, query: dict[str, Any]) -> int:
        return len([doc for doc in self.docs if _matches(doc, query)])

    async def find_one_and_update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        **_kwargs: Any,
    ) -> dict[str, Any] | None:
        for index, doc in enumerate(self.docs):
            if not _matches(doc, query):
                continue
            updated = dict(doc)
            for key, value in dict(update.get("$set") or {}).items():
                updated[key] = value
            self.docs[index] = updated
            return dict(updated)
        return None


class _FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, _FakeCollection] = {}

    def __getitem__(self, name: str) -> _FakeCollection:
        if name not in self.collections:
            self.collections[name] = _FakeCollection()
        return self.collections[name]


def _get_nested(doc: dict[str, Any], field: str) -> Any:
    value: Any = doc
    for part in str(field).split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _sort_value(value: Any) -> tuple[int, str]:
    if value is None:
        return (1, "")
    return (0, str(value))


def _matches(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    if not query:
        return True
    if "$and" in query:
        clauses = query.get("$and")
        return isinstance(clauses, list) and all(_matches(doc, clause) for clause in clauses)
    if "$or" in query:
        clauses = query.get("$or")
        return isinstance(clauses, list) and any(_matches(doc, clause) for clause in clauses)
    for key, expected in query.items():
        actual = _get_nested(doc, key)
        if isinstance(expected, dict) and "$regex" in expected:
            flags = re.IGNORECASE if "i" in str(expected.get("$options") or "") else 0
            if not re.search(str(expected["$regex"]), str(actual or ""), flags=flags):
                return False
            continue
        if isinstance(expected, dict) and "$in" in expected:
            if actual not in expected["$in"]:
                return False
            continue
        if actual != expected:
            return False
    return True


def test_benchmark_models_validate_required_fields() -> None:
    run = BenchmarkRun(query="restaurantes cordoba", city="Cordoba")
    assert run.status == "queued"

    business = BenchmarkBusiness(
        benchmark_id="benchmark-1",
        business_name="Dulce Lokura",
        business_name_normalized="dulce lokura",
        discovery_rank=4,
        rating=4.6,
        review_count=157,
    )
    assert business.rating == 4.6
    assert business.discovery_rank == 4

    competitor_set = CompetitorSet(
        benchmark_id="benchmark-1",
        target_business_id="business-1",
        competitors=[{"business_name": "Competidor A", "rating": 4.8, "discovery_rank": 2}],
    )
    assert competitor_set.competitors[0].business_name == "Competidor A"
    assert competitor_set.competitors[0].discovery_rank == 2

    report = LeadReport(
        benchmark_id="benchmark-1",
        benchmark_business_id="business-1",
        business_name="Dulce Lokura",
        html="<html>Dulce Lokura</html>",
    )
    assert report.report_type == "lead"

    paid_report = PaidReport(
        benchmark_id="benchmark-1",
        benchmark_business_id="business-1",
        report_month="2026-05",
        business_name="Dulce Lokura",
        html="<html>Dulce Lokura paid</html>",
    )
    assert paid_report.report_type == "paid"


def test_benchmark_repositories_crud_and_upsert(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)

    async def _run() -> None:
        runs = MongoBenchmarkRunRepository()
        businesses = MongoBenchmarkBusinessRepository()
        competitor_sets = MongoCompetitorSetRepository()
        reports = MongoLeadReportRepository()
        paid_reports = MongoPaidReportRepository()

        run = await runs.create_run({"query": "restaurantes cordoba", "city": "Cordoba", "limit": 100})
        benchmark_id = run["benchmark_run_id"]
        assert run["status"] == "queued"

        running = await runs.mark_running(benchmark_run_id=benchmark_id)
        assert running and running["status"] == "running"

        first = await businesses.upsert_business(
            benchmark_id=benchmark_id,
            payload={
                "business_name": "Dulce Lokura",
                "city": "Cordoba",
                "category": "Pasteleria",
                "address": "C. San Alvaro, 1",
                "maps_url": "https://www.google.com/maps/place/Dulce+Lokura?hl=es",
                "discovery_rank": 7,
                "rating": "4,6",
                "review_count": "157",
                "opportunity_score": 44,
                "source_ref": {"listing_enriched": True},
            },
        )
        assert first["action"] == "inserted"
        business = first["business"]
        assert business["business_name_normalized"] == "dulce lokura"
        assert business["maps_url_canonical"] == "https://www.google.com/maps/place/Dulce+Lokura"
        assert business["listing_enriched"] is True
        assert business["discovery_rank"] == 7
        assert business["visibility_score"] == 70.0
        assert business["opportunity_score"] == 41.9

        second = await businesses.upsert_business(
            benchmark_id=benchmark_id,
            payload={
                "business_name": "Dulce Lokura",
                "maps_url": "https://www.google.com/maps/place/Dulce+Lokura?hl=es",
                "source_ref": {"discovery_rank": 2},
                "rating": 4.7,
                "review_count": 180,
                "opportunity_score": 50,
            },
        )
        assert second["action"] == "updated"
        assert await fake_db["benchmark_businesses"].count_documents({}) == 1
        assert second["business"]["rating"] == 4.7
        assert second["business"]["discovery_rank"] == 2
        assert second["business"]["visibility_score"] == 95.0

        listed = await businesses.list_businesses(
            benchmark_id=benchmark_id,
            page=1,
            page_size=10,
            sort_by="discovery_rank",
            sort_dir="asc",
        )
        assert listed["total"] == 1
        assert listed["items"][0]["benchmark_business_id"] == business["benchmark_business_id"]

        competitor_result = await competitor_sets.upsert_set(
            benchmark_id=benchmark_id,
            target_business_id=business["benchmark_business_id"],
            competitors=[{"business_name": "Pasteleria Norte", "rating": 4.8, "similarity_score": 92}],
        )
        assert competitor_result["action"] == "inserted"

        competitor_update = await competitor_sets.upsert_set(
            benchmark_id=benchmark_id,
            target_business_id=business["benchmark_business_id"],
            competitors=[{"business_name": "Pasteleria Sur", "rating": 4.5, "similarity_score": 80}],
        )
        assert competitor_update["action"] == "updated"
        saved_set = await competitor_sets.get_for_business(target_business_id=business["benchmark_business_id"])
        assert saved_set and saved_set["competitors"][0]["business_name"] == "Pasteleria Sur"

        report_result = await reports.upsert_for_business(
            benchmark_business_id=business["benchmark_business_id"],
            payload={
                "benchmark_id": benchmark_id,
                "business_name": "Dulce Lokura",
                "html": "<html>Dulce Lokura</html>",
                "deep_study_snapshot": {"score_breakdown": {"opportunity": 30}},
                "source_payload": {"business": business},
            },
        )
        assert report_result["action"] == "inserted"
        lead_report = report_result["lead_report"]
        assert lead_report["benchmark_business_id"] == business["benchmark_business_id"]

        report_update = await reports.upsert_for_business(
            benchmark_business_id=business["benchmark_business_id"],
            payload={
                "benchmark_id": benchmark_id,
                "business_name": "Dulce Lokura",
                "html": "<html>Actualizado</html>",
            },
        )
        assert report_update["action"] == "updated"
        assert await fake_db["lead_reports"].count_documents({}) == 1
        fetched_report = await reports.get_for_business(benchmark_business_id=business["benchmark_business_id"])
        assert fetched_report and "Actualizado" in fetched_report["html"]

        paid_result = await paid_reports.upsert_for_business_month(
            benchmark_business_id=business["benchmark_business_id"],
            report_month="2026-05",
            payload={
                "benchmark_id": benchmark_id,
                "business_name": "Dulce Lokura",
                "html": "<html>Paid</html>",
                "history": [{"month": "2026-04", "health_score": 62}],
            },
        )
        assert paid_result["action"] == "inserted"
        paid_update = await paid_reports.upsert_for_business_month(
            benchmark_business_id=business["benchmark_business_id"],
            report_month="2026-05",
            payload={
                "benchmark_id": benchmark_id,
                "business_name": "Dulce Lokura",
                "html": "<html>Paid actualizado</html>",
            },
        )
        assert paid_update["action"] == "updated"
        assert await fake_db["paid_reports"].count_documents({}) == 1
        fetched_paid = await paid_reports.get_for_business_month(
            benchmark_business_id=business["benchmark_business_id"],
            report_month="2026-05",
        )
        assert fetched_paid and "Paid actualizado" in fetched_paid["html"]

        completed = await runs.finalize(
            benchmark_run_id=benchmark_id,
            status="completed",
            metrics={"businesses": 1},
        )
        assert completed and completed["status"] == "completed"
        assert completed["metrics"]["businesses"] == 1

    asyncio.run(_run())


def test_benchmark_indexes_are_bootstrapped(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    monkeypatch.setattr(bootstrap_module, "get_database", lambda: fake_db)

    asyncio.run(CRMRepositoryBootstrap().ensure_indexes())

    benchmark_run_indexes = {item["name"] for item in fake_db["benchmark_runs"].indexes}
    benchmark_business_indexes = {item["name"] for item in fake_db["benchmark_businesses"].indexes}
    competitor_set_indexes = {item["name"] for item in fake_db["competitor_sets"].indexes}
    lead_report_indexes = {item["name"] for item in fake_db["lead_reports"].indexes}
    paid_report_indexes = {item["name"] for item in fake_db["paid_reports"].indexes}

    assert "idx_benchmark_runs_status_updated" in benchmark_run_indexes
    assert "idx_benchmark_businesses_benchmark_rating_reviews" in benchmark_business_indexes
    assert "idx_benchmark_businesses_benchmark_discovery_rank" in benchmark_business_indexes
    assert "idx_benchmark_businesses_benchmark_opportunity" in benchmark_business_indexes
    assert "idx_competitor_sets_benchmark_target_unique" in competitor_set_indexes
    assert "idx_lead_reports_business_type_unique" in lead_report_indexes
    assert "idx_paid_reports_business_month_unique" in paid_report_indexes
