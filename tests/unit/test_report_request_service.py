from __future__ import annotations

import asyncio
import re
from typing import Any

from bson import ObjectId

import src.crm.repositories.mongo as mongo_module
import src.services.crm_service as crm_service_module
from src.services.crm_service import CRMService


class _InsertOneResult:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _UpdateResult:
    def __init__(self, modified_count: int = 1) -> None:
        self.modified_count = modified_count


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

    async def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> _UpdateResult:
        for index, doc in enumerate(self.docs):
            if not _matches(doc, query):
                continue
            updated = dict(doc)
            for key, value in dict(update.get("$set") or {}).items():
                updated[key] = value
            self.docs[index] = updated
            return _UpdateResult(1)
        return _UpdateResult(0)

    async def find_one_and_update(self, query: dict[str, Any], update: dict[str, Any], **_kwargs: Any) -> dict[str, Any] | None:
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


class _FakeJobService:
    def __init__(self) -> None:
        self.calls = 0

    async def enqueue_job(self, *, task_payload: Any, queue_name: str, job_type: str) -> dict[str, Any]:
        self.calls += 1
        return {
            "job_id": f"job-report-{self.calls}",
            "queue_name": queue_name,
            "job_type": job_type,
            "payload": task_payload.model_dump(mode="python"),
        }


class _FailingJobService:
    async def enqueue_job(self, *, task_payload: Any, queue_name: str, job_type: str) -> dict[str, Any]:
        raise RuntimeError("queue unavailable")


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
    for key, expected in query.items():
        if key == "$and":
            clauses = expected if isinstance(expected, list) else []
            if not all(_matches(doc, clause) for clause in clauses):
                return False
            continue
        if key == "$or":
            clauses = expected if isinstance(expected, list) else []
            if not any(_matches(doc, clause) for clause in clauses):
                return False
            continue
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


def test_create_report_request_persists_consents_utm_and_enqueues(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    fake_jobs = _FakeJobService()
    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)
    service = CRMService(job_service=fake_jobs)
    service._indexes_ensured = True

    async def _run() -> None:
        result = await service.create_report_request(
            business_name="Dulce Lokura",
            city="Cordoba",
            category="Pasteleria",
            contact_name="Javier",
            email="hola@example.com",
            phone=None,
            website=None,
            message="Quiero ver mi posicion",
            consent_report=True,
            consent_marketing=False,
            utm={"utm_source": "linkedin", "utm_campaign": "benchmark"},
            source_page="/solicitud?utm_source=linkedin",
        )

        assert result["report_request_id"]
        assert result["job_id"] == "job-report-1"
        assert result["benchmark_run_id"]
        stored = fake_db["report_requests"].docs[0]
        assert stored["consents"]["report_delivery"]["granted"] is True
        assert stored["consents"]["marketing"]["granted"] is False
        assert stored["utm"]["source"] == "linkedin"
        assert stored["utm"]["campaign"] == "benchmark"
        assert fake_db["benchmark_runs"].docs[0]["query"] == "Dulce Lokura Cordoba"

    asyncio.run(_run())


def test_create_report_request_requires_delivery_consent(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)
    service = CRMService(job_service=_FakeJobService())
    service._indexes_ensured = True

    async def _run() -> None:
        try:
            await service.create_report_request(
                business_name="Dulce Lokura",
                city="Cordoba",
                email="hola@example.com",
                consent_report=False,
            )
        except ValueError as exc:
            assert "consent_report" in str(exc)
            return
        raise AssertionError("Expected consent validation error.")

    asyncio.run(_run())


def test_create_report_request_keeps_failed_queue_request_retriable(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)
    service = CRMService(job_service=_FailingJobService())
    service._indexes_ensured = True

    async def _run() -> None:
        result = await service.create_report_request(
            business_name="Dulce Lokura",
            city="Cordoba",
            email="hola@example.com",
            consent_report=True,
        )

        assert result["status"] == "failed_to_queue"
        assert result["failure_reason"] == "queue unavailable"
        assert result["job_id"] is None
        stored = fake_db["report_requests"].docs[0]
        assert stored["status"] == "failed_to_queue"

    asyncio.run(_run())


def test_process_pending_report_requests_requeues_failed_and_pending(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    fake_jobs = _FakeJobService()
    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)
    service = CRMService(job_service=fake_jobs)
    service._indexes_ensured = True

    async def _run() -> None:
        collection = fake_db["report_requests"]
        await collection.insert_one(
            {
                "business_name": "Bar Centro",
                "city": "Cordoba",
                "category": "Restaurante",
                "query": "Bar Centro Cordoba",
                "status": "failed_to_queue",
                "job_id": None,
                "benchmark_run_id": None,
                "created_at": service._now_utc(),
                "updated_at": service._now_utc(),
            }
        )

        result = await service.process_pending_report_requests(limit=10)

        assert result["processed"] == 1
        assert result["retried"] == 1
        assert result["failed"] == 0
        stored = fake_db["report_requests"].docs[0]
        assert stored["status"] == "queued"
        assert stored["job_id"] == "job-report-1"
        assert stored["benchmark_run_id"]
        assert fake_db["benchmark_runs"].docs[0]["query"] == "Bar Centro Cordoba"

    asyncio.run(_run())


def test_list_report_requests_filters_and_searches(monkeypatch: Any) -> None:
    fake_db = _FakeDatabase()
    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)
    monkeypatch.setattr(mongo_module, "get_database", lambda: fake_db)
    service = CRMService(job_service=_FakeJobService())
    service._indexes_ensured = True

    async def _run() -> None:
        now = service._now_utc()
        await fake_db["report_requests"].insert_one(
            {"business_name": "Dulce Lokura", "city": "Cordoba", "email": "dulce@example.com", "status": "queued", "created_at": now}
        )
        await fake_db["report_requests"].insert_one(
            {"business_name": "Otro Bar", "city": "Sevilla", "email": "otro@example.com", "status": "failed_to_queue", "created_at": now}
        )

        result = await service.list_report_requests(status_filter="queued", q="dulce")

        assert result["total"] == 1
        assert result["items"][0]["business_name"] == "Dulce Lokura"
        assert result["items"][0]["report_request_id"]

    asyncio.run(_run())
