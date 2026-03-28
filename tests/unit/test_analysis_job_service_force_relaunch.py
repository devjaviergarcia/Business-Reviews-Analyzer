from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from bson import ObjectId

from src.services import analysis_job_service as analysis_job_module
from src.services.analysis_job_service import AnalysisJobService


class _InsertOneResult:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _FakeJobsCollection:
    def __init__(self, docs: dict[ObjectId, dict[str, Any]]) -> None:
        self.docs = docs

    async def find_one(self, filter_doc: dict[str, Any]) -> dict[str, Any] | None:
        object_id = filter_doc.get("_id")
        if not isinstance(object_id, ObjectId):
            return None
        doc = self.docs.get(object_id)
        return dict(doc) if isinstance(doc, dict) else None

    async def find_one_and_update(
        self,
        filter_doc: dict[str, Any],
        update_doc: dict[str, Any],
        **kwargs: Any,  # noqa: ARG002
    ) -> dict[str, Any] | None:
        object_id = filter_doc.get("_id")
        if not isinstance(object_id, ObjectId):
            return None
        current = self.docs.get(object_id)
        if not isinstance(current, dict):
            return None
        payload = dict(current)
        set_doc = update_doc.get("$set") if isinstance(update_doc, dict) else None
        if isinstance(set_doc, dict):
            payload.update(set_doc)
        push_doc = update_doc.get("$push") if isinstance(update_doc, dict) else None
        if isinstance(push_doc, dict):
            for key, value in push_doc.items():
                existing = payload.get(key)
                if not isinstance(existing, list):
                    existing = []
                existing = [*existing, value]
                payload[key] = existing
        self.docs[object_id] = payload
        return dict(payload)

    async def insert_one(self, doc: dict[str, Any]) -> _InsertOneResult:
        inserted_id = ObjectId()
        payload = dict(doc)
        payload["_id"] = inserted_id
        self.docs[inserted_id] = payload
        return _InsertOneResult(inserted_id=inserted_id)


class _FakeDatabase:
    def __init__(self, jobs_collection: _FakeJobsCollection) -> None:
        self._jobs_collection = jobs_collection

    def __getitem__(self, name: str) -> _FakeJobsCollection:
        assert name == "analysis_jobs"
        return self._jobs_collection


def _build_job_doc(*, status: str) -> tuple[ObjectId, dict[str, Any]]:
    object_id = ObjectId()
    now = datetime.now(timezone.utc)
    doc: dict[str, Any] = {
        "_id": object_id,
        "queue_name": "scrape_tripadvisor",
        "job_type": "business_analyze",
        "payload": {"name": "Hotel de los Faroles", "force": False},
        "name": "Hotel de los Faroles",
        "name_normalized": "hotel de los faroles",
        "status": status,
        "progress": {"status": status, "stage": status, "message": status},
        "events": [{"status": status, "stage": status, "message": status}],
        "attempts": 1,
        "error": None,
        "result": None,
        "created_at": now,
        "updated_at": now,
        "started_at": now if status in {"running", "retrying", "partial"} else None,
        "finished_at": None,
        "cancel_requested": False,
        "cancel_requested_at": None,
        "cancel_reason": None,
    }
    return object_id, doc


def test_relaunch_job_active_without_force_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    object_id, doc = _build_job_doc(status="running")
    jobs = _FakeJobsCollection(docs={object_id: doc})
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    with pytest.raises(ValueError, match="Active jobs cannot be relaunched"):
        asyncio.run(service.relaunch_job(job_id=str(object_id), reason="retry"))


def test_relaunch_job_active_with_force_creates_new_queued_job(monkeypatch: pytest.MonkeyPatch) -> None:
    object_id, doc = _build_job_doc(status="running")
    jobs = _FakeJobsCollection(docs={object_id: doc})
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    result = asyncio.run(service.relaunch_job(job_id=str(object_id), reason="force retry", force=True))

    assert result["status"] == "queued"
    assert result["force_relaunch"] is True
    assert result["origin_job_id"] == str(object_id)
    assert result["job_id"] != str(object_id)
    assert len(jobs.docs) == 2
    assert jobs.docs[object_id]["status"] == "running"

    cloned_id = ObjectId(result["job_id"])
    cloned_doc = jobs.docs[cloned_id]
    assert cloned_doc["attempts"] == 0
    assert cloned_doc["status"] == "queued"
    assert cloned_doc["events"][-1]["data"]["force"] is True
    assert cloned_doc["events"][-1]["data"]["origin_job_id"] == str(object_id)


def test_relaunch_job_active_from_zero_sets_strict_rescrape(monkeypatch: pytest.MonkeyPatch) -> None:
    object_id, doc = _build_job_doc(status="running")
    jobs = _FakeJobsCollection(docs={object_id: doc})
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    result = asyncio.run(
        service.relaunch_job(
            job_id=str(object_id),
            reason="force strict retry",
            force=True,
            restart_from_zero=True,
        )
    )

    cloned_id = ObjectId(result["job_id"])
    cloned_doc = jobs.docs[cloned_id]
    assert cloned_doc["payload"]["force"] is True
    assert cloned_doc["payload"]["force_mode"] == "strict_rescrape"
    assert cloned_doc["force"] is True
    assert cloned_doc["force_mode"] == "strict_rescrape"
    assert cloned_doc["events"][-1]["data"]["restart_from_zero"] is True


def test_relaunch_job_inactive_with_force_requeues_same_job(monkeypatch: pytest.MonkeyPatch) -> None:
    object_id, doc = _build_job_doc(status="failed")
    jobs = _FakeJobsCollection(docs={object_id: doc})
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    result = asyncio.run(service.relaunch_job(job_id=str(object_id), reason="force retry", force=True))

    assert result["job_id"] == str(object_id)
    assert result["status"] == "queued"
    assert len(jobs.docs) == 1
    assert jobs.docs[object_id]["events"][-1]["data"]["force"] is True


def test_relaunch_job_inactive_from_zero_requeues_same_job_with_strict_rescrape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    object_id, doc = _build_job_doc(status="done")
    jobs = _FakeJobsCollection(docs={object_id: doc})
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    result = asyncio.run(
        service.relaunch_job(
            job_id=str(object_id),
            reason="strict rescrape",
            force=True,
            restart_from_zero=True,
        )
    )

    assert result["job_id"] == str(object_id)
    assert result["status"] == "queued"
    assert jobs.docs[object_id]["payload"]["force"] is True
    assert jobs.docs[object_id]["payload"]["force_mode"] == "strict_rescrape"
    assert jobs.docs[object_id]["force"] is True
    assert jobs.docs[object_id]["force_mode"] == "strict_rescrape"
    assert jobs.docs[object_id]["events"][-1]["data"]["restart_from_zero"] is True
