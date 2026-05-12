from __future__ import annotations

import asyncio

from bson import ObjectId

from src.services import analysis_job_service as analysis_job_module
from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import ReportGenerateTaskPayload, parse_report_generate_payload


class _InsertOneResult:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _FakeJobsCollection:
    def __init__(self) -> None:
        self.last_doc: dict | None = None

    async def insert_one(self, doc: dict) -> _InsertOneResult:
        payload = dict(doc)
        payload["_id"] = ObjectId()
        self.last_doc = payload
        return _InsertOneResult(inserted_id=payload["_id"])


class _FakeDatabase:
    def __init__(self, jobs_collection: _FakeJobsCollection) -> None:
        self._jobs_collection = jobs_collection

    def __getitem__(self, name: str) -> _FakeJobsCollection:
        assert name == "analysis_jobs"
        return self._jobs_collection


def test_parse_report_generate_payload_without_new_fields_uses_defaults() -> None:
    payload = parse_report_generate_payload(
        {
            "queue_name": "report",
            "job_type": "report_generate",
            "payload": {
                "business_id": "b1",
                "analysis_id": "a1",
            },
        }
    )

    assert payload.source_mode == "auto"
    assert payload.selected_source is None


def test_parse_report_generate_payload_single_tripadvisor() -> None:
    payload = parse_report_generate_payload(
        {
            "queue_name": "report",
            "job_type": "report_generate",
            "payload": {
                "business_id": "b1",
                "analysis_id": "a1",
                "source_mode": "single",
                "selected_source": "tripadvisor",
            },
        }
    )

    assert payload.source_mode == "single"
    assert payload.selected_source == "tripadvisor"


def test_parse_report_generate_payload_explicit_auto_is_same() -> None:
    payload = parse_report_generate_payload(
        {
            "queue_name": "report",
            "job_type": "report_generate",
            "payload": {
                "business_id": "b1",
                "analysis_id": "a1",
                "source_mode": "auto",
            },
        }
    )

    assert payload.source_mode == "auto"
    assert payload.selected_source is None


def test_enqueue_report_generate_job_omits_default_source_fields(monkeypatch) -> None:
    jobs = _FakeJobsCollection()
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    task = ReportGenerateTaskPayload(
        business_id="b1",
        analysis_id="a1",
        output_format="pdf",
    )
    result = asyncio.run(service.enqueue_report_generate_job(task_payload=task))

    assert result["status"] == "queued"
    assert isinstance(jobs.last_doc, dict)
    payload = jobs.last_doc.get("payload") if isinstance(jobs.last_doc, dict) else {}
    assert isinstance(payload, dict)
    assert "source_mode" not in payload
    assert "selected_source" not in payload


def test_enqueue_report_generate_job_keeps_non_default_source_fields(monkeypatch) -> None:
    jobs = _FakeJobsCollection()
    monkeypatch.setattr(analysis_job_module, "get_database", lambda: _FakeDatabase(jobs))
    service = AnalysisJobService()

    task = ReportGenerateTaskPayload(
        business_id="b1",
        analysis_id="a1",
        output_format="pdf",
        source_mode="single",
        selected_source="tripadvisor",
    )
    result = asyncio.run(service.enqueue_report_generate_job(task_payload=task))

    assert result["status"] == "queued"
    assert isinstance(jobs.last_doc, dict)
    payload = jobs.last_doc.get("payload") if isinstance(jobs.last_doc, dict) else {}
    assert isinstance(payload, dict)
    assert payload.get("source_mode") == "single"
    assert payload.get("selected_source") == "tripadvisor"
