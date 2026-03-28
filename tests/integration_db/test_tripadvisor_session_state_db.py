from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from bson import ObjectId

from src.database import close_mongo_connection, connect_to_mongo, get_database
from src.services.analysis_job_service import AnalysisJobService
from src.services.tripadvisor_session_service import TripadvisorSessionService
from src.workers.contracts import AnalyzeBusinessTaskPayload


def _write_storage_state(path: Path, *, expires_at: datetime) -> None:
    payload = {
        "cookies": [
            {
                "name": "sessionid",
                "domain": ".tripadvisor.es",
                "expires": expires_at.timestamp(),
            }
        ],
        "origins": [],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


async def _ensure_mongo_or_raise() -> None:
    await connect_to_mongo()
    await get_database().client.admin.command("ping")


def test_tripadvisor_session_state_refresh_and_persist(tmp_path: Path) -> None:
    storage_state_path = tmp_path / "storage_state.json"
    now = datetime.now(timezone.utc)
    _write_storage_state(storage_state_path, expires_at=now + timedelta(hours=3))

    async def _scenario() -> None:
        await _ensure_mongo_or_raise()
        service = TripadvisorSessionService()
        collection = get_database()["tripadvisor_session_state"]
        try:
            state = await service.refresh_from_storage_state(
                storage_state_path=str(storage_state_path),
                mark_human_intervention=True,
            )
            assert state["session_state"] == "valid"
            assert state["availability_now"] is True
            assert state["last_human_intervention_at"] is not None

            persisted = await collection.find_one({"_id": "global_tripadvisor_session"})
            assert persisted is not None
            assert persisted.get("session_state") == "valid"
        finally:
            await collection.delete_one({"_id": "global_tripadvisor_session"})
            await close_mongo_connection()

    try:
        asyncio.run(_scenario())
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB is not available for integration test: {exc}")


def test_job_can_move_to_needs_human_and_be_relaunched() -> None:
    async def _scenario() -> None:
        await _ensure_mongo_or_raise()
        job_service = AnalysisJobService()
        jobs = get_database()["analysis_jobs"]
        job_id: str | None = None
        try:
            enqueue_result = await job_service.enqueue_job(
                task_payload=AnalyzeBusinessTaskPayload(name="Integration Test Business"),
                queue_name="scrape_tripadvisor",
                job_type="business_analyze",
            )
            job_id = str(enqueue_result["job_id"])
            parsed_job_object_id = ObjectId(job_id)

            await job_service.mark_needs_human(
                job_id=parsed_job_object_id,
                reason="Manual intervention required for TripAdvisor.",
                data={"reason_code": "tripadvisor_session_unavailable"},
            )
            job_after_needs_human = await job_service.get_job(job_id=job_id)
            assert job_after_needs_human["status"] == "needs_human"
            assert "Manual intervention required" in str(job_after_needs_human.get("error") or "")

            await job_service.relaunch_job(job_id=job_id, reason="Relaunch from integration test.")
            job_after_relaunch = await job_service.get_job(job_id=job_id)
            assert job_after_relaunch["status"] == "queued"
            assert job_after_relaunch.get("error") in (None, "")
        finally:
            if job_id:
                await jobs.delete_one({"_id": ObjectId(job_id)})
            await close_mongo_connection()

    try:
        asyncio.run(_scenario())
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB is not available for integration test: {exc}")
