from __future__ import annotations

import asyncio
from typing import Any

from src.crm.discovery import DiscoveryOrchestrator
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


class _FakeRuns:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.running: list[str] = []
        self.steps: list[dict[str, Any]] = []
        self.finalized: list[dict[str, Any]] = []
        self._run_id = "run-1"

    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.created.append(dict(payload))
        return {"discovery_run_id": self._run_id}

    async def mark_running(self, *, run_id: str) -> dict[str, Any] | None:
        self.running.append(run_id)
        return {"discovery_run_id": run_id, "status": "running"}

    async def append_step(
        self,
        *,
        run_id: str,
        step: str,
        ok: bool,
        duration_ms: int,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any] | None:
        self.steps.append(
            {
                "run_id": run_id,
                "step": step,
                "ok": ok,
                "duration_ms": duration_ms,
                "data": dict(data or {}),
                "error": error,
            }
        )
        return {"discovery_run_id": run_id}

    async def finalize(
        self,
        *,
        run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None:
        self.finalized.append(
            {
                "run_id": run_id,
                "status": status,
                "metrics": dict(metrics or {}),
                "failure_reason": failure_reason,
            }
        )
        return {"discovery_run_id": run_id, "status": status}


def test_discovery_orchestrator_completed_flow() -> None:
    runs = _FakeRuns()
    events: list[dict[str, Any]] = []

    async def _discover(_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        return [
            {"business_name": "A", "source_ref": {"listing_enriched": True}},
            {"business_name": "B", "source_ref": {"listing_enriched": True}},
        ]

    actions = ["inserted", "updated"]

    async def _upsert(_candidate: dict[str, Any]) -> str:
        return actions.pop(0)

    async def _record_event(**kwargs: Any) -> None:
        events.append(kwargs)

    orchestrator = DiscoveryOrchestrator(
        runs=runs,
        discover_candidates=_discover,
        upsert_candidate=_upsert,
        record_event=_record_event,
    )

    payload = CRMLeadDiscoveryTaskPayload(query="restaurantes sevilla")
    result = asyncio.run(orchestrator.run(task_payload=payload, job_id="job-1"))

    assert result["status"] == "completed"
    assert result["discovery_run_id"] == "run-1"
    assert result["candidates"] == 2
    assert result["inserted"] == 1
    assert result["updated"] == 1
    assert runs.finalized[0]["status"] == "completed"
    assert runs.finalized[0]["metrics"]["enriched_ok"] == 2
    assert events and events[0]["event_type"] == "lead_discovery_processed"


def test_discovery_orchestrator_failed_flow() -> None:
    runs = _FakeRuns()
    events: list[dict[str, Any]] = []

    async def _discover(_payload: CRMLeadDiscoveryTaskPayload) -> list[dict[str, Any]]:
        raise RuntimeError("no feed")

    async def _upsert(_candidate: dict[str, Any]) -> str:
        return "skipped"

    async def _record_event(**kwargs: Any) -> None:
        events.append(kwargs)

    orchestrator = DiscoveryOrchestrator(
        runs=runs,
        discover_candidates=_discover,
        upsert_candidate=_upsert,
        record_event=_record_event,
    )

    payload = CRMLeadDiscoveryTaskPayload(query="hotel")
    result = asyncio.run(orchestrator.run(task_payload=payload, job_id="job-2"))

    assert result["status"] == "failed"
    assert result["discovery_run_id"] == "run-1"
    assert runs.finalized[0]["status"] == "failed"
    assert "no feed" in str(runs.finalized[0]["failure_reason"])
    assert events and events[0]["event_type"] == "lead_discovery_processed"
