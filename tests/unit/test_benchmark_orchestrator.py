from __future__ import annotations

import asyncio
from typing import Any

from src.crm.benchmark import BenchmarkOrchestrator
from src.workers.contracts import BenchmarkLocalStudyTaskPayload


class FakeBenchmarkRunRepository:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.running: list[str] = []
        self.finalized: list[dict[str, Any]] = []
        self.next_id = "benchmark-1"

    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.created.append(dict(payload))
        return {"benchmark_run_id": self.next_id, **payload}

    async def mark_running(self, *, benchmark_run_id: str) -> dict[str, Any]:
        self.running.append(benchmark_run_id)
        return {"benchmark_run_id": benchmark_run_id, "status": "running"}

    async def finalize(
        self,
        *,
        benchmark_run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "benchmark_run_id": benchmark_run_id,
            "status": status,
            "metrics": dict(metrics or {}),
            "failure_reason": failure_reason,
        }
        self.finalized.append(payload)
        return payload


class FakeBenchmarkBusinessRepository:
    def __init__(self, actions: list[str] | None = None) -> None:
        self.actions = list(actions or [])
        self.upserts: list[dict[str, Any]] = []

    async def upsert_business(self, *, benchmark_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.upserts.append({"benchmark_id": benchmark_id, "payload": dict(payload)})
        action = self.actions.pop(0) if self.actions else "inserted"
        return {
            "action": action,
            "business": {
                "benchmark_business_id": f"business-{len(self.upserts)}",
                "benchmark_id": benchmark_id,
                **dict(payload),
            },
        }


class FakeCompetitorSetRepository:
    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []

    async def upsert_set(
        self,
        *,
        benchmark_id: str,
        target_business_id: str,
        competitors: list[dict[str, Any]],
        selection_version: str = "v1",
    ) -> dict[str, Any]:
        payload = {
            "benchmark_id": benchmark_id,
            "target_business_id": target_business_id,
            "competitors": list(competitors),
            "selection_version": selection_version,
        }
        self.upserts.append(payload)
        return {"action": "inserted", "competitor_set": payload}


def test_benchmark_orchestrator_creates_run_discovers_and_persists() -> None:
    async def scenario() -> None:
        runs = FakeBenchmarkRunRepository()
        businesses = FakeBenchmarkBusinessRepository(actions=["inserted", "updated"])

        async def discover(_: BenchmarkLocalStudyTaskPayload) -> list[dict[str, Any]]:
            return [
                {
                    "business_name": "Cafe Uno",
                    "rating": 4.7,
                    "review_count": 100,
                    "source_ref": {"listing_enriched": True},
                },
                {
                    "business_name": "Cafe Dos",
                    "rating": 4.1,
                    "review_count": 40,
                    "listing_enriched": True,
                },
            ]

        orchestrator = BenchmarkOrchestrator(runs=runs, businesses=businesses, discover_candidates=discover)
        result = await orchestrator.run(
            task_payload=BenchmarkLocalStudyTaskPayload(query="cafeterias cordoba", limit=10),
            job_id="job-1",
        )

        assert result["benchmark_run_id"] == "benchmark-1"
        assert result["status"] == "completed"
        assert result["candidates"] == 2
        assert result["inserted"] == 1
        assert result["updated"] == 1
        assert runs.created[0]["query"] == "cafeterias cordoba"
        assert runs.running == ["benchmark-1"]
        assert runs.finalized[0]["metrics"]["enriched_ok"] == 2
        assert runs.finalized[0]["metrics"]["ranked_candidates"] == 2
        assert businesses.upserts[0]["payload"]["discovery_rank"] == 1
        assert businesses.upserts[0]["payload"]["source_ref"]["discovery_rank"] == 1
        assert businesses.upserts[1]["payload"]["discovery_rank"] == 2
        assert businesses.upserts[0]["payload"]["raw_snapshot"]["business_name"] == "Cafe Uno"

    asyncio.run(scenario())


def test_benchmark_orchestrator_writes_competitor_sets_when_configured() -> None:
    async def scenario() -> None:
        runs = FakeBenchmarkRunRepository()
        businesses = FakeBenchmarkBusinessRepository(actions=["inserted", "inserted", "inserted"])
        competitor_sets = FakeCompetitorSetRepository()

        async def discover(_: BenchmarkLocalStudyTaskPayload) -> list[dict[str, Any]]:
            return [
                {"business_name": "Target", "category": "Restaurante", "city": "Cordoba", "rating": 4.2, "review_count": 100},
                {"business_name": "Leader", "category": "Restaurante", "city": "Cordoba", "rating": 4.7, "review_count": 400},
                {"business_name": "Similar", "category": "Restaurante", "city": "Cordoba", "rating": 4.1, "review_count": 95},
            ]

        orchestrator = BenchmarkOrchestrator(
            runs=runs,
            businesses=businesses,
            discover_candidates=discover,
            competitor_sets=competitor_sets,
        )
        await orchestrator.run(
            task_payload=BenchmarkLocalStudyTaskPayload(query="restaurantes cordoba"),
            job_id="job-competitors",
        )

        assert len(competitor_sets.upserts) == 3
        assert runs.finalized[0]["metrics"]["competitor_sets"] == 3
        assert all(len(item["competitors"]) >= 1 for item in competitor_sets.upserts)

    asyncio.run(scenario())


def test_benchmark_orchestrator_marks_partial_when_persist_errors() -> None:
    async def scenario() -> None:
        runs = FakeBenchmarkRunRepository()

        class ErrorBusinessRepository(FakeBenchmarkBusinessRepository):
            async def upsert_business(self, *, benchmark_id: str, payload: dict[str, Any]) -> dict[str, Any]:
                await super().upsert_business(benchmark_id=benchmark_id, payload=payload)
                raise RuntimeError("mongo write failed")

        async def discover(_: BenchmarkLocalStudyTaskPayload) -> list[dict[str, Any]]:
            return [{"business_name": "Cafe Roto", "source_ref": {"listing_enriched": True}}]

        orchestrator = BenchmarkOrchestrator(
            runs=runs,
            businesses=ErrorBusinessRepository(),
            discover_candidates=discover,
        )
        result = await orchestrator.run(
            task_payload=BenchmarkLocalStudyTaskPayload(query="cafeterias cordoba"),
            job_id="job-2",
        )

        assert result["status"] == "partial"
        assert result["skipped"] == 1
        assert runs.finalized[0]["metrics"]["persist_errors"] == 1

    asyncio.run(scenario())


def test_benchmark_orchestrator_marks_failed_when_discovery_fails() -> None:
    async def scenario() -> None:
        runs = FakeBenchmarkRunRepository()
        businesses = FakeBenchmarkBusinessRepository()

        async def discover(_: BenchmarkLocalStudyTaskPayload) -> list[dict[str, Any]]:
            raise RuntimeError("maps unavailable")

        orchestrator = BenchmarkOrchestrator(runs=runs, businesses=businesses, discover_candidates=discover)
        result = await orchestrator.run(
            task_payload=BenchmarkLocalStudyTaskPayload(query="cafeterias cordoba", benchmark_run_id="existing-run"),
            job_id="job-3",
        )

        assert result["benchmark_run_id"] == "existing-run"
        assert result["status"] == "failed"
        assert result["failure_reason"] == "maps unavailable"
        assert runs.created == []
        assert runs.running == ["existing-run"]
        assert runs.finalized[0]["status"] == "failed"

    asyncio.run(scenario())
