from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.studies.benchmark_report_runtime import BenchmarkReportRuntime


class GenerateCRMPublicStudyUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        benchmark_report_runtime: BenchmarkReportRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._benchmark_report_runtime = benchmark_report_runtime

    async def execute(
        self,
        *,
        benchmark_run_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._benchmark_report_runtime.generate_public_study_for_benchmark_run(
            benchmark_run_id=benchmark_run_id,
            cta=cta,
        )
