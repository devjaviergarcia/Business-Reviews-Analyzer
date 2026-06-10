from __future__ import annotations

from typing import Awaitable, Callable

from src.crm.studies.benchmark_report_runtime import BenchmarkReportRuntime


class GenerateCRMLeadReportUseCase:
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
        benchmark_business_id: str,
        cta: dict[str, object] | None = None,
    ) -> dict[str, object]:
        await self._ensure_indexes()
        return await self._benchmark_report_runtime.generate_lead_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            cta=cta,
        )
