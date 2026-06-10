from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.studies.benchmark_report_runtime import BenchmarkReportRuntime


class GenerateCRMPaidReportUseCase:
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
        report_month: str | None = None,
        history: list[dict[str, Any]] | None = None,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._benchmark_report_runtime.generate_paid_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            report_month=report_month,
            history=history,
            cta=cta,
        )
