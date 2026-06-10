from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable

from src.crm.benchmark import build_deep_study_snapshot
from src.crm.reports import render_lead_report_html, render_paid_report_html, render_public_study_html


GetBenchmarkBusinessFn = Callable[..., Awaitable[dict[str, Any] | None]]
GetCompetitorSetForBusinessFn = Callable[..., Awaitable[dict[str, Any] | None]]
SelectCompetitorsForBenchmarkBusinessFn = Callable[..., Awaitable[dict[str, Any]]]
ResolveLeadReportCtaFn = Callable[..., dict[str, Any]]
UpsertLeadReportForBusinessFn = Callable[..., Awaitable[dict[str, Any]]]
UpsertPaidReportForBusinessMonthFn = Callable[..., Awaitable[dict[str, Any]]]
GetBenchmarkRunFn = Callable[..., Awaitable[dict[str, Any] | None]]
ListBenchmarkBusinessesFn = Callable[..., Awaitable[dict[str, Any]]]
ResolveGeoGridStatsForPublicStudyFn = Callable[..., Awaitable[dict[str, Any] | None]]
NowUtcFn = Callable[[], datetime]
RecordEventFn = Callable[..., Awaitable[None]]
SanitizePayloadFn = Callable[[Any], Any]


class BenchmarkReportRuntime:
    def __init__(
        self,
        *,
        get_benchmark_business: GetBenchmarkBusinessFn,
        get_competitor_set_for_business: GetCompetitorSetForBusinessFn,
        select_competitors_for_benchmark_business: SelectCompetitorsForBenchmarkBusinessFn,
        resolve_lead_report_cta: ResolveLeadReportCtaFn,
        upsert_lead_report_for_business: UpsertLeadReportForBusinessFn,
        upsert_paid_report_for_business_month: UpsertPaidReportForBusinessMonthFn,
        get_benchmark_run: GetBenchmarkRunFn,
        list_benchmark_businesses: ListBenchmarkBusinessesFn,
        resolve_geo_grid_stats_for_public_study: ResolveGeoGridStatsForPublicStudyFn,
        now_utc: NowUtcFn,
        record_event: RecordEventFn,
        sanitize_payload: SanitizePayloadFn,
    ) -> None:
        self._get_benchmark_business = get_benchmark_business
        self._get_competitor_set_for_business = get_competitor_set_for_business
        self._select_competitors_for_benchmark_business = select_competitors_for_benchmark_business
        self._resolve_lead_report_cta = resolve_lead_report_cta
        self._upsert_lead_report_for_business = upsert_lead_report_for_business
        self._upsert_paid_report_for_business_month = upsert_paid_report_for_business_month
        self._get_benchmark_run = get_benchmark_run
        self._list_benchmark_businesses = list_benchmark_businesses
        self._resolve_geo_grid_stats_for_public_study = resolve_geo_grid_stats_for_public_study
        self._now_utc = now_utc
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload

    async def generate_lead_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = await self._load_benchmark_business_report_context(
            benchmark_business_id=benchmark_business_id,
        )
        business = context["business"]
        benchmark_id = context["benchmark_id"]
        competitors = context["competitors"]
        competitor_set = context["competitor_set"]
        deep_study_snapshot = context["deep_study_snapshot"]

        initial_cta = self._resolve_lead_report_cta(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            cta=cta,
            lead_report_id=None,
        )
        persisted = await self._persist_lead_report(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            business=business,
            competitors=competitors,
            competitor_set=competitor_set,
            deep_study_snapshot=deep_study_snapshot,
            cta=initial_cta,
        )
        report = persisted.get("lead_report") if isinstance(persisted.get("lead_report"), dict) else {}
        lead_report_id = str(report.get("lead_report_id") or "").strip() or None
        resolved_cta = self._resolve_lead_report_cta(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            cta=initial_cta,
            lead_report_id=lead_report_id,
        )
        if resolved_cta != initial_cta:
            persisted = await self._persist_lead_report(
                benchmark_business_id=benchmark_business_id,
                benchmark_id=benchmark_id,
                business=business,
                competitors=competitors,
                competitor_set=competitor_set,
                deep_study_snapshot=deep_study_snapshot,
                cta=resolved_cta,
            )
            report = persisted.get("lead_report") if isinstance(persisted.get("lead_report"), dict) else report

        await self._record_event(
            event_type="lead_report_generated",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "lead_report_id": report.get("lead_report_id"),
                "competitors": len(competitors),
            },
        )
        return self._sanitize_payload(persisted)

    async def generate_paid_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        report_month: str | None = None,
        history: list[dict[str, Any]] | None = None,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        context = await self._load_benchmark_business_report_context(
            benchmark_business_id=benchmark_business_id,
        )
        business = context["business"]
        benchmark_id = context["benchmark_id"]
        competitors = context["competitors"]
        competitor_set = context["competitor_set"]
        deep_study_snapshot = context["deep_study_snapshot"]

        normalized_month = str(report_month or self._now_utc().strftime("%Y-%m")).strip()
        history_items = [dict(item) for item in history or [] if isinstance(item, dict)]
        html = render_paid_report_html(
            business=business,
            deep_study_snapshot=deep_study_snapshot,
            competitors=competitors,
            history=history_items,
            report_month=normalized_month,
            cta=cta,
        )
        persisted = await self._upsert_paid_report_for_business_month(
            benchmark_business_id=benchmark_business_id,
            report_month=normalized_month,
            payload={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "report_month": normalized_month,
                "business_name": business.get("business_name"),
                "html": html,
                "deep_study_snapshot": deep_study_snapshot,
                "history": history_items,
                "source_payload": {
                    "business": business,
                    "competitors": competitors,
                    "competitor_set_id": competitor_set.get("competitor_set_id")
                    if isinstance(competitor_set, dict)
                    else None,
                },
                "cta": dict(cta or {}),
            },
        )
        report = persisted.get("paid_report") if isinstance(persisted.get("paid_report"), dict) else {}
        await self._record_event(
            event_type="paid_report_generated",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "paid_report_id": report.get("paid_report_id"),
                "report_month": normalized_month,
                "competitors": len(competitors),
                "history_points": len(history_items),
            },
        )
        return self._sanitize_payload(persisted)

    async def generate_public_study_for_benchmark_run(
        self,
        *,
        benchmark_run_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        benchmark = await self._get_benchmark_run(benchmark_run_id=benchmark_run_id)
        if benchmark is None:
            raise LookupError(f"Benchmark run '{benchmark_run_id}' not found.")

        businesses_payload = await self._list_benchmark_businesses(
            benchmark_id=benchmark_run_id,
            page=1,
            page_size=200,
            sort_by="discovery_rank",
            sort_dir="asc",
        )
        businesses = [
            dict(item)
            for item in businesses_payload.get("items", [])
            if isinstance(item, dict)
        ]
        geo_grid_stats = await self._resolve_geo_grid_stats_for_public_study(benchmark=benchmark)
        html = render_public_study_html(
            benchmark_run=benchmark,
            businesses=businesses,
            cta=cta,
            geo_grid_stats=geo_grid_stats,
        )
        await self._record_event(
            event_type="public_benchmark_study_generated",
            data={
                "benchmark_run_id": benchmark_run_id,
                "businesses": len(businesses),
                "geo_visibility": bool(geo_grid_stats),
                "cta_url": str((cta or {}).get("url") or "").strip() or None,
            },
        )
        return self._sanitize_payload(
            {
                "benchmark_run_id": benchmark_run_id,
                "businesses": len(businesses),
                "html": html,
            }
        )

    async def _load_benchmark_business_report_context(
        self,
        *,
        benchmark_business_id: str,
    ) -> dict[str, Any]:
        business = await self._get_benchmark_business(
            benchmark_business_id=benchmark_business_id,
        )
        if business is None:
            raise LookupError(f"Benchmark business '{benchmark_business_id}' not found.")

        benchmark_id = str(business.get("benchmark_id") or "").strip()
        if not benchmark_id:
            raise ValueError("Benchmark business has no benchmark_id.")

        competitor_set = await self._get_competitor_set_for_business(
            target_business_id=benchmark_business_id,
        )
        if competitor_set is None:
            selected = await self._select_competitors_for_benchmark_business(
                benchmark_business_id=benchmark_business_id,
                max_competitors=5,
            )
            competitor_set = (
                selected.get("competitor_set")
                if isinstance(selected.get("competitor_set"), dict)
                else None
            )

        competitors: list[dict[str, Any]] = []
        if isinstance(competitor_set, dict) and isinstance(competitor_set.get("competitors"), list):
            competitors = [
                dict(item)
                for item in competitor_set.get("competitors")
                if isinstance(item, dict)
            ]

        deep_study_snapshot = build_deep_study_snapshot(
            business=business,
            competitors=competitors,
        )
        return {
            "business": business,
            "benchmark_id": benchmark_id,
            "competitor_set": competitor_set,
            "competitors": competitors,
            "deep_study_snapshot": deep_study_snapshot,
        }

    async def _persist_lead_report(
        self,
        *,
        benchmark_business_id: str,
        benchmark_id: str,
        business: dict[str, Any],
        competitors: list[dict[str, Any]],
        competitor_set: dict[str, Any] | None,
        deep_study_snapshot: dict[str, Any],
        cta: dict[str, Any],
    ) -> dict[str, Any]:
        html = render_lead_report_html(
            business=business,
            deep_study_snapshot=deep_study_snapshot,
            competitors=competitors,
            cta=cta,
        )
        return await self._upsert_lead_report_for_business(
            benchmark_business_id=benchmark_business_id,
            payload={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "business_name": business.get("business_name"),
                "html": html,
                "deep_study_snapshot": deep_study_snapshot,
                "source_payload": {
                    "business": business,
                    "competitors": competitors,
                    "competitor_set_id": competitor_set.get("competitor_set_id")
                    if isinstance(competitor_set, dict)
                    else None,
                },
                "cta": dict(cta),
            },
        )
