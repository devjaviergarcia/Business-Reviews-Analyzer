from __future__ import annotations

from typing import Any, Awaitable, Callable
from urllib.parse import quote_plus, urlencode

from src.config import settings
from src.crm.benchmark import select_competitors_for_business


GetGeoGridRunListFn = Callable[..., Awaitable[dict[str, Any]]]
ListGeoGridResultsFn = Callable[..., Awaitable[list[dict[str, Any]]]]
BuildGeoGridStatsFn = Callable[..., dict[str, Any]]
NormalizeTextFn = Callable[[Any], str]
GetBenchmarkBusinessFn = Callable[..., Awaitable[dict[str, Any] | None]]
ListBenchmarkBusinessesFn = Callable[..., Awaitable[dict[str, Any]]]
UpsertCompetitorSetFn = Callable[..., Awaitable[dict[str, Any]]]
RecordEventFn = Callable[..., Awaitable[None]]
SanitizePayloadFn = Callable[[Any], Any]


class CRMStudySupportRuntime:
    def __init__(
        self,
        *,
        list_geo_grid_runs: GetGeoGridRunListFn,
        list_geo_grid_results: ListGeoGridResultsFn,
        build_geo_grid_stats: BuildGeoGridStatsFn,
        normalize_text: NormalizeTextFn,
        get_benchmark_business: GetBenchmarkBusinessFn,
        list_benchmark_businesses: ListBenchmarkBusinessesFn,
        upsert_competitor_set: UpsertCompetitorSetFn,
        record_event: RecordEventFn,
        sanitize_payload: SanitizePayloadFn,
    ) -> None:
        self._list_geo_grid_runs = list_geo_grid_runs
        self._list_geo_grid_results = list_geo_grid_results
        self._build_geo_grid_stats = build_geo_grid_stats
        self._normalize_text = normalize_text
        self._get_benchmark_business = get_benchmark_business
        self._list_benchmark_businesses = list_benchmark_businesses
        self._upsert_competitor_set = upsert_competitor_set
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload

    async def resolve_geo_grid_stats_for_public_study(self, *, benchmark: dict[str, Any]) -> dict[str, Any] | None:
        benchmark_city_slug = str(benchmark.get("city_slug") or "").strip().lower()
        if not benchmark_city_slug:
            benchmark_city = str(benchmark.get("city") or "").strip()
            if benchmark_city:
                benchmark_city_slug = self._normalize_text(benchmark_city).replace(" ", "-")
        benchmark_query = self._normalize_text(str(benchmark.get("query") or ""))
        try:
            runs_payload = await self._list_geo_grid_runs(
                page=1,
                page_size=120,
                city_slug=benchmark_city_slug or None,
                status_filter=None,
            )
        except Exception:
            return None
        raw_items = runs_payload.get("items") if isinstance(runs_payload, dict) else []
        runs = [dict(item) for item in raw_items if isinstance(item, dict)]
        if not runs:
            return None
        candidates: list[dict[str, Any]] = []
        for run in runs:
            status = str(run.get("status") or "").strip().lower()
            if status not in {"completed", "partial"}:
                continue
            run_query = self._normalize_text(str(run.get("keyword") or ""))
            if benchmark_query and run_query:
                if benchmark_query not in run_query and run_query not in benchmark_query:
                    continue
            candidates.append(run)
        if not candidates:
            return None
        selected_run = candidates[0]
        selected_run_id = str(selected_run.get("geo_grid_run_id") or "").strip()
        if not selected_run_id:
            return None
        results = await self._list_geo_grid_results(geo_grid_run_id=selected_run_id)
        stats = self._build_geo_grid_stats(run=selected_run, results=results)
        points = stats.get("points") if isinstance(stats.get("points"), list) else []
        return stats if points else None

    async def select_competitors_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        max_competitors: int = 5,
    ) -> dict[str, Any]:
        target = await self._get_benchmark_business(benchmark_business_id=benchmark_business_id)
        if target is None:
            raise LookupError(f"Benchmark business '{benchmark_business_id}' not found.")
        benchmark_id = str(target.get("benchmark_id") or "").strip()
        if not benchmark_id:
            raise ValueError("Benchmark business has no benchmark_id.")
        candidates_payload = await self._list_benchmark_businesses(
            benchmark_id=benchmark_id,
            page=1,
            page_size=200,
            sort_by="review_count",
            sort_dir="desc",
        )
        candidates = candidates_payload.get("items") if isinstance(candidates_payload.get("items"), list) else []
        competitors = select_competitors_for_business(target, candidates, max_competitors=max_competitors)
        persisted = await self._upsert_competitor_set(
            benchmark_id=benchmark_id,
            target_business_id=str(target.get("benchmark_business_id") or benchmark_business_id),
            competitors=competitors,
            selection_version="v1",
        )
        await self._record_event(
            event_type="benchmark_competitors_selected",
            data={
                "benchmark_id": benchmark_id,
                "benchmark_business_id": benchmark_business_id,
                "competitors": len(competitors),
            },
        )
        return self._sanitize_payload(persisted)

    def resolve_lead_report_cta(
        self,
        *,
        benchmark_business_id: str,
        benchmark_id: str | None,
        cta: dict[str, Any] | None,
        lead_report_id: str | None,
    ) -> dict[str, Any]:
        resolved = dict(cta or {})
        resolved.setdefault("label", "Valorar este informe")
        resolved.setdefault(
            "description",
            "Cuéntanos en 1 minuto si este analisis te ha resultado util y que mejorarias.",
        )
        url_value = str(resolved.get("url") or "").strip()
        if not url_value:
            resolved["url"] = self.build_onboarding_form_url(
                lead_report_id=lead_report_id,
                benchmark_business_id=benchmark_business_id,
                benchmark_id=benchmark_id,
            )
            return resolved
        if lead_report_id and "lead_report_id=" not in url_value:
            separator = "&" if "?" in url_value else "?"
            resolved["url"] = f"{url_value}{separator}lead_report_id={quote_plus(lead_report_id)}"
        return resolved

    def build_onboarding_form_url(
        self,
        *,
        lead_report_id: str | None,
        benchmark_business_id: str,
        benchmark_id: str | None,
    ) -> str:
        base_url = str(settings.crm_onboarding_form_base_url or "").strip() or "/valoracion"
        params: list[tuple[str, str]] = []
        if lead_report_id:
            params.append(("lead_report_id", lead_report_id))
        if benchmark_business_id:
            params.append(("benchmark_business_id", benchmark_business_id))
        if benchmark_id:
            params.append(("benchmark_id", benchmark_id))
        if not params:
            return base_url
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}{urlencode(params)}"
