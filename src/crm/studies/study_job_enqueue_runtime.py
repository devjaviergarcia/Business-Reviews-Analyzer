from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.benchmark import normalize_grid_size
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
from src.workers.contracts import BenchmarkLocalStudyTaskPayload, GeoGridStudyTaskPayload


CreateBenchmarkRunFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
CreateGeoGridRunFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
GetGeoCityBySlugFn = Callable[..., Awaitable[dict[str, Any] | None]]
SetGeoGridRunJobIdFn = Callable[..., Awaitable[None]]
RecordEventFn = Callable[..., Awaitable[None]]
SanitizePayloadFn = Callable[[Any], Any]
EnqueueJobFn = Callable[..., Awaitable[dict[str, Any]]]
DefaultGeoGridProviderModeFn = Callable[[], str]
DefaultGeoGridSizeFn = Callable[[], int]
DefaultGeoGridSpacingKmFn = Callable[[], float]
DefaultUuleRadiusMetersFn = Callable[[], int]
DefaultThrottleMillisecondsFn = Callable[[], int]


class StudyJobEnqueueRuntime:
    def __init__(
        self,
        *,
        enqueue_job: EnqueueJobFn,
        create_benchmark_run: CreateBenchmarkRunFn,
        create_geo_grid_run: CreateGeoGridRunFn,
        get_geo_city_by_slug: GetGeoCityBySlugFn,
        set_geo_grid_run_job_id: SetGeoGridRunJobIdFn,
        record_event: RecordEventFn,
        sanitize_payload: SanitizePayloadFn,
        live_google_discovery_aliases: tuple[str, ...],
        default_geo_grid_provider_mode: DefaultGeoGridProviderModeFn,
        default_geo_grid_size: DefaultGeoGridSizeFn,
        default_geo_grid_spacing_km: DefaultGeoGridSpacingKmFn,
        default_uule_radius_m: DefaultUuleRadiusMetersFn,
        default_throttle_ms: DefaultThrottleMillisecondsFn,
    ) -> None:
        self._enqueue_job = enqueue_job
        self._create_benchmark_run = create_benchmark_run
        self._create_geo_grid_run = create_geo_grid_run
        self._get_geo_city_by_slug = get_geo_city_by_slug
        self._set_geo_grid_run_job_id = set_geo_grid_run_job_id
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._live_google_discovery_aliases = live_google_discovery_aliases
        self._default_geo_grid_provider_mode = default_geo_grid_provider_mode
        self._default_geo_grid_size = default_geo_grid_size
        self._default_geo_grid_spacing_km = default_geo_grid_spacing_km
        self._default_uule_radius_m = default_uule_radius_m
        self._default_throttle_ms = default_throttle_ms

    async def enqueue_benchmark_study_job(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
        title: str | None = None,
    ) -> dict[str, Any]:
        normalized_source = str(source or "").strip().lower()
        if normalized_source in self._live_google_discovery_aliases:
            normalized_source = "auto_live_google_maps"
        if not normalized_source:
            normalized_source = "auto_live_google_maps"

        base_payload = BenchmarkLocalStudyTaskPayload(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=normalized_source,
            title=title,
        )
        run_doc = await self._create_benchmark_run(
            {
                "title": base_payload.title,
                "query": base_payload.query,
                "city": base_payload.city,
                "category": base_payload.category,
                "limit": base_payload.limit,
                "source": base_payload.source,
            }
        )
        benchmark_run_id = str(run_doc.get("benchmark_run_id") or "").strip() or None
        payload = base_payload.model_copy(update={"benchmark_run_id": benchmark_run_id})
        queued = await self._enqueue_job(
            task_payload=payload,
            queue_name="crm",
            job_type="benchmark_local_study",
        )
        await self._record_event(
            event_type="benchmark_study_job_queued",
            data={
                "crm_job_id": queued.get("job_id"),
                "benchmark_run_id": benchmark_run_id,
                "query": payload.query,
                "city": payload.city,
                "category": payload.category,
                "limit": payload.limit,
                "source": payload.source,
            },
        )
        queued["benchmark_run_id"] = benchmark_run_id
        return self._sanitize_payload(queued)

    async def enqueue_geo_grid_study_job(
        self,
        *,
        keyword: str,
        city_slug: str,
        top_n: int = 10,
        provider_mode: str | None = None,
        grid_size: int | None = None,
        grid_spacing_km: float | None = None,
        uule_radius_m: int | None = None,
        throttle_ms: int | None = None,
    ) -> dict[str, Any]:
        normalized_keyword = str(keyword or "").strip()
        if not normalized_keyword:
            raise ValueError("keyword is required.")
        safe_top_n = max(1, min(100, int(top_n or 10)))
        requested_provider_mode = str(
            provider_mode or self._default_geo_grid_provider_mode() or "maps_live"
        ).strip().lower()
        safe_provider_mode = requested_provider_mode if requested_provider_mode in {"maps_live", "uule"} else "maps_live"
        safe_grid_size = None
        if grid_size is not None:
            safe_grid_size = normalize_grid_size(int(grid_size))
        elif safe_provider_mode == "uule" and int(self._default_geo_grid_size() or 0) >= 3:
            safe_grid_size = normalize_grid_size(int(self._default_geo_grid_size()))
        safe_grid_spacing_km = (
            float(grid_spacing_km)
            if grid_spacing_km is not None
            else float(self._default_geo_grid_spacing_km() or 0.4)
        )
        safe_uule_radius_m = (
            max(100, int(uule_radius_m))
            if uule_radius_m is not None
            else max(100, int(self._default_uule_radius_m() or 1000))
        )
        safe_throttle_ms = (
            max(100, int(throttle_ms))
            if throttle_ms is not None
            else max(100, int(self._default_throttle_ms() or 1200))
        )
        city = await self._get_geo_city_by_slug(city_slug=city_slug)
        if city is None:
            raise LookupError(f"Geo city '{city_slug}' not found.")
        city_points = city.get("points") if isinstance(city.get("points"), list) else []
        expected_point_count = int(city.get("point_count") or len(city_points))
        if safe_provider_mode == "uule" and safe_grid_size is not None and safe_grid_size >= 3:
            expected_point_count = int(safe_grid_size) * int(safe_grid_size)

        run = await self._create_geo_grid_run(
            {
                "keyword": normalized_keyword,
                "city": city.get("city"),
                "city_slug": city.get("city_slug"),
                "center": city.get("center"),
                "points": city_points,
                "point_count": expected_point_count,
                "top_n": safe_top_n,
                "provider_mode": safe_provider_mode,
                "grid_size": safe_grid_size,
                "grid_spacing_km": safe_grid_spacing_km,
                "uule_radius_m": safe_uule_radius_m,
                "throttle_ms": safe_throttle_ms,
            }
        )
        geo_grid_run_id = str(run.get("geo_grid_run_id") or "").strip()
        payload = GeoGridStudyTaskPayload(geo_grid_run_id=geo_grid_run_id)
        queued = await self._enqueue_job(
            task_payload=payload,
            queue_name="scrape_google_maps",
            job_type="geo_grid_study",
            source="google_maps",
            runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
            execution_mode=DEFAULT_BROWSER_EXECUTION_MODE,
            requested_by="crm_geo_grid_api",
            fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
            source_display_name="Google Maps",
        )
        await self._set_geo_grid_run_job_id(
            geo_grid_run_id=geo_grid_run_id,
            job_id=str(queued.get("job_id") or "").strip() or None,
        )
        await self._record_event(
            event_type="geo_grid_study_job_queued",
            data={
                "job_id": queued.get("job_id"),
                "geo_grid_run_id": geo_grid_run_id,
                "keyword": normalized_keyword,
                "city_slug": city.get("city_slug"),
                "top_n": safe_top_n,
                "provider_mode": safe_provider_mode,
                "grid_size": safe_grid_size,
                "grid_spacing_km": safe_grid_spacing_km,
                "uule_radius_m": safe_uule_radius_m,
                "throttle_ms": safe_throttle_ms,
            },
        )
        queued["geo_grid_run_id"] = geo_grid_run_id
        return self._sanitize_payload(queued)
