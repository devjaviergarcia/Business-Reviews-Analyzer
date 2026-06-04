from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.config import settings
from src.crm.benchmark import normalize_grid_size
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
)
from src.services.analysis_job_service import AnalysisJobService
from src.workers.contracts import GeoGridStudyTaskPayload


class EnqueueGeoGridStudyJobUseCase:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        ensure_indexes: Callable[[], Awaitable[None]],
        get_geo_city_by_slug: Callable[..., Awaitable[dict[str, Any] | None]],
        create_geo_grid_run: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        set_geo_grid_run_job_id: Callable[..., Awaitable[None]],
        record_event: Callable[..., Awaitable[None]],
    ) -> None:
        self._job_service = job_service
        self._ensure_indexes = ensure_indexes
        self._get_geo_city_by_slug = get_geo_city_by_slug
        self._create_geo_grid_run = create_geo_grid_run
        self._set_geo_grid_run_job_id = set_geo_grid_run_job_id
        self._record_event = record_event

    async def execute(
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
        await self._ensure_indexes()
        normalized_keyword = str(keyword or "").strip()
        if not normalized_keyword:
            raise ValueError("keyword is required.")
        safe_top_n = max(1, min(100, int(top_n or 10)))
        requested_provider_mode = str(provider_mode or settings.geo_grid_provider_mode or "maps_live").strip().lower()
        safe_provider_mode = requested_provider_mode if requested_provider_mode in {"maps_live", "uule"} else "maps_live"
        safe_grid_size = None
        if grid_size is not None:
            safe_grid_size = normalize_grid_size(int(grid_size))
        elif safe_provider_mode == "uule" and int(settings.geo_grid_uule_grid_size or 0) >= 3:
            safe_grid_size = normalize_grid_size(int(settings.geo_grid_uule_grid_size))
        safe_grid_spacing_km = (
            float(grid_spacing_km)
            if grid_spacing_km is not None
            else float(settings.geo_grid_uule_spacing_km or 0.4)
        )
        safe_uule_radius_m = (
            max(100, int(uule_radius_m))
            if uule_radius_m is not None
            else max(100, int(settings.geo_grid_uule_radius_m or 1000))
        )
        safe_throttle_ms = (
            max(100, int(throttle_ms))
            if throttle_ms is not None
            else max(100, int(settings.geo_grid_uule_throttle_ms or 1200))
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
        queued = await self._job_service.enqueue_job(
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
                "geo_grid_run_id": geo_grid_run_id,
                "job_id": str(queued.get("job_id") or "").strip() or None,
                "city_slug": str(city.get("city_slug") or "").strip() or None,
                "keyword": normalized_keyword,
                "provider_mode": safe_provider_mode,
            },
        )
        queued["geo_grid_run_id"] = geo_grid_run_id
        return queued
