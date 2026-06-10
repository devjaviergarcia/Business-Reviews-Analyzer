from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.studies.study_job_enqueue_runtime import StudyJobEnqueueRuntime


class EnqueueGeoGridStudyJobUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        study_job_enqueue_runtime: StudyJobEnqueueRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._study_job_enqueue_runtime = study_job_enqueue_runtime

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
        return await self._study_job_enqueue_runtime.enqueue_geo_grid_study_job(
            keyword=keyword,
            city_slug=city_slug,
            top_n=top_n,
            provider_mode=provider_mode,
            grid_size=grid_size,
            grid_spacing_km=grid_spacing_km,
            uule_radius_m=uule_radius_m,
            throttle_ms=throttle_ms,
        )
