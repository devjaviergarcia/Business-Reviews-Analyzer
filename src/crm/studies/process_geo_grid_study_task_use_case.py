from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.crm.studies.geo_grid_study_runtime import GeoGridStudyRuntime
from src.workers.contracts import GeoGridStudyTaskPayload


class ProcessGeoGridStudyTaskUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        geo_grid_study_runtime: GeoGridStudyRuntime,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._geo_grid_study_runtime = geo_grid_study_runtime

    async def execute(
        self,
        *,
        task_payload: GeoGridStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        return await self._geo_grid_study_runtime.process_geo_grid_study_task(
            geo_grid_run_id=task_payload.geo_grid_run_id,
            job_id=job_id,
        )
