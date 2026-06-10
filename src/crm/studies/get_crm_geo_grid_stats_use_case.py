from __future__ import annotations

from typing import Any, Awaitable, Callable


class GetCRMGeoGridStatsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        geo_grid_run_repository: Any,
        geo_grid_result_repository: Any,
        build_geo_grid_stats: Callable[..., dict[str, Any]],
        sanitize_payload: Callable[[Any], Any],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._geo_grid_run_repository = geo_grid_run_repository
        self._geo_grid_result_repository = geo_grid_result_repository
        self._build_geo_grid_stats = build_geo_grid_stats
        self._sanitize_payload = sanitize_payload

    async def execute(self, *, geo_grid_run_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        results = await self._geo_grid_result_repository.list_results(geo_grid_run_id=geo_grid_run_id)
        return self._sanitize_payload(self._build_geo_grid_stats(run=run, results=results))
