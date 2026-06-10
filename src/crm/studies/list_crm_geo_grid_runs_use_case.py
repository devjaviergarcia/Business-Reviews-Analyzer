from __future__ import annotations

from typing import Any, Awaitable, Callable


class ListCRMGeoGridRunsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        geo_grid_run_repository: Any,
        sanitize_payload: Callable[[Any], Any],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._geo_grid_run_repository = geo_grid_run_repository
        self._sanitize_payload = sanitize_payload

    async def execute(
        self,
        *,
        page: int,
        page_size: int,
        city_slug: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        payload = await self._geo_grid_run_repository.list_runs(
            page=page,
            page_size=page_size,
            city_slug=city_slug,
            status_filter=status_filter,
        )
        return self._sanitize_payload(payload)
