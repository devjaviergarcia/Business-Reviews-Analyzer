from __future__ import annotations

from typing import Any, Awaitable, Callable


class ListCRMGeoCitiesUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        geo_city_repository: Any,
        sanitize_payload: Callable[[Any], Any],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._geo_city_repository = geo_city_repository
        self._sanitize_payload = sanitize_payload

    async def execute(self) -> list[dict[str, Any]]:
        await self._ensure_indexes()
        return self._sanitize_payload(await self._geo_city_repository.list_enabled())
