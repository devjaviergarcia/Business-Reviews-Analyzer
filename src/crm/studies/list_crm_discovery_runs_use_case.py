from __future__ import annotations

from typing import Any, Awaitable, Callable


class ListCRMDiscoveryRunsUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        discovery_run_repository: Any,
        sanitize_payload: Callable[[Any], Any],
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._discovery_run_repository = discovery_run_repository
        self._sanitize_payload = sanitize_payload

    async def execute(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        payload = await self._discovery_run_repository.list_runs(page=page, page_size=page_size)
        return self._sanitize_payload(payload)
