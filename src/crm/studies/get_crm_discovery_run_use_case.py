from __future__ import annotations

from typing import Any, Awaitable, Callable


class GetCRMDiscoveryRunUseCase:
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

    async def execute(self, *, discovery_run_id: str) -> dict[str, Any]:
        await self._ensure_indexes()
        run_doc = await self._discovery_run_repository.get_run(run_id=discovery_run_id)
        if run_doc is None:
            raise LookupError(f"Discovery run '{discovery_run_id}' not found.")
        return self._sanitize_payload(run_doc)
