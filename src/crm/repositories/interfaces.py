from __future__ import annotations

from typing import Any, Protocol


class LeadRepository(Protocol):
    async def list(
        self,
        *,
        page: int,
        page_size: int,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
        sort_by: str,
        sort_dir: str,
    ) -> dict[str, Any]: ...

    async def get_by_id(self, *, lead_id: str) -> dict[str, Any] | None: ...

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None: ...

    async def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> Any: ...

    async def find_one_and_update(self, query: dict[str, Any], update: dict[str, Any]) -> dict[str, Any] | None: ...

    async def bulk_delete(
        self,
        *,
        lead_ids: list[str] | None,
        delete_all_matching: bool,
        exclude_lead_ids: list[str] | None,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]: ...


class EventRepository(Protocol):
    async def insert(self, event_doc: dict[str, Any]) -> Any: ...

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        lead_id: str | None,
        campaign_id: str | None,
    ) -> dict[str, Any]: ...


class CampaignRepository(Protocol):
    async def collection(self) -> Any: ...


class MessageRepository(Protocol):
    async def collection(self) -> Any: ...


class SuppressionRepository(Protocol):
    async def collection(self) -> Any: ...


class DiscoveryRunRepository(Protocol):
    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def mark_running(self, *, run_id: str) -> dict[str, Any] | None: ...

    async def append_step(
        self,
        *,
        run_id: str,
        step: str,
        ok: bool,
        duration_ms: int,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any] | None: ...

    async def finalize(
        self,
        *,
        run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None: ...

    async def list_runs(self, *, page: int, page_size: int) -> dict[str, Any]: ...

    async def get_run(self, *, run_id: str) -> dict[str, Any] | None: ...
