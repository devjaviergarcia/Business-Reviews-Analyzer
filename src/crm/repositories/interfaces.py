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


class BenchmarkRunRepository(Protocol):
    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def mark_running(self, *, benchmark_run_id: str) -> dict[str, Any] | None: ...

    async def finalize(
        self,
        *,
        benchmark_run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None: ...

    async def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        status_filter: str | None = None,
        city: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]: ...

    async def get_run(self, *, benchmark_run_id: str) -> dict[str, Any] | None: ...


class BenchmarkBusinessRepository(Protocol):
    async def upsert_business(self, *, benchmark_id: str, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def get_business(self, *, benchmark_business_id: str) -> dict[str, Any] | None: ...

    async def list_businesses(
        self,
        *,
        benchmark_id: str | None,
        page: int,
        page_size: int,
        city: str | None = None,
        category: str | None = None,
        q: str | None = None,
        sort_by: str = "opportunity_score",
        sort_dir: str = "desc",
    ) -> dict[str, Any]: ...


class CompetitorSetRepository(Protocol):
    async def upsert_set(
        self,
        *,
        benchmark_id: str,
        target_business_id: str,
        competitors: list[dict[str, Any]],
        selection_version: str = "v1",
    ) -> dict[str, Any]: ...

    async def get_for_business(self, *, target_business_id: str) -> dict[str, Any] | None: ...

    async def list_by_benchmark(self, *, benchmark_id: str, page: int, page_size: int) -> dict[str, Any]: ...


class LeadReportRepository(Protocol):
    async def upsert_for_business(
        self,
        *,
        benchmark_business_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]: ...

    async def get_for_business(
        self,
        *,
        benchmark_business_id: str,
        report_type: str = "lead",
    ) -> dict[str, Any] | None: ...

    async def get_report(self, *, lead_report_id: str) -> dict[str, Any] | None: ...


class PaidReportRepository(Protocol):
    async def upsert_for_business_month(
        self,
        *,
        benchmark_business_id: str,
        report_month: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]: ...

    async def get_for_business_month(
        self,
        *,
        benchmark_business_id: str,
        report_month: str,
    ) -> dict[str, Any] | None: ...

    async def get_report(self, *, paid_report_id: str) -> dict[str, Any] | None: ...


class GeoCityRepository(Protocol):
    async def seed_default_cities(self) -> dict[str, Any]: ...

    async def list_enabled(self) -> list[dict[str, Any]]: ...

    async def get_by_slug(self, *, city_slug: str) -> dict[str, Any] | None: ...


class GeoGridRunRepository(Protocol):
    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def set_job_id(self, *, geo_grid_run_id: str, job_id: str | None) -> dict[str, Any] | None: ...

    async def mark_running(self, *, geo_grid_run_id: str) -> dict[str, Any] | None: ...

    async def update_progress(
        self,
        *,
        geo_grid_run_id: str,
        completed_points: int,
        completed_units: int,
        metrics: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None: ...

    async def finalize(
        self,
        *,
        geo_grid_run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None: ...

    async def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        city_slug: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]: ...

    async def get_run(self, *, geo_grid_run_id: str) -> dict[str, Any] | None: ...


class GeoGridResultRepository(Protocol):
    async def replace_point_results(
        self,
        *,
        geo_grid_run_id: str,
        city_slug: str,
        keyword: str,
        point: dict[str, Any],
        results: list[dict[str, Any]],
    ) -> int: ...

    async def list_results(self, *, geo_grid_run_id: str) -> list[dict[str, Any]]: ...
