from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.pipeline.client_audit.preparation_runtime import ClientAuditPreparationRuntime
from src.services.business_service import BusinessService


class _FakeCRMService:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def enqueue_geo_grid_study_job(
        self,
        *,
        keyword: str,
        city_slug: str,
        top_n: int = 10,
        execution_mode: str | None = None,
        live_display_mode: str | None = None,
        requested_by: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "keyword": keyword,
                "city_slug": city_slug,
                "top_n": top_n,
                "execution_mode": execution_mode,
                "live_display_mode": live_display_mode,
                "requested_by": requested_by,
            }
        )
        return {
            "job_id": "geo-job-2",
            "geo_grid_run_id": "geo-run-2",
        }


def test_build_geo_grid_scraper_uses_isolated_incognito_profile() -> None:
    scraper = BusinessService.build_geo_grid_scraper()

    assert getattr(scraper, "_incognito", None) is True


def test_failed_geogrid_dependency_is_requeued_once_for_hydrated_report() -> None:
    fake_crm_service = _FakeCRMService()
    runtime = ClientAuditPreparationRuntime(job_service=object(), crm_service=fake_crm_service)

    async def _find_latest_compatible_geo_grid(*, scope: dict[str, object]) -> None:
        _ = scope
        return None

    async def _load_geo_grid_run(geo_grid_run_id: str) -> dict[str, object]:
        return {
            "geo_grid_run_id": geo_grid_run_id,
            "status": "failed",
        }

    runtime._find_latest_compatible_geo_grid = _find_latest_compatible_geo_grid  # type: ignore[attr-defined]
    runtime._load_geo_grid_run = _load_geo_grid_run  # type: ignore[attr-defined]

    result = asyncio.run(
        runtime._resolve_geogrid_context(  # type: ignore[attr-defined]
            business_doc={},
            scope={
                "benchmark_query": "restaurante cordoba",
                "city_slug": "cordoba",
            },
            dependency={
                "geo_grid_run_id": "geo-run-1",
            },
            study_resolution_mode="auto_ttl",
            include_geogrid=True,
        )
    )

    assert result["waiting"] is True
    assert result["dependency"]["status"] == "queued"
    assert result["dependency"]["geo_grid_run_id"] == "geo-run-2"
    assert result["dependency"]["refresh_attempts"] == 1
    assert fake_crm_service.calls == [
        {
            "keyword": "restaurante cordoba",
            "city_slug": "cordoba",
            "top_n": 10,
            "execution_mode": "automatic",
            "live_display_mode": "native",
            "requested_by": "client_audit_hydration",
        }
    ]


def test_failed_geogrid_dependency_does_not_loop_after_retry() -> None:
    fake_crm_service = _FakeCRMService()
    runtime = ClientAuditPreparationRuntime(job_service=object(), crm_service=fake_crm_service)

    async def _find_latest_compatible_geo_grid(*, scope: dict[str, object]) -> None:
        _ = scope
        return None

    async def _load_geo_grid_run(geo_grid_run_id: str) -> dict[str, object]:
        return {
            "geo_grid_run_id": geo_grid_run_id,
            "status": "failed",
        }

    runtime._find_latest_compatible_geo_grid = _find_latest_compatible_geo_grid  # type: ignore[attr-defined]
    runtime._load_geo_grid_run = _load_geo_grid_run  # type: ignore[attr-defined]

    result = asyncio.run(
        runtime._resolve_geogrid_context(  # type: ignore[attr-defined]
            business_doc={},
            scope={
                "benchmark_query": "restaurante cordoba",
                "city_slug": "cordoba",
            },
            dependency={
                "geo_grid_run_id": "geo-run-1",
                "refresh_attempts": 1,
            },
            study_resolution_mode="auto_ttl",
            include_geogrid=True,
        )
    )

    assert result["waiting"] is False
    assert result["dependency"]["status"] == "failed"
    assert fake_crm_service.calls == []


def test_is_fresh_accepts_naive_utc_datetimes() -> None:
    runtime = ClientAuditPreparationRuntime(job_service=object(), crm_service=object())

    aware_recent = datetime.now(timezone.utc)
    naive_recent = aware_recent.replace(tzinfo=None)

    assert runtime._is_fresh({"updated_at": naive_recent}, ttl_days=30) is True  # type: ignore[attr-defined]
    assert runtime._is_fresh({"updated_at": aware_recent}, ttl_days=30) is True  # type: ignore[attr-defined]
