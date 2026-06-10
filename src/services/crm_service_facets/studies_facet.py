from __future__ import annotations

from typing import Any

from src.workers.contracts import BenchmarkLocalStudyTaskPayload, GeoGridStudyTaskPayload


class CRMServiceStudiesFacet:

    async def enqueue_benchmark_study_job(
        self,
        *,
        query: str,
        city: str | None = None,
        category: str | None = None,
        limit: int = 100,
        source: str = "auto_live_google_maps",
        title: str | None = None,
    ) -> dict[str, Any]:
        if self._enqueue_benchmark_study_job_use_case is not None:
            return await self._enqueue_benchmark_study_job_use_case.execute(
                query=query,
                city=city,
                category=category,
                limit=limit,
                source=source,
                title=title,
            )
        await self.ensure_indexes()
        return await self._study_job_enqueue_runtime.enqueue_benchmark_study_job(
            query=query,
            city=city,
            category=category,
            limit=limit,
            source=source,
            title=title,
        )

    async def list_geo_cities(self) -> list[dict[str, Any]]:
        if self._list_crm_geo_cities_use_case is not None:
            return await self._list_crm_geo_cities_use_case.execute()
        await self.ensure_indexes()
        return self._sanitize_payload(await self._geo_city_repository.list_enabled())

    async def enqueue_geo_grid_study_job(
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
        if self._enqueue_geo_grid_study_job_use_case is not None:
            return await self._enqueue_geo_grid_study_job_use_case.execute(
                keyword=keyword,
                city_slug=city_slug,
                top_n=top_n,
                provider_mode=provider_mode,
                grid_size=grid_size,
                grid_spacing_km=grid_spacing_km,
                uule_radius_m=uule_radius_m,
                throttle_ms=throttle_ms,
            )
        await self.ensure_indexes()
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

    async def list_geo_grid_runs(
        self,
        *,
        page: int,
        page_size: int,
        city_slug: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        if self._list_crm_geo_grid_runs_use_case is not None:
            return await self._list_crm_geo_grid_runs_use_case.execute(
                page=page,
                page_size=page_size,
                city_slug=city_slug,
                status_filter=status_filter,
            )
        await self.ensure_indexes()
        return self._sanitize_payload(
            await self._geo_grid_run_repository.list_runs(
                page=page,
                page_size=page_size,
                city_slug=city_slug,
                status_filter=status_filter,
            )
        )

    async def get_geo_grid_run(self, *, geo_grid_run_id: str) -> dict[str, Any]:
        if self._get_crm_geo_grid_run_use_case is not None:
            return await self._get_crm_geo_grid_run_use_case.execute(geo_grid_run_id=geo_grid_run_id)
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        return self._sanitize_payload(run)

    async def list_geo_grid_results(self, *, geo_grid_run_id: str) -> list[dict[str, Any]]:
        if self._list_crm_geo_grid_results_use_case is not None:
            return await self._list_crm_geo_grid_results_use_case.execute(geo_grid_run_id=geo_grid_run_id)
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        return self._sanitize_payload(await self._geo_grid_result_repository.list_results(geo_grid_run_id=geo_grid_run_id))

    async def get_geo_grid_stats(self, *, geo_grid_run_id: str) -> dict[str, Any]:
        if self._get_crm_geo_grid_stats_use_case is not None:
            return await self._get_crm_geo_grid_stats_use_case.execute(geo_grid_run_id=geo_grid_run_id)
        await self.ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")
        results = await self._geo_grid_result_repository.list_results(geo_grid_run_id=geo_grid_run_id)
        return self._sanitize_payload(self._build_geo_grid_stats(run=run, results=results))

    async def process_benchmark_study_task(
        self,
        *,
        task_payload: BenchmarkLocalStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_benchmark_study_task_use_case is not None:
            return await self._process_benchmark_study_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        return self._sanitize_payload(
            await self._benchmark_study_processing_runtime.process_task(
                task_payload=task_payload,
                job_id=job_id,
            )
        )

    async def process_geo_grid_study_task(
        self,
        *,
        task_payload: GeoGridStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        if self._process_geo_grid_study_task_use_case is not None:
            return await self._process_geo_grid_study_task_use_case.execute(
                task_payload=task_payload,
                job_id=job_id,
            )
        await self.ensure_indexes()
        return await self._geo_grid_study_runtime.process_geo_grid_study_task(
            geo_grid_run_id=task_payload.geo_grid_run_id,
            job_id=job_id,
        )

    def _build_geo_grid_stats(self, *, run: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        return self._geo_grid_stats_builder.build_geo_grid_stats(run=run, results=results)

    async def _resolve_geo_grid_stats_for_public_study(self, *, benchmark: dict[str, Any]) -> dict[str, Any] | None:
        return await self._study_support_runtime.resolve_geo_grid_stats_for_public_study(benchmark=benchmark)

    def _population_stddev(self, values: list[int]) -> float:
        return self._geo_grid_stats_builder.population_stddev(values)

    async def select_competitors_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        max_competitors: int = 5,
    ) -> dict[str, Any]:
        await self.ensure_indexes()
        return await self._study_support_runtime.select_competitors_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            max_competitors=max_competitors,
        )

    async def generate_lead_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._generate_crm_lead_report_use_case is not None:
            return await self._generate_crm_lead_report_use_case.execute(
                benchmark_business_id=benchmark_business_id,
                cta=cta,
            )
        await self.ensure_indexes()
        return await self._benchmark_report_runtime.generate_lead_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            cta=cta,
        )

    async def generate_paid_report_for_benchmark_business(
        self,
        *,
        benchmark_business_id: str,
        report_month: str | None = None,
        history: list[dict[str, Any]] | None = None,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._generate_crm_paid_report_use_case is not None:
            return await self._generate_crm_paid_report_use_case.execute(
                benchmark_business_id=benchmark_business_id,
                report_month=report_month,
                history=history,
                cta=cta,
            )
        await self.ensure_indexes()
        return await self._benchmark_report_runtime.generate_paid_report_for_benchmark_business(
            benchmark_business_id=benchmark_business_id,
            report_month=report_month,
            history=history,
            cta=cta,
        )

    async def generate_public_study_for_benchmark_run(
        self,
        *,
        benchmark_run_id: str,
        cta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._generate_crm_public_study_use_case is not None:
            return await self._generate_crm_public_study_use_case.execute(
                benchmark_run_id=benchmark_run_id,
                cta=cta,
            )
        await self.ensure_indexes()
        return await self._benchmark_report_runtime.generate_public_study_for_benchmark_run(
            benchmark_run_id=benchmark_run_id,
            cta=cta,
        )

    def _resolve_lead_report_cta(
        self,
        *,
        benchmark_business_id: str,
        benchmark_id: str | None,
        cta: dict[str, Any] | None,
        lead_report_id: str | None,
    ) -> dict[str, Any]:
        return self._study_support_runtime.resolve_lead_report_cta(
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
            cta=cta,
            lead_report_id=lead_report_id,
        )

    def _build_onboarding_form_url(
        self,
        *,
        lead_report_id: str | None,
        benchmark_business_id: str,
        benchmark_id: str | None,
    ) -> str:
        return self._study_support_runtime.build_onboarding_form_url(
            lead_report_id=lead_report_id,
            benchmark_business_id=benchmark_business_id,
            benchmark_id=benchmark_id,
        )
