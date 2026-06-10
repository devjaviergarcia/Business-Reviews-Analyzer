from __future__ import annotations

from typing import Any, Awaitable, Callable


GetRunFn = Callable[..., Awaitable[dict[str, Any] | None]]
GetGeoCityBySlugFn = Callable[..., Awaitable[dict[str, Any] | None]]
SetGeoGridRunJobIdFn = Callable[..., Awaitable[None]]
MarkGeoGridRunRunningFn = Callable[..., Awaitable[None]]
UpdateGeoGridRunProgressFn = Callable[..., Awaitable[None]]
FinalizeGeoGridRunFn = Callable[..., Awaitable[dict[str, Any]]]
ReplacePointResultsFn = Callable[..., Awaitable[int]]
DiscoverGeoGridPointResultsFn = Callable[..., Awaitable[list[dict[str, Any]]]]
ScraperFactory = Callable[[], Any]
RecordEventFn = Callable[..., Awaitable[None]]
SanitizePayloadFn = Callable[[Any], Any]
DefaultProviderModeFn = Callable[[], str]
DefaultGridSizeFn = Callable[[], int]
DefaultGridSpacingKmFn = Callable[[], float]
DefaultUuleRadiusMetersFn = Callable[[], int]
DefaultThrottleMillisecondsFn = Callable[[], int]
BuildGeoGridPointsFn = Callable[..., list[dict[str, Any]]]


class GeoGridStudyRuntime:
    def __init__(
        self,
        *,
        get_geo_grid_run: GetRunFn,
        get_geo_city_by_slug: GetGeoCityBySlugFn,
        set_geo_grid_run_job_id: SetGeoGridRunJobIdFn,
        mark_geo_grid_run_running: MarkGeoGridRunRunningFn,
        update_geo_grid_run_progress: UpdateGeoGridRunProgressFn,
        finalize_geo_grid_run: FinalizeGeoGridRunFn,
        replace_geo_grid_point_results: ReplacePointResultsFn,
        discover_geo_grid_point_results: DiscoverGeoGridPointResultsFn,
        discover_geo_grid_point_results_uule: DiscoverGeoGridPointResultsFn,
        scraper_factory: ScraperFactory,
        record_event: RecordEventFn,
        sanitize_payload: SanitizePayloadFn,
        default_provider_mode: DefaultProviderModeFn,
        default_grid_size: DefaultGridSizeFn,
        default_grid_spacing_km: DefaultGridSpacingKmFn,
        default_uule_radius_m: DefaultUuleRadiusMetersFn,
        default_throttle_ms: DefaultThrottleMillisecondsFn,
        build_geo_grid_points: BuildGeoGridPointsFn,
    ) -> None:
        self._get_geo_grid_run = get_geo_grid_run
        self._get_geo_city_by_slug = get_geo_city_by_slug
        self._set_geo_grid_run_job_id = set_geo_grid_run_job_id
        self._mark_geo_grid_run_running = mark_geo_grid_run_running
        self._update_geo_grid_run_progress = update_geo_grid_run_progress
        self._finalize_geo_grid_run = finalize_geo_grid_run
        self._replace_geo_grid_point_results = replace_geo_grid_point_results
        self._discover_geo_grid_point_results = discover_geo_grid_point_results
        self._discover_geo_grid_point_results_uule = discover_geo_grid_point_results_uule
        self._scraper_factory = scraper_factory
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._default_provider_mode = default_provider_mode
        self._default_grid_size = default_grid_size
        self._default_grid_spacing_km = default_grid_spacing_km
        self._default_uule_radius_m = default_uule_radius_m
        self._default_throttle_ms = default_throttle_ms
        self._build_geo_grid_points = build_geo_grid_points

    async def process_geo_grid_study_task(
        self,
        *,
        geo_grid_run_id: str,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        run = await self._get_geo_grid_run(geo_grid_run_id=geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{geo_grid_run_id}' not found.")

        city_slug = str(run.get("city_slug") or "")
        city = await self._get_geo_city_by_slug(city_slug=city_slug)
        if city is None:
            raise LookupError(f"Geo city '{run.get('city_slug')}' not found.")

        persisted_run_id = str(run.get("geo_grid_run_id") or geo_grid_run_id)
        await self._set_geo_grid_run_job_id(
            geo_grid_run_id=persisted_run_id,
            job_id=str(job_id) if job_id is not None else str(run.get("job_id") or "").strip() or None,
        )
        await self._mark_geo_grid_run_running(geo_grid_run_id=persisted_run_id)

        keyword = str(run.get("keyword") or "").strip()
        top_n = max(1, min(100, int(run.get("top_n") or 10)))
        provider_mode = self._resolve_provider_mode(run)
        configured_grid_size = int(run.get("grid_size") or self._default_grid_size() or 0)
        configured_grid_spacing_km = float(
            run.get("grid_spacing_km") or self._default_grid_spacing_km() or 0.4
        )
        configured_uule_radius_m = max(
            100,
            int(run.get("uule_radius_m") or self._default_uule_radius_m() or 1000),
        )
        configured_throttle_ms = max(
            100,
            int(run.get("throttle_ms") or self._default_throttle_ms() or 1200),
        )

        points = self._resolve_points(
            run=run,
            city=city,
            provider_mode=provider_mode,
            configured_grid_size=configured_grid_size,
            configured_grid_spacing_km=configured_grid_spacing_km,
        )
        total_points = len(points)
        total_units = total_points * top_n
        metrics = self._build_initial_metrics(
            provider_mode=provider_mode,
            total_points=total_points,
            top_n=top_n,
            total_units=total_units,
            configured_grid_size=configured_grid_size,
            configured_grid_spacing_km=configured_grid_spacing_km,
            configured_uule_radius_m=configured_uule_radius_m,
            configured_throttle_ms=configured_throttle_ms,
        )
        failures: list[dict[str, Any]] = []

        scraper = self._scraper_factory()
        try:
            await scraper.start()
            for index, point in enumerate(points, start=1):
                try:
                    point_results = await self._discover_point_results(
                        scraper=scraper,
                        provider_mode=provider_mode,
                        keyword=keyword,
                        point=point,
                        top_n=top_n,
                        radius_m=configured_uule_radius_m,
                        throttle_ms=configured_throttle_ms,
                    )
                    inserted = await self._replace_geo_grid_point_results(
                        geo_grid_run_id=persisted_run_id,
                        city_slug=city_slug,
                        keyword=keyword,
                        point=point,
                        results=point_results,
                    )
                    self._accumulate_point_metrics(
                        metrics=metrics,
                        inserted=inserted,
                        point_results=point_results,
                    )
                except Exception as exc:  # noqa: BLE001
                    metrics["points_failed"] = int(metrics.get("points_failed") or 0) + 1
                    metrics["points_not_found"] = int(metrics.get("points_not_found") or 0) + 1
                    failures.append(
                        {
                            "point_order": point.get("order"),
                            "point_label": point.get("label"),
                            "error": str(exc),
                        }
                    )

                self._update_progress_shares(metrics=metrics, points_completed=index)
                completed_units = min(total_units, index * top_n)
                await self._update_geo_grid_run_progress(
                    geo_grid_run_id=persisted_run_id,
                    completed_points=index,
                    completed_units=completed_units,
                    metrics=metrics,
                )

            self._finalize_visibility_metrics(metrics=metrics, total_points=total_points)
            final_status = "partial" if failures else "completed"
            final_run = await self._finalize_geo_grid_run(
                geo_grid_run_id=persisted_run_id,
                status=final_status,
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=f"{len(failures)} puntos fallaron." if failures else None,
            )
            result = {
                "geo_grid_run_id": persisted_run_id,
                "job_id": str(job_id) if job_id is not None else None,
                "keyword": keyword,
                "city": run.get("city"),
                "city_slug": city_slug,
                "top_n": top_n,
                "point_count": total_points,
                "status": final_status,
                "metrics": metrics,
                "run": final_run,
            }
            await self._record_event(event_type="geo_grid_study_processed", data=result)
            return self._sanitize_payload(result)
        except Exception as exc:
            await self._finalize_geo_grid_run(
                geo_grid_run_id=persisted_run_id,
                status="failed",
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=str(exc),
            )
            raise
        finally:
            await scraper.close()

    def _resolve_provider_mode(self, run: dict[str, Any]) -> str:
        provider_mode = str(
            run.get("provider_mode") or self._default_provider_mode() or "maps_live"
        ).strip().lower()
        if provider_mode not in {"maps_live", "uule"}:
            return "maps_live"
        return provider_mode

    def _resolve_points(
        self,
        *,
        run: dict[str, Any],
        city: dict[str, Any],
        provider_mode: str,
        configured_grid_size: int,
        configured_grid_spacing_km: float,
    ) -> list[dict[str, Any]]:
        center = run.get("center") if isinstance(run.get("center"), dict) else city.get("center")
        if provider_mode == "uule" and configured_grid_size >= 3 and isinstance(center, dict):
            return self._build_geo_grid_points(
                center_lat=float(center.get("lat", 0.0)),
                center_lng=float(center.get("lng", 0.0)),
                size=configured_grid_size,
                spacing_km=configured_grid_spacing_km,
                label_prefix="Grid",
            )
        return [dict(point) for point in city.get("points") or [] if isinstance(point, dict)]

    def _build_initial_metrics(
        self,
        *,
        provider_mode: str,
        total_points: int,
        top_n: int,
        total_units: int,
        configured_grid_size: int,
        configured_grid_spacing_km: float,
        configured_uule_radius_m: int,
        configured_throttle_ms: int,
    ) -> dict[str, Any]:
        return {
            "provider_mode": provider_mode,
            "point_count": total_points,
            "top_n": top_n,
            "total_units": total_units,
            "points_completed": 0,
            "points_failed": 0,
            "results_found": 0,
            "points_with_results": 0,
            "points_top3": 0,
            "points_top10": 0,
            "points_not_found": 0,
            "grid_size": configured_grid_size if configured_grid_size >= 3 else None,
            "grid_spacing_km": configured_grid_spacing_km,
            "uule_radius_m": configured_uule_radius_m,
            "throttle_ms": configured_throttle_ms,
        }

    async def _discover_point_results(
        self,
        *,
        scraper: Any,
        provider_mode: str,
        keyword: str,
        point: dict[str, Any],
        top_n: int,
        radius_m: int,
        throttle_ms: int,
    ) -> list[dict[str, Any]]:
        if provider_mode == "uule":
            return await self._discover_geo_grid_point_results_uule(
                scraper=scraper,
                keyword=keyword,
                point=point,
                top_n=top_n,
                radius_m=radius_m,
                throttle_ms=throttle_ms,
            )
        return await self._discover_geo_grid_point_results(
            scraper=scraper,
            keyword=keyword,
            point=point,
            top_n=top_n,
        )

    def _accumulate_point_metrics(
        self,
        *,
        metrics: dict[str, Any],
        inserted: int,
        point_results: list[dict[str, Any]],
    ) -> None:
        metrics["results_found"] = int(metrics.get("results_found") or 0) + int(inserted)
        if inserted > 0:
            metrics["points_with_results"] = int(metrics.get("points_with_results") or 0) + 1

        ranks: list[int] = []
        for item in point_results:
            try:
                rank_value = int(item.get("rank") or 0)
            except (TypeError, ValueError):
                continue
            if rank_value > 0:
                ranks.append(rank_value)

        best_rank = min(ranks) if ranks else None
        if best_rank is None:
            metrics["points_not_found"] = int(metrics.get("points_not_found") or 0) + 1
            return
        if best_rank <= 3:
            metrics["points_top3"] = int(metrics.get("points_top3") or 0) + 1
        if best_rank <= 10:
            metrics["points_top10"] = int(metrics.get("points_top10") or 0) + 1

    def _update_progress_shares(self, *, metrics: dict[str, Any], points_completed: int) -> None:
        metrics["points_completed"] = points_completed
        safe_points_completed = max(1, int(metrics.get("points_completed") or 0))
        metrics["share_top3"] = round(float(metrics.get("points_top3") or 0) / safe_points_completed, 4)
        metrics["share_top10"] = round(float(metrics.get("points_top10") or 0) / safe_points_completed, 4)
        metrics["share_not_found"] = round(
            float(metrics.get("points_not_found") or 0) / safe_points_completed,
            4,
        )

    def _finalize_visibility_metrics(self, *, metrics: dict[str, Any], total_points: int) -> None:
        points_total = max(1, total_points)
        top3 = int(metrics.get("points_top3") or 0)
        top10 = int(metrics.get("points_top10") or 0)
        with_results = int(metrics.get("points_with_results") or 0)
        top4_10 = max(0, top10 - top3)
        top11_plus = max(0, with_results - top10)
        visibility_ratio = (top3 + (top4_10 * 0.6) + (top11_plus * 0.3)) / points_total
        metrics["visibility_score"] = round(visibility_ratio * 100.0, 2)
        metrics["share_top3"] = round(top3 / points_total, 4)
        metrics["share_top10"] = round(top10 / points_total, 4)
        metrics["share_not_found"] = round(float(metrics.get("points_not_found") or 0) / points_total, 4)
