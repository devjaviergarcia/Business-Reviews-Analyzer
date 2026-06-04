from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.config import settings
from src.crm.benchmark import build_geo_grid_points
from src.services.business_service import BusinessService
from src.workers.contracts import GeoGridStudyTaskPayload


class ProcessGeoGridStudyTaskUseCase:
    def __init__(
        self,
        *,
        ensure_indexes: Callable[[], Awaitable[None]],
        geo_grid_run_repository: Any,
        geo_city_repository: Any,
        geo_grid_result_repository: Any,
        record_event: Callable[..., Awaitable[None]],
        sanitize_payload: Callable[[Any], Any],
        discover_geo_grid_point_results: Callable[..., Awaitable[list[dict[str, Any]]]],
        discover_geo_grid_point_results_uule: Callable[..., Awaitable[list[dict[str, Any]]]],
        scraper_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._ensure_indexes = ensure_indexes
        self._geo_grid_run_repository = geo_grid_run_repository
        self._geo_city_repository = geo_city_repository
        self._geo_grid_result_repository = geo_grid_result_repository
        self._record_event = record_event
        self._sanitize_payload = sanitize_payload
        self._discover_geo_grid_point_results = discover_geo_grid_point_results
        self._discover_geo_grid_point_results_uule = discover_geo_grid_point_results_uule
        self._scraper_factory = scraper_factory or BusinessService.build_default_scraper

    async def execute(
        self,
        *,
        task_payload: GeoGridStudyTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        await self._ensure_indexes()
        run = await self._geo_grid_run_repository.get_run(geo_grid_run_id=task_payload.geo_grid_run_id)
        if run is None:
            raise LookupError(f"Geo grid run '{task_payload.geo_grid_run_id}' not found.")
        city = await self._geo_city_repository.get_by_slug(city_slug=str(run.get("city_slug") or ""))
        if city is None:
            raise LookupError(f"Geo city '{run.get('city_slug')}' not found.")

        geo_grid_run_id = str(run.get("geo_grid_run_id") or task_payload.geo_grid_run_id)
        await self._geo_grid_run_repository.set_job_id(
            geo_grid_run_id=geo_grid_run_id,
            job_id=str(job_id) if job_id is not None else str(run.get("job_id") or "").strip() or None,
        )
        await self._geo_grid_run_repository.mark_running(geo_grid_run_id=geo_grid_run_id)

        keyword = str(run.get("keyword") or "").strip()
        top_n = max(1, min(100, int(run.get("top_n") or 10)))
        provider_mode = str(run.get("provider_mode") or settings.geo_grid_provider_mode or "maps_live").strip().lower()
        if provider_mode not in {"maps_live", "uule"}:
            provider_mode = "maps_live"
        configured_grid_size = int(run.get("grid_size") or settings.geo_grid_uule_grid_size or 0)
        configured_grid_spacing_km = float(run.get("grid_spacing_km") or settings.geo_grid_uule_spacing_km or 0.4)
        configured_uule_radius_m = max(100, int(run.get("uule_radius_m") or settings.geo_grid_uule_radius_m or 1000))
        configured_throttle_ms = max(100, int(run.get("throttle_ms") or settings.geo_grid_uule_throttle_ms or 1200))
        center = run.get("center") if isinstance(run.get("center"), dict) else city.get("center")

        if provider_mode == "uule" and configured_grid_size >= 3 and isinstance(center, dict):
            points = build_geo_grid_points(
                center_lat=float(center.get("lat", 0.0)),
                center_lng=float(center.get("lng", 0.0)),
                size=configured_grid_size,
                spacing_km=configured_grid_spacing_km,
                label_prefix="Grid",
            )
        else:
            points = [dict(point) for point in city.get("points") or [] if isinstance(point, dict)]

        total_points = len(points)
        total_units = total_points * top_n
        metrics: dict[str, Any] = {
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
        failures: list[dict[str, Any]] = []

        scraper = self._scraper_factory()
        try:
            await scraper.start()
            for index, point in enumerate(points, start=1):
                point_results: list[dict[str, Any]] = []
                try:
                    if provider_mode == "uule":
                        point_results = await self._discover_geo_grid_point_results_uule(
                            scraper=scraper,
                            keyword=keyword,
                            point=point,
                            top_n=top_n,
                            radius_m=configured_uule_radius_m,
                            throttle_ms=configured_throttle_ms,
                        )
                    else:
                        point_results = await self._discover_geo_grid_point_results(
                            scraper=scraper,
                            keyword=keyword,
                            point=point,
                            top_n=top_n,
                        )
                    inserted = await self._geo_grid_result_repository.replace_point_results(
                        geo_grid_run_id=geo_grid_run_id,
                        city_slug=str(run.get("city_slug") or ""),
                        keyword=keyword,
                        point=point,
                        results=point_results,
                    )
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
                    else:
                        if best_rank <= 3:
                            metrics["points_top3"] = int(metrics.get("points_top3") or 0) + 1
                        if best_rank <= 10:
                            metrics["points_top10"] = int(metrics.get("points_top10") or 0) + 1
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

                metrics["points_completed"] = index
                completed_units = min(total_units, index * top_n)
                points_completed = max(1, int(metrics.get("points_completed") or 0))
                metrics["share_top3"] = round(float(metrics.get("points_top3") or 0) / points_completed, 4)
                metrics["share_top10"] = round(float(metrics.get("points_top10") or 0) / points_completed, 4)
                metrics["share_not_found"] = round(float(metrics.get("points_not_found") or 0) / points_completed, 4)
                await self._geo_grid_run_repository.update_progress(
                    geo_grid_run_id=geo_grid_run_id,
                    completed_points=index,
                    completed_units=completed_units,
                    metrics=metrics,
                )

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

            final_status = "partial" if failures else "completed"
            final_run = await self._geo_grid_run_repository.finalize(
                geo_grid_run_id=geo_grid_run_id,
                status=final_status,
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=f"{len(failures)} puntos fallaron." if failures else None,
            )
            result = {
                "geo_grid_run_id": geo_grid_run_id,
                "job_id": str(job_id) if job_id is not None else None,
                "keyword": keyword,
                "city": run.get("city"),
                "city_slug": run.get("city_slug"),
                "top_n": top_n,
                "point_count": total_points,
                "status": final_status,
                "metrics": metrics,
                "run": final_run,
            }
            await self._record_event(event_type="geo_grid_study_processed", data=result)
            return self._sanitize_payload(result)
        except Exception as exc:
            await self._geo_grid_run_repository.finalize(
                geo_grid_run_id=geo_grid_run_id,
                status="failed",
                metrics={**metrics, "failures": failures[:25]},
                failure_reason=str(exc),
            )
            raise
        finally:
            await scraper.close()
