from __future__ import annotations

from typing import Any


class GeoGridStatsBuilder:
    def build_geo_grid_stats(
        self,
        *,
        run: dict[str, Any],
        results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        point_count = int(run.get("point_count") or 0)
        top_n = int(run.get("top_n") or 10)
        run_metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
        provider_mode = str(
            run.get("provider_mode") or run_metrics.get("provider_mode") or "maps_live"
        ).strip().lower()
        points: dict[int, dict[str, Any]] = {}
        businesses: dict[str, dict[str, Any]] = {}

        for item in results:
            if not isinstance(item, dict):
                continue
            point_order = int(item.get("point_order") or 0)
            rank = int(item.get("rank") or 0)
            if point_order < 1 or rank < 1:
                continue

            point_payload = points.setdefault(
                point_order,
                {
                    "point_order": point_order,
                    "point_label": item.get("point_label"),
                    "grid_row": item.get("grid_row"),
                    "grid_col": item.get("grid_col"),
                    "lat": item.get("lat"),
                    "lng": item.get("lng"),
                    "top_results": [],
                },
            )
            point_payload["top_results"].append(
                {
                    "rank": rank,
                    "business_key": item.get("business_key"),
                    "business_name": item.get("business_name"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "maps_url": item.get("maps_url"),
                }
            )

            business_key = str(item.get("business_key") or item.get("business_name_normalized") or "").strip()
            if not business_key:
                continue

            business = businesses.setdefault(
                business_key,
                {
                    "business_key": business_key,
                    "business_name": item.get("business_name"),
                    "maps_url": item.get("maps_url"),
                    "maps_url_canonical": item.get("maps_url_canonical"),
                    "rating": item.get("rating"),
                    "review_count": item.get("review_count"),
                    "appearances": 0,
                    "ranks": [],
                    "points": [],
                    "top_1_count": 0,
                    "top_3_count": 0,
                    "top_5_count": 0,
                    "top_10_count": 0,
                    "top_20_count": 0,
                },
            )
            business["appearances"] += 1
            business["ranks"].append(rank)
            business["points"].append(
                {
                    "point_order": point_order,
                    "point_label": item.get("point_label"),
                    "lat": item.get("lat"),
                    "lng": item.get("lng"),
                    "rank": rank,
                }
            )
            if rank == 1:
                business["top_1_count"] += 1
            if rank <= 3:
                business["top_3_count"] += 1
            if rank <= 5:
                business["top_5_count"] += 1
            if rank <= 10:
                business["top_10_count"] += 1
            if rank <= 20:
                business["top_20_count"] += 1

        if point_count <= 0:
            point_count = len(points)

        business_rows: list[dict[str, Any]] = []
        for business in businesses.values():
            ranks = [int(rank) for rank in business.pop("ranks", [])]
            appearances = int(business.get("appearances") or 0)
            avg_rank = round(sum(ranks) / len(ranks), 2) if ranks else None
            best_rank = min(ranks) if ranks else None
            worst_rank = max(ranks) if ranks else None
            rank_stddev = self.population_stddev(ranks)
            coverage = round((appearances / point_count) * 100, 2) if point_count else 0.0
            missing_points = max(0, point_count - appearances)
            business_rows.append(
                {
                    **business,
                    "coverage_percent": coverage,
                    "missing_points": missing_points,
                    "avg_rank": avg_rank,
                    "best_rank": best_rank,
                    "worst_rank": worst_rank,
                    "rank_stddev": rank_stddev,
                }
            )

        business_rows.sort(
            key=lambda item: (
                -int(item.get("appearances") or 0),
                float(item.get("avg_rank") or 9999),
                str(item.get("business_name") or ""),
            )
        )
        weakest_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("avg_rank") or 0),
                int(item.get("appearances") or 0),
            ),
            reverse=True,
        )
        consistent_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("rank_stddev") or 9999),
                -int(item.get("appearances") or 0),
                float(item.get("avg_rank") or 9999),
            ),
        )
        dispersed_rows = sorted(
            business_rows,
            key=lambda item: (
                float(item.get("rank_stddev") or 0),
                int(item.get("appearances") or 0),
            ),
            reverse=True,
        )
        point_rows = sorted(points.values(), key=lambda item: int(item.get("point_order") or 0))
        for point in point_rows:
            point["top_results"] = sorted(
                point.get("top_results") or [],
                key=lambda item: int(item.get("rank") or 9999),
            )

        return {
            "geo_grid_run_id": run.get("geo_grid_run_id"),
            "summary": {
                "keyword": run.get("keyword"),
                "city": run.get("city"),
                "city_slug": run.get("city_slug"),
                "provider_mode": provider_mode,
                "point_count": point_count,
                "top_n": top_n,
                "total_results": len(results),
                "unique_businesses": len(business_rows),
                "visibility_score": run_metrics.get("visibility_score"),
                "share_top3": run_metrics.get("share_top3"),
                "share_top10": run_metrics.get("share_top10"),
                "share_not_found": run_metrics.get("share_not_found"),
            },
            "businesses": business_rows,
            "leaders": business_rows[:10],
            "weakest": weakest_rows[:10],
            "most_consistent": consistent_rows[:10],
            "most_dispersed": dispersed_rows[:10],
            "points": point_rows,
            "run_metrics": run_metrics,
        }

    def population_stddev(self, values: list[int]) -> float:
        if len(values) <= 1:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((float(value) - mean) ** 2 for value in values) / len(values)
        return round(variance ** 0.5, 2)
