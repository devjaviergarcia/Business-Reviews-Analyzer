from __future__ import annotations

from collections import Counter
from typing import Any, Callable


class AdvancedReportAnnexBuilder:
    def __init__(
        self,
        *,
        average_dimension: Callable[[list[dict[str, Any]], str], float],
        safe_float: Callable[[Any, float], float],
        safe_int: Callable[[Any, int], int],
        generic_comment_problem: str,
    ) -> None:
        self._average_dimension = average_dimension
        self._safe_float = safe_float
        self._safe_int = safe_int
        self._generic_comment_problem = generic_comment_problem

    def build_full_data_annex(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        avg_dims = {
            "sentiment": round(self._average_dimension(review_metrics, "sentiment"), 4),
            "expectation_gap": round(self._average_dimension(review_metrics, "expectation_gap"), 4),
            "satisfaction": round(self._average_dimension(review_metrics, "satisfaction"), 4),
            "tranquility_aggressiveness": round(
                self._average_dimension(review_metrics, "tranquility_aggressiveness"), 4
            ),
            "improvement_intent": round(self._average_dimension(review_metrics, "improvement_intent"), 4),
        }
        by_source = Counter(str(item.get("source", "unknown") or "unknown") for item in review_metrics)
        by_problem = Counter(
            str(item.get("dominant_problem", self._generic_comment_problem) or self._generic_comment_problem)
            for item in review_metrics
        )

        customer_points = self.extract_customer_scatter_points(customer_clusters=customer_clusters)
        point_cluster_map, cluster_label_map = self.build_annex_cluster_lookup(
            customer_clusters=customer_clusters,
            customer_points=customer_points,
        )
        compact_points = self.build_annex_compact_points(customer_points=customer_points)
        review_rows = self.build_annex_review_rows(
            review_metrics=review_metrics,
            point_cluster_map=point_cluster_map,
            cluster_label_map=cluster_label_map,
        )
        dataset_summary = self.build_annex_dataset_summary(
            stats=stats,
            review_metrics=review_metrics,
            by_source=by_source,
            by_problem=by_problem,
            avg_dims=avg_dims,
        )

        return {
            "dataset_summary": dataset_summary,
            "stats_snapshot": stats,
            "analysis_topics": {
                "overall_sentiment": analysis_payload.get("overall_sentiment"),
                "main_topics": analysis_payload.get("main_topics"),
                "strengths": analysis_payload.get("strengths"),
                "weaknesses": analysis_payload.get("weaknesses"),
            },
            "dimension_averages": avg_dims,
            "counts": {
                "total_reviews": len(review_metrics),
                "by_source": dict(by_source),
                "by_problem": dict(by_problem),
            },
            "review_rows": review_rows,
            "cluster_assignments_compact": compact_points,
            "problem_clusters_summary": (
                problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
            ),
        }

    def extract_customer_scatter_points(self, *, customer_clusters: dict[str, Any]) -> list[dict[str, Any]]:
        customer_points = (
            ((customer_clusters.get("scatter") or {}).get("points") if isinstance(customer_clusters, dict) else [])
            or []
        )
        return [item for item in customer_points if isinstance(item, dict)]

    def build_annex_cluster_lookup(
        self,
        *,
        customer_clusters: dict[str, Any],
        customer_points: list[dict[str, Any]],
    ) -> tuple[dict[str, int], dict[int, str]]:
        point_cluster_map: dict[str, int] = {}
        for point in customer_points:
            customer_id = str(point.get("customer_id", "") or "").strip()
            if not customer_id:
                continue
            try:
                point_cluster_map[customer_id] = int(point.get("cluster_id"))
            except (TypeError, ValueError):
                continue

        cluster_label_map: dict[int, str] = {}
        clusters_full = customer_clusters.get("clusters") if isinstance(customer_clusters.get("clusters"), list) else []
        for cluster in clusters_full:
            if not isinstance(cluster, dict):
                continue
            try:
                cluster_id = int(cluster.get("cluster_id"))
            except (TypeError, ValueError):
                continue
            label = str(cluster.get("label", "") or "").strip()
            if label:
                cluster_label_map[cluster_id] = label
        return point_cluster_map, cluster_label_map

    def build_annex_compact_points(self, *, customer_points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        compact_points: list[dict[str, Any]] = []
        for item in customer_points[:200]:
            compact_points.append(
                {
                    "customer_id": item.get("customer_id"),
                    "cluster_id": item.get("cluster_id"),
                    "x": item.get("x"),
                    "y": item.get("y"),
                    "review_count": item.get("review_count"),
                }
            )
        return compact_points

    def build_annex_review_rows(
        self,
        *,
        review_metrics: list[dict[str, Any]],
        point_cluster_map: dict[str, int],
        cluster_label_map: dict[int, str],
    ) -> list[dict[str, Any]]:
        review_rows: list[dict[str, Any]] = []
        for item in review_metrics:
            dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
            customer_key = str(item.get("customer_key", "") or "").strip()
            cluster_id = point_cluster_map.get(customer_key)
            cluster_label = cluster_label_map.get(cluster_id) if cluster_id is not None else None
            review_rows.append(
                {
                    "review_index": self._safe_int(item.get("index"), 0),
                    "customer_key": customer_key or None,
                    "cluster_id": cluster_id,
                    "cluster_label": cluster_label,
                    "source": str(item.get("source", "") or "").strip() or "unknown",
                    "author_name": str(item.get("author_name", "") or "").strip() or "Cliente anónimo",
                    "rating": round(self._safe_float(item.get("rating"), 0.0), 2),
                    "sentiment": round(self._safe_float(dims.get("sentiment"), 0.0), 4),
                    "expectation_gap": round(self._safe_float(dims.get("expectation_gap"), 0.0), 4),
                    "satisfaction": round(self._safe_float(dims.get("satisfaction"), 0.0), 4),
                    "tranquility_aggressiveness": round(
                        self._safe_float(dims.get("tranquility_aggressiveness"), 0.0), 4
                    ),
                    "improvement_intent": round(self._safe_float(dims.get("improvement_intent"), 0.0), 4),
                    "dominant_problem": (
                        str(item.get("dominant_problem", "") or "").strip() or self._generic_comment_problem
                    ),
                    "has_owner_reply": bool(item.get("has_owner_reply")),
                    "owner_reply_excerpt": str(item.get("owner_reply", "") or "").strip()[:280],
                    "review_excerpt": str(item.get("text", "") or "").strip()[:500],
                }
            )
        return review_rows

    def build_annex_dataset_summary(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        by_source: Counter[str],
        by_problem: Counter[str],
        avg_dims: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "total_reviews": len(review_metrics),
            "avg_rating": round(self._safe_float((stats or {}).get("avg_rating"), 0.0), 3),
            "response_rate": round(self._safe_float((stats or {}).get("response_rate"), 0.0), 4),
            "by_source": dict(by_source),
            "by_problem": dict(by_problem),
            "dimension_averages": avg_dims,
        }
