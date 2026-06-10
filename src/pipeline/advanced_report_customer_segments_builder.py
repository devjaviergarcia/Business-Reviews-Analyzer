from __future__ import annotations

import statistics
from typing import Any, Callable


class AdvancedReportCustomerSegmentsBuilder:
    def __init__(
        self,
        *,
        safe_float: Callable[[Any, float], float],
        safe_int: Callable[[Any, int], int],
        kmeans: Callable[..., tuple[list[int], list[list[float]]]],
        label_customer_cluster: Callable[..., tuple[str, str]],
    ) -> None:
        self._safe_float = safe_float
        self._safe_int = safe_int
        self._kmeans = kmeans
        self._label_customer_cluster = label_customer_cluster

    def build_customer_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        profiles: dict[str, dict[str, Any]] = {}
        for item in review_metrics:
            key = str(item.get("customer_key") or "").strip() or f"anon_{item.get('index', 0)}"
            profile = profiles.get(key)
            if profile is None:
                profile = {
                    "customer_id": key,
                    "display_name": str(item.get("author_name", "Cliente anónimo") or "Cliente anónimo"),
                    "review_count": 0,
                    "ratings": [],
                    "sentiment": [],
                    "expectation_gap": [],
                    "satisfaction": [],
                    "tranquility_aggressiveness": [],
                    "improvement_intent": [],
                }
                profiles[key] = profile

            dims = item.get("dimensions") or {}
            profile["review_count"] += 1
            profile["ratings"].append(self._safe_float(item.get("rating")))
            profile["sentiment"].append(self._safe_float(dims.get("sentiment")))
            profile["expectation_gap"].append(self._safe_float(dims.get("expectation_gap")))
            profile["satisfaction"].append(self._safe_float(dims.get("satisfaction")))
            profile["tranquility_aggressiveness"].append(self._safe_float(dims.get("tranquility_aggressiveness")))
            profile["improvement_intent"].append(self._safe_float(dims.get("improvement_intent")))

        customers = []
        for profile in profiles.values():
            customer = {
                "customer_id": profile["customer_id"],
                "display_name": profile["display_name"],
                "review_count": int(profile["review_count"]),
                "avg_rating": round(statistics.mean(profile["ratings"]) if profile["ratings"] else 0.0, 4),
                "sentiment": round(statistics.mean(profile["sentiment"]) if profile["sentiment"] else 0.0, 4),
                "expectation_gap": round(
                    statistics.mean(profile["expectation_gap"]) if profile["expectation_gap"] else 0.0,
                    4,
                ),
                "satisfaction": round(
                    statistics.mean(profile["satisfaction"]) if profile["satisfaction"] else 0.0,
                    4,
                ),
                "tranquility_aggressiveness": round(
                    statistics.mean(profile["tranquility_aggressiveness"])
                    if profile["tranquility_aggressiveness"]
                    else 0.0,
                    4,
                ),
                "improvement_intent": round(
                    statistics.mean(profile["improvement_intent"]) if profile["improvement_intent"] else 0.0,
                    4,
                ),
            }
            customers.append(customer)

        if not customers:
            return {
                "cluster_count": 0,
                "clusters": [],
                "scatter": {"type": "scatter_d", "axes": {}, "bubbles": [], "circles": [], "points": []},
                "bar_chart": {"type": "bar_chart_c", "rows": []},
                "scatter_points_annex": [],
            }

        features = []
        for customer in customers:
            features.append(
                [
                    float(customer["sentiment"]),
                    float(customer["expectation_gap"]),
                    float(customer["satisfaction"]),
                    float(customer["tranquility_aggressiveness"]),
                    float(customer["improvement_intent"]),
                    float((customer["avg_rating"] - 3.0) / 2.0),
                ]
            )

        k = 1
        total_customers = len(customers)
        if total_customers >= 20:
            k = 4
        elif total_customers >= 9:
            k = 3
        elif total_customers >= 4:
            k = 2

        labels, centroids = self._kmeans(features=features, k=k, max_iter=30)

        expectation_values = [float(customer["expectation_gap"]) for customer in customers]
        satisfaction_values = [float(customer["satisfaction"]) for customer in customers]
        min_expectation = min(expectation_values) if expectation_values else 0.0
        max_expectation = max(expectation_values) if expectation_values else 1.0
        min_satisfaction = min(satisfaction_values) if satisfaction_values else 0.0
        max_satisfaction = max(satisfaction_values) if satisfaction_values else 1.0
        expectation_range = max(max_expectation - min_expectation, 0.01)
        satisfaction_range = max(max_satisfaction - min_satisfaction, 0.01)

        for index, customer in enumerate(customers):
            customer["cluster_id"] = int(labels[index])
            customer["x"] = round(
                ((float(customer["expectation_gap"]) - min_expectation) / expectation_range) * 100.0,
                3,
            )
            customer["y"] = round(
                ((float(customer["satisfaction"]) - min_satisfaction) / satisfaction_range) * 100.0,
                3,
            )
            customer["size"] = max(1.0, float(customer["review_count"]))

        clusters_map: dict[int, dict[str, Any]] = {}
        for customer in customers:
            cluster_id = int(customer["cluster_id"])
            cluster = clusters_map.get(cluster_id)
            if cluster is None:
                cluster = {"cluster_id": cluster_id, "customers": []}
                clusters_map[cluster_id] = cluster
            cluster["customers"].append(customer)

        clusters = []
        used_labels: set[str] = set()
        for cluster_id in sorted(clusters_map.keys()):
            customers_in_cluster = clusters_map[cluster_id]["customers"]
            centroid = centroids[cluster_id]
            label, description = self._label_customer_cluster(
                centroid,
                cluster_id=cluster_id,
                used_labels=used_labels,
            )
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "label": label,
                    "description": description,
                    "count_customers": len(customers_in_cluster),
                    "count_reviews": int(sum(float(item["review_count"]) for item in customers_in_cluster)),
                    "centroid": {
                        "sentiment": round(float(centroid[0]), 4),
                        "expectation_gap": round(float(centroid[1]), 4),
                        "satisfaction": round(float(centroid[2]), 4),
                        "tranquility_aggressiveness": round(float(centroid[3]), 4),
                        "improvement_intent": round(float(centroid[4]), 4),
                    },
                    "sample_customers": [
                        {
                            "display_name": item["display_name"],
                            "review_count": item["review_count"],
                            "avg_rating": item["avg_rating"],
                        }
                        for item in sorted(
                            customers_in_cluster,
                            key=lambda item: (float(item["review_count"]), float(item["avg_rating"])),
                            reverse=True,
                        )[:5]
                    ],
                }
            )

        return {
            "cluster_count": len(clusters),
            "clusters": clusters,
            "scatter": self.build_scatter_vista_d(clusters=clusters),
            "bar_chart": self.build_bar_chart_vista_c(clusters=clusters),
            "scatter_points_annex": [
                {
                    "customer_id": item["customer_id"],
                    "cluster_id": item["cluster_id"],
                    "x": item["x"],
                    "y": item["y"],
                    "review_count": item["review_count"],
                    "avg_rating": item["avg_rating"],
                }
                for item in customers
            ],
        }

    def build_scatter_vista_d(self, *, clusters: list[dict[str, Any]]) -> dict[str, Any]:
        if not clusters:
            return {"type": "scatter_d", "bubbles": [], "axes": {}, "circles": [], "points": []}

        colors = ["#0A7567", "#12B08A", "#D4950A", "#C23B18", "#64748B"]
        ranked = sorted(clusters, key=lambda c: self._safe_int(c.get("count_reviews")), reverse=True)
        total_reviews = max(sum(self._safe_int(c.get("count_reviews")) for c in ranked), 1)

        bubbles: list[dict[str, Any]] = []
        circles_compat: list[dict[str, Any]] = []
        for idx, cluster in enumerate(ranked):
            centroid = cluster.get("centroid") or {}
            count = self._safe_int(cluster.get("count_reviews"))
            weight = count / total_reviews
            gap_raw = self._safe_float(centroid.get("expectation_gap"))
            sat_raw = self._safe_float(centroid.get("satisfaction"))
            zone = self.assign_scatter_zone(expectation_gap=gap_raw, satisfaction=sat_raw)
            color = colors[idx % len(colors)]
            sentiment = self._safe_float(centroid.get("sentiment"))
            satisfaction_pct = round(sat_raw * 100.0, 1)

            bubbles.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "label": cluster.get("label", ""),
                    "x": 0.0,
                    "y": 0.0,
                    "radius": round(weight, 4),
                    "color": color,
                    "count_reviews": count,
                    "weight_pct": round(weight * 100.0, 1),
                    "satisfaction_pct": satisfaction_pct,
                    "sentiment": round(sentiment, 2),
                    "expectation_gap": round(gap_raw, 3),
                    "satisfaction": round(sat_raw, 3),
                    "zone": zone,
                }
            )
            circles_compat.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "label": cluster.get("label", ""),
                    "center": {"x": 0.0, "y": 0.0},
                    "radius": 0.0,
                    "count": count,
                }
            )

        return {
            "type": "scatter_d",
            "axes": {
                "x_label": "Brecha de expectativa",
                "y_label": "Satisfacción",
                "x_low": "Expectativas cumplidas",
                "x_high": "Expectativas no cumplidas",
                "y_low": "Baja satisfacción",
                "y_high": "Alta satisfacción",
            },
            "quadrant_labels": {
                "top_left": "Satisfechos · Expectativas cumplidas",
                "top_right": "Satisfechos · Expectativas no cumplidas",
                "bottom_left": "Insatisfechos · Expectativas cumplidas",
                "bottom_right": "Insatisfechos · Expectativas no cumplidas",
            },
            "bubbles": bubbles,
            "circles": circles_compat,
            "points": [],
        }

    def assign_scatter_zone(self, *, expectation_gap: float, satisfaction: float) -> str:
        sat_high = satisfaction >= 0.55
        gap_low = expectation_gap <= 0.20
        if sat_high and gap_low:
            return "top_left"
        if sat_high and not gap_low:
            return "top_right"
        if not sat_high and gap_low:
            return "bottom_left"
        return "bottom_right"

    def build_bar_chart_vista_c(self, *, clusters: list[dict[str, Any]]) -> dict[str, Any]:
        if not clusters:
            return {"type": "bar_chart_c", "rows": []}

        colors = ["#0A7567", "#12B08A", "#D4950A", "#C23B18", "#64748B"]
        ranked = sorted(clusters, key=lambda c: self._safe_int(c.get("count_reviews")), reverse=True)
        total_reviews = max(sum(self._safe_int(c.get("count_reviews")) for c in ranked), 1)

        rows: list[dict[str, Any]] = []
        for idx, cluster in enumerate(ranked):
            centroid = cluster.get("centroid") or {}
            count = self._safe_int(cluster.get("count_reviews"))
            weight = count / total_reviews
            sentiment = self._safe_float(centroid.get("sentiment"))
            satisfaction = self._safe_float(centroid.get("satisfaction"))

            if sentiment >= 0.4:
                sentiment_label = "Muy positivo"
            elif sentiment >= 0.1:
                sentiment_label = "Positivo"
            elif sentiment >= -0.1:
                sentiment_label = "Neutro"
            elif sentiment >= -0.4:
                sentiment_label = "Negativo"
            else:
                sentiment_label = "Muy negativo"

            if satisfaction >= 0.75:
                satisfaction_label = "Alta"
            elif satisfaction >= 0.5:
                satisfaction_label = "Media"
            else:
                satisfaction_label = "Baja"

            rows.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "label": cluster.get("label", ""),
                    "color": colors[idx % len(colors)],
                    "count_reviews": count,
                    "weight_pct": round(weight * 100.0, 1),
                    "bar_width_pct": round(weight * 100.0, 1),
                    "sentiment": round(sentiment, 2),
                    "sentiment_label": sentiment_label,
                    "satisfaction_pct": round(satisfaction * 100.0, 1),
                    "satisfaction_label": satisfaction_label,
                }
            )

        return {
            "type": "bar_chart_c",
            "rows": rows,
            "columns": ["Segmento", "Peso", "Satisfacción", "Sentimiento"],
        }
