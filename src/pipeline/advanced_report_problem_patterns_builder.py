from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any, Callable


class AdvancedReportProblemPatternsBuilder:
    def __init__(
        self,
        *,
        safe_float: Callable[[Any, float], float],
        clamp01: Callable[[float], float],
        extract_top_keywords: Callable[..., list[str]],
        generic_comment_problem: str,
    ) -> None:
        self._safe_float = safe_float
        self._clamp01 = clamp01
        self._extract_top_keywords = extract_top_keywords
        self._generic_comment_problem = generic_comment_problem

    def build_problem_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        relevant_reviews = []
        for review in review_metrics:
            rating = self._safe_float(review.get("rating"))
            sentiment = self._safe_float((review.get("dimensions") or {}).get("sentiment"))
            if rating <= 3.0 or sentiment < 0:
                relevant_reviews.append(review)

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for review in relevant_reviews:
            groups[
                str(review.get("dominant_problem", self._generic_comment_problem) or self._generic_comment_problem)
            ].append(review)

        clusters = []
        circles = []
        total = max(1, len(relevant_reviews))
        for cluster_index, (problem, items) in enumerate(
            sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)
        ):
            count = len(items)
            avg_rating = statistics.mean(self._safe_float(item.get("rating")) for item in items) if items else 0.0
            avg_sentiment = (
                statistics.mean(self._safe_float((item.get("dimensions") or {}).get("sentiment")) for item in items)
                if items
                else 0.0
            )
            avg_expectation = (
                statistics.mean(
                    self._safe_float((item.get("dimensions") or {}).get("expectation_gap")) for item in items
                )
                if items
                else 0.0
            )
            share = count / total
            severity = self._clamp01(((5.0 - avg_rating) / 4.0) * 0.7 + max(0.0, -avg_sentiment) * 0.3)
            keywords = self._extract_top_keywords(items=items, limit=8)

            x = round(share * 100.0, 3)
            y = round(severity * 100.0, 3)
            radius = round(max(4.0, math.sqrt(count) * 4.0), 3)
            circles.append(
                {
                    "cluster_id": cluster_index,
                    "label": problem,
                    "center": {"x": x, "y": y},
                    "radius": radius,
                    "count": count,
                }
            )

            sample_quotes = []
            for item in items:
                text = str(item.get("text", "") or "").strip()
                if not text:
                    continue
                sample_quotes.append(
                    {
                        "author_name": item.get("author_name"),
                        "rating": item.get("rating"),
                        "quote": text[:280],
                    }
                )
            sample_quotes = sample_quotes[:5]

            clusters.append(
                {
                    "cluster_id": cluster_index,
                    "problem": problem,
                    "count": count,
                    "share": round(share, 4),
                    "avg_rating": round(avg_rating, 4),
                    "avg_sentiment": round(avg_sentiment, 4),
                    "avg_expectation_gap": round(avg_expectation, 4),
                    "severity": round(severity, 4),
                    "keywords": keywords,
                    "sample_quotes": sample_quotes,
                }
            )

        return {
            "cluster_count": len(clusters),
            "clusters": clusters,
            "scatter": {
                "axes": {
                    "x": "frequency_share",
                    "x_label": "Frecuencia del problema",
                    "y": "severity",
                    "y_label": "Severidad",
                    "size": "review_count",
                },
                "circles": circles,
            },
        }
