from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from src.pipeline.preprocessor import ReviewPreprocessor


def build_source_reports(
    *,
    reviews: list[dict[str, Any]],
    source_preprocessor: ReviewPreprocessor,
    score_review_dimensions: Callable[..., dict[str, Any]],
    build_customer_clusters: Callable[..., dict[str, Any]],
    build_problem_clusters: Callable[..., dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not isinstance(reviews, list) or not reviews:
        return {}

    reviews_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for review in reviews:
        if not isinstance(review, dict):
            continue
        source = str(review.get("source") or "unknown").strip().lower()
        reviews_by_source[source].append(review)

    source_reports: dict[str, dict[str, Any]] = {}
    for source in ("google_maps", "tripadvisor"):
        source_reviews = reviews_by_source.get(source)
        if not source_reviews:
            continue
        processed_reviews = source_preprocessor.process(source_reviews)
        source_stats = source_preprocessor.compute_stats(processed_reviews)
        source_review_metrics = [
            score_review_dimensions(index=idx, review=review)
            for idx, review in enumerate(processed_reviews)
        ]
        source_reports[source] = {
            "stats": source_stats,
            "review_metrics": source_review_metrics,
            "customer_clusters": build_customer_clusters(review_metrics=source_review_metrics),
            "problem_clusters": build_problem_clusters(review_metrics=source_review_metrics),
            "review_count": len(source_review_metrics),
        }
    return source_reports
