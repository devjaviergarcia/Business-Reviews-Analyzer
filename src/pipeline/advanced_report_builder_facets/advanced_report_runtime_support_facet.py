from __future__ import annotations

from typing import Any

from src.pipeline.advanced_report_llm_runtime import (
    extract_json_object,
    extract_llm_text,
    llm_generate_text,
    sanitize_llm_text,
)
from src.pipeline.advanced_report_metrics_runtime import (
    average_dimension,
    clamp,
    clamp01,
    compute_reputation_score,
    kmeans,
    label_customer_cluster,
    linear_slope,
    negative_ratio,
    resolve_dominant_problem,
    safe_float,
    safe_int,
    safe_rating,
    theme_scores,
    upper_ratio,
    zscore,
)
from src.pipeline.advanced_report_text_runtime import (
    compress_text,
    count_keyword_hits,
    extract_top_keywords,
    friendly_problem_label,
    human_label_problem,
    infer_action_tool,
    infer_action_type,
    normalize_action_type,
    normalize_text,
    plainify_business_text,
    severity_label,
)


class AdvancedReportRuntimeSupportFacet:
    def _llm_generate_text(self, prompt: str) -> tuple[str, str | None]:
        return llm_generate_text(
            prompt=prompt,
            client=self.client,
            primary_model_name=self.model_name,
            fallback_models=self.fallback_models,
            genai_errors_module=getattr(self, "_genai_errors", None),
            extract_llm_text=self._extract_llm_text,
        )

    def _extract_json_object(self, text: str) -> str:
        return extract_json_object(text)

    def _extract_llm_text(self, response: object) -> str:
        return extract_llm_text(response)

    def _fallback_clustering_text(
        self,
        *,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> str:
        return self._section_narrative_builder.fallback_clustering_text(
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
        )

    def _sanitize_llm_text(self, text: str) -> str:
        return sanitize_llm_text(text)

    def _normalize_action_type(self, value: str) -> str:
        return normalize_action_type(value, normalize_text=self._normalize_text)

    def _infer_action_type(self, text: str) -> str:
        return infer_action_type(text, normalize_text=self._normalize_text)

    def _infer_action_tool(self, text: str) -> str:
        return infer_action_tool(text, normalize_text=self._normalize_text)

    def _human_label_problem(self, label: str) -> str:
        return human_label_problem(label, plainify_business_text=self._plainify_business_text)

    def _severity_label(self, value: float) -> str:
        return severity_label(value)

    def _plainify_business_text(self, text: str) -> str:
        return plainify_business_text(text)

    def _compute_reputation_score(
        self,
        *,
        avg_rating: float,
        response_rate: float,
        negative_ratio: float,
        sentiment_avg: float,
        tranquility_avg: float,
    ) -> float:
        return compute_reputation_score(
            avg_rating=avg_rating,
            response_rate=response_rate,
            negative_ratio=negative_ratio,
            sentiment_avg=sentiment_avg,
            tranquility_avg=tranquility_avg,
            clamp=self._clamp,
            clamp01=self._clamp01,
        )

    def _negative_ratio(self, *, review_metrics: list[dict[str, Any]]) -> float:
        return negative_ratio(review_metrics=review_metrics, safe_float=self._safe_float)

    def _average_dimension(self, review_metrics: list[dict[str, Any]], key: str) -> float:
        return average_dimension(review_metrics, key, safe_float=self._safe_float)

    def _label_customer_cluster(
        self,
        centroid: list[float],
        *,
        cluster_id: int | None = None,
        used_labels: set[str] | None = None,
    ) -> tuple[str, str]:
        return label_customer_cluster(centroid, cluster_id=cluster_id, used_labels=used_labels)

    def _theme_scores(self, text_norm: str) -> dict[str, int]:
        return theme_scores(
            text_norm,
            theme_keywords=self._THEME_KEYWORDS,
            count_keyword_hits=self._count_keyword_hits,
        )

    def _resolve_dominant_problem(
        self,
        *,
        rating: float,
        text_norm: str,
        sentiment: float,
        theme_scores: dict[str, int],
    ) -> str:
        return resolve_dominant_problem(
            rating=rating,
            text_norm=text_norm,
            sentiment=sentiment,
            theme_scores=theme_scores,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
            positive_comment_problem=self._POSITIVE_COMMENT_PROBLEM,
            negative_comment_problem=self._NEGATIVE_COMMENT_PROBLEM,
            no_comment_high_problem=self._NO_COMMENT_HIGH_PROBLEM,
            no_comment_medium_problem=self._NO_COMMENT_MEDIUM_PROBLEM,
            no_comment_low_problem=self._NO_COMMENT_LOW_PROBLEM,
        )

    def _extract_top_keywords(self, *, items: list[dict[str, Any]], limit: int = 8) -> list[str]:
        return extract_top_keywords(
            items=items,
            limit=limit,
            stopwords=self._STOPWORDS,
            normalize_text=self._normalize_text,
        )

    def _compress_text(self, text: str, *, max_chars: int) -> str:
        return compress_text(text, max_chars=max_chars)

    def _friendly_problem_label(self, value: str) -> str:
        return friendly_problem_label(
            value,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
            positive_comment_problem=self._POSITIVE_COMMENT_PROBLEM,
            negative_comment_problem=self._NEGATIVE_COMMENT_PROBLEM,
            no_comment_high_problem=self._NO_COMMENT_HIGH_PROBLEM,
            no_comment_medium_problem=self._NO_COMMENT_MEDIUM_PROBLEM,
            no_comment_low_problem=self._NO_COMMENT_LOW_PROBLEM,
        )

    def _normalize_text(self, value: str) -> str:
        return normalize_text(value)

    def _count_keyword_hits(self, text: str, keywords: tuple[str, ...]) -> int:
        return count_keyword_hits(text, keywords, normalize_text=self._normalize_text)

    def _safe_rating(self, value: Any) -> float:
        return safe_rating(value, clamp=self._clamp)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        return safe_float(value, default=default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        return safe_int(value, default=default)

    def _upper_ratio(self, text: str) -> float:
        return upper_ratio(text)

    def _clamp(self, value: float, min_value: float, max_value: float) -> float:
        return clamp(value, min_value, max_value)

    def _clamp01(self, value: float) -> float:
        return clamp01(value)

    def _linear_slope(self, values: list[float]) -> float:
        return linear_slope(values)

    def _kmeans(self, *, features: list[list[float]], k: int, max_iter: int) -> tuple[list[int], list[list[float]]]:
        return kmeans(features=features, k=k, max_iter=max_iter)

    def _zscore(self, features: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
        return zscore(features)
