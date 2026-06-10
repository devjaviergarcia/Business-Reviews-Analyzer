from __future__ import annotations

from typing import Any, Callable

from src.pipeline.advanced_report_source_comparison_builder import (
    build_llm_source_comparison,
)
from src.pipeline.advanced_report_source_narrative_builder import (
    build_llm_source_narrative,
)
from src.pipeline.advanced_report_source_report_builder import build_source_reports
from src.pipeline.preprocessor import ReviewPreprocessor


class AdvancedReportSourceInsightsBuilder:
    def __init__(
        self,
        *,
        source_preprocessor: ReviewPreprocessor,
        score_review_dimensions: Callable[..., dict[str, Any]],
        build_customer_clusters: Callable[..., dict[str, Any]],
        build_problem_clusters: Callable[..., dict[str, Any]],
        can_use_llm: Callable[[], bool],
        llm_generate_text: Callable[[str], tuple[str, str | None]],
        extract_json_object: Callable[[str], str],
        summarize_problem_clusters: Callable[..., list[dict[str, Any]]],
        human_label_problem: Callable[[str], str],
        negative_ratio: Callable[..., float],
        average_dimension: Callable[[list[dict[str, Any]], str], float],
        safe_float: Callable[[Any, float], float],
        safe_int: Callable[[Any, int], int],
        sanitize_llm_text: Callable[[str], str],
        plainify_business_text: Callable[[str], str],
    ) -> None:
        self._source_preprocessor = source_preprocessor
        self._score_review_dimensions = score_review_dimensions
        self._build_customer_clusters = build_customer_clusters
        self._build_problem_clusters = build_problem_clusters
        self._can_use_llm = can_use_llm
        self._llm_generate_text = llm_generate_text
        self._extract_json_object = extract_json_object
        self._summarize_problem_clusters = summarize_problem_clusters
        self._human_label_problem = human_label_problem
        self._negative_ratio = negative_ratio
        self._average_dimension = average_dimension
        self._safe_float = safe_float
        self._safe_int = safe_int
        self._sanitize_llm_text = sanitize_llm_text
        self._plainify_business_text = plainify_business_text

    def build_source_reports(self, *, reviews: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return build_source_reports(
            reviews=reviews,
            source_preprocessor=self._source_preprocessor,
            score_review_dimensions=self._score_review_dimensions,
            build_customer_clusters=self._build_customer_clusters,
            build_problem_clusters=self._build_problem_clusters,
        )

    async def build_source_analysis_bundle(
        self,
        *,
        source_reports: dict[str, dict[str, Any]],
        business_name: str,
        business_context: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
        source_analysis: dict[str, dict[str, Any]] = {}
        for source_name, source_data in source_reports.items():
            if not isinstance(source_data, dict):
                continue
            source_narrative = await self.build_llm_source_narrative(
                source=source_name,
                business_name=business_name,
                business_context=business_context,
                stats=source_data.get("stats") if isinstance(source_data.get("stats"), dict) else {},
                customer_clusters=(
                    source_data.get("customer_clusters")
                    if isinstance(source_data.get("customer_clusters"), dict)
                    else {}
                ),
                problem_clusters=(
                    source_data.get("problem_clusters")
                    if isinstance(source_data.get("problem_clusters"), dict)
                    else {}
                ),
            )
            source_analysis[source_name] = {
                **source_data,
                "narrativa": source_narrative,
            }

        source_comparison: dict[str, Any] | None = None
        if "google_maps" in source_reports and "tripadvisor" in source_reports:
            source_comparison = await self.build_llm_source_comparison(
                business_name=business_name,
                google_data=source_reports["google_maps"],
                tripadvisor_data=source_reports["tripadvisor"],
            )
        return source_analysis, source_comparison

    async def build_llm_source_narrative(
        self,
        *,
        source: str,
        business_name: str,
        business_context: dict[str, Any],
        stats: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return await build_llm_source_narrative(
            source=source,
            business_name=business_name,
            business_context=business_context,
            stats=stats,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            summarize_problem_clusters=self._summarize_problem_clusters,
            human_label_problem=self._human_label_problem,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
        )

    async def build_llm_source_comparison(
        self,
        *,
        business_name: str,
        google_data: dict[str, Any],
        tripadvisor_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await build_llm_source_comparison(
            business_name=business_name,
            google_data=google_data,
            tripadvisor_data=tripadvisor_data,
            summarize_problem_clusters=self._summarize_problem_clusters,
            human_label_problem=self._human_label_problem,
            negative_ratio=self._negative_ratio,
            average_dimension=self._average_dimension,
            safe_float=self._safe_float,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
        )
