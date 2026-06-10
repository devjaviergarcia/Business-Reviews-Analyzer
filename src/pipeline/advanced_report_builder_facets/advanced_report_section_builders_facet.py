from __future__ import annotations

from collections import Counter
from typing import Any

from src.pipeline.advanced_report_business_story_builder import (
    build_business_context,
    build_executive_summary,
    score_label,
    summarize_customer_clusters,
    summarize_problem_clusters,
)
from src.pipeline.advanced_report_llm_runtime import extract_json_object
from src.pipeline.advanced_report_voice_quotes_builder import (
    build_strengths_weaknesses_payload,
    build_structured_strengths,
    build_voice_of_customer,
    infer_strength_concept,
)


class AdvancedReportSectionBuildersFacet:
    def _build_customer_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        return self._customer_segments_builder.build_customer_clusters(review_metrics=review_metrics)

    def _build_scatter_vista_d(
        self,
        *,
        clusters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._customer_segments_builder.build_scatter_vista_d(clusters=clusters)

    def _assign_scatter_zone(self, *, expectation_gap: float, satisfaction: float) -> str:
        return self._customer_segments_builder.assign_scatter_zone(
            expectation_gap=expectation_gap,
            satisfaction=satisfaction,
        )

    def _build_bar_chart_vista_c(
        self,
        *,
        clusters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._customer_segments_builder.build_bar_chart_vista_c(clusters=clusters)

    def _build_problem_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        return self._problem_patterns_builder.build_problem_clusters(review_metrics=review_metrics)

    def _build_voice_of_customer(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        return build_voice_of_customer(review_metrics=review_metrics, safe_float=self._safe_float)

    def _build_structured_strengths(
        self,
        *,
        voice_of_customer: dict[str, Any],
        limit: int = 3,
    ) -> list[dict[str, str]]:
        return build_structured_strengths(
            voice_of_customer=voice_of_customer,
            limit=limit,
            normalize_text=self._normalize_text,
            infer_strength_concept=self._infer_strength_concept,
        )

    def _build_strengths_weaknesses_payload(
        self,
        *,
        voice_of_customer: dict[str, Any],
        problem_clusters_top: list[dict[str, Any]],
        action_plan: dict[str, Any],
    ) -> dict[str, list[dict[str, str]]]:
        return build_strengths_weaknesses_payload(
            voice_of_customer=voice_of_customer,
            problem_clusters_top=problem_clusters_top,
            action_plan=action_plan,
            build_structured_strengths=self._build_structured_strengths,
            human_label_problem=self._human_label_problem,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            severity_label=self._severity_label,
            infer_action_type=self._infer_action_type,
        )

    def _infer_strength_concept(self, quote: str) -> str:
        return infer_strength_concept(quote, normalize_text=self._normalize_text)

    async def _build_action_plan(
        self,
        *,
        problem_clusters: dict[str, Any],
        customer_clusters: dict[str, Any],
        business_name: str = "",
        business_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._action_plan_builder.build_action_plan(
            problem_clusters=problem_clusters,
            customer_clusters=customer_clusters,
            business_name=business_name,
            business_context=business_context,
        )

    async def _build_llm_action_plan(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        top_problems: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]] | None:
        return await self._action_plan_builder.build_llm_action_plan(
            business_name=business_name,
            business_context=business_context,
            top_problems=top_problems,
        )

    def _build_action_plan_fallback(
        self,
        *,
        top_problems: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return self._action_plan_builder.build_action_plan_fallback(
            top_problems=top_problems,
            customer_clusters=customer_clusters,
        )

    def _build_quick_wins(
        self,
        *,
        stats: dict[str, Any],
        problem_clusters: dict[str, Any],
        action_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return self._action_plan_builder.build_quick_wins(
            stats=stats,
            problem_clusters=problem_clusters,
            action_plan=action_plan,
        )

    def _build_invisible_and_opportunities(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return self._action_plan_builder.build_invisible_and_opportunities(
            stats=stats,
            review_metrics=review_metrics,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
        )

    def _build_full_data_annex(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._annex_builder.build_full_data_annex(
            stats=stats,
            review_metrics=review_metrics,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            analysis_payload=analysis_payload,
        )

    def _extract_customer_scatter_points(self, *, customer_clusters: dict[str, Any]) -> list[dict[str, Any]]:
        return self._annex_builder.extract_customer_scatter_points(customer_clusters=customer_clusters)

    def _build_annex_cluster_lookup(
        self,
        *,
        customer_clusters: dict[str, Any],
        customer_points: list[dict[str, Any]],
    ) -> tuple[dict[str, int], dict[int, str]]:
        return self._annex_builder.build_annex_cluster_lookup(
            customer_clusters=customer_clusters,
            customer_points=customer_points,
        )

    def _build_annex_compact_points(self, *, customer_points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return self._annex_builder.build_annex_compact_points(customer_points=customer_points)

    def _build_annex_review_rows(
        self,
        *,
        review_metrics: list[dict[str, Any]],
        point_cluster_map: dict[str, int],
        cluster_label_map: dict[int, str],
    ) -> list[dict[str, Any]]:
        return self._annex_builder.build_annex_review_rows(
            review_metrics=review_metrics,
            point_cluster_map=point_cluster_map,
            cluster_label_map=cluster_label_map,
        )

    def _build_annex_dataset_summary(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        by_source: Counter[str],
        by_problem: Counter[str],
        avg_dims: dict[str, Any],
    ) -> dict[str, Any]:
        return self._annex_builder.build_annex_dataset_summary(
            stats=stats,
            review_metrics=review_metrics,
            by_source=by_source,
            by_problem=by_problem,
            avg_dims=avg_dims,
        )

    def _build_executive_summary(
        self,
        *,
        business_name: str,
        score_and_evolution: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        benchmarking: dict[str, Any],
        quick_wins: dict[str, Any],
        llm_clustering_insights: dict[str, Any],
    ) -> dict[str, Any]:
        return build_executive_summary(
            business_name=business_name,
            score_and_evolution=score_and_evolution,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            benchmarking=benchmarking,
            quick_wins=quick_wins,
            llm_clustering_insights=llm_clustering_insights,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            friendly_problem_label=self._friendly_problem_label,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )

    def _build_business_context(
        self,
        *,
        business_name: str,
        listing: dict[str, Any],
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        return build_business_context(
            business_name=business_name,
            listing=listing,
            stats=stats,
            normalize_text=self._normalize_text,
            safe_float=self._safe_float,
        )

    def _score_label(self, score_value: float) -> str:
        return score_label(score_value)

    def _summarize_customer_clusters(
        self,
        *,
        customer_clusters: dict[str, Any],
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        return summarize_customer_clusters(
            customer_clusters=customer_clusters,
            limit=limit,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
        )

    def _summarize_problem_clusters(
        self,
        *,
        problem_clusters: dict[str, Any],
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        return summarize_problem_clusters(
            problem_clusters=problem_clusters,
            limit=limit,
            friendly_problem_label=self._friendly_problem_label,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )

    async def _build_llm_section_narratives(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        stats: dict[str, Any],
        score_and_evolution: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        voice_of_customer: dict[str, Any],
        invisible_and_opportunities: dict[str, Any],
        full_data_annex: dict[str, Any],
        action_plan: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> dict[str, str]:
        return await self._section_narrative_builder.build_llm_section_narratives(
            business_name=business_name,
            business_context=business_context,
            stats=stats,
            score_and_evolution=score_and_evolution,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            voice_of_customer=voice_of_customer,
            invisible_and_opportunities=invisible_and_opportunities,
            full_data_annex=full_data_annex,
            action_plan=action_plan,
            quick_wins=quick_wins,
        )

    def _build_llm_user_prompt_payload(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        stats: dict[str, Any],
        score_and_evolution: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        voice_of_customer: dict[str, Any],
        invisible_and_opportunities: dict[str, Any],
        full_data_annex: dict[str, Any],
    ) -> dict[str, Any]:
        return self._section_narrative_builder.build_llm_user_prompt_payload(
            business_name=business_name,
            business_context=business_context,
            stats=stats,
            score_and_evolution=score_and_evolution,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            voice_of_customer=voice_of_customer,
            invisible_and_opportunities=invisible_and_opportunities,
            full_data_annex=full_data_annex,
        )

    def _extract_json_object(self, text: str) -> str:
        return extract_json_object(text)

    async def _build_llm_clustering_insights(
        self,
        *,
        business_name: str,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._section_narrative_builder.build_llm_clustering_insights(
            business_name=business_name,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
        )
