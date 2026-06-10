from __future__ import annotations

from typing import Any

from src.pipeline.advanced_report_benchmarking_builder import (
    build_benchmarking,
    build_score_and_evolution,
)
from src.pipeline.advanced_report_input_bundle_builder import (
    build_report_sections_input_bundle,
    build_review_analysis_bundle,
)
from src.pipeline.advanced_report_payload_assembly import (
    build_advanced_report_payload,
    build_benchmarking_summary_payload,
)
from src.pipeline.advanced_report_review_dimension_scorer import score_review_dimensions


class AdvancedReportPipelineFacet:
    def _can_use_llm(self) -> bool:
        return bool(self.llm_enabled and self.client is not None)

    async def build(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        reviews: list[dict[str, Any]],
        analysis_payload: dict[str, Any],
        businesses_collection,
        analyses_collection,
    ) -> dict[str, Any]:
        review_analysis = await self._build_review_analysis_bundle(
            business_name=business_name,
            listing=listing if isinstance(listing, dict) else {},
            stats=stats,
            reviews=reviews,
        )
        source_reports = review_analysis["source_reports"]
        review_metrics = review_analysis["review_metrics"]
        customer_clusters = review_analysis["customer_clusters"]
        problem_clusters = review_analysis["problem_clusters"]
        business_context = review_analysis["business_context"]
        source_analysis = review_analysis["source_analysis"]
        source_comparison = review_analysis["source_comparison"]

        benchmarking = await self._build_benchmarking(
            business_id=business_id,
            business_name=business_name,
            listing=listing,
            stats=stats,
            review_metrics=review_metrics,
            businesses_collection=businesses_collection,
        )
        score_and_evolution = await self._build_score_and_evolution(
            business_id=business_id,
            stats=stats,
            review_metrics=review_metrics,
            analyses_collection=analyses_collection,
        )

        report_sections_inputs = await self._build_report_sections_input_bundle(
            business_name=business_name,
            stats=stats,
            problem_clusters=problem_clusters,
            customer_clusters=customer_clusters,
            business_context=business_context,
            review_metrics=review_metrics,
            analysis_payload=analysis_payload,
            score_and_evolution=score_and_evolution,
        )
        voice_of_customer = report_sections_inputs["voice_of_customer"]
        action_plan = report_sections_inputs["action_plan"]
        quick_wins = report_sections_inputs["quick_wins"]
        invisible_and_opportunities = report_sections_inputs["invisible_and_opportunities"]
        full_data_annex = report_sections_inputs["full_data_annex"]
        llm_clustering_insights = report_sections_inputs["llm_clustering_insights"]
        llm_section_narratives = report_sections_inputs["llm_section_narratives"]

        score_value = self._safe_float(score_and_evolution.get("reputation_score"))
        score_label = self._score_label(score_value)
        customer_clusters_top = self._summarize_customer_clusters(customer_clusters=customer_clusters, limit=3)
        problem_clusters_top = self._summarize_problem_clusters(problem_clusters=problem_clusters, limit=3)
        benchmarking_summary = build_benchmarking_summary_payload(
            benchmarking=benchmarking,
            safe_int=self._safe_int,
        )

        return build_advanced_report_payload(
            business_id=business_id,
            business_name=business_name,
            business_context=business_context,
            source_reports=source_reports,
            source_analysis=source_analysis,
            source_comparison=source_comparison,
            score_value=score_value,
            score_label=score_label,
            customer_clusters=customer_clusters,
            customer_clusters_top=customer_clusters_top,
            problem_clusters=problem_clusters,
            problem_clusters_top=problem_clusters_top,
            score_and_evolution=score_and_evolution,
            voice_of_customer=voice_of_customer,
            action_plan=action_plan,
            quick_wins=quick_wins,
            invisible_and_opportunities=invisible_and_opportunities,
            full_data_annex=full_data_annex,
            benchmarking=benchmarking,
            benchmarking_summary=benchmarking_summary,
            llm_clustering_insights=llm_clustering_insights,
            llm_section_narratives=llm_section_narratives,
            structured_strengths=self._build_structured_strengths(
                voice_of_customer=voice_of_customer,
                limit=3,
            ),
            strengths_weaknesses_payload=self._build_strengths_weaknesses_payload(
                voice_of_customer=voice_of_customer,
                problem_clusters_top=problem_clusters_top,
                action_plan=action_plan,
            ),
        )

    async def _build_review_analysis_bundle(
        self,
        *,
        business_name: str,
        listing: dict[str, Any],
        stats: dict[str, Any],
        reviews: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return await build_review_analysis_bundle(
            business_name=business_name,
            listing=listing,
            stats=stats,
            reviews=reviews,
            build_source_reports=self._build_source_reports,
            score_review_dimensions=self._score_review_dimensions,
            build_customer_clusters=self._build_customer_clusters,
            build_problem_clusters=self._build_problem_clusters,
            build_business_context=self._build_business_context,
            build_source_analysis_bundle=self._build_source_analysis_bundle,
        )

    async def _build_report_sections_input_bundle(
        self,
        *,
        business_name: str,
        stats: dict[str, Any],
        problem_clusters: dict[str, Any],
        customer_clusters: dict[str, Any],
        business_context: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        analysis_payload: dict[str, Any],
        score_and_evolution: dict[str, Any],
    ) -> dict[str, Any]:
        return await build_report_sections_input_bundle(
            business_name=business_name,
            stats=stats,
            problem_clusters=problem_clusters,
            customer_clusters=customer_clusters,
            business_context=business_context,
            review_metrics=review_metrics,
            analysis_payload=analysis_payload,
            score_and_evolution=score_and_evolution,
            build_voice_of_customer=self._build_voice_of_customer,
            build_action_plan=self._build_action_plan,
            build_quick_wins=self._build_quick_wins,
            build_invisible_and_opportunities=self._build_invisible_and_opportunities,
            build_full_data_annex=self._build_full_data_annex,
            build_llm_clustering_insights=self._build_llm_clustering_insights,
            build_llm_section_narratives=self._build_llm_section_narratives,
        )

    async def _build_source_analysis_bundle(
        self,
        *,
        source_reports: dict[str, dict[str, Any]],
        business_name: str,
        business_context: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
        return await self._source_insights_builder.build_source_analysis_bundle(
            source_reports=source_reports,
            business_name=business_name,
            business_context=business_context,
        )

    def _build_source_reports(self, *, reviews: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return self._source_insights_builder.build_source_reports(reviews=reviews)

    async def _build_llm_source_narrative(
        self,
        *,
        source: str,
        business_name: str,
        business_context: dict[str, Any],
        stats: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._source_insights_builder.build_llm_source_narrative(
            source=source,
            business_name=business_name,
            business_context=business_context,
            stats=stats,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
        )

    async def _build_llm_source_comparison(
        self,
        *,
        business_name: str,
        google_data: dict[str, Any],
        tripadvisor_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._source_insights_builder.build_llm_source_comparison(
            business_name=business_name,
            google_data=google_data,
            tripadvisor_data=tripadvisor_data,
        )

    def build_preview_report(
        self,
        *,
        advanced_report: dict[str, Any],
        business_name: str | None = None,
        max_comments: int = 3,
    ) -> dict[str, Any]:
        return self._preview_builder.build_preview_report(
            advanced_report=advanced_report,
            business_name=business_name,
            max_comments=max_comments,
        )

    def _score_review_dimensions(self, *, index: int, review: dict[str, Any]) -> dict[str, Any]:
        return score_review_dimensions(
            index=index,
            review=review,
            positive_tokens=self._POSITIVE_TOKENS,
            negative_tokens=self._NEGATIVE_TOKENS,
            expectation_tokens=self._EXPECTATION_TOKENS,
            improvement_tokens=self._IMPROVEMENT_TOKENS,
            aggressive_tokens=self._AGGRESSIVE_TOKENS,
            safe_rating=self._safe_rating,
            normalize_text=self._normalize_text,
            count_keyword_hits=self._count_keyword_hits,
            clamp=self._clamp,
            clamp01=self._clamp01,
            upper_ratio=self._upper_ratio,
            theme_scores=self._theme_scores,
            resolve_dominant_problem=self._resolve_dominant_problem,
        )

    async def _build_benchmarking(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        businesses_collection,
    ) -> dict[str, Any]:
        return await build_benchmarking(
            business_id=business_id,
            business_name=business_name,
            listing=listing,
            stats=stats,
            review_metrics=review_metrics,
            businesses_collection=businesses_collection,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            negative_ratio=self._negative_ratio,
            compute_reputation_score=self._compute_reputation_score,
            average_dimension=self._average_dimension,
        )

    async def _build_score_and_evolution(
        self,
        *,
        business_id: str,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        analyses_collection,
    ) -> dict[str, Any]:
        return await build_score_and_evolution(
            business_id=business_id,
            stats=stats,
            review_metrics=review_metrics,
            analyses_collection=analyses_collection,
            safe_float=self._safe_float,
            negative_ratio=self._negative_ratio,
            average_dimension=self._average_dimension,
            compute_reputation_score=self._compute_reputation_score,
            linear_slope=self._linear_slope,
        )

