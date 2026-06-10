from __future__ import annotations

from typing import Any, Callable

from src.pipeline.advanced_report_prompt_payload_builder import (
    build_llm_user_prompt_payload,
)
from src.pipeline.advanced_report_section_narratives_generation import (
    build_llm_clustering_insights,
    build_llm_section_narratives,
    fallback_clustering_text,
)


class AdvancedReportSectionNarrativeBuilder:
    def __init__(
        self,
        *,
        safe_float: Callable[[Any, float], float],
        safe_int: Callable[[Any, int], int],
        score_label: Callable[[float], str],
        summarize_customer_clusters: Callable[..., list[dict[str, Any]]],
        summarize_problem_clusters: Callable[..., list[dict[str, Any]]],
        friendly_problem_label: Callable[[str], str],
        can_use_llm: Callable[[], bool],
        llm_generate_text: Callable[[str], tuple[str, str | None]],
        extract_json_object: Callable[[str], str],
        sanitize_llm_text: Callable[[str], str],
        plainify_business_text: Callable[[str], str],
        generic_comment_problem: str,
    ) -> None:
        self._safe_float = safe_float
        self._safe_int = safe_int
        self._score_label = score_label
        self._summarize_customer_clusters = summarize_customer_clusters
        self._summarize_problem_clusters = summarize_problem_clusters
        self._friendly_problem_label = friendly_problem_label
        self._can_use_llm = can_use_llm
        self._llm_generate_text = llm_generate_text
        self._extract_json_object = extract_json_object
        self._sanitize_llm_text = sanitize_llm_text
        self._plainify_business_text = plainify_business_text
        self._generic_comment_problem = generic_comment_problem

    async def build_llm_section_narratives(
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
        payload = self.build_llm_user_prompt_payload(
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
        return await build_llm_section_narratives(
            business_name=business_name,
            score_and_evolution=score_and_evolution,
            payload=payload,
            safe_float=self._safe_float,
            score_label=self._score_label,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
        )

    def build_llm_user_prompt_payload(
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
        return build_llm_user_prompt_payload(
            business_name=business_name,
            business_context=business_context,
            stats=stats,
            score_and_evolution=score_and_evolution,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            voice_of_customer=voice_of_customer,
            invisible_and_opportunities=invisible_and_opportunities,
            full_data_annex=full_data_annex,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            score_label=self._score_label,
            summarize_customer_clusters=self._summarize_customer_clusters,
            friendly_problem_label=self._friendly_problem_label,
            generic_comment_problem=self._generic_comment_problem,
        )

    async def build_llm_clustering_insights(
        self,
        *,
        business_name: str,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> dict[str, Any]:
        fallback_text = self.fallback_clustering_text(
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
        )
        return await build_llm_clustering_insights(
            business_name=business_name,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
            fallback_text=fallback_text,
            generic_comment_problem=self._generic_comment_problem,
        )

    def fallback_clustering_text(
        self,
        *,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> str:
        return fallback_clustering_text(
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
            generic_comment_problem=self._generic_comment_problem,
        )
