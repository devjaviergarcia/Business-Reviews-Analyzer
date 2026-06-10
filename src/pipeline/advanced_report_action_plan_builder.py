from __future__ import annotations

from typing import Any, Callable

from src.pipeline.advanced_report_action_plan_generation import (
    AdvancedReportActionPlanGeneration,
)
from src.pipeline.advanced_report_opportunity_builder import AdvancedReportOpportunityBuilder


class AdvancedReportActionPlanBuilder:
    def __init__(
        self,
        *,
        can_use_llm: Callable[[], bool],
        llm_generate_text: Callable[[str], tuple[str, str | None]],
        extract_json_object: Callable[[str], str],
        safe_float: Callable[[Any, float], float],
        safe_int: Callable[[Any, int], int],
        sanitize_llm_text: Callable[[str], str],
        plainify_business_text: Callable[[str], str],
        normalize_action_type: Callable[[str], str],
        infer_action_type: Callable[[str], str],
        infer_action_tool: Callable[[str], str],
        friendly_problem_label: Callable[[str], str],
        generic_comment_problem: str,
    ) -> None:
        self._action_plan_generation = AdvancedReportActionPlanGeneration(
            can_use_llm=can_use_llm,
            llm_generate_text=llm_generate_text,
            extract_json_object=extract_json_object,
            safe_float=safe_float,
            safe_int=safe_int,
            sanitize_llm_text=sanitize_llm_text,
            plainify_business_text=plainify_business_text,
            normalize_action_type=normalize_action_type,
            infer_action_type=infer_action_type,
            infer_action_tool=infer_action_tool,
            friendly_problem_label=friendly_problem_label,
            generic_comment_problem=generic_comment_problem,
        )
        self._opportunity_builder = AdvancedReportOpportunityBuilder(
            safe_float=safe_float,
            friendly_problem_label=friendly_problem_label,
            generic_comment_problem=generic_comment_problem,
        )

    async def build_action_plan(
        self,
        *,
        problem_clusters: dict[str, Any],
        customer_clusters: dict[str, Any],
        business_name: str = "",
        business_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._action_plan_generation.build_action_plan(
            problem_clusters=problem_clusters,
            customer_clusters=customer_clusters,
            business_name=business_name,
            business_context=business_context,
        )

    async def build_llm_action_plan(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        top_problems: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]] | None:
        return await self._action_plan_generation.build_llm_action_plan(
            business_name=business_name,
            business_context=business_context,
            top_problems=top_problems,
        )

    def build_action_plan_fallback(
        self,
        *,
        top_problems: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return self._action_plan_generation.build_action_plan_fallback(
            top_problems=top_problems,
            customer_clusters=customer_clusters,
        )

    def build_quick_wins(
        self,
        *,
        stats: dict[str, Any],
        problem_clusters: dict[str, Any],
        action_plan: dict[str, Any],
    ) -> dict[str, Any]:
        return self._opportunity_builder.build_quick_wins(
            stats=stats,
            problem_clusters=problem_clusters,
            action_plan=action_plan,
        )

    def build_invisible_and_opportunities(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return self._opportunity_builder.build_invisible_and_opportunities(
            stats=stats,
            review_metrics=review_metrics,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
        )
