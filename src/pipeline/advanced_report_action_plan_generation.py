from __future__ import annotations

from typing import Any, Callable

from src.pipeline.advanced_report_action_plan_fallback_builder import (
    build_action_plan_fallback,
)
from src.pipeline.advanced_report_action_plan_llm_builder import (
    build_llm_action_plan,
)


class AdvancedReportActionPlanGeneration:
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
        self._can_use_llm = can_use_llm
        self._llm_generate_text = llm_generate_text
        self._extract_json_object = extract_json_object
        self._safe_float = safe_float
        self._safe_int = safe_int
        self._sanitize_llm_text = sanitize_llm_text
        self._plainify_business_text = plainify_business_text
        self._normalize_action_type = normalize_action_type
        self._infer_action_type = infer_action_type
        self._infer_action_tool = infer_action_tool
        self._friendly_problem_label = friendly_problem_label
        self._generic_comment_problem = generic_comment_problem

    async def build_action_plan(
        self,
        *,
        problem_clusters: dict[str, Any],
        customer_clusters: dict[str, Any],
        business_name: str = "",
        business_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []
        top_problems = clusters[:3]

        if self._can_use_llm() and top_problems:
            llm_actions = await self.build_llm_action_plan(
                business_name=business_name,
                business_context=business_context or {},
                top_problems=top_problems,
            )
            if llm_actions:
                cluster_count = self._safe_int(customer_clusters.get("cluster_count"), 0)
                return {
                    "inmediato_0_30_dias": llm_actions.get("inmediato", [])[:5],
                    "medio_30_90_dias": llm_actions.get("medio", [])[:5],
                    "largo_90_mas_dias": llm_actions.get("largo", [])[:5],
                    "notes": [
                        f"Se detectaron {cluster_count} segmentos de clientes para personalizar acciones.",
                        "Acciones priorizadas con base en patrones reales de reseñas del negocio.",
                    ],
                    "llm_generated": True,
                }

        return self.build_action_plan_fallback(
            top_problems=top_problems,
            customer_clusters=customer_clusters,
        )

    async def build_llm_action_plan(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        top_problems: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]] | None:
        return await build_llm_action_plan(
            business_name=business_name,
            business_context=business_context,
            top_problems=top_problems,
            friendly_problem_label=self._friendly_problem_label,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
            normalize_action_type=self._normalize_action_type,
            infer_action_type=self._infer_action_type,
            infer_action_tool=self._infer_action_tool,
        )

    def build_action_plan_fallback(
        self,
        *,
        top_problems: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        return build_action_plan_fallback(
            top_problems=top_problems,
            customer_clusters=customer_clusters,
            generic_comment_problem=self._generic_comment_problem,
            friendly_problem_label=self._friendly_problem_label,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            infer_action_type=self._infer_action_type,
            infer_action_tool=self._infer_action_tool,
        )
