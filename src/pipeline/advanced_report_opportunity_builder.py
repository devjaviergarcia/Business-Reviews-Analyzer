from __future__ import annotations

from typing import Any, Callable


class AdvancedReportOpportunityBuilder:
    def __init__(
        self,
        *,
        safe_float: Callable[[Any, float], float],
        friendly_problem_label: Callable[[str], str],
        generic_comment_problem: str,
    ) -> None:
        self._safe_float = safe_float
        self._friendly_problem_label = friendly_problem_label
        self._generic_comment_problem = generic_comment_problem

    def build_quick_wins(
        self,
        *,
        stats: dict[str, Any],
        problem_clusters: dict[str, Any],
        action_plan: dict[str, Any],
    ) -> dict[str, Any]:
        response_rate = self._safe_float((stats or {}).get("response_rate"), 0.0)
        quick_wins = []
        if response_rate < 0.35:
            quick_wins.append(
                {
                    "title": "Responder reseñas en menos de 24 horas",
                    "why": "Una respuesta rápida transmite cercanía y reduce el impacto de una mala experiencia.",
                    "effort": "bajo",
                    "impact": "alto",
                }
            )

        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []
        for cluster in clusters[:3]:
            problem = self._friendly_problem_label(
                str(cluster.get("problem", self._generic_comment_problem) or self._generic_comment_problem)
            )
            quick_wins.append(
                {
                    "title": f"Atajar ya el tema '{problem}' con una mejora simple",
                    "why": "Este punto se repite en reseñas críticas y tiene impacto directo en la experiencia.",
                    "effort": "medio",
                    "impact": "alto",
                }
            )

        immediate = action_plan.get("inmediato_0_30_dias") if isinstance(action_plan, dict) else []
        if isinstance(immediate, list):
            for item in immediate[:2]:
                action = str((item or {}).get("action", "")).strip()
                if action:
                    quick_wins.append(
                        {
                            "title": action,
                            "why": "Ya priorizado en plan inmediato.",
                            "effort": "medio",
                            "impact": str((item or {}).get("impact", "medio")),
                        }
                    )

        deduped: list[dict[str, Any]] = []
        seen = set()
        for item in quick_wins:
            key = str(item.get("title", "") or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return {"items": deduped[:7]}

    def build_invisible_and_opportunities(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        total_reviews = max(1, len(review_metrics))
        without_text = sum(1 for item in review_metrics if not str(item.get("text", "") or "").strip())
        no_text_ratio = without_text / total_reviews
        high_aggressive = sum(
            1
            for item in review_metrics
            if self._safe_float((item.get("dimensions") or {}).get("tranquility_aggressiveness"), 0.0) <= -0.35
        )
        aggressive_ratio = high_aggressive / total_reviews
        response_rate = self._safe_float((stats or {}).get("response_rate"), 0.0)

        invisible = []
        if no_text_ratio >= 0.25:
            invisible.append(
                {
                    "risk": "Volumen alto de reseñas sin texto",
                    "detail": "Puede ocultar fricciones no diagnosticadas.",
                    "metric": round(no_text_ratio, 4),
                }
            )
        if aggressive_ratio >= 0.15:
            invisible.append(
                {
                    "risk": "Tono agresivo relevante",
                    "detail": "Existe un subgrupo con experiencia emocionalmente intensa.",
                    "metric": round(aggressive_ratio, 4),
                }
            )
        if response_rate < 0.35:
            invisible.append(
                {
                    "risk": "Baja tasa de respuesta",
                    "detail": "Se pierde oportunidad de recuperación de cliente.",
                    "metric": round(response_rate, 4),
                }
            )

        clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []
        opportunities = []
        for cluster in clusters[:3]:
            label = str(cluster.get("label", "") or "").strip()
            centroid = cluster.get("centroid") if isinstance(cluster.get("centroid"), dict) else {}
            satisfaction = self._safe_float(centroid.get("satisfaction"), 0.0)
            improvement = self._safe_float(centroid.get("improvement_intent"), 0.0)
            if satisfaction >= 0.65:
                opportunities.append(
                    {
                        "opportunity": f"Activar programa de recomendación para cluster '{label}'.",
                        "metric": round(satisfaction, 4),
                    }
                )
            if improvement >= 0.45:
                opportunities.append(
                    {
                        "opportunity": f"Co-crear mejoras con clientes del cluster '{label}'.",
                        "metric": round(improvement, 4),
                    }
                )

        problem_data = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        if isinstance(problem_data, list) and problem_data:
            main_problem = str(problem_data[0].get("problem", "") or "").strip()
            if main_problem:
                opportunities.append(
                    {
                        "opportunity": f"Convertir '{main_problem}' en palanca de diferenciación operativa.",
                        "metric": round(self._safe_float(problem_data[0].get("severity"), 0.0), 4),
                    }
                )

        return {
            "invisible_problems": invisible[:6],
            "opportunities": opportunities[:6],
        }
