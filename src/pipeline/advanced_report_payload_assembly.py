from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_benchmarking_summary_payload(
    *,
    benchmarking: dict[str, Any],
    safe_int,
) -> dict[str, Any]:
    target_benchmark_payload = benchmarking.get("target") if isinstance(benchmarking, dict) else {}
    total_competitors_compared = safe_int(benchmarking.get("total_competitors_compared"))
    return {
        "target_rank": benchmarking.get("target_rank"),
        "total_competitors_compared": total_competitors_compared,
        "total_businesses_compared": (total_competitors_compared + 1) if total_competitors_compared > 0 else 0,
        "target_reputation_score": (
            target_benchmark_payload.get("reputation_score")
            if isinstance(target_benchmark_payload, dict)
            else None
        ),
        "top_competitors": (benchmarking.get("top_competitors") or [])[:3],
    }


def build_advanced_report_payload(
    *,
    business_id: str,
    business_name: str,
    business_context: dict[str, Any],
    source_reports: dict[str, dict[str, Any]],
    source_analysis: dict[str, dict[str, Any]],
    source_comparison: dict[str, Any] | None,
    score_value: float,
    score_label: str,
    customer_clusters: dict[str, Any],
    customer_clusters_top: list[dict[str, Any]],
    problem_clusters: dict[str, Any],
    problem_clusters_top: list[dict[str, Any]],
    score_and_evolution: dict[str, Any],
    voice_of_customer: dict[str, Any],
    action_plan: dict[str, Any],
    quick_wins: dict[str, Any],
    invisible_and_opportunities: dict[str, Any],
    full_data_annex: dict[str, Any],
    benchmarking: dict[str, Any],
    benchmarking_summary: dict[str, Any],
    llm_clustering_insights: dict[str, Any],
    llm_section_narratives: dict[str, str],
    structured_strengths: list[dict[str, Any]],
    strengths_weaknesses_payload: dict[str, Any],
) -> dict[str, Any]:
    sections = {
        "1_resumen_ejecutivo": {
            "diagnostico": llm_section_narratives["resumen_ejecutivo"],
            "estado_actual": {
                "score_reputacion": score_value,
                "nivel_reputacion": score_label,
                "cluster_count": customer_clusters.get("cluster_count"),
                "problemas_principales": [
                    str(item.get("problem", "") or "") for item in problem_clusters_top
                ],
            },
            "aciertos_notorios": [
                str(item.get("quote", "") or "")
                for item in (voice_of_customer.get("positive_quotes") or [])[:3]
            ],
            "aciertos_estructurados": structured_strengths,
        },
        "2_score_reputacion": {
            "score_display": f"{round(score_value, 1)}/100",
            "score_value": score_value,
            "nivel_reputacion": score_label,
            "explicacion": llm_section_narratives["score"],
            "componentes_numericos": score_and_evolution.get("components"),
            "evolucion": score_and_evolution.get("evolution"),
        },
        "3_quien_es_tu_cliente_y_que_le_preocupa": {
            "lectura_ejecutiva": llm_section_narratives["cliente_y_preocupaciones"],
            "tipologias_cliente_top3": customer_clusters_top,
            "preocupaciones_top3": problem_clusters_top,
            "scatter_clientes": customer_clusters.get("scatter"),
            "bar_chart_clientes": customer_clusters.get("bar_chart"),
            "fortalezas_debilidades": strengths_weaknesses_payload,
        },
        "4_plan_de_accion": {
            "lectura_ejecutiva": llm_section_narratives["plan_accion"],
            "problemas_invisibles": invisible_and_opportunities.get("invisible_problems"),
            "corto_plazo_0_30_dias": action_plan.get("inmediato_0_30_dias"),
            "medio_plazo_30_90_dias": action_plan.get("medio_30_90_dias"),
            "largo_plazo_90_mas_dias": action_plan.get("largo_90_mas_dias"),
            "quick_wins_esta_semana": quick_wins.get("items"),
        },
        "5_anexos_resumen": {
            "nota": (
                "Los anexos completos se entregan fuera del PDF principal en archivos separados "
                "(CSV y PDF de anexos)."
            ),
            "resumen_dataset": full_data_annex.get("dataset_summary"),
            "benchmarking_resumen": benchmarking_summary,
            "voz_literal_muestra": voice_of_customer,
        },
    }

    annexes = {
        "full_data": full_data_annex,
        "benchmarking_full": benchmarking,
        "voice_of_customer": voice_of_customer,
        "customer_clusters_full": customer_clusters,
        "problem_clusters_full": problem_clusters,
    }

    return {
        "report_version": "2026.2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "business_id": business_id,
        "business_name": business_name,
        "business_context": business_context,
        "source_reports": source_reports,
        "source_analysis": source_analysis,
        "source_comparison": source_comparison,
        "section_order": list(sections.keys()),
        "sections": sections,
        "llm_clustering_insights": llm_clustering_insights,
        "llm_section_narratives": llm_section_narratives,
        "annexes": annexes,
    }


def build_advanced_report_preview_payload(
    *,
    resolved_name: str,
    source_report_version: str | None,
    score_display: str | None,
    score_label: str | None,
    summary_preview: str,
    selected_comments: list[dict[str, Any]],
    preview_types: list[dict[str, Any]],
    max_comments: int,
) -> dict[str, Any]:
    cta_text = (
        "Este documento es un aperitivo del diagnóstico. "
        "Si quieres recibir el plan de acción completo, el análisis integral de clústeres y "
        "la priorización detallada, rellena el formulario para solicitar el reporte completo."
    )

    return {
        "preview_version": "2026.1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "business_name": resolved_name,
        "source_report_version": source_report_version,
        "sections": {
            "1_resumen_ejecutivo_preview": {
                "texto": summary_preview,
                "score": score_display or None,
                "nivel_reputacion": score_label or None,
                "nota": "Resumen parcial basado en el informe principal.",
            },
            "2_tipos_cliente_y_comentarios_relevantes": {
                "tipos_cliente": preview_types,
                "comentarios_relevantes": selected_comments[: max(1, int(max_comments))],
            },
            "3_llamada_a_la_accion": {
                "texto": cta_text,
                "accion_recomendada": "Completar formulario para recibir el reporte completo.",
            },
        },
    }
