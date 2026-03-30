from __future__ import annotations

import asyncio
from pathlib import Path

from src.pipeline.report_renderer import StructuredReportRenderer


def test_report_renderer_applies_context_banner_and_annex_details(tmp_path: Path) -> None:
    renderer = StructuredReportRenderer(artifacts_root=tmp_path)
    report_payload = {
        "business_name": "El Gato Verde",
        "generated_at": "2026-03-28T12:56:31+00:00",
        "section_order": [
            "1_resumen_ejecutivo",
            "2_score_reputacion",
            "3_quien_es_tu_cliente_y_que_le_preocupa",
            "4_plan_de_accion",
            "5_anexos_resumen",
        ],
        "sections": {
            "1_resumen_ejecutivo": {
                "diagnostico": "Resumen simple",
                "estado_actual": {
                    "score_reputacion": 72.1,
                    "nivel_reputacion": "reputación sólida",
                    "cluster_count": 3,
                    "problemas_principales": ["servicio"],
                },
                "aciertos_notorios": ["Muy buen trato"],
                "aciertos_estructurados": [{"concepto": "Atención cercana", "cita": "Muy buen trato"}],
            },
            "2_score_reputacion": {
                "score_display": "72.1/100",
                "score_value": 72.1,
                "nivel_reputacion": "reputación sólida",
                "explicacion": "Explicación",
                "componentes_numericos": {
                    "avg_rating": 4.55,
                    "response_rate": 0.0,
                    "negative_ratio": 0.04,
                    "sentiment_avg": 0.69,
                    "tranquility_avg": 0.97,
                },
                "evolucion": {"trend": "estable", "analyses_history": [{"created_at": "2026-03-28T12:56:31+00:00"}]},
            },
            "3_quien_es_tu_cliente_y_que_le_preocupa": {
                "lectura_ejecutiva": "Lectura",
                "tipologias_cliente_top3": [{"label": "Promotores", "descripcion_segmento": "OK"}],
                "preocupaciones_top3": [{"problema": "servicio", "volumen": 3, "severidad": 0.6}],
                "fortalezas_debilidades": {
                    "fortalezas": [{"titulo": "Atención", "descripcion": "Bien", "como_mantener": "Mantener ritmo"}],
                    "debilidades": [{"titulo": "Espera", "descripcion": "Lenta", "tipo": "proceso"}],
                },
                "scatter_clientes": {
                    "axes": {"x_label": "Brecha", "y_label": "Satisfacción"},
                    "circles": [{"label": "Promotores", "center": {"x": 30, "y": 80}, "radius": 10, "count": 5}],
                    "points": [],
                },
            },
            "4_plan_de_accion": {
                "lectura_ejecutiva": "Plan",
                "problemas_invisibles": [{"risk": "Riesgo", "detail": "Detalle"}],
                "corto_plazo_0_30_dias": [
                    {
                        "accion": "Duplicada textual",
                        "por_que": "Motivo",
                        "encargado": "Encargado",
                        "horizon_days": 14,
                        "kpi": "KPI",
                        "tipo": "proceso",
                    }
                ],
                "medio_plazo_30_90_dias": [],
                "largo_plazo_90_mas_dias": [],
                "quick_wins_esta_semana": [
                    {"title": "Duplicada textual", "why": "Repetida", "effort": "low", "impact": "high"},
                    {"title": "Urgente prueba", "why": "Única", "effort": "low", "impact": "high"},
                ],
            },
            "5_anexos_resumen": {
                "nota": "Nota",
                "resumen_dataset": {
                    "total_reviews": 10,
                    "avg_rating": 4.2,
                    "response_rate": 0.0,
                    "by_source": {"google_maps": 10},
                    "by_problem": {"servicio": 4},
                    "dimension_averages": {"sentiment": 0.6},
                },
                "benchmarking_resumen": {"target_reputation_score": None},
                "voz_literal_muestra": {"positive_quotes": [{"author_name": "Ana", "rating": 5, "source": "google_maps", "quote": "Muy bien"}]},
            },
        },
        "annexes": {"full_data": {"review_rows": []}},
    }

    artifacts = asyncio.run(
        renderer.render(
            report_payload=report_payload,
            intro_context_text="Contexto corto",
            business_id="69c597fe28c0a48668059680",
            analysis_id="69c5b08526802372d3588a02",
            output_format="html",
        )
    )
    html_path = Path(str((artifacts.get("html") or {}).get("path")))
    content = html_path.read_text(encoding="utf-8")

    assert "opiniones analizadas" in content
    assert "Análisis elaborado por Repiq" in content
    assert "generado automáticamente" not in content
    assert "<details class='annex-details'>" in content
    assert "Proceso interno" in content
    assert content.count("Duplicada textual") == 1
    assert "Urgente prueba" in content
