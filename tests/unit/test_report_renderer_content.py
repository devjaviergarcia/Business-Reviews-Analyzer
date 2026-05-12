from __future__ import annotations

import asyncio
import html as html_std
import re
from pathlib import Path

import pytest

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


def test_scatter_vista_d_scales_radius_by_zone_weight() -> None:
    renderer = StructuredReportRenderer()
    payload = {
        "type": "scatter_d",
        "axes": {"x_label": "Brecha", "y_label": "Satisfacción"},
        "quadrant_labels": {"top_left": "Zona alta"},
        "bubbles": [
            {
                "label": "Mayor peso",
                "zone": "top_left",
                "count_reviews": 100,
                "weight_pct": 80.0,
                "radius": 0.80,
                "color": "#111111",
            },
            {
                "label": "Menor peso",
                "zone": "top_left",
                "count_reviews": 20,
                "weight_pct": 20.0,
                "radius": 0.20,
                "color": "#222222",
            },
        ],
    }

    html = renderer._render_scatter_vista_d(payload)
    assert html

    big_match = re.search(r'<circle[^>]*r="([0-9.]+)"[^>]*fill="#111111"', html)
    small_match = re.search(r'<circle[^>]*r="([0-9.]+)"[^>]*fill="#222222"', html)
    assert big_match is not None
    assert small_match is not None

    big_r = float(big_match.group(1))
    small_r = float(small_match.group(1))
    assert big_r > small_r
    assert small_r >= 16.0


def _extract_svg_text_nodes(svg_html: str) -> list[str]:
    values: list[str] = []
    for match in re.finditer(r"<text[^>]*>(.*?)</text>", svg_html, flags=re.DOTALL):
        raw = match.group(1)
        values.append(html_std.unescape(raw).strip())
    return [value for value in values if value]


@pytest.mark.parametrize(
    ("scenario_name", "bubbles"),
    [
        (
            "seis_clusters_mixtos",
            [
                {"label": "Lider principal", "zone": "top_left", "weight_pct": 52.0, "radius": 0.52, "color": "#0A7567"},
                {"label": "Promotores fieles", "zone": "top_left", "weight_pct": 24.0, "radius": 0.24, "color": "#12B08A"},
                {"label": "Neutrales", "zone": "top_right", "weight_pct": 9.0, "radius": 0.09, "color": "#D4950A"},
                {"label": "Exigentes", "zone": "bottom_left", "weight_pct": 7.0, "radius": 0.07, "color": "#8B95A5"},
                {"label": "Riesgo medio", "zone": "bottom_right", "weight_pct": 5.0, "radius": 0.05, "color": "#C23B18"},
                {"label": "Riesgo bajo", "zone": "bottom_right", "weight_pct": 3.0, "radius": 0.03, "color": "#B45309"},
            ],
        ),
        (
            "siete_clusters_desbalanceados",
            [
                {"label": "Mayoritario", "zone": "top_left", "weight_pct": 91.0, "radius": 0.91, "color": "#0A7567"},
                {"label": "Secundario", "zone": "top_left", "weight_pct": 4.0, "radius": 0.04, "color": "#12B08A"},
                {"label": "Riesgo medio", "zone": "bottom_right", "weight_pct": 2.8, "radius": 0.028, "color": "#D4950A"},
                {"label": "Riesgo bajo", "zone": "bottom_right", "weight_pct": 2.2, "radius": 0.022, "color": "#C23B18"},
                {"label": "Foco operativo", "zone": "top_right", "weight_pct": 0.6, "radius": 0.006, "color": "#8B5CF6"},
                {"label": "Recuperables", "zone": "bottom_left", "weight_pct": 0.5, "radius": 0.005, "color": "#475569"},
                {"label": "Residual", "zone": "bottom_left", "weight_pct": 0.4, "radius": 0.004, "color": "#A16207"},
            ],
        ),
        (
            "siete_clusters_en_satisfechos",
            [
                {"label": "Satisfechos 1", "zone": "top_left", "weight_pct": 32.0, "radius": 0.32, "color": "#0A7567"},
                {"label": "Satisfechos 2", "zone": "top_left", "weight_pct": 18.0, "radius": 0.18, "color": "#12B08A"},
                {"label": "Satisfechos 3", "zone": "top_left", "weight_pct": 14.0, "radius": 0.14, "color": "#0E9F6E"},
                {"label": "Satisfechos 4", "zone": "top_left", "weight_pct": 12.0, "radius": 0.12, "color": "#0F766E"},
                {"label": "Satisfechos 5", "zone": "top_left", "weight_pct": 10.0, "radius": 0.10, "color": "#16A34A"},
                {"label": "Satisfechos 6", "zone": "top_left", "weight_pct": 8.0, "radius": 0.08, "color": "#22C55E"},
                {"label": "Satisfechos 7", "zone": "top_left", "weight_pct": 6.0, "radius": 0.06, "color": "#84CC16"},
            ],
        ),
    ],
)
def test_scatter_vista_d_renders_all_labels_and_percentages_for_dense_distributions(
    scenario_name: str,
    bubbles: list[dict[str, float | str]],
) -> None:
    renderer = StructuredReportRenderer()
    payload = {
        "type": "scatter_d",
        "axes": {"x_label": "Brecha", "y_label": "Satisfacción"},
        "quadrant_labels": {
            "top_left": "Satisfechos · Expectativas cumplidas",
            "top_right": "Satisfechos · Expectativas no cumplidas",
            "bottom_left": "Insatisfechos · Expectativas cumplidas",
            "bottom_right": "Insatisfechos · Expectativas no cumplidas",
        },
        "bubbles": [
            {
                "label": str(item["label"]),
                "zone": str(item["zone"]),
                "count_reviews": int(round(float(item["weight_pct"]) * 10)),
                "weight_pct": float(item["weight_pct"]),
                "radius": float(item["radius"]),
                "color": str(item["color"]),
            }
            for item in bubbles
        ],
    }

    html = renderer._render_scatter_vista_d(payload)
    assert html, scenario_name
    assert html.count("<circle ") == len(bubbles), scenario_name

    nodes = _extract_svg_text_nodes(html)
    node_text = " | ".join(nodes)
    for item in bubbles:
        label = str(item["label"])
        pct = f"{float(item['weight_pct']):.1f}%"
        assert label in node_text, f"{scenario_name}: falta etiqueta '{label}'"
        assert pct in node_text, f"{scenario_name}: falta porcentaje '{pct}'"
