#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.pipeline.report_renderer import StructuredReportRenderer


def _base_axes() -> dict[str, str]:
    return {
        "x_label": "Brecha de expectativa",
        "y_label": "Satisfacción",
        "x_low": "Expectativas cumplidas",
        "x_high": "Expectativas no cumplidas",
        "y_low": "Baja satisfacción",
        "y_high": "Alta satisfacción",
    }


def _base_quadrants() -> dict[str, str]:
    return {
        "top_left": "Satisfechos · Expectativas cumplidas",
        "top_right": "Satisfechos · Expectativas no cumplidas",
        "bottom_left": "Insatisfechos · Expectativas cumplidas",
        "bottom_right": "Insatisfechos · Expectativas no cumplidas",
    }


def _scenario_payloads() -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "01_balanceado",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {
                        "label": "Promotores fieles",
                        "zone": "top_left",
                        "count_reviews": 120,
                        "weight_pct": 48.0,
                        "radius": 0.48,
                        "color": "#0A7567",
                    },
                    {
                        "label": "Promotores satisfechos",
                        "zone": "top_left",
                        "count_reviews": 80,
                        "weight_pct": 32.0,
                        "radius": 0.32,
                        "color": "#12B08A",
                    },
                    {
                        "label": "Neutrales pragmáticos",
                        "zone": "top_right",
                        "count_reviews": 35,
                        "weight_pct": 14.0,
                        "radius": 0.14,
                        "color": "#D4950A",
                    },
                    {
                        "label": "Riesgo",
                        "zone": "bottom_right",
                        "count_reviews": 15,
                        "weight_pct": 6.0,
                        "radius": 0.06,
                        "color": "#C23B18",
                    },
                ],
            },
        ),
        (
            "02_muy_desbalanceado",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {
                        "label": "Mayoritario",
                        "zone": "top_left",
                        "count_reviews": 910,
                        "weight_pct": 91.0,
                        "radius": 0.91,
                        "color": "#0A7567",
                    },
                    {
                        "label": "Secundario",
                        "zone": "top_left",
                        "count_reviews": 60,
                        "weight_pct": 6.0,
                        "radius": 0.06,
                        "color": "#12B08A",
                    },
                    {
                        "label": "Riesgo medio",
                        "zone": "bottom_right",
                        "count_reviews": 28,
                        "weight_pct": 2.8,
                        "radius": 0.028,
                        "color": "#D4950A",
                    },
                    {
                        "label": "Riesgo bajo",
                        "zone": "bottom_right",
                        "count_reviews": 4,
                        "weight_pct": 0.4,
                        "radius": 0.004,
                        "color": "#C23B18",
                    },
                ],
            },
        ),
        (
            "03_cuatro_en_una_zona",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {
                        "label": "A",
                        "zone": "top_left",
                        "count_reviews": 100,
                        "weight_pct": 40.0,
                        "radius": 0.40,
                        "color": "#0A7567",
                    },
                    {
                        "label": "B",
                        "zone": "top_left",
                        "count_reviews": 70,
                        "weight_pct": 28.0,
                        "radius": 0.28,
                        "color": "#12B08A",
                    },
                    {
                        "label": "C",
                        "zone": "top_left",
                        "count_reviews": 45,
                        "weight_pct": 18.0,
                        "radius": 0.18,
                        "color": "#D4950A",
                    },
                    {
                        "label": "D",
                        "zone": "top_left",
                        "count_reviews": 35,
                        "weight_pct": 14.0,
                        "radius": 0.14,
                        "color": "#C23B18",
                    },
                ],
            },
        ),
        (
            "04_seis_clusters_mixtos",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {"label": "Lider principal", "zone": "top_left", "count_reviews": 520, "weight_pct": 52.0, "radius": 0.52, "color": "#0A7567"},
                    {"label": "Promotores fieles", "zone": "top_left", "count_reviews": 240, "weight_pct": 24.0, "radius": 0.24, "color": "#12B08A"},
                    {"label": "Neutrales", "zone": "top_right", "count_reviews": 90, "weight_pct": 9.0, "radius": 0.09, "color": "#D4950A"},
                    {"label": "Exigentes", "zone": "bottom_left", "count_reviews": 70, "weight_pct": 7.0, "radius": 0.07, "color": "#8B95A5"},
                    {"label": "Riesgo medio", "zone": "bottom_right", "count_reviews": 50, "weight_pct": 5.0, "radius": 0.05, "color": "#C23B18"},
                    {"label": "Riesgo bajo", "zone": "bottom_right", "count_reviews": 30, "weight_pct": 3.0, "radius": 0.03, "color": "#B45309"},
                ],
            },
        ),
        (
            "05_siete_clusters_desbalanceados",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {"label": "Mayoritario", "zone": "top_left", "count_reviews": 910, "weight_pct": 91.0, "radius": 0.91, "color": "#0A7567"},
                    {"label": "Secundario", "zone": "top_left", "count_reviews": 40, "weight_pct": 4.0, "radius": 0.04, "color": "#12B08A"},
                    {"label": "Riesgo medio", "zone": "bottom_right", "count_reviews": 28, "weight_pct": 2.8, "radius": 0.028, "color": "#D4950A"},
                    {"label": "Riesgo bajo", "zone": "bottom_right", "count_reviews": 22, "weight_pct": 2.2, "radius": 0.022, "color": "#C23B18"},
                    {"label": "Foco operativo", "zone": "top_right", "count_reviews": 6, "weight_pct": 0.6, "radius": 0.006, "color": "#8B5CF6"},
                    {"label": "Recuperables", "zone": "bottom_left", "count_reviews": 5, "weight_pct": 0.5, "radius": 0.005, "color": "#475569"},
                    {"label": "Residual", "zone": "bottom_left", "count_reviews": 4, "weight_pct": 0.4, "radius": 0.004, "color": "#A16207"},
                ],
            },
        ),
        (
            "06_seis_clusters_satisfechos",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {"label": "Satisfechos 1", "zone": "top_left", "count_reviews": 300, "weight_pct": 30.0, "radius": 0.30, "color": "#0A7567"},
                    {"label": "Satisfechos 2", "zone": "top_left", "count_reviews": 210, "weight_pct": 21.0, "radius": 0.21, "color": "#12B08A"},
                    {"label": "Satisfechos 3", "zone": "top_left", "count_reviews": 170, "weight_pct": 17.0, "radius": 0.17, "color": "#0E9F6E"},
                    {"label": "Satisfechos 4", "zone": "top_left", "count_reviews": 130, "weight_pct": 13.0, "radius": 0.13, "color": "#0F766E"},
                    {"label": "Satisfechos 5", "zone": "top_left", "count_reviews": 110, "weight_pct": 11.0, "radius": 0.11, "color": "#16A34A"},
                    {"label": "Satisfechos 6", "zone": "top_left", "count_reviews": 80, "weight_pct": 8.0, "radius": 0.08, "color": "#22C55E"},
                ],
            },
        ),
        (
            "07_siete_clusters_satisfechos",
            {
                "type": "scatter_d",
                "axes": _base_axes(),
                "quadrant_labels": _base_quadrants(),
                "bubbles": [
                    {"label": "Satisfechos 1", "zone": "top_left", "count_reviews": 320, "weight_pct": 32.0, "radius": 0.32, "color": "#0A7567"},
                    {"label": "Satisfechos 2", "zone": "top_left", "count_reviews": 180, "weight_pct": 18.0, "radius": 0.18, "color": "#12B08A"},
                    {"label": "Satisfechos 3", "zone": "top_left", "count_reviews": 140, "weight_pct": 14.0, "radius": 0.14, "color": "#0E9F6E"},
                    {"label": "Satisfechos 4", "zone": "top_left", "count_reviews": 120, "weight_pct": 12.0, "radius": 0.12, "color": "#0F766E"},
                    {"label": "Satisfechos 5", "zone": "top_left", "count_reviews": 100, "weight_pct": 10.0, "radius": 0.10, "color": "#16A34A"},
                    {"label": "Satisfechos 6", "zone": "top_left", "count_reviews": 80, "weight_pct": 8.0, "radius": 0.08, "color": "#22C55E"},
                    {"label": "Satisfechos 7", "zone": "top_left", "count_reviews": 60, "weight_pct": 6.0, "radius": 0.06, "color": "#84CC16"},
                ],
            },
        ),
    ]


def _extract_svg(scatter_html: str) -> str:
    start = scatter_html.find("<svg")
    end = scatter_html.rfind("</svg>")
    if start < 0 or end < 0:
        raise RuntimeError("No se pudo extraer el bloque <svg> del scatter renderizado")
    return scatter_html[start : end + len("</svg>")]


def _build_preview_html(title: str, scatter_html: str, payload: dict[str, Any]) -> str:
    pretty = json.dumps(payload, ensure_ascii=False, indent=2)
    return (
        "<!doctype html><html lang='es'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{title}</title>"
        "<style>body{font-family:Inter,Arial,sans-serif;background:#f5f5f4;margin:0;padding:24px;}"
        ".wrap{max-width:1100px;margin:0 auto;background:#fff;padding:20px;border-radius:12px;"
        "box-shadow:0 8px 28px rgba(0,0,0,.06);}"
        "h1{font-size:20px;margin:0 0 12px;color:#0f172a;}"
        "pre{white-space:pre-wrap;background:#0f172a;color:#e2e8f0;padding:12px;border-radius:10px;font-size:12px;}"
        "</style></head><body><div class='wrap'>"
        f"<h1>{title}</h1>{scatter_html}<h2>Payload</h2><pre>{pretty}</pre>"
        "</div></body></html>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera artefactos SVG/HTML para testear el scatter Vista D.")
    parser.add_argument(
        "--output-dir",
        default="artifacts/report_scatter_vista_d",
        help="Directorio de salida para artefactos.",
    )
    parser.add_argument(
        "--timestamp-subdir",
        action="store_true",
        help="Crea subdirectorio por timestamp para no sobreescribir.",
    )
    args = parser.parse_args()

    output_root = Path(args.output_dir).resolve()
    if args.timestamp_subdir:
        output_root = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root.mkdir(parents=True, exist_ok=True)

    renderer = StructuredReportRenderer(artifacts_root=output_root)
    manifest: list[dict[str, str]] = []

    for name, payload in _scenario_payloads():
        scatter_html = renderer._render_scatter_vista_d(payload)
        if not scatter_html:
            raise RuntimeError(f"No se pudo renderizar el escenario: {name}")
        svg = _extract_svg(scatter_html)

        svg_path = output_root / f"{name}.svg"
        html_path = output_root / f"{name}.html"
        json_path = output_root / f"{name}.json"

        svg_path.write_text(svg, encoding="utf-8")
        html_path.write_text(_build_preview_html(name, scatter_html, payload), encoding="utf-8")
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        manifest.append(
            {
                "scenario": name,
                "svg": str(svg_path),
                "html": str(html_path),
                "json": str(json_path),
            }
        )

    manifest_path = output_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Artefactos generados en: {output_root}")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
