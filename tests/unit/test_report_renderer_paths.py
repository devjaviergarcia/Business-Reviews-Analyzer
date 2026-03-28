from __future__ import annotations

import asyncio
from pathlib import Path

from src.pipeline.report_renderer import StructuredReportRenderer


def test_render_uses_descriptive_paths_and_annex_folder(tmp_path: Path) -> None:
    renderer = StructuredReportRenderer(artifacts_root=tmp_path)
    report_payload = {
        "business_name": "El Gato Verde",
        "generated_at": "2026-03-27T10:00:00+00:00",
        "sections": {},
        "annexes": {"full_data": {"review_rows": []}},
    }

    artifacts = asyncio.run(
        renderer.render(
            report_payload=report_payload,
            intro_context_text="Contexto",
            business_id="69c597fe28c0a48668059680",
            analysis_id="69c5b08526802372d3588a02",
            output_format="html",
        )
    )

    main_pdf = artifacts.get("pdf") or {}
    main_json = artifacts.get("json") or {}
    annex = artifacts.get("annex") or {}
    annex_csv = annex.get("csv") or {}
    annex_html = annex.get("html") or {}

    assert "el-gato-verde__69c597fe28c0a48668059680" in str(main_json.get("path"))
    assert "/analisis_69c5b08526802372d3588a02/reportes/" in str(main_json.get("path"))
    assert str(main_json.get("filename", "")).startswith("reporte_final_el-gato-verde_69c5b08526802372d3588a02")
    assert str(main_pdf.get("filename", "")).startswith("reporte_final_el-gato-verde_69c5b08526802372d3588a02")
    assert "/anexos/" in str(annex_csv.get("path"))
    assert str(annex_csv.get("filename", "")).startswith("anexo_datos_el-gato-verde_69c5b08526802372d3588a02")
    assert str(annex_html.get("filename", "")).startswith("anexo_completo_el-gato-verde_69c5b08526802372d3588a02")


def test_render_preview_uses_bienvenida_naming(tmp_path: Path) -> None:
    renderer = StructuredReportRenderer(artifacts_root=tmp_path)
    preview_payload = {
        "business_name": "El Gato Verde",
        "generated_at": "2026-03-27T10:00:00+00:00",
        "sections": {},
    }

    artifacts = asyncio.run(
        renderer.render_preview(
            preview_payload=preview_payload,
            business_id="69c597fe28c0a48668059680",
            analysis_id="69c5b08526802372d3588a02",
            output_format="html",
        )
    )

    preview_json = artifacts.get("json") or {}
    preview_pdf = artifacts.get("pdf") or {}
    assert "/reportes/" in str(preview_json.get("path"))
    assert str(preview_json.get("filename", "")).startswith(
        "reporte_bienvenida_el-gato-verde_69c5b08526802372d3588a02"
    )
    assert str(preview_pdf.get("filename", "")).startswith(
        "reporte_bienvenida_el-gato-verde_69c5b08526802372d3588a02"
    )
