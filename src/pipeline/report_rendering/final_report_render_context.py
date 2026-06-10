from __future__ import annotations

from typing import Any


def build_final_report_render_context(
    *,
    renderer: Any,
    report_payload: dict[str, Any],
) -> dict[str, Any]:
    sections = report_payload.get("sections")
    if not isinstance(sections, dict):
        sections = {}
    source_analysis = report_payload.get("source_analysis")
    if not isinstance(source_analysis, dict):
        source_analysis = {}
    source_comparison = report_payload.get("source_comparison")
    if not isinstance(source_comparison, dict):
        source_comparison = None

    render_sections = dict(sections)
    google_source_payload = (
        source_analysis.get("google_maps")
        if isinstance(source_analysis.get("google_maps"), dict)
        else None
    )
    tripadvisor_source_payload = (
        source_analysis.get("tripadvisor")
        if isinstance(source_analysis.get("tripadvisor"), dict)
        else None
    )
    has_both_sources = bool(google_source_payload and tripadvisor_source_payload)
    if has_both_sources:
        render_sections["4_lectura_fuente_google_maps"] = google_source_payload
        render_sections["5_lectura_fuente_tripadvisor"] = tripadvisor_source_payload
    if has_both_sources and source_comparison:
        render_sections["7_comparativa_fuentes"] = {
            **source_comparison,
            "google_data": google_source_payload,
            "tripadvisor_data": tripadvisor_source_payload,
        }

    ordered_keys = ordered_report_section_keys(
        report_payload=report_payload,
        render_sections=render_sections,
    )

    anexo_resumen = sections.get("5_anexos_resumen") if isinstance(sections.get("5_anexos_resumen"), dict) else {}
    resumen_dataset = (
        anexo_resumen.get("resumen_dataset")
        if isinstance(anexo_resumen.get("resumen_dataset"), dict)
        else {}
    )
    report_metadata = report_payload.get("report_metadata")
    if not isinstance(report_metadata, dict):
        report_metadata = {}
    metadata_source_counts = (
        report_metadata.get("source_counts")
        if isinstance(report_metadata.get("source_counts"), dict)
        else {}
    )
    if not isinstance(metadata_source_counts, dict):
        metadata_source_counts = {}

    fuentes = (
        {
            str(source).strip().lower(): renderer._safe_int(count)
            for source, count in metadata_source_counts.items()
            if str(source).strip() and renderer._safe_int(count) > 0
        }
        if metadata_source_counts
        else (
            resumen_dataset.get("by_source")
            if isinstance(resumen_dataset.get("by_source"), dict)
            else {}
        )
    )
    total_reviews = renderer._safe_int(sum(fuentes.values())) if fuentes else renderer._safe_int(
        resumen_dataset.get("total_reviews")
    )
    fuentes_label = ", ".join(
        f"{renderer._source_name_spanish(str(source))} ({renderer._safe_int(count)})"
        for source, count in list(fuentes.items())[:4]
        if str(source).strip()
    )

    return {
        "render_sections": render_sections,
        "ordered_keys": ordered_keys,
        "total_reviews": total_reviews,
        "fuentes_label": fuentes_label,
    }


def ordered_report_section_keys(*, report_payload: dict[str, Any], render_sections: dict[str, Any]) -> list[str]:
    preferred_order = [
        "1_resumen_ejecutivo",
        "2_score_reputacion",
        "3_quien_es_tu_cliente_y_que_le_preocupa",
        "4_lectura_fuente_google_maps",
        "5_lectura_fuente_tripadvisor",
        "4_plan_de_accion",
        "7_comparativa_fuentes",
        "5_anexos_resumen",
    ]
    ordered_keys = [key for key in preferred_order if key in render_sections]
    section_order = report_payload.get("section_order")
    if isinstance(section_order, list):
        for key in section_order:
            normalized_key = str(key or "").strip()
            if normalized_key and normalized_key in render_sections and normalized_key not in ordered_keys:
                ordered_keys.append(normalized_key)
    for key in render_sections.keys():
        if key not in ordered_keys:
            ordered_keys.append(key)
    return ordered_keys
