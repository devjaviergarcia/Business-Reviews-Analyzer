from __future__ import annotations

import html
from typing import Any

from src.pipeline.report_rendering.final_report_intro_block import build_final_report_intro_block
from src.pipeline.report_rendering.final_report_stylesheet import build_final_report_stylesheet
from src.pipeline.report_rendering.font_embedding import load_embedded_font_css


def build_client_audit_report_html(
    *,
    renderer: Any,
    report_payload: dict[str, Any],
    intro_context_text: str,
) -> str:
    business_name = str(report_payload.get("business_name", "") or "").strip() or "Negocio"
    generated_at = str(report_payload.get("generated_at", "") or "")
    generated_human = renderer._format_human_date(generated_at)

    advanced_report = (
        report_payload.get("advanced_report") if isinstance(report_payload.get("advanced_report"), dict) else {}
    )
    report_sections = advanced_report.get("sections") if isinstance(advanced_report.get("sections"), dict) else {}
    source_analysis = (
        advanced_report.get("source_analysis")
        if isinstance(advanced_report.get("source_analysis"), dict)
        else {}
    )
    report_metadata = (
        report_payload.get("report_metadata")
        if isinstance(report_payload.get("report_metadata"), dict)
        else {}
    )
    source_availability = (
        report_metadata.get("source_availability")
        if isinstance(report_metadata.get("source_availability"), dict)
        else {}
    )
    source_counts = (
        report_metadata.get("source_counts") if isinstance(report_metadata.get("source_counts"), dict) else {}
    )
    total_reviews = renderer._safe_int(sum(renderer._safe_int(count) for count in source_counts.values()))
    fuentes_label = ", ".join(
        f"{renderer._source_name_spanish(str(source))} ({renderer._safe_int(count)})"
        for source, count in source_counts.items()
        if renderer._safe_int(count) > 0
    )

    parts: list[str] = [
        build_final_report_intro_block(
            renderer=renderer,
            intro_context_text=intro_context_text,
            total_reviews=total_reviews,
            fuentes_label=fuentes_label,
            generated_human=generated_human,
        )
    ]

    complexity = str(report_payload.get("report_complexity") or "basic").strip().lower()
    cadence = str(report_payload.get("report_cadence") or "one_off").strip().lower()
    hydration = (
        report_payload.get("study_hydration") if isinstance(report_payload.get("study_hydration"), dict) else {}
    )
    include_competitors = bool(report_metadata.get("include_competitors"))
    include_geogrid = bool(report_metadata.get("include_geogrid"))

    parts.append(_render_report_framing(cadence=cadence, complexity=complexity))
    parts.append(
        _wrap_section(
            title="Resumen ejecutivo",
            css_class="section section--diagnostico",
            inner_html=_render_report_section_content(
                renderer=renderer,
                section_key="1_resumen_ejecutivo",
                section_payload=report_sections.get("1_resumen_ejecutivo"),
                extra_payload={"source_availability": source_availability},
            ),
        )
    )
    parts.append(
        _wrap_section(
            title="Puntuación local y por qué",
            css_class="section section--puntuacion",
            inner_html=(
                _render_report_section_content(
                    renderer=renderer,
                    section_key="2_score_reputacion",
                    section_payload=report_sections.get("2_score_reputacion"),
                )
                + _render_score_explanation(hydration=hydration)
            ),
        )
    )
    parts.append(_render_listing_readiness(report_payload=report_payload))
    parts.append(
        _wrap_section(
            title="Qué están diciendo tus clientes",
            css_class="section section--cliente",
            inner_html=_render_report_section_content(
                renderer=renderer,
                section_key="3_quien_es_tu_cliente_y_que_le_preocupa",
                section_payload=report_sections.get("3_quien_es_tu_cliente_y_que_le_preocupa"),
            ),
        )
    )

    if complexity == "hydrated" and include_competitors:
        parts.append(_render_competitor_comparison(hydration=hydration))
    if complexity == "hydrated" and include_geogrid:
        parts.append(_render_geo_visibility(hydration=hydration))

    parts.append(
        _render_source_reading(
            renderer=renderer,
            source_analysis=source_analysis,
            source_availability=source_availability,
        )
    )
    parts.append(
        _wrap_section(
            title="Plan de acción",
            css_class="section section--accion",
            inner_html=_render_report_section_content(
                renderer=renderer,
                section_key="4_plan_de_accion",
                section_payload=report_sections.get("4_plan_de_accion"),
            ),
        )
    )
    if complexity == "hydrated":
        parts.append(_render_ready_to_use_templates(hydration=hydration))

    stylesheet = build_final_report_stylesheet(
        renderer=renderer,
        font_face_css=load_embedded_font_css(),
    )
    return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Auditoría local - {html.escape(business_name)}</title>
    <style>
{stylesheet}
    </style>
  </head>
  <body>
    <main class="wrap">
      <header class="header">
        <h1>Auditoría local de {html.escape(business_name)}</h1>
        <div class="meta">Generado: {html.escape(generated_human)}</div>
      </header>
      {''.join(part for part in parts if part.strip())}
      <div class="footer">Análisis elaborado por Repiq · {html.escape(generated_human)}</div>
    </main>
  </body>
</html>
"""


def _render_report_section_content(
    *,
    renderer: Any,
    section_key: str,
    section_payload: Any,
    extra_payload: dict[str, Any] | None = None,
) -> str:
    payload = section_payload
    if isinstance(section_payload, dict) and extra_payload:
        payload = {**section_payload, **extra_payload}
    if not isinstance(payload, dict):
        return renderer._render_payload(payload)
    generator = renderer._get_section_generator_map().get(section_key)
    if generator is not None:
        return generator.render(payload)
    return renderer._render_payload(payload)


def _wrap_section(*, title: str, css_class: str, inner_html: str) -> str:
    content = str(inner_html or "").strip()
    if not content:
        return ""
    return f"<section class='{css_class}'><h2>{html.escape(title)}</h2>{content}</section>"


def _render_report_framing(*, cadence: str, complexity: str) -> str:
    cadence_label = {
        "one_off": "Informe puntual",
        "monthly": "Lectura mensual",
        "quarterly": "Lectura trimestral",
    }.get(cadence, "Informe puntual")
    complexity_label = "Auditado e hidratado" if complexity == "hydrated" else "Auditoría base"
    return (
        "<section class='intro context-banner'>"
        "<div class='context-row'>"
        f"<div class='context-item'><strong>Formato:</strong> {html.escape(cadence_label)}</div>"
        f"<div class='context-item'><strong>Nivel:</strong> {html.escape(complexity_label)}</div>"
        "<div class='context-item'><strong>Objetivo:</strong> entender qué está funcionando, qué frena la conversión local y qué priorizar ahora.</div>"
        "</div>"
        "</section>"
    )


def _render_listing_readiness(*, report_payload: dict[str, Any]) -> str:
    readiness = (
        report_payload.get("listing_readiness")
        if isinstance(report_payload.get("listing_readiness"), dict)
        else {}
    )
    items = readiness.get("items") if isinstance(readiness.get("items"), list) else []
    if not items:
        return ""

    cards = []
    for item in items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "missing").strip().lower()
        css = "fw-card fw-strong" if status == "ok" else "fw-card fw-weak"
        marker = "+" if status == "ok" else "!"
        value = str(item.get("value") or "").strip()
        note = str(item.get("note") or "").strip()
        cards.append(
            f"<article class='{css}'>"
            f"<div class='fw-icon'>{marker}</div>"
            "<div>"
            f"<div class='fw-title'>{html.escape(str(item.get('label') or ''))}</div>"
            + (f"<div class='fw-desc'>{html.escape(value)}</div>" if value else "")
            + (f"<div class='fw-action'>{html.escape(note)}</div>" if note else "")
            + "</div></article>"
        )

    headline = (
        f"{int(readiness.get('completed') or 0)}/{int(readiness.get('total') or len(items))} "
        "elementos clave visibles en la ficha"
    )
    return (
        "<section class='section section--diagnostico'>"
        "<h2>Preparación de ficha</h2>"
        f"<p>{html.escape(headline)}</p>"
        f"<div class='fw-grid'>{''.join(cards)}</div>"
        "</section>"
    )


def _render_score_explanation(*, hydration: dict[str, Any]) -> str:
    benchmark = hydration.get("benchmark") if isinstance(hydration.get("benchmark"), dict) else {}
    deep_study = (
        benchmark.get("deep_study_snapshot")
        if isinstance(benchmark.get("deep_study_snapshot"), dict)
        else {}
    )
    explanation = (
        deep_study.get("score_explanation") if isinstance(deep_study.get("score_explanation"), dict) else {}
    )
    positives = explanation.get("strengths") if isinstance(explanation.get("strengths"), list) else []
    frictions = explanation.get("frictions") if isinstance(explanation.get("frictions"), list) else []
    if not positives and not frictions:
        return ""

    positive_cards = "".join(
        "<article class='fw-card fw-strong'>"
        "<div class='fw-icon'>+</div><div>"
        f"<div class='fw-title'>{html.escape(str(item or ''))}</div>"
        "</div></article>"
        for item in positives[:4]
        if str(item or "").strip()
    )
    friction_cards = "".join(
        "<article class='fw-card fw-weak'>"
        "<div class='fw-icon'>!</div><div>"
        f"<div class='fw-title'>{html.escape(str(item or ''))}</div>"
        "</div></article>"
        for item in frictions[:4]
        if str(item or "").strip()
    )
    if not positive_cards and not friction_cards:
        return ""

    return (
        "<h3>Qué empuja el score y qué hoy lo frena</h3>"
        "<div class='fw-grid'>"
        "<div>"
        "<div class='fw-col-title fw-col-strong'>Lo que hoy ayuda</div>"
        f"{positive_cards or '<p class=\"muted\">Sin palancas destacadas en este corte.</p>'}"
        "</div>"
        "<div>"
        "<div class='fw-col-title fw-col-weak'>Lo que hoy penaliza</div>"
        f"{friction_cards or '<p class=\"muted\">Sin frenos destacados en este corte.</p>'}"
        "</div>"
        "</div>"
    )


def _render_competitor_comparison(*, hydration: dict[str, Any]) -> str:
    benchmark = hydration.get("benchmark") if isinstance(hydration.get("benchmark"), dict) else {}
    status = str(
        benchmark.get("presence_state") or hydration.get("business_presence_state") or "study_scope_unresolved"
    ).strip().lower()
    benchmark_run = benchmark.get("benchmark_run") if isinstance(benchmark.get("benchmark_run"), dict) else {}
    benchmark_business = (
        benchmark.get("benchmark_business") if isinstance(benchmark.get("benchmark_business"), dict) else {}
    )
    competitors = benchmark.get("competitors") if isinstance(benchmark.get("competitors"), list) else []
    deep_study = (
        benchmark.get("deep_study_snapshot")
        if isinstance(benchmark.get("deep_study_snapshot"), dict)
        else {}
    )

    if status != "present_in_study" or not benchmark_business:
        return _render_study_presence_note(
            title="Comparativa con competidores",
            status=status,
            scope=hydration.get("scope"),
        )

    competitor_rows = []
    for item in competitors[:5]:
        if not isinstance(item, dict):
            continue
        competitor_rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('business_name') or ''))}</td>"
            f"<td>{html.escape(str(item.get('relative_position') or item.get('discovery_rank') or '—'))}</td>"
            f"<td>{html.escape(str(item.get('rating') or '—'))}</td>"
            f"<td>{html.escape(str(item.get('review_count') or '—'))}</td>"
            "</tr>"
        )

    gaps = deep_study.get("competitor_gaps") if isinstance(deep_study.get("competitor_gaps"), list) else []
    gap_list = (
        "<ul>"
        + "".join(f"<li>{html.escape(str(item or ''))}</li>" for item in gaps[:4] if str(item or "").strip())
        + "</ul>"
        if gaps
        else "<p class='muted'>No se generaron gaps competitivos claros en este corte.</p>"
    )
    target_metrics = (
        f"<p><strong>Negocio analizado:</strong> posición #{benchmark_business.get('discovery_rank') or '—'} · "
        f"rating {benchmark_business.get('rating') or '—'} · "
        f"{benchmark_business.get('review_count') or '—'} reseñas</p>"
    )

    return (
        "<section class='section section--diagnostico'>"
        "<h2>Comparativa con competidores</h2>"
        f"<p><strong>Scope del estudio:</strong> {html.escape(str(benchmark_run.get('query') or ''))}</p>"
        f"{target_metrics}"
        "<table><thead><tr><th>Negocio</th><th>Posición</th><th>Rating</th><th>Reseñas</th></tr></thead><tbody>"
        + ("".join(competitor_rows) or "<tr><td colspan='4'>No hay competidores comparables en este corte.</td></tr>")
        + "</tbody></table>"
        "<h3>Qué dice esto comercialmente</h3>"
        f"{gap_list}"
        "</section>"
    )


def _render_geo_visibility(*, hydration: dict[str, Any]) -> str:
    geogrid = hydration.get("geogrid") if isinstance(hydration.get("geogrid"), dict) else {}
    status = str(
        geogrid.get("presence_state") or hydration.get("business_presence_state") or "study_scope_unresolved"
    ).strip().lower()
    run = geogrid.get("geo_grid_run") if isinstance(geogrid.get("geo_grid_run"), dict) else {}
    stats = geogrid.get("geo_grid_stats") if isinstance(geogrid.get("geo_grid_stats"), dict) else {}
    business = geogrid.get("geo_grid_business") if isinstance(geogrid.get("geo_grid_business"), dict) else {}

    if status != "present_in_study" or not business:
        return _render_study_presence_note(
            title="Visibilidad geográfica",
            status=status,
            scope=hydration.get("scope"),
        )

    summary = stats.get("summary") if isinstance(stats.get("summary"), dict) else {}
    coverage = business.get("coverage_percent")
    avg_rank = business.get("avg_rank")
    best_rank = business.get("best_rank")
    top3 = business.get("top_3_count")
    notes = [
        f"Keyword analizada: {run.get('keyword') or summary.get('keyword') or '—'}",
        f"Cobertura: {coverage if coverage is not None else '—'}%",
        f"Ranking medio: {avg_rank if avg_rank is not None else '—'}",
        f"Mejor posición: {best_rank if best_rank is not None else '—'}",
        f"Apariciones en top 3: {top3 if top3 is not None else '—'}",
    ]
    rows = "".join(f"<li>{html.escape(str(item))}</li>" for item in notes)
    return (
        "<section class='section section--diagnostico'>"
        "<h2>Visibilidad geográfica</h2>"
        "<p>Esta lectura indica cómo cambia la visibilidad del negocio según desde qué zona de la ciudad busca el usuario.</p>"
        f"<ul>{rows}</ul>"
        "</section>"
    )


def _render_source_reading(
    *,
    renderer: Any,
    source_analysis: dict[str, Any],
    source_availability: dict[str, Any],
) -> str:
    google_payload = (
        source_analysis.get("google_maps")
        if isinstance(source_analysis.get("google_maps"), dict)
        else None
    )
    trip_payload = (
        source_analysis.get("tripadvisor")
        if isinstance(source_analysis.get("tripadvisor"), dict)
        else None
    )
    trip_availability = (
        source_availability.get("tripadvisor")
        if isinstance(source_availability.get("tripadvisor"), dict)
        else None
    )

    google_html = _render_report_section_content(
        renderer=renderer,
        section_key="4_lectura_fuente_google_maps",
        section_payload=google_payload,
    )
    if trip_payload is None and trip_availability is not None:
        trip_payload = {"availability_notice": trip_availability}
    trip_html = _render_report_section_content(
        renderer=renderer,
        section_key="5_lectura_fuente_tripadvisor",
        section_payload=trip_payload,
    )
    return _wrap_section(
        title="Lectura por fuente",
        css_class="section section--fuente",
        inner_html=f"{google_html}{trip_html}",
    )


def _render_ready_to_use_templates(*, hydration: dict[str, Any]) -> str:
    benchmark = hydration.get("benchmark") if isinstance(hydration.get("benchmark"), dict) else {}
    deep_study = (
        benchmark.get("deep_study_snapshot")
        if isinstance(benchmark.get("deep_study_snapshot"), dict)
        else {}
    )
    templates = deep_study.get("response_templates") if isinstance(deep_study.get("response_templates"), list) else []
    actions = deep_study.get("monthly_actions") if isinstance(deep_study.get("monthly_actions"), list) else []
    if not templates and not actions:
        return ""

    template_cards = []
    for item in templates[:4]:
        if not isinstance(item, dict):
            continue
        template_cards.append(
            "<article class='fw-card fw-strong'>"
            "<div class='fw-icon'>↺</div><div>"
            f"<div class='fw-title'>{html.escape(str(item.get('scenario') or 'Template'))}</div>"
            f"<div class='fw-desc'>{html.escape(str(item.get('template') or ''))}</div>"
            "</div></article>"
        )

    action_items = []
    for item in actions[:4]:
        if not isinstance(item, dict):
            continue
        action_items.append(
            "<article class='fw-card fw-weak'>"
            "<div class='fw-icon'>•</div><div>"
            f"<div class='fw-title'>{html.escape(str(item.get('title') or 'Action'))}</div>"
            f"<div class='fw-desc'>{html.escape(str(item.get('rationale') or ''))}</div>"
            f"<div class='fw-action'>{html.escape(str(item.get('expected_impact') or ''))}</div>"
            "</div></article>"
        )

    return (
        "<section class='section section--accion'>"
        "<h2>Plantillas listas para usar</h2>"
        + ("<h3>Respuestas sugeridas</h3><div class='fw-grid'>" + "".join(template_cards) + "</div>" if template_cards else "")
        + ("<h3>Siguientes acciones recomendadas</h3><div class='fw-grid'>" + "".join(action_items) + "</div>" if action_items else "")
        + "</section>"
    )


def _render_study_presence_note(*, title: str, status: str, scope: Any) -> str:
    scope_payload = scope if isinstance(scope, dict) else {}
    query = str(scope_payload.get("benchmark_query") or "").strip() or "el scope actual"
    messages = {
        "not_in_latest_study": "El negocio no aparece dentro del último estudio compatible reutilizado, así que esta sección se muestra como no disponible en lugar de inventar comparativas.",
        "not_in_fresh_study": "Se ha resuelto un estudio nuevo, pero el negocio sigue sin aparecer dentro de ese scope. La sección queda marcada explícitamente.",
        "study_scope_unresolved": "No se pudo resolver con suficiente confianza la ciudad o categoría desde la ficha actual, así que esta parte se degrada de forma explícita.",
    }
    detail = messages.get(status, messages["study_scope_unresolved"])
    return (
        "<section class='section section--diagnostico'>"
        f"<h2>{html.escape(title)}</h2>"
        f"<p>{html.escape(detail)}</p>"
        f"<p class='muted'><strong>Scope:</strong> {html.escape(query)}</p>"
        "</section>"
    )
