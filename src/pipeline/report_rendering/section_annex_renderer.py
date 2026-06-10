from __future__ import annotations

import html
from typing import Any


def render_dataset_summary_spanish(renderer: Any, dataset: Any) -> str:
    if not isinstance(dataset, dict):
        return ""
    total = renderer._safe_int(dataset.get("total_reviews"))
    avg_rating = renderer._safe_float(dataset.get("avg_rating"))
    response_rate = renderer._safe_float(dataset.get("response_rate"))
    by_source = dataset.get("by_source") if isinstance(dataset.get("by_source"), dict) else {}
    by_problem = dataset.get("by_problem") if isinstance(dataset.get("by_problem"), dict) else {}

    cards = [
        "<article class='metric-card'>"
        "<div class='metric-title'>Reseñas analizadas</div>"
        f"<div class='metric-value'>{total}</div>"
        "<div class='metric-explain'>Cantidad total de opiniones incluidas en este informe.</div>"
        "</article>",
        "<article class='metric-card'>"
        "<div class='metric-title'>Valoración media</div>"
        f"<div class='metric-value'>{avg_rating:.2f} / 5</div>"
        "<div class='metric-explain'>Media de puntuación. Por encima de 4 suele indicar buena percepción.</div>"
        "</article>",
        "<article class='metric-card'>"
        "<div class='metric-title'>Tasa de respuesta a comentarios</div>"
        f"<div class='metric-value'>{response_rate * 100:.1f}%</div>"
        "<div class='metric-explain'>Porcentaje de reseñas que reciben respuesta del negocio.</div>"
        "</article>",
    ]
    source_text = ", ".join(
        f"{renderer._source_name_spanish(str(k))}: {renderer._safe_int(v)}"
        for k, v in by_source.items()
        if str(k).strip()
    )
    problem_text = ", ".join(
        f"{renderer._clean_narrative_text(renderer._humanize_action_text(str(k).replace('_', ' ')))}: {renderer._safe_int(v)}"
        for k, v in list(by_problem.items())[:6]
        if str(k).strip()
    )
    extra = []
    if source_text:
        extra.append(f"<p><strong>Distribución por fuente:</strong> {html.escape(source_text)}</p>")
    if problem_text:
        extra.append(f"<p><strong>Temas más repetidos:</strong> {html.escape(problem_text)}</p>")
    return f"<div class='metric-grid'>{''.join(cards)}</div>{''.join(extra)}"


def render_dimension_guide(renderer: Any, dataset: Any) -> str:
    if not isinstance(dataset, dict):
        return ""
    dims = dataset.get("dimension_averages") if isinstance(dataset.get("dimension_averages"), dict) else {}
    if not dims:
        return ""
    guide = [
        (
            "sentiment",
            "Sentimiento",
            "Resume el tono general de las reseñas. Valores más altos suelen ser mejor.",
            "Una señal saludable suele estar claramente por encima de 0.",
        ),
        (
            "expectation_gap",
            "Brecha de expectativas",
            "Mide cuánto se aleja la experiencia de lo que esperaba el cliente.",
            "Cuanto más cerca de 0, mejor alineación con lo prometido.",
        ),
        (
            "satisfaction",
            "Satisfacción",
            "Nivel de satisfacción global detectado en opiniones y valoración.",
            "Valores altos indican más probabilidad de repetición o recomendación.",
        ),
        (
            "tranquility_aggressiveness",
            "Tranquilidad vs agresividad",
            "Captura si el lenguaje es calmado o tenso/agresivo.",
            "Más alto suele reflejar una conversación más sana con el cliente.",
        ),
        (
            "improvement_intent",
            "Intención de mejora",
            "Cuánto piden cambios concretos los clientes.",
            "Alto no es malo por sí mismo: puede señalar oportunidades claras de mejora.",
        ),
    ]
    rows = []
    for key, title, meaning, reading in guide:
        if key not in dims:
            continue
        value = renderer._safe_float(dims.get(key))
        context_label = renderer._metric_context_label(key, value)
        display_value = f"{value:.2f}"
        if context_label:
            display_value = f"{display_value} · {context_label}"
        rows.append(
            "<article class='metric-card'>"
            f"<div class='metric-title'>{html.escape(title)}</div>"
            f"<div class='metric-value'>{html.escape(display_value)}</div>"
            f"<div class='metric-explain'>{html.escape(meaning)}</div>"
            f"<div class='metric-explain'>{html.escape(reading)}</div>"
            "</article>"
        )
    return f"<div class='metric-grid'>{''.join(rows)}</div>" if rows else ""


def render_voice_quotes(renderer: Any, voces: Any) -> str:
    if not isinstance(voces, dict):
        return ""
    positive = voces.get("positive_quotes") if isinstance(voces.get("positive_quotes"), list) else []
    negative = voces.get("negative_quotes") if isinstance(voces.get("negative_quotes"), list) else []
    improvement = voces.get("improvement_quotes") if isinstance(voces.get("improvement_quotes"), list) else []
    selected = [*positive[:2], *negative[:2], *improvement[:2]]
    cards: list[str] = []
    for item in selected:
        if not isinstance(item, dict):
            continue
        quote = str(item.get("quote", "") or "").strip()
        if not quote:
            continue
        source_label = renderer._source_name_spanish(str(item.get("source", "") or "desconocida"))
        cards.append(
            "<li class='voice-card'>"
            f"<div class='voice-meta'>{html.escape(renderer._anonymize_person_name(str(item.get('author_name', '') or 'Cliente')))} · "
            f"Valoración {renderer._safe_float(item.get('rating')):.1f} · "
            f"Fuente {html.escape(source_label)}</div>"
            f"<div>{html.escape(renderer._clean_narrative_text(quote))}</div>"
            "</li>"
        )
    if not cards:
        return ""
    return f"<ul class='voice-list'>{''.join(cards)}</ul>"
