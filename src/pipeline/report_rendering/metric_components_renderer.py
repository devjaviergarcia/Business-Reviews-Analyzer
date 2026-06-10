from __future__ import annotations

import html
from typing import Any


def render_score_components(renderer: Any, components: Any) -> str:
    if not isinstance(components, dict):
        return ""
    labels = [
        (
            "avg_rating",
            "Valoración media",
            "Media de estrellas. Cuanto más cerca de 5, mejor percepción global.",
            lambda v: f"{renderer._safe_float(v):.2f} / 5",
        ),
        (
            "response_rate",
            "Tasa de respuesta a comentarios",
            "Porcentaje de reseñas respondidas por el negocio.",
            lambda v: f"{renderer._safe_float(v) * 100:.1f}%",
        ),
        (
            "negative_ratio",
            "Proporción de reseñas negativas",
            "Parte de reseñas con experiencia negativa. Cuanto más baja, mejor.",
            lambda v: f"{renderer._safe_float(v) * 100:.1f}%",
        ),
        (
            "sentiment_avg",
            "Sentimiento medio",
            "Mide el tono global de las reseñas (de negativo a positivo).",
            lambda v: f"{renderer._safe_float(v):.2f}",
        ),
        (
            "tranquility_avg",
            "Calma percibida",
            "Mide si el tono es tranquilo o agresivo. Más alto suele ser mejor.",
            lambda v: f"{renderer._safe_float(v):.2f}",
        ),
    ]
    cards: list[str] = []
    for key, title, explain, formatter in labels:
        if key not in components:
            continue
        raw = components.get(key)
        value_num = renderer._safe_float(raw)
        context_label = renderer._metric_context_label(key, value_num)
        display_value = formatter(raw)
        if context_label:
            display_value = f"{display_value} · {context_label}"
        cards.append(
            "<article class='metric-card'>"
            f"<div class='metric-title'>{html.escape(title)}</div>"
            f"<div class='metric-value'>{html.escape(display_value)}</div>"
            f"<div class='metric-explain'>{html.escape(explain)}</div>"
            "</article>"
        )
    return f"<div class='metric-grid'>{''.join(cards)}</div>" if cards else ""
