from __future__ import annotations


def metric_context_label(key: str, value: float, *, normalize_text) -> str:
    normalized = normalize_text(key)
    if normalized in {"sentiment avg", "sentiment", "overall sentiment"}:
        if value >= 0.6:
            return "Tono positivo"
        if value >= 0.2:
            return "Tono favorable"
        if value > -0.2:
            return "Tono mixto"
        return "Tono negativo"
    if normalized in {"tranquility avg", "tranquility aggressiveness"}:
        if value >= 0.85:
            return "Muy tranquilo"
        if value >= 0.65:
            return "Tranquilo"
        if value >= 0.45:
            return "Con algo de tensión"
        return "Tenso"
    if normalized == "satisfaction":
        if value >= 0.8:
            return "Alta"
        if value >= 0.6:
            return "Media"
        return "Baja"
    if normalized == "expectation gap":
        if value <= 0.12:
            return "Expectativas bien gestionadas"
        if value <= 0.3:
            return "Hay margen de ajuste"
        return "Brecha relevante"
    if normalized == "improvement intent":
        if value <= 0.15:
            return "Baja - clientes satisfechos"
        if value <= 0.35:
            return "Moderada"
        return "Alta — piden cambios"
    if normalized == "negative ratio":
        if value <= 0.08:
            return "Bajo"
        if value <= 0.18:
            return "Medio"
        return "Alto"
    if normalized == "avg rating":
        if value >= 4.5:
            return "Excelente"
        if value >= 4.0:
            return "Buena"
        if value >= 3.5:
            return "Aceptable"
        return "Mejorable"
    if normalized == "response rate":
        if value >= 0.7:
            return "Muy activa"
        if value >= 0.4:
            return "Aceptable"
        if value > 0.0:
            return "Baja"
        return "Sin respuestas"
    return ""


def severity_band(value: float) -> str:
    if value >= 0.7:
        return "Alta"
    if value >= 0.4:
        return "Media"
    return "Baja"


def humanize_effort(*, effort: str) -> str:
    value = str(effort or "").strip().lower()
    mapping = {
        "low": "bajo",
        "medium": "medio",
        "high": "alto",
        "bajo": "bajo",
        "medio": "medio",
        "alto": "alto",
    }
    return mapping.get(value, "medio")


def humanize_impact(*, impact: str) -> str:
    value = str(impact or "").strip().lower()
    mapping = {
        "low": "bajo",
        "medium": "medio",
        "high": "alto",
        "bajo": "bajo",
        "medio": "medio",
        "alto": "alto",
    }
    return mapping.get(value, "medio")


def humanize_action_type_label(action_type: str) -> str:
    value = str(action_type or "").strip().lower()
    mapping = {
        "proceso": "Proceso interno",
        "negocio": "Decisión de negocio",
        "implementacion": "Implementación",
        "tecnologico": "Solución tecnológica",
    }
    return mapping.get(value, "Proceso interno")


def action_type_badge(action_type: str) -> dict[str, str]:
    value = str(action_type or "").strip().lower()
    mapping = {
        "proceso": {
            "label": "Proceso interno",
            "bg": "#e3f0ff",
            "text": "#1a5fa8",
            "border": "#b3d1f5",
        },
        "negocio": {
            "label": "Decisión de negocio",
            "bg": "#fdf0e3",
            "text": "#a85f1a",
            "border": "#f5d1b3",
        },
        "implementacion": {
            "label": "Implementación",
            "bg": "#f3e3ff",
            "text": "#7a1aa8",
            "border": "#d9b3f5",
        },
        "tecnologico": {
            "label": "Solución tecnológica",
            "bg": "#e3fff0",
            "text": "#1aa85f",
            "border": "#b3f5d1",
        },
    }
    return mapping.get(value, mapping["proceso"])


def score_badge(score: float) -> str:
    if score >= 85.0:
        return "<span class='badge good'>Excelente reputación</span>"
    if score >= 70.0:
        return "<span class='badge good'>Reputación sólida</span>"
    if score >= 55.0:
        return "<span class='badge warn'>Reputación media mejorable</span>"
    if score >= 40.0:
        return "<span class='badge warn'>Reputación mejorable</span>"
    return "<span class='badge bad'>Reputación crítica</span>"
