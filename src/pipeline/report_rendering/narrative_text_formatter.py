from __future__ import annotations

import html
import re


def clean_narrative_text(value: str, *, humanize_action_text) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("**", "")
    text = humanize_action_text(text)
    text = re.sub(r"\bimpactoo\b", "impacto", text, flags=re.IGNORECASE)
    text = re.sub(
        r"\b([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{4,})([aeiouáéíóúüAEIOUÁÉÍÓÚÜ])\2\b",
        r"\1\2",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def anonymize_person_name(name: str) -> str:
    clean = str(name or "").strip()
    if not clean:
        return "C********"
    first = clean[0].upper()
    return f"{first}{'*' * 7}"


def source_name_spanish(source: str) -> str:
    normalized = str(source or "").strip().lower()
    if not normalized:
        return "fuente no identificada"
    mapping = {
        "google_maps": "Google Maps",
        "tripadvisor": "Tripadvisor",
        "trustpilot": "Trustpilot",
        "booking": "Booking",
        "reddit": "Reddit",
        "unknown": "fuente no identificada",
    }
    if normalized in mapping:
        return mapping[normalized]
    return normalized.replace("_", " ")


def icon_slot(icon_name: str) -> str:
    safe_name = html.escape(str(icon_name or "").strip().lower(), quote=True)
    return f"<span class='icon-slot' data-icon='{safe_name}' aria-hidden='true'></span>"


def labelize_key_spanish(key: str) -> str:
    normalized = str(key or "").strip()
    if not normalized:
        return "Dato"
    mapping = {
        "avg_rating": "Valoración media",
        "response_rate": "Tasa de respuesta a comentarios",
        "negative_ratio": "Proporción de reseñas negativas",
        "sentiment_avg": "Sentimiento medio",
        "tranquility_avg": "Calma percibida",
        "trend": "Evolución general",
        "trend_slope": "Ritmo de cambio",
        "analyses_history": "Histórico de análisis",
        "satisfaction_by_relative_time_bucket": "Satisfacción por antigüedad de reseña",
        "satisfaccion_por_antiguedad_resena": "Satisfacción por antigüedad de reseña",
        "score_scale": "Escala de puntuación",
        "target_rank": "Posición del negocio",
        "total_competitors_compared": "Competidores comparados",
        "total_businesses_compared": "Negocios analizados en la comparativa",
        "target_reputation_score": "Puntuación del negocio",
        "top_competitors": "Competidores destacados",
        "total_reviews": "Reseñas totales",
        "by_source": "Distribución por fuente",
        "by_problem": "Distribución por tema",
        "dimension_averages": "Promedio de dimensiones",
        "overall_sentiment": "Sentimiento del periodo",
        "review_count": "Número de reseñas",
        "cluster_count": "Número de tipos de cliente",
        "cluster_id": "Tipo de cliente",
        "review_rows": "Reseñas",
        "dominant_problem": "Tema principal",
        "has_owner_reply": "Tiene respuesta del negocio",
        "owner_reply_excerpt": "Respuesta del negocio (extracto)",
        "review_excerpt": "Extracto de reseña",
        "created_at": "Fecha",
        "source": "Fuente",
        "author_name": "Cliente",
        "rating": "Valoración",
        "score_display": "Puntuación mostrada",
        "nivel_reputacion": "Nivel de reputación",
        "problema": "Problema",
        "severidad": "Severidad",
        "volumen": "Volumen",
        "ejemplo_literal": "Ejemplo literal",
        "impact": "Impacto",
        "owner": "Encargado",
        "horizon_days": "Plazo (días)",
        "kpi": "Indicador de seguimiento",
        "old": "Antiguas",
        "medium": "Intermedias",
        "recent": "Recientes",
        "unknown": "Sin fecha clara",
    }
    if normalized in mapping:
        return mapping[normalized]
    prettified = normalized.replace("_", " ").replace("-", " ").strip()
    if not prettified:
        return "Dato"
    return prettified[:1].upper() + prettified[1:]


def humanize_action_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    output = value
    output = re.sub(
        r"satisfaction by relative time bucket",
        "Satisfacción por antigüedad de reseña",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"^corregir incidencias de ['\"]?([^'\"]+)['\"]? con checklist operativo diario\.?$",
        r"Mejorar de inmediato '\1' con una rutina diaria de revisión.",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"^estandarizar proceso y formación sobre ['\"]?([^'\"]+)['\"]?\.?$",
        r"Ordenar el proceso y formar al equipo para evitar fallos en '\1'.",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"^automatizar seguimiento de señales tempranas de ['\"]?([^'\"]+)['\"]?\.?$",
        r"Crear un seguimiento continuo para detectar pronto fallos en '\1'.",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"^micro-acción sobre ['\"]?([^'\"]+)['\"]?\.?$",
        r"Acción rápida sobre '\1'.",
        output,
        flags=re.IGNORECASE,
    )
    replacements = (
        ("checklist operativo diario", "rutina diaria de revisión"),
        ("checklist", "guía de tareas"),
        ("checklists", "guías de tareas"),
        ("micro-acción", "acción rápida"),
        ("quick wins", "acciones rápidas"),
        ("Data/Producto", "Dirección y mejora de procesos"),
        ("Gerencia + Calidad", "Gerencia y calidad"),
        ("Responsable de operación", "Encargado de operaciones"),
        ("precio_valor", "relación calidad-precio"),
        ("calidad_comida", "calidad de la comida"),
        ("tiempo_espera", "tiempo de espera"),
        ("gestion_reservas", "gestión de reservas"),
        ("ambiente_ruido", "ambiente y ruido"),
        ("<24h", "menos de 24 horas"),
        ("KPI", "indicador de seguimiento"),
        ("KPIs", "indicadores de seguimiento"),
        ("owner", "encargado"),
        ("impact", "impacto"),
        ("score", "puntuación de reputación"),
        ("trend", "tendencia"),
        ("response rate", "tasa de respuesta a comentarios"),
        ("bucket", "tramo temporal"),
        ("dataset", "conjunto de reseñas"),
        ("old", "antiguas"),
        ("medium", "intermedias"),
        ("recent", "recientes"),
    )
    for src, dst in replacements:
        output = re.sub(re.escape(src), dst, output, flags=re.IGNORECASE)
    output = re.sub(r"\bel tendencia\b", "la tendencia", output, flags=re.IGNORECASE)
    output = re.sub(
        r"\bservicio en reseñas negativas un 25%\b",
        "las menciones negativas sobre el servicio en un 25%",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"\bcalidad de la comida en reseñas negativas un 25%\b",
        "las menciones negativas sobre la calidad de la comida en un 25%",
        output,
        flags=re.IGNORECASE,
    )
    output = re.sub(
        r"\brelación calidad-precio en reseñas negativas un 25%\b",
        "las menciones negativas sobre la relación calidad-precio en un 25%",
        output,
        flags=re.IGNORECASE,
    )
    return output


def humanize_sentiment_value(value: str, *, normalize_text) -> str:
    normalized = normalize_text(value)
    mapping = {
        "positive": "Positivo",
        "mixed": "Mixto",
        "negative": "Negativo",
        "positivo": "Positivo",
        "mixto": "Mixto",
        "negativo": "Negativo",
    }
    return mapping.get(normalized, value)


def humanize_trend_value(value: str, *, normalize_text) -> str:
    normalized = normalize_text(value)
    mapping = {
        "al alza": "Al alza",
        "al_alza": "Al alza",
        "a la baja": "A la baja",
        "a_la_baja": "A la baja",
        "estable": "Estable",
    }
    return mapping.get(normalized, value)
