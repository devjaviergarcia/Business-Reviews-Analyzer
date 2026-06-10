from __future__ import annotations

import re
import unicodedata
from collections import Counter
from typing import Any


def normalize_action_type(value: str, *, normalize_text) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    mapping = {
        "proceso": "proceso",
        "process": "proceso",
        "negocio": "negocio",
        "business": "negocio",
        "implementacion": "implementacion",
        "implementación": "implementacion",
        "implementation": "implementacion",
        "tecnologico": "tecnologico",
        "tecnológica": "tecnologico",
        "tecnologico/a": "tecnologico",
        "tecnologia": "tecnologico",
        "technology": "tecnologico",
    }
    for key, item in mapping.items():
        if key in normalized:
            return item
    return ""


def infer_action_type(text: str, *, normalize_text) -> str:
    normalized = normalize_text(text)
    if any(token in normalized for token in ("crm", "software", "automat", "panel", "dashboard", "alerta")):
        return "tecnologico"
    if any(token in normalized for token in ("implementar", "desarrollar", "integrar", "despliegue", "prototipo")):
        return "implementacion"
    if any(token in normalized for token in ("precio", "menu", "margen", "tarifa", "promocion", "estrategia")):
        return "negocio"
    return "proceso"


def infer_action_tool(text: str, *, normalize_text) -> str:
    normalized = normalize_text(text)
    if any(token in normalized for token in ("resena", "reseña", "responder", "review")):
        return "Panel de reseñas y plantilla breve de respuesta"
    if any(token in normalized for token in ("tiempo", "espera", "minuto", "comanda")):
        return "Registro de tiempos por turno"
    if any(token in normalized for token in ("formacion", "formación", "protocolo", "equipo")):
        return "Guía operativa y sesión interna semanal"
    if any(token in normalized for token in ("precio", "menu", "carta")):
        return "Revisión de carta y tabla simple de costes"
    return ""


def human_label_problem(label: str, *, plainify_business_text) -> str:
    value = str(label or "").strip()
    if not value:
        return "Experiencia general"
    return plainify_business_text(value).replace("_", " ")


def severity_label(value: float) -> str:
    if value >= 0.7:
        return "alta"
    if value >= 0.4:
        return "media"
    return "baja"


def plainify_business_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    replacements = (
        ("satisfaction by relative time bucket", "satisfacción por antigüedad de reseña"),
        ("cluster", "tipo de cliente"),
        ("clusters", "tipos de cliente"),
        ("kpi", "indicador de seguimiento"),
        ("kpis", "indicadores de seguimiento"),
        ("owner", "encargado"),
        ("impact", "impacto"),
        ("benchmark", "comparativa"),
        ("benchmarking", "comparativa con competidores"),
        ("quick wins", "acciones rápidas"),
        ("insight", "hallazgo"),
        ("roadmap", "plan"),
        ("horizon", "plazo"),
        ("score", "puntuación de reputación"),
        ("trend", "tendencia"),
        ("response rate", "tasa de respuesta a comentarios"),
        ("dataset", "conjunto de reseñas"),
        ("bucket", "tramo temporal"),
        ("<24h", "menos de 24 horas"),
        ("old", "antiguas"),
        ("medium", "intermedias"),
        ("recent", "recientes"),
    )
    lowered = value
    for src, dst in replacements:
        lowered = re.sub(rf"\b{re.escape(src)}\b", dst, lowered, flags=re.IGNORECASE)
    lowered = re.sub(r"\bel tendencia\b", "la tendencia", lowered, flags=re.IGNORECASE)
    lowered = lowered.replace("**", "")
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def friendly_problem_label(
    value: str,
    *,
    generic_comment_problem: str,
    positive_comment_problem: str,
    negative_comment_problem: str,
    no_comment_high_problem: str,
    no_comment_medium_problem: str,
    no_comment_low_problem: str,
) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "experiencia del cliente"
    mapped = {
        generic_comment_problem: "experiencia general",
        positive_comment_problem: "experiencia general positiva",
        negative_comment_problem: "experiencia general negativa",
        no_comment_high_problem: "valoración alta sin comentario",
        no_comment_medium_problem: "valoración media sin comentario",
        no_comment_low_problem: "valoración baja sin comentario",
        "tiempo_espera": "tiempo de espera",
        "precio_valor": "relación calidad-precio",
        "calidad_comida": "calidad de la comida",
        "ambiente_ruido": "ambiente y ruido",
        "gestion_reservas": "gestión de reservas",
    }
    if raw in mapped:
        return mapped[raw]
    normalized = raw.replace("_", " ").strip()
    return normalized


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def count_keyword_hits(text: str, keywords: tuple[str, ...], *, normalize_text) -> int:
    if not text:
        return 0
    count = 0
    for keyword in keywords:
        normalized = normalize_text(keyword)
        if not normalized:
            continue
        if normalized in text:
            count += 1
    return count


def extract_top_keywords(
    *,
    items: list[dict[str, Any]],
    limit: int,
    stopwords: set[str],
    normalize_text,
) -> list[str]:
    token_counter: Counter[str] = Counter()
    for item in items:
        text = normalize_text(str(item.get("text", "") or ""))
        tokens = re.findall(r"[a-záéíóúñü]{3,}", text, flags=re.IGNORECASE)
        for token in tokens:
            normalized = normalize_text(token)
            if not normalized or normalized in stopwords:
                continue
            token_counter[normalized] += 1
    return [term for term, _ in token_counter.most_common(limit)]


def compress_text(text: str, *, max_chars: int) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    max_chars = max(40, int(max_chars))
    normalized = re.sub(r"\s+", " ", raw).strip()
    if len(normalized) <= max_chars:
        return normalized
    clipped = normalized[: max_chars - 1].rstrip()
    last_space = clipped.rfind(" ")
    if last_space >= 32:
        clipped = clipped[:last_space]
    return clipped.rstrip(".,;:- ") + "…"
