from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def is_empty_payload(payload: Any) -> bool:
    if payload is None:
        return True
    if isinstance(payload, str):
        return not str(payload).strip()
    if isinstance(payload, (list, tuple, set)):
        return len(payload) == 0
    if isinstance(payload, dict):
        return len(payload) == 0
    return False


def normalize_text(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = re.sub(r"\s+", " ", raw)
    normalized = normalized.replace("_", " ")
    normalized = re.sub(r"[^a-z0-9áéíóúüñ ]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def format_human_date(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "fecha no disponible"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return raw[:10] if len(raw) >= 10 else raw
    months = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    return f"{dt.day} de {months[dt.month - 1]} de {dt.year}"


def infer_action_type_from_text(text: str, *, normalize_text) -> str:
    normalized = normalize_text(text)
    if any(token in normalized for token in ("crm", "software", "automat", "dashboard", "alerta")):
        return "tecnologico"
    if any(token in normalized for token in ("implementar", "integrar", "desarrollar")):
        return "implementacion"
    if any(token in normalized for token in ("precio", "margen", "carta", "menu", "estrategia")):
        return "negocio"
    return "proceso"


def infer_action_tool_from_text(text: str, *, normalize_text) -> str:
    normalized = normalize_text(text)
    if any(token in normalized for token in ("resena", "reseña", "responder")):
        return "Panel de reseñas y plantilla de respuesta"
    if any(token in normalized for token in ("tiempo", "espera", "comanda")):
        return "Registro de tiempos por turno"
    if any(token in normalized for token in ("formacion", "formación", "protocolo", "equipo")):
        return "Guía operativa y sesión interna"
    if any(token in normalized for token in ("precio", "menu", "carta")):
        return "Revisión de carta y costes"
    return ""


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def slugify(value: str) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9._-]+", "-", raw)
    raw = raw.strip("-")
    return raw or "item"


def safe_identifier_slug(value: str) -> str:
    slug = slugify(value)
    return slug[:64] if slug else "id"


def safe_name_slug(value: str) -> str:
    slug = slugify(value)
    if not slug:
        return "negocio"
    return slug[:60]


def json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
