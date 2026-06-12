from __future__ import annotations

import asyncio
import json
from typing import Any, Callable


async def build_llm_source_narrative(
    *,
    source: str,
    business_name: str,
    business_context: dict[str, Any],
    stats: dict[str, Any],
    customer_clusters: dict[str, Any],
    problem_clusters: dict[str, Any],
    summarize_problem_clusters: Callable[..., list[dict[str, Any]]],
    human_label_problem: Callable[[str], str],
    can_use_llm: Callable[[], bool],
    llm_generate_text: Callable[[str], tuple[str, str | None]],
    extract_json_object: Callable[[str], str],
    safe_float: Callable[[Any, float], float],
    safe_int: Callable[[Any, int], int],
    sanitize_llm_text: Callable[[str], str],
    plainify_business_text: Callable[[str], str],
) -> dict[str, Any]:
    source_key = str(source or "").strip().lower() or "unknown"
    source_label = {
        "google_maps": "Google Maps",
        "tripadvisor": "Tripadvisor",
    }.get(source_key, source_key.replace("_", " ").title())
    top_problems = summarize_problem_clusters(problem_clusters=problem_clusters, limit=3)
    top_problem_labels = _normalize_problem_labels(
        [
            human_label_problem(str(item.get("problema", "") or item.get("problem", "") or ""))
            for item in top_problems
            if isinstance(item, dict)
        ]
    )
    fallback_note = (
        "En Tripadvisor el perfil suele ser más crítico y turístico; conviene comparar con Google Maps "
        "antes de sacar conclusiones definitivas."
        if source_key == "tripadvisor"
        else None
    )
    fallback: dict[str, Any] = {
        "narrativa": (
            f"En {source_label}, {business_name or 'el negocio'} tiene una valoración media de "
            f"{safe_float(stats.get('avg_rating'), 0.0):.2f}/5 y una tasa de respuesta del "
            f"{safe_float(stats.get('response_rate'), 0.0) * 100:.1f}%. "
            + (
                "Los puntos de fricción más repetidos son: "
                + ", ".join(top_problem_labels[:3])
                + "."
                if top_problem_labels
                else "No se detectan focos de fricción repetidos con suficiente volumen."
            )
        ),
        "top_fortalezas": [
            f"Valoración media de {safe_float(stats.get('avg_rating'), 0.0):.2f} sobre 5",
            "Hay opiniones positivas recientes que sostienen la reputación.",
        ][:3],
        "top_problemas": top_problem_labels[:3],
        "nota_sesgo": fallback_note,
    }

    prompt_payload = {
        "business_name": business_name,
        "source": source_key,
        "tipo_negocio": str((business_context or {}).get("tipo_negocio", "") or "negocio local"),
        "stats": {
            "avg_rating": round(safe_float(stats.get("avg_rating"), 0.0), 4),
            "response_rate": round(safe_float(stats.get("response_rate"), 0.0), 4),
            "review_count": safe_int(
                customer_clusters.get("total_reviews") if isinstance(customer_clusters, dict) else 0,
                0,
            ),
        },
        "customer_clusters": (
            customer_clusters.get("clusters")[:4]
            if isinstance(customer_clusters.get("clusters"), list)
            else []
        ),
        "problem_clusters": (
            problem_clusters.get("clusters")[:5]
            if isinstance(problem_clusters.get("clusters"), list)
            else []
        ),
    }

    if not can_use_llm():
        return fallback

    tripadvisor_bias_instruction = (
        "IMPORTANTE: Tripadvisor suele concentrar perfiles más críticos y más turísticos que Google Maps. "
        "Las valoraciones negativas pueden verse amplificadas por ese sesgo de perfil. "
        "Distingue entre sesgo de plataforma y problema operativo real; no sobrerreacciones si el volumen es bajo.\n"
        if source_key == "tripadvisor"
        else ""
    )
    prompt = (
        "Eres consultor de reputación para negocios locales en España. "
        f"Analiza ÚNICAMENTE la fuente '{source_key}' del negocio '{business_name or 'negocio local'}'.\n"
        f"{tripadvisor_bias_instruction}"
        "Escribe en español de España, sin anglicismos y sin jerga técnica.\n"
        "Devuelve SOLO JSON válido con esta estructura exacta:\n"
        "{\n"
        '  "narrativa": "3-5 frases directas sobre lo que muestra esta fuente",\n'
        '  "top_fortalezas": ["string"],\n'
        '  "top_problemas": ["string"],\n'
        '  "nota_sesgo": "string o null"\n'
        "}\n"
        "Sin markdown y sin texto fuera del JSON.\n"
        f"Datos de la fuente:\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )
    try:
        text, _model_used = await asyncio.to_thread(llm_generate_text, prompt)
        extracted = extract_json_object(text)
        parsed = json.loads(extracted)
    except Exception:
        return fallback

    if not isinstance(parsed, dict):
        return fallback
    narrativa = sanitize_llm_text(
        plainify_business_text(str(parsed.get("narrativa", "") or "").strip())
    )
    raw_fortalezas = parsed.get("top_fortalezas")
    raw_problemas = parsed.get("top_problemas")
    fortalezas = (
        _normalize_short_text_items(
            [
                sanitize_llm_text(plainify_business_text(str(item or "").strip()))
                for item in raw_fortalezas
                if str(item or "").strip()
            ]
        )[:3]
        if isinstance(raw_fortalezas, list)
        else []
    )
    problemas = (
        _normalize_problem_labels(
            [
                sanitize_llm_text(plainify_business_text(str(item or "").strip()))
                for item in raw_problemas
                if str(item or "").strip()
            ]
        )[:3]
        if isinstance(raw_problemas, list)
        else []
    )
    note_value = parsed.get("nota_sesgo")
    note_text = None
    if note_value is not None and str(note_value).strip():
        candidate_note = sanitize_llm_text(plainify_business_text(str(note_value).strip()))
        note_text = _normalize_note_text(candidate_note)
    if source_key == "tripadvisor" and not note_text:
        note_text = fallback_note
    if not narrativa:
        narrativa = str(fallback.get("narrativa") or "")
    if not fortalezas:
        fortalezas = list(fallback.get("top_fortalezas") or [])
    if not problemas:
        problemas = list(fallback.get("top_problemas") or [])
    return {
        "narrativa": narrativa,
        "top_fortalezas": fortalezas,
        "top_problemas": problemas,
        "nota_sesgo": note_text,
    }


def _normalize_short_text_items(items: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        cleaned = str(item or "").strip()
        key = cleaned.casefold()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        normalized.append(cleaned)
    return normalized


def _normalize_problem_labels(items: list[str]) -> list[str]:
    generic_keys = {
        "experiencia general",
        "experiencia del cliente",
        "general",
    }
    cleaned_items = [str(item or "").strip(" .,:;") for item in items if str(item or "").strip(" .,:;")]
    has_specific = any(item.casefold() not in generic_keys for item in cleaned_items)

    normalized: list[str] = []
    seen: set[str] = set()
    generic_seen = False
    for cleaned in cleaned_items:
        key = cleaned.casefold()
        is_generic = key in generic_keys
        if has_specific and is_generic:
            continue
        if key in seen:
            continue
        if is_generic and generic_seen:
            continue
        seen.add(key)
        if is_generic:
            generic_seen = True
        normalized.append(cleaned)
    return normalized


def _normalize_note_text(value: str | None) -> str | None:
    cleaned = str(value or "").strip(" .,:;")
    if not cleaned:
        return None
    if len(cleaned) < 12:
        return None
    if len(cleaned.split()) < 3:
        return None
    return cleaned
