from __future__ import annotations

import asyncio
import json
from typing import Any, Callable


async def build_llm_source_comparison(
    *,
    business_name: str,
    google_data: dict[str, Any],
    tripadvisor_data: dict[str, Any],
    summarize_problem_clusters: Callable[..., list[dict[str, Any]]],
    human_label_problem: Callable[[str], str],
    negative_ratio: Callable[..., float],
    average_dimension: Callable[[list[dict[str, Any]], str], float],
    safe_float: Callable[[Any, float], float],
    can_use_llm: Callable[[], bool],
    llm_generate_text: Callable[[str], tuple[str, str | None]],
    extract_json_object: Callable[[str], str],
    sanitize_llm_text: Callable[[str], str],
    plainify_business_text: Callable[[str], str],
) -> dict[str, Any]:
    google_stats = google_data.get("stats") if isinstance(google_data.get("stats"), dict) else {}
    trip_stats = tripadvisor_data.get("stats") if isinstance(tripadvisor_data.get("stats"), dict) else {}
    google_metrics = google_data.get("review_metrics") if isinstance(google_data.get("review_metrics"), list) else []
    trip_metrics = tripadvisor_data.get("review_metrics") if isinstance(tripadvisor_data.get("review_metrics"), list) else []
    google_problem_clusters = google_data.get("problem_clusters") if isinstance(google_data.get("problem_clusters"), dict) else {}
    trip_problem_clusters = tripadvisor_data.get("problem_clusters") if isinstance(tripadvisor_data.get("problem_clusters"), dict) else {}

    google_top = summarize_problem_clusters(problem_clusters=google_problem_clusters, limit=3)
    trip_top = summarize_problem_clusters(problem_clusters=trip_problem_clusters, limit=3)
    google_problems = [
        human_label_problem(str(item.get("problem", "") or ""))
        for item in google_top
        if isinstance(item, dict)
    ]
    trip_problems = [
        human_label_problem(str(item.get("problem", "") or ""))
        for item in trip_top
        if isinstance(item, dict)
    ]
    google_negative_ratio = negative_ratio(review_metrics=google_metrics)
    trip_negative_ratio = negative_ratio(review_metrics=trip_metrics)
    google_avg_rating = safe_float(google_stats.get("avg_rating"), 0.0)
    trip_avg_rating = safe_float(trip_stats.get("avg_rating"), 0.0)
    google_sentiment = average_dimension(google_metrics, "sentiment")
    trip_sentiment = average_dimension(trip_metrics, "sentiment")

    google_set = {str(item or "").strip().lower() for item in google_problems if item}
    trip_set = {str(item or "").strip().lower() for item in trip_problems if item}
    coincidences = [item for item in google_problems if str(item or "").strip().lower() in trip_set][:3]
    divergences = [
        item
        for item in [*google_problems, *trip_problems]
        if str(item or "").strip().lower() not in (google_set & trip_set)
    ][:4]
    if trip_avg_rating < google_avg_rating - 0.2:
        harder_source = "tripadvisor"
    elif google_avg_rating < trip_avg_rating - 0.2:
        harder_source = "google_maps"
    else:
        harder_source = "similar"
    fallback: dict[str, Any] = {
        "narrativa_comparacion": (
            f"Comparando ambas fuentes de {business_name or 'este negocio'}, Tripadvisor muestra un tono más exigente "
            "que Google Maps, algo habitual por el perfil más turístico y crítico de la plataforma. "
            "Aun así, conviene priorizar los problemas que se repiten en ambas fuentes, porque esos sí indican "
            "un patrón operativo real."
        ),
        "coincidencias": coincidences,
        "divergencias": divergences,
        "fuente_mas_dura": harder_source,
        "explicacion_diferencia": (
            "Tripadvisor suele concentrar perfiles más críticos; interpreta las diferencias junto al volumen de reseñas."
        ),
        "recomendaciones": [
            "Prioriza primero los problemas repetidos en ambas fuentes.",
            "Responde reseñas críticas en menos de 24 horas en las dos plataformas.",
            "Mide semanalmente si baja la repetición de esos problemas clave.",
        ][:4],
    }

    if not can_use_llm():
        return fallback

    prompt_payload = {
        "business_name": business_name,
        "google_maps": {
            "avg_rating": round(google_avg_rating, 4),
            "response_rate": round(safe_float(google_stats.get("response_rate"), 0.0), 4),
            "negative_ratio": round(google_negative_ratio, 4),
            "sentiment_avg": round(google_sentiment, 4),
            "top_problems": google_problems,
        },
        "tripadvisor": {
            "avg_rating": round(trip_avg_rating, 4),
            "response_rate": round(safe_float(trip_stats.get("response_rate"), 0.0), 4),
            "negative_ratio": round(trip_negative_ratio, 4),
            "sentiment_avg": round(trip_sentiment, 4),
            "top_problems": trip_problems,
        },
    }
    prompt = (
        f"Compara Google Maps vs Tripadvisor del negocio '{business_name or 'negocio local'}'.\n"
        "Analiza: 1) coincidencias, 2) divergencias, 3) qué fuente es más dura y si hay sesgo de plataforma, "
        "4) recomendaciones prácticas priorizadas.\n"
        "Ten en cuenta que Tripadvisor concentra perfil más crítico y turístico.\n"
        "Escribe en español de España y devuelve SOLO JSON válido con estructura exacta:\n"
        "{\n"
        '  "narrativa_comparacion": "string",\n'
        '  "coincidencias": ["string"],\n'
        '  "divergencias": ["string"],\n'
        '  "fuente_mas_dura": "google_maps|tripadvisor|similar",\n'
        '  "explicacion_diferencia": "string",\n'
        '  "recomendaciones": ["string"]\n'
        "}\n"
        "Sin markdown y sin texto fuera del JSON.\n"
        f"Datos:\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}"
    )
    try:
        text, _model_used = await asyncio.to_thread(llm_generate_text, prompt)
        extracted = extract_json_object(text)
        parsed = json.loads(extracted)
    except Exception:
        return fallback

    if not isinstance(parsed, dict):
        return fallback
    narrative = sanitize_llm_text(
        plainify_business_text(str(parsed.get("narrativa_comparacion", "") or "").strip())
    )
    coincidence_items = parsed.get("coincidencias")
    divergence_items = parsed.get("divergencias")
    recommendation_items = parsed.get("recomendaciones")
    parsed_harder_source = str(parsed.get("fuente_mas_dura", "") or "").strip().lower()
    harder_source_value = (
        parsed_harder_source
        if parsed_harder_source in {"google_maps", "tripadvisor", "similar"}
        else fallback["fuente_mas_dura"]
    )
    explanation = sanitize_llm_text(
        plainify_business_text(str(parsed.get("explicacion_diferencia", "") or "").strip())
    )
    return {
        "narrativa_comparacion": narrative or str(fallback["narrativa_comparacion"]),
        "coincidencias": (
            [
                sanitize_llm_text(plainify_business_text(str(item or "").strip()))
                for item in coincidence_items
                if str(item or "").strip()
            ][:4]
            if isinstance(coincidence_items, list)
            else list(fallback["coincidencias"])
        ),
        "divergencias": (
            [
                sanitize_llm_text(plainify_business_text(str(item or "").strip()))
                for item in divergence_items
                if str(item or "").strip()
            ][:4]
            if isinstance(divergence_items, list)
            else list(fallback["divergencias"])
        ),
        "fuente_mas_dura": harder_source_value,
        "explicacion_diferencia": explanation or str(fallback["explicacion_diferencia"]),
        "recomendaciones": (
            [
                sanitize_llm_text(plainify_business_text(str(item or "").strip()))
                for item in recommendation_items
                if str(item or "").strip()
            ][:4]
            if isinstance(recommendation_items, list)
            else list(fallback["recomendaciones"])
        ),
    }
