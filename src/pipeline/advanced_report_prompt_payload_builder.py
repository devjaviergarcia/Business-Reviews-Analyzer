from __future__ import annotations

from typing import Any, Callable


def build_llm_user_prompt_payload(
    *,
    business_name: str,
    business_context: dict[str, Any],
    stats: dict[str, Any],
    score_and_evolution: dict[str, Any],
    customer_clusters: dict[str, Any],
    problem_clusters: dict[str, Any],
    voice_of_customer: dict[str, Any],
    invisible_and_opportunities: dict[str, Any],
    full_data_annex: dict[str, Any],
    safe_float: Callable[[Any, float], float],
    safe_int: Callable[[Any, int], int],
    score_label: Callable[[float], str],
    summarize_customer_clusters: Callable[..., list[dict[str, Any]]],
    friendly_problem_label: Callable[[str], str],
    generic_comment_problem: str,
) -> dict[str, Any]:
    components = score_and_evolution.get("components") if isinstance(score_and_evolution, dict) else {}
    if not isinstance(components, dict):
        components = {}
    evolution = score_and_evolution.get("evolution") if isinstance(score_and_evolution, dict) else {}
    if not isinstance(evolution, dict):
        evolution = {}
    dataset_summary = full_data_annex.get("dataset_summary") if isinstance(full_data_annex, dict) else {}
    if not isinstance(dataset_summary, dict):
        dataset_summary = {}
    by_problem = dataset_summary.get("by_problem")
    if not isinstance(by_problem, dict):
        by_problem = {}

    score_value = round(safe_float(score_and_evolution.get("reputation_score")), 2)
    score_label_value = score_label(score_value)
    cluster_count = safe_int(customer_clusters.get("cluster_count"))
    cluster_limit = max(1, cluster_count) if cluster_count > 0 else 4
    summarized_segments = summarize_customer_clusters(
        customer_clusters=customer_clusters,
        limit=cluster_limit,
    )
    segments_payload: list[dict[str, Any]] = []
    for segment in summarized_segments:
        if not isinstance(segment, dict):
            continue
        segments_payload.append(
            {
                "nombre": str(segment.get("label", "") or "").strip(),
                "descripcion": str(segment.get("descripcion_segmento", "") or "").strip(),
                "num_resenas": safe_int(segment.get("peso_reseñas")),
                "estado_emocional": str(segment.get("estado_emocional", "") or "").strip(),
                "intencion": str(segment.get("intencion_detectada", "") or "").strip(),
                "expectativas": str(segment.get("expectativas", "") or "").strip(),
            }
        )

    problem_data = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
    if not isinstance(problem_data, list):
        problem_data = []
    problems_payload: list[dict[str, Any]] = []
    for item in problem_data[:8]:
        if not isinstance(item, dict):
            continue
        sample_quotes = item.get("sample_quotes") if isinstance(item.get("sample_quotes"), list) else []
        first_quote = sample_quotes[0] if sample_quotes else {}
        problems_payload.append(
            {
                "problema": friendly_problem_label(
                    str(item.get("problem", "") or "").strip() or generic_comment_problem
                ),
                "num_menciones": safe_int(item.get("count")),
                "severidad": round(safe_float(item.get("severity")), 4),
                "valoracion_media_afectados": round(safe_float(item.get("avg_rating")), 2),
                "sentimiento_medio": round(safe_float(item.get("avg_sentiment")), 4),
                "ejemplo_literal": str(first_quote.get("quote", "") or "").strip()[:280],
            }
        )

    positives = voice_of_customer.get("positive_quotes") if isinstance(voice_of_customer, dict) else []
    negatives = voice_of_customer.get("negative_quotes") if isinstance(voice_of_customer, dict) else []
    if not isinstance(positives, list):
        positives = []
    if not isinstance(negatives, list):
        negatives = []

    invisible_items = (
        invisible_and_opportunities.get("invisible_problems")
        if isinstance(invisible_and_opportunities, dict)
        else []
    )
    if not isinstance(invisible_items, list):
        invisible_items = []
    alerts_payload: list[dict[str, Any]] = []
    for item in invisible_items[:8]:
        if not isinstance(item, dict):
            continue
        alerts_payload.append(
            {
                "riesgo": str(item.get("risk", "") or "").strip(),
                "detalle": str(item.get("detail", "") or "").strip(),
                "metrica": round(safe_float(item.get("metric")), 4),
            }
        )

    cliente_espera = business_context.get("cliente_espera") if isinstance(business_context, dict) else []
    if not isinstance(cliente_espera, list):
        cliente_espera = []
    fricciones_habituales = (
        business_context.get("fricciones_habituales") if isinstance(business_context, dict) else []
    )
    if not isinstance(fricciones_habituales, list):
        fricciones_habituales = []

    return {
        "business_name": business_name,
        "tipo_negocio": str(business_context.get("tipo_negocio", "") or "").strip() or "servicio local",
        "metricas": {
            "puntuacion_reputacion": score_value,
            "nivel_reputacion": score_label_value,
            "avg_rating": round(safe_float(components.get("avg_rating", stats.get("avg_rating"))), 3),
            "response_rate": round(safe_float(components.get("response_rate", stats.get("response_rate"))), 4),
            "negative_ratio": round(safe_float(components.get("negative_ratio")), 4),
            "sentiment_avg": round(safe_float(components.get("sentiment_avg")), 4),
            "tranquility_avg": round(safe_float(components.get("tranquility_avg")), 4),
            "total_resenas_analizadas": safe_int(dataset_summary.get("total_reviews")),
            "evolucion": str(evolution.get("trend", "estable") or "estable"),
        },
        "cliente_tipico": {
            "lo_que_espera": [str(item or "").strip() for item in cliente_espera if str(item or "").strip()],
            "motivacion_de_visita": str(business_context.get("motivacion_de_visita", "") or "").strip(),
            "fricciones_habituales_del_sector": [
                str(item or "").strip() for item in fricciones_habituales if str(item or "").strip()
            ],
        },
        "segmentos_cliente_detectados": segments_payload,
        "problemas_detectados": problems_payload,
        "citas_positivas_literales": [
            str(item.get("quote", "") or "").strip()[:280]
            for item in positives[:6]
            if isinstance(item, dict) and str(item.get("quote", "") or "").strip()
        ],
        "citas_negativas_literales": [
            str(item.get("quote", "") or "").strip()[:280]
            for item in negatives[:6]
            if isinstance(item, dict) and str(item.get("quote", "") or "").strip()
        ],
        "temas_mas_repetidos": by_problem,
        "alertas_invisibles_detectadas": alerts_payload,
        "instruccion_aciertos": (
            "Extrae fortalezas como conceptos de negocio y deja la cita literal solo como evidencia."
        ),
    }
