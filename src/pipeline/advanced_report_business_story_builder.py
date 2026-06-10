from __future__ import annotations

from typing import Any


def build_executive_summary(
    *,
    business_name: str,
    score_and_evolution: dict[str, Any],
    customer_clusters: dict[str, Any],
    problem_clusters: dict[str, Any],
    benchmarking: dict[str, Any],
    quick_wins: dict[str, Any],
    llm_clustering_insights: dict[str, Any],
    safe_float,
    safe_int,
    friendly_problem_label,
    generic_comment_problem: str,
) -> dict[str, Any]:
    score = safe_float(score_and_evolution.get("reputation_score"))
    trend = str(((score_and_evolution.get("evolution") or {}).get("trend") or "estable")).strip()
    target_rank = safe_int(benchmarking.get("target_rank"))
    total_competitors = safe_int(benchmarking.get("total_competitors_compared"))
    top_problem = ""
    clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
    if isinstance(clusters, list) and clusters:
        top_problem_raw = str(clusters[0].get("problem", "") or "").strip()
        top_problem = friendly_problem_label(top_problem_raw)
    cluster_count = safe_int(customer_clusters.get("cluster_count"))
    quick_win_items = quick_wins.get("items") if isinstance(quick_wins, dict) else []
    quick_win_count = len(quick_win_items) if isinstance(quick_win_items, list) else 0

    headline = (
        f"{business_name}: reputación {round(score, 1)}/100, tendencia {trend.replace('_', ' ')}, "
        f"{cluster_count} segmentos de cliente detectados."
    )
    bullets = [
        f"Ranking competitivo: posición {target_rank} de {max(1, total_competitors + 1)}.",
        f"Principal foco de fricción: {top_problem or generic_comment_problem}.",
        f"Acciones rápidas identificadas esta semana: {quick_win_count}.",
        "Tenemos tipos de cliente diferenciados para personalizar mejor las decisiones.",
    ]

    llm_text = str(llm_clustering_insights.get("text", "") or "").strip()
    if llm_text:
        bullets.append(f"Lectura del modelo sobre tipos de cliente: {llm_text[:220]}")

    return {
        "headline": headline,
        "bullets": bullets,
        "one_page_takeaway": (
            "Priorizar respuesta rápida a feedback crítico, atacar el problema dominante "
            "y convertir segmentos satisfechos en palanca de recomendación."
        ),
    }


def build_business_context(
    *,
    business_name: str,
    listing: dict[str, Any],
    stats: dict[str, Any],
    normalize_text,
    safe_float,
) -> dict[str, Any]:
    categories_raw = listing.get("categories") if isinstance(listing.get("categories"), list) else []
    categories = [str(item or "").strip() for item in categories_raw if str(item or "").strip()]
    normalized_scope = normalize_text(" ".join([business_name, *categories]))

    if any(token in normalized_scope for token in ("hotel", "hostal", "hostel", "pension", "hospederia")):
        profile = {
            "tipo_negocio": "alojamiento",
            "cliente_espera": [
                "limpieza consistente y descanso real",
                "trato resolutivo en recepción",
                "buena relación calidad-precio",
                "check-in/check-out ágiles",
            ],
            "motivacion_de_visita": "descansar, dormir bien y resolver necesidades básicas sin fricción",
            "fricciones_habituales": [
                "ruido nocturno",
                "higiene mejorable",
                "incidencias no resueltas en tiempo",
            ],
        }
    elif any(token in normalized_scope for token in ("restaurante", "foodestablishment", "bar", "burger", "pizza", "cafe")):
        profile = {
            "tipo_negocio": "restauración",
            "cliente_espera": [
                "comida consistente y sabrosa",
                "servicio atento y tiempos razonables",
                "ambiente agradable según ocasión",
                "precio percibido como justo frente a lo recibido",
            ],
            "motivacion_de_visita": "disfrutar una experiencia gastronómica con buena atención y valor claro",
            "fricciones_habituales": [
                "demoras en sala",
                "raciones o calidad percibidas como insuficientes",
                "desalineación precio-valor",
            ],
        }
    else:
        profile = {
            "tipo_negocio": "servicio local",
            "cliente_espera": [
                "atención humana clara",
                "cumplimiento de expectativas básicas",
                "resolución rápida de incidencias",
            ],
            "motivacion_de_visita": "resolver una necesidad concreta con seguridad y confianza",
            "fricciones_habituales": [
                "falta de claridad operativa",
                "tiempos de espera",
                "experiencia inconsistente",
            ],
        }

    avg_rating = round(safe_float((stats or {}).get("avg_rating")), 2)
    profile["rating_medio_observado"] = avg_rating
    profile["categorias_detectadas"] = categories[:8]
    return profile


def score_label(score_value: float) -> str:
    if score_value >= 85.0:
        return "excelente reputación"
    if score_value >= 70.0:
        return "reputación sólida"
    if score_value >= 55.0:
        return "reputación media mejorable"
    if score_value >= 40.0:
        return "reputación mejorable"
    return "reputación crítica"


def summarize_customer_clusters(
    *,
    customer_clusters: dict[str, Any],
    limit: int,
    safe_float,
    safe_int,
) -> list[dict[str, Any]]:
    clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
    if not isinstance(clusters, list):
        return []
    ranked = sorted(clusters, key=lambda item: safe_int(item.get("count_reviews")), reverse=True)
    output: list[dict[str, Any]] = []
    for cluster in ranked[: max(0, int(limit))]:
        centroid = cluster.get("centroid") if isinstance(cluster.get("centroid"), dict) else {}
        satisfaction = safe_float(centroid.get("satisfaction"))
        expectation_gap = safe_float(centroid.get("expectation_gap"))
        tranquility = safe_float(centroid.get("tranquility_aggressiveness"))
        improvement_intent = safe_float(centroid.get("improvement_intent"))

        if satisfaction >= 0.75 and tranquility >= 0.4:
            emotional_state = "satisfecho y calmado"
        elif satisfaction <= 0.5 or tranquility <= 0.0:
            emotional_state = "frustrado o tensionado"
        else:
            emotional_state = "neutral-pragmático"

        if improvement_intent >= 0.45:
            intent_state = "alta intención de mejora explícita"
        elif improvement_intent >= 0.2:
            intent_state = "intención de mejora moderada"
        else:
            intent_state = "baja intención de cambio, prioriza continuidad"

        if expectation_gap >= 0.35:
            expectation_state = "expectativas no cubiertas de forma relevante"
        elif expectation_gap >= 0.15:
            expectation_state = "brecha parcial entre expectativa y experiencia"
        else:
            expectation_state = "expectativas mayoritariamente cumplidas"

        output.append(
            {
                "cluster_id": cluster.get("cluster_id"),
                "label": cluster.get("label"),
                "descripcion_segmento": cluster.get("description"),
                "peso_reseñas": safe_int(cluster.get("count_reviews")),
                "peso_clientes": safe_int(cluster.get("count_customers")),
                "estado_emocional": emotional_state,
                "intencion_detectada": intent_state,
                "expectativas": expectation_state,
            }
        )
    return output


def summarize_problem_clusters(
    *,
    problem_clusters: dict[str, Any],
    limit: int,
    friendly_problem_label,
    safe_float,
    safe_int,
    generic_comment_problem: str,
) -> list[dict[str, Any]]:
    clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
    if not isinstance(clusters, list):
        return []
    output: list[dict[str, Any]] = []
    for cluster in clusters[: max(0, int(limit))]:
        quotes = cluster.get("sample_quotes") if isinstance(cluster.get("sample_quotes"), list) else []
        first_quote = quotes[0] if quotes else {}
        output.append(
            {
                "problema": friendly_problem_label(
                    str(cluster.get("problem", "") or "").strip() or generic_comment_problem
                ),
                "volumen": safe_int(cluster.get("count")),
                "severidad": round(safe_float(cluster.get("severity")), 4),
                "rating_medio_asociado": round(safe_float(cluster.get("avg_rating")), 2),
                "tono_medio": round(safe_float(cluster.get("avg_sentiment")), 4),
                "ejemplo_literal": str(first_quote.get("quote", "") or "").strip()[:280],
            }
        )
    return output
