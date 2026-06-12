from __future__ import annotations

import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class RecurringTopic(BaseModel):
    topic: str
    mentions: int = Field(ge=0)
    sentiment: Literal["positive", "mixed", "negative", "unknown"] = "unknown"
    evidence: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class MonthlyAction(BaseModel):
    priority: Literal["high", "medium", "low"] = "medium"
    title: str
    rationale: str
    expected_impact: str

    model_config = ConfigDict(extra="forbid")


class ResponseTemplate(BaseModel):
    scenario: str
    template: str

    model_config = ConfigDict(extra="forbid")


class DeepStudySnapshot(BaseModel):
    business_name: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    executive_summary: str
    strengths: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    recurring_topics: list[RecurringTopic] = Field(default_factory=list)
    competitor_gaps: list[str] = Field(default_factory=list)
    monthly_actions: list[MonthlyAction] = Field(default_factory=list)
    response_templates: list[ResponseTemplate] = Field(default_factory=list)
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    score_explanation: dict[str, Any] = Field(default_factory=dict)
    data_quality: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


_TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "comida": ("comida", "plato", "tapa", "menu", "carta", "sabor", "racion", "postre"),
    "servicio": ("servicio", "camarero", "personal", "atencion", "trato", "amable"),
    "precio": ("precio", "caro", "barato", "calidad precio", "cuenta"),
    "ambiente": ("ambiente", "terraza", "local", "sitio", "decoracion", "ruido"),
    "espera": ("espera", "tard", "lento", "rapido", "cola", "reserva"),
    "limpieza": ("limpio", "limpieza", "sucio", "bano"),
}


def build_deep_study_snapshot(
    *,
    business: dict[str, Any],
    listing: dict[str, Any] | None = None,
    reviews: list[dict[str, Any]] | None = None,
    competitors: list[dict[str, Any]] | None = None,
    benchmark: dict[str, Any] | None = None,
) -> dict[str, Any]:
    listing_data = dict(listing or {})
    business_data = {**listing_data, **dict(business or {})}
    review_items = [dict(item) for item in reviews or [] if isinstance(item, dict)]
    competitor_items = [dict(item) for item in competitors or [] if isinstance(item, dict)]
    benchmark_data = dict(benchmark or {})

    business_name = str(business_data.get("business_name") or business_data.get("name") or "Negocio").strip() or "Negocio"
    rating = _coerce_float(business_data.get("rating"))
    review_count = _coerce_int(business_data.get("review_count"))
    discovery_rank = _coerce_int(business_data.get("discovery_rank"))
    website = str(business_data.get("website") or "").strip() or None
    phone = str(business_data.get("phone") or "").strip() or None

    warnings: list[str] = []
    if not review_items:
        warnings.append("missing_reviews: snapshot degradado a listing + benchmark")
    if not competitor_items:
        warnings.append("missing_competitors: comparativa local limitada")
    if rating is None:
        warnings.append("missing_rating")
    if review_count is None:
        warnings.append("missing_review_count")
    if discovery_rank is None:
        warnings.append("missing_discovery_rank")
    if not website:
        warnings.append("missing_website")
    if not phone:
        warnings.append("missing_phone")

    recurring_topics = _build_recurring_topics(review_items)
    competitor_stats = _build_competitor_stats(competitor_items, benchmark_data)
    strengths = _build_strengths(
        rating=rating,
        review_count=review_count,
        discovery_rank=discovery_rank,
        website=website,
        phone=phone,
        topics=recurring_topics,
    )
    risks = _build_risks(
        rating=rating,
        review_count=review_count,
        discovery_rank=discovery_rank,
        website=website,
        phone=phone,
        topics=recurring_topics,
        competitor_stats=competitor_stats,
    )
    competitor_gaps = _build_competitor_gaps(
        rating=rating,
        review_count=review_count,
        website=website,
        competitor_stats=competitor_stats,
        discovery_rank=discovery_rank,
    )
    score_breakdown = _build_score_breakdown(
        rating=rating,
        review_count=review_count,
        discovery_rank=discovery_rank,
        website=website,
        phone=phone,
        competitor_stats=competitor_stats,
        has_reviews=bool(review_items),
    )
    score_explanation = _build_score_explanation(
        rating=rating,
        review_count=review_count,
        discovery_rank=discovery_rank,
        website=website,
        phone=phone,
        competitor_stats=competitor_stats,
        score_breakdown=score_breakdown,
    )
    monthly_actions = _build_monthly_actions(
        risks=risks,
        competitor_gaps=competitor_gaps,
        score_breakdown=score_breakdown,
        has_reviews=bool(review_items),
    )
    response_templates = _build_response_templates(business_name=business_name, topics=recurring_topics)
    executive_summary = _build_executive_summary(
        business_name=business_name,
        rating=rating,
        review_count=review_count,
        discovery_rank=discovery_rank,
        strengths=strengths,
        risks=risks,
        competitor_gaps=competitor_gaps,
    )

    snapshot = DeepStudySnapshot(
        business_name=business_name,
        executive_summary=executive_summary,
        strengths=strengths,
        risks=risks,
        recurring_topics=recurring_topics,
        competitor_gaps=competitor_gaps,
        monthly_actions=monthly_actions,
        response_templates=response_templates,
        score_breakdown=score_breakdown,
        score_explanation=score_explanation,
        data_quality={
            "reviews_available": len(review_items),
            "competitors_available": len(competitor_items),
            "listing_fields": {
                "rating": rating is not None,
                "review_count": review_count is not None,
                "discovery_rank": discovery_rank is not None,
                "website": bool(website),
                "phone": bool(phone),
            },
        },
        warnings=warnings,
    )
    return snapshot.model_dump(mode="python")


def _build_recurring_topics(reviews: list[dict[str, Any]]) -> list[RecurringTopic]:
    topic_mentions: Counter[str] = Counter()
    topic_ratings: dict[str, list[float]] = defaultdict(list)
    topic_evidence: dict[str, list[str]] = defaultdict(list)

    for review in reviews:
        text = _review_text(review)
        if not text:
            continue
        normalized = _normalize_text(text)
        rating = _coerce_float(review.get("rating") or review.get("score") or review.get("stars"))
        for topic, keywords in _TOPIC_KEYWORDS.items():
            if any(keyword in normalized for keyword in keywords):
                topic_mentions[topic] += 1
                if rating is not None:
                    topic_ratings[topic].append(rating)
                if len(topic_evidence[topic]) < 2:
                    topic_evidence[topic].append(_shorten(text, 160))

    topics: list[RecurringTopic] = []
    for topic, mentions in topic_mentions.most_common(6):
        ratings = topic_ratings.get(topic) or []
        avg_rating = statistics.mean(ratings) if ratings else None
        sentiment = "unknown"
        if avg_rating is not None:
            sentiment = "positive" if avg_rating >= 4 else "negative" if avg_rating < 3 else "mixed"
        topics.append(
            RecurringTopic(
                topic=topic,
                mentions=mentions,
                sentiment=sentiment,
                evidence=topic_evidence.get(topic, []),
            )
        )
    return topics


def _build_competitor_stats(competitors: list[dict[str, Any]], benchmark: dict[str, Any]) -> dict[str, Any]:
    ratings = [_coerce_float(item.get("rating")) for item in competitors]
    ratings = [item for item in ratings if item is not None]
    reviews = [_coerce_int(item.get("review_count")) for item in competitors]
    reviews = [item for item in reviews if item is not None]
    websites = [bool(str(item.get("website") or "").strip()) for item in competitors]
    ranks = [_coerce_int(item.get("discovery_rank")) for item in competitors]
    ranks = [item for item in ranks if item is not None]

    return {
        "avg_rating": _coerce_float(benchmark.get("avg_rating")) or (round(statistics.mean(ratings), 2) if ratings else None),
        "avg_review_count": _coerce_float(benchmark.get("avg_review_count")) or (round(statistics.mean(reviews), 2) if reviews else None),
        "website_share": _coerce_float(benchmark.get("website_share"))
        if benchmark.get("website_share") is not None
        else (round(sum(1 for item in websites if item) / len(websites), 2) if websites else None),
        "avg_discovery_rank": _coerce_float(benchmark.get("avg_discovery_rank"))
        or (round(statistics.mean(ranks), 2) if ranks else None),
        "competitor_count": len(competitors),
    }


def _build_strengths(
    *,
    rating: float | None,
    review_count: int | None,
    discovery_rank: int | None,
    website: str | None,
    phone: str | None,
    topics: list[RecurringTopic],
) -> list[str]:
    strengths: list[str] = []
    if rating is not None and rating >= 4.5:
        strengths.append("Reputacion visible fuerte: rating por encima de 4,5.")
    elif rating is not None and rating >= 4.2:
        strengths.append("Rating competitivo para captar demanda local.")
    if review_count is not None and review_count >= 200:
        strengths.append("Volumen de resenas suficiente para generar confianza.")
    if discovery_rank is not None and discovery_rank <= 5:
        strengths.append(f"Aparece en una posicion alta del benchmark local: #{discovery_rank}.")
    if website:
        strengths.append("Tiene web o enlace externo para convertir visitas.")
    if phone:
        strengths.append("Telefono visible para contacto directo.")
    for topic in topics:
        if topic.sentiment == "positive" and len(strengths) < 5:
            strengths.append(f"Las resenas destacan positivamente {topic.topic}.")
    return strengths or ["Hay una ficha base que permite construir un diagnostico accionable."]


def _build_risks(
    *,
    rating: float | None,
    review_count: int | None,
    website: str | None,
    phone: str | None,
    topics: list[RecurringTopic],
    competitor_stats: dict[str, Any],
    discovery_rank: int | None,
) -> list[str]:
    risks: list[str] = []
    avg_rating = _coerce_float(competitor_stats.get("avg_rating"))
    avg_reviews = _coerce_float(competitor_stats.get("avg_review_count"))
    avg_rank = _coerce_float(competitor_stats.get("avg_discovery_rank"))
    if rating is not None and rating < 4.2:
        risks.append("Rating por debajo del umbral de confianza local (4,2).")
    if avg_rating is not None and rating is not None and rating < avg_rating:
        risks.append("Rating por debajo de la media de competidores seleccionados.")
    if review_count is not None and review_count < 50:
        risks.append("Poco volumen de resenas: menor prueba social.")
    if avg_reviews is not None and review_count is not None and review_count < avg_reviews * 0.6:
        risks.append("Volumen de resenas claramente inferior al entorno competitivo.")
    if discovery_rank is not None and discovery_rank > 10:
        risks.append(f"Aparece tarde en el benchmark local: posicion #{discovery_rank}.")
    if avg_rank is not None and discovery_rank is not None and discovery_rank > avg_rank:
        risks.append("Posicion peor que la media de competidores seleccionados.")
    if not website:
        risks.append("Sin web visible: se pierde conversion desde Google Maps.")
    if not phone:
        risks.append("Sin telefono visible: friccion para reservas o contacto.")
    for topic in topics:
        if topic.sentiment == "negative" and len(risks) < 6:
            risks.append(f"Tema con senal negativa en resenas: {topic.topic}.")
    return risks


def _build_competitor_gaps(
    *,
    rating: float | None,
    review_count: int | None,
    discovery_rank: int | None,
    website: str | None,
    competitor_stats: dict[str, Any],
) -> list[str]:
    gaps: list[str] = []
    avg_rating = _coerce_float(competitor_stats.get("avg_rating"))
    avg_reviews = _coerce_float(competitor_stats.get("avg_review_count"))
    avg_rank = _coerce_float(competitor_stats.get("avg_discovery_rank"))
    website_share = _coerce_float(competitor_stats.get("website_share"))
    if avg_rating is not None and rating is not None and rating < avg_rating:
        gaps.append(f"Rating {rating:.1f} frente a media competidores {avg_rating:.1f}.")
    if avg_reviews is not None and review_count is not None and review_count < avg_reviews:
        gaps.append(f"{review_count} resenas frente a media competidores {avg_reviews:.0f}.")
    if avg_rank is not None and discovery_rank is not None and discovery_rank > avg_rank:
        gaps.append(f"Posicion #{discovery_rank} frente a media competidores #{avg_rank:.1f}.")
    if not website and website_share is not None and website_share >= 0.5:
        gaps.append("Competidores con mas presencia web visible.")
    return gaps


def _build_score_breakdown(
    *,
    rating: float | None,
    review_count: int | None,
    discovery_rank: int | None,
    website: str | None,
    phone: str | None,
    competitor_stats: dict[str, Any],
    has_reviews: bool,
) -> dict[str, float]:
    rating_score = min(max((rating or 0.0) / 5.0 * 100.0, 0.0), 100.0)
    volume_score = min(max((review_count or 0) / 300.0 * 100.0, 0.0), 100.0)
    avg_reviews = _coerce_float(competitor_stats.get("avg_review_count"))
    relative_volume = 50.0
    if avg_reviews and review_count is not None:
        relative_volume = min(max(review_count / max(avg_reviews, 1.0) * 100.0, 0.0), 100.0)
    rank_visibility = _rank_visibility_score(discovery_rank)

    reputation = round(rating_score * 0.75 + volume_score * 0.25, 2)
    visibility = round(relative_volume * 0.40 + rank_visibility * 0.35 + (15.0 if website else 0.0) + (10.0 if phone else 0.0), 2)
    conversion = round((45.0 if website else 0.0) + (25.0 if phone else 0.0) + rating_score * 0.30, 2)
    response = 65.0 if has_reviews else 40.0
    opportunity = round(100.0 - (reputation * 0.35 + visibility * 0.30 + conversion * 0.25 + response * 0.10), 2)
    return {
        "reputation": _bound(reputation),
        "visibility": _bound(visibility),
        "conversion": _bound(conversion),
        "response": _bound(response),
        "opportunity": _bound(opportunity),
    }


def _build_score_explanation(
    *,
    rating: float | None,
    review_count: int | None,
    discovery_rank: int | None,
    website: str | None,
    phone: str | None,
    competitor_stats: dict[str, Any],
    score_breakdown: dict[str, float],
) -> dict[str, Any]:
    positives: list[str] = []
    negatives: list[str] = []

    avg_rating = _coerce_float(competitor_stats.get("avg_rating"))
    avg_reviews = _coerce_float(competitor_stats.get("avg_review_count"))
    avg_rank = _coerce_float(competitor_stats.get("avg_discovery_rank"))

    if rating is not None and rating >= 4.4:
        positives.append(f"El rating visible está en {rating:.1f}/5, un nivel competitivo.")
    elif rating is not None and rating < 4.2:
        negatives.append(f"El rating visible cae a {rating:.1f}/5 y resta confianza.")
    if avg_rating is not None and rating is not None and rating < avg_rating:
        negatives.append(f"El rating queda por debajo de la media competitiva ({avg_rating:.1f}).")

    if review_count is not None and review_count >= 200:
        positives.append(f"Hay volumen suficiente de reseñas ({review_count}) para generar prueba social.")
    elif review_count is not None and review_count < 50:
        negatives.append(f"El volumen de reseñas es bajo ({review_count}) y limita la prueba social.")
    if avg_reviews is not None and review_count is not None and review_count < avg_reviews:
        negatives.append(f"El negocio tiene menos reseñas que la media del benchmark ({avg_reviews:.0f}).")

    if discovery_rank is not None and discovery_rank <= 5:
        positives.append(f"Aparece arriba en discovery local (posición #{discovery_rank}).")
    elif discovery_rank is not None and discovery_rank > 10:
        negatives.append(f"Aparece tarde en discovery local (posición #{discovery_rank}).")
    if avg_rank is not None and discovery_rank is not None and discovery_rank > avg_rank:
        negatives.append(f"Su posición está por detrás de la media competitiva (#{avg_rank:.1f}).")

    if website:
        positives.append("La ficha tiene web visible y eso ayuda a convertir visitas en clics.")
    else:
        negatives.append("No hay web visible en la ficha, así que se pierde capacidad de conversión.")

    if phone:
        positives.append("El teléfono está visible para contacto directo.")
    else:
        negatives.append("No hay teléfono visible y eso añade fricción al contacto.")

    top_positive = positives[0] if positives else "No se detecta una palanca claramente positiva."
    top_negative = negatives[0] if negatives else "No se detecta un freno principal con los datos actuales."
    summary = f"Suma sobre todo por: {top_positive} Frena sobre todo por: {top_negative}"

    return {
        "summary": summary,
        "positives": positives[:4],
        "negatives": negatives[:4],
        "component_scores": {
            "reputation": _bound(_coerce_float(score_breakdown.get("reputation")) or 0.0),
            "visibility": _bound(_coerce_float(score_breakdown.get("visibility")) or 0.0),
            "conversion": _bound(_coerce_float(score_breakdown.get("conversion")) or 0.0),
            "response": _bound(_coerce_float(score_breakdown.get("response")) or 0.0),
            "opportunity": _bound(_coerce_float(score_breakdown.get("opportunity")) or 0.0),
        },
    }


def _build_monthly_actions(
    *,
    risks: list[str],
    competitor_gaps: list[str],
    score_breakdown: dict[str, float],
    has_reviews: bool,
) -> list[MonthlyAction]:
    actions: list[MonthlyAction] = []
    if any("Sin web" in risk for risk in risks):
        actions.append(
            MonthlyAction(
                priority="high",
                title="Anadir enlace de conversion en la ficha",
                rationale="La ficha capta demanda, pero sin destino web se pierde intencion.",
                expected_impact="Mas clics medibles desde Google Maps y mejor atribucion.",
            )
        )
    if any("volumen" in item.lower() or "resenas" in item.lower() for item in [*risks, *competitor_gaps]):
        actions.append(
            MonthlyAction(
                priority="high",
                title="Activar rutina de captacion de resenas",
                rationale="El volumen de resenas condiciona confianza y posicion frente a competidores.",
                expected_impact="Aumentar prueba social y reducir brecha competitiva.",
            )
        )
    if any("Rating" in item or "rating" in item for item in [*risks, *competitor_gaps]):
        actions.append(
            MonthlyAction(
                priority="medium",
                title="Responder resenas criticas con patron estable",
                rationale="La respuesta publica reduce riesgo percibido y muestra gestion activa.",
                expected_impact="Mejorar conversion de usuarios que comparan alternativas.",
            )
        )
    if not has_reviews:
        actions.append(
            MonthlyAction(
                priority="medium",
                title="Completar captura de resenas antes del informe final",
                rationale="El diagnostico actual usa listing y benchmark, pero faltan evidencias textuales.",
                expected_impact="Informe mas preciso y accionable.",
            )
        )
    if score_breakdown.get("visibility", 0) < 60:
        actions.append(
            MonthlyAction(
                priority="medium",
                title="Revisar categoria, servicios y atributos visibles",
                rationale="La visibilidad depende de senales basicas de la ficha y comparativa local.",
                expected_impact="Mejor encaje en busquedas locales de alta intencion.",
            )
        )
    if not actions:
        actions.append(
            MonthlyAction(
                priority="low",
                title="Convertir fortalezas actuales en contenido local",
                rationale="Hay senales positivas que pueden amplificarse en publicaciones y landing.",
                expected_impact="Mas confianza antes de la reserva o visita.",
            )
        )
    return actions[:5]


def _build_response_templates(*, business_name: str, topics: list[RecurringTopic]) -> list[ResponseTemplate]:
    topic = topics[0].topic if topics else "la experiencia"
    return [
        ResponseTemplate(
            scenario="resena positiva",
            template=f"Gracias por valorar {business_name}. Nos alegra saber que {topic} estuvo a la altura. Te esperamos de nuevo pronto.",
        ),
        ResponseTemplate(
            scenario="resena critica",
            template=f"Gracias por avisarnos. Sentimos que {topic} no cumpliera tus expectativas. Revisaremos lo ocurrido con el equipo para mejorar.",
        ),
    ]


def _build_executive_summary(
    *,
    business_name: str,
    rating: float | None,
    review_count: int | None,
    discovery_rank: int | None,
    strengths: list[str],
    risks: list[str],
    competitor_gaps: list[str],
) -> str:
    rating_text = f"rating {rating:.1f}" if rating is not None else "rating no disponible"
    reviews_text = f"{review_count} resenas" if review_count is not None else "volumen de resenas no disponible"
    rank_text = f", posicion #{discovery_rank} en el benchmark" if discovery_rank is not None else ""
    main_strength = strengths[0] if strengths else "Tiene base suficiente para analizar su reputacion local."
    main_risk = risks[0] if risks else "No aparece un riesgo critico con los datos disponibles."
    gap_text = competitor_gaps[0] if competitor_gaps else "La comparativa competitiva no muestra una brecha prioritaria con los datos disponibles."
    return f"{business_name} presenta {rating_text}, {reviews_text}{rank_text}. {main_strength} Riesgo principal: {main_risk} Comparativa: {gap_text}"


def _review_text(review: dict[str, Any]) -> str:
    for key in ("text", "review_text", "content", "body", "snippet"):
        value = str(review.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    raw = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", raw)


def _shorten(value: str, max_length: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if len(cleaned) <= max_length:
        return cleaned
    return cleaned[: max_length - 3].rstrip() + "..."


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(".", "").replace(",", "")))
    except (TypeError, ValueError):
        return None


def _rank_visibility_score(rank: int | None) -> float:
    if rank is None:
        return 50.0
    return max(20.0, min(100.0, 105.0 - (float(rank) * 5.0)))


def _bound(value: float) -> float:
    return round(min(max(value, 0.0), 100.0), 2)
