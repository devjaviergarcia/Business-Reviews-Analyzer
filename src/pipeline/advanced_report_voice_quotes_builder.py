from __future__ import annotations

from typing import Any


def build_voice_of_customer(*, review_metrics: list[dict[str, Any]], safe_float) -> dict[str, Any]:
    positives = []
    negatives = []
    improvements = []
    owner_replies = []

    for item in review_metrics:
        text = str(item.get("text", "") or "").strip()
        if not text:
            continue
        dims = item.get("dimensions") or {}
        sentiment = safe_float(dims.get("sentiment"))
        improvement_intent = safe_float(dims.get("improvement_intent"))
        payload = {
            "author_name": str(item.get("author_name", "") or "").strip() or "Cliente anónimo",
            "rating": safe_float(item.get("rating")),
            "source": item.get("source"),
            "quote": text[:320],
        }
        if sentiment >= 0.35 and len(positives) < 8:
            positives.append(payload)
        if sentiment <= -0.2 and len(negatives) < 8:
            negatives.append(payload)
        if improvement_intent >= 0.35 and len(improvements) < 8:
            improvements.append(payload)

        owner_reply = str(item.get("owner_reply", "") or "").strip()
        if owner_reply and len(owner_replies) < 8:
            owner_replies.append(
                {
                    "author_name": payload["author_name"],
                    "rating": payload["rating"],
                    "customer_quote": payload["quote"],
                    "owner_reply": owner_reply[:320],
                }
            )

    return {
        "positive_quotes": positives[:5],
        "negative_quotes": negatives[:5],
        "improvement_quotes": improvements[:5],
        "owner_reply_examples": owner_replies[:5],
    }


def build_structured_strengths(
    *,
    voice_of_customer: dict[str, Any],
    limit: int,
    normalize_text,
    infer_strength_concept,
) -> list[dict[str, str]]:
    positives = voice_of_customer.get("positive_quotes") if isinstance(voice_of_customer, dict) else []
    if not isinstance(positives, list):
        positives = []
    strengths: list[dict[str, str]] = []
    max_items = max(0, int(limit))
    seen_concepts: set[str] = set()
    seen_quotes: set[str] = set()
    for item in positives:
        if not isinstance(item, dict):
            continue
        quote = str(item.get("quote", "") or "").strip()
        if not quote:
            continue
        quote_key = normalize_text(quote)
        if not quote_key or quote_key in seen_quotes:
            continue
        concept = infer_strength_concept(quote)
        concept_key = normalize_text(concept)
        if not concept_key or concept_key in seen_concepts:
            continue
        strengths.append({"concepto": concept, "cita": quote[:240]})
        seen_quotes.add(quote_key)
        seen_concepts.add(concept_key)
        if len(strengths) >= max_items:
            break
    return strengths


def build_strengths_weaknesses_payload(
    *,
    voice_of_customer: dict[str, Any],
    problem_clusters_top: list[dict[str, Any]],
    action_plan: dict[str, Any],
    build_structured_strengths,
    human_label_problem,
    safe_float,
    safe_int,
    severity_label,
    infer_action_type,
) -> dict[str, list[dict[str, str]]]:
    strengths_raw = build_structured_strengths(voice_of_customer=voice_of_customer, limit=4)
    strengths: list[dict[str, str]] = []
    for item in strengths_raw:
        if not isinstance(item, dict):
            continue
        concepto = str(item.get("concepto", "") or "").strip()
        cita = str(item.get("cita", "") or "").strip()
        if not concepto:
            continue
        strengths.append(
            {
                "titulo": concepto,
                "descripcion": "Hay evidencia directa en reseñas positivas recientes.",
                "como_mantener": "Mantén el estándar actual y réplica esta práctica en los momentos de mayor demanda.",
                "cita": cita,
            }
        )

    weak_points: list[dict[str, str]] = []
    for item in problem_clusters_top[:4]:
        if not isinstance(item, dict):
            continue
        problem = human_label_problem(str(item.get("problema", "") or "experiencia general"))
        severity_value = safe_float(item.get("severidad"))
        weak_points.append(
            {
                "titulo": problem,
                "descripcion": (
                    f"Aparece en {safe_int(item.get('volumen'))} reseñas y con severidad "
                    f"{severity_label(severity_value)}."
                ),
                "tipo": infer_action_type(problem),
            }
        )

    if not weak_points and isinstance(action_plan, dict):
        for item in (action_plan.get("inmediato_0_30_dias") or [])[:3]:
            if not isinstance(item, dict):
                continue
            action = str(item.get("accion") or item.get("action") or "").strip()
            if not action:
                continue
            weak_points.append(
                {
                    "titulo": human_label_problem(
                        str(item.get("problema", "") or "experiencia operativa")
                    ),
                    "descripcion": action[:180],
                    "tipo": infer_action_type(action),
                }
            )

    return {
        "fortalezas": strengths[:4],
        "debilidades": weak_points[:4],
    }


def infer_strength_concept(quote: str, *, normalize_text) -> str:
    normalized = normalize_text(quote)
    if any(token in normalized for token in ("lasa", "plato", "comida", "sabor", "cocina")):
        return "Producto destacado que deja recuerdo"
    if any(token in normalized for token in ("trato", "amable", "atencion", "servicio")):
        return "Atención cercana y bien valorada"
    if any(token in normalized for token in ("ambiente", "local", "terraza", "decoracion")):
        return "Ambiente agradable y con personalidad"
    if any(token in normalized for token in ("precio", "valor", "calidad precio")):
        return "Percepción de buena relación calidad-precio"
    return "Experiencia general positiva y consistente"
