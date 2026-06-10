from __future__ import annotations

import asyncio
import json
from typing import Any, Callable


async def build_llm_section_narratives(
    *,
    business_name: str,
    score_and_evolution: dict[str, Any],
    payload: dict[str, Any],
    safe_float: Callable[[Any, float], float],
    score_label: Callable[[float], str],
    can_use_llm: Callable[[], bool],
    llm_generate_text: Callable[[str], tuple[str, str | None]],
    extract_json_object: Callable[[str], str],
    sanitize_llm_text: Callable[[str], str],
    plainify_business_text: Callable[[str], str],
) -> dict[str, str]:
    score_value = safe_float(score_and_evolution.get("reputation_score"))
    score_label_value = score_label(score_value)
    fallback = {
        "resumen_ejecutivo": (
            f"Ahora mismo {business_name} está en {score_label_value} ({round(score_value, 1)}/100). "
            "Hay un grupo claro de clientes contentos que sostiene la reputación, "
            "pero también hay otro grupo que se queja de esperas, servicio y relación calidad-precio. "
            "La oportunidad está en arreglar esos fallos repetidos sin perder lo que ya funciona bien."
        ),
        "score": (
            f"La puntuación {round(score_value, 1)}/100 indica {score_label_value}. "
            "No sale solo de la media de estrellas: también cuenta cómo habla la gente en sus reseñas, "
            "si el negocio responde y si hay muchas opiniones claramente negativas. "
            "En resumen: combina números y sensación real del cliente."
        ),
        "cliente_y_preocupaciones": (
            "Se ven tres tipos de cliente bastante claros: el que sale encantado, "
            "el que ve cosas mejorables y el que acaba frustrado. "
            "Cada uno viene con expectativas distintas, así que conviene ajustar el servicio "
            "a lo que más se repite en sus comentarios."
        ),
        "plan_accion": (
            "El plan tiene que ir por fases: primero arreglos rápidos que se noten ya, "
            "luego cambios de proceso para que no se repitan errores, "
            "y por último mejoras más grandes de fondo. "
            "Todo con tareas concretas y responsables claros."
        ),
    }

    if not can_use_llm():
        return fallback

    prompt = (
        "Eres consultor de reputación para pymes. Escribe en español de España, cercano y fácil de entender, "
        "como si se lo explicaras al dueño de un negocio local sin formación técnica.\n"
        "No uses jerga ni anglicismos. Evita palabras como cluster, KPI, owner, insight, benchmark.\n"
        "Usa literalmente el bloque JSON del prompt de usuario como fuente de verdad para construir el diagnóstico.\n"
        "Devuelve SOLO JSON válido con claves exactas:\n"
        "{\n"
        '  "resumen_ejecutivo": "...",\n'
        '  "score": "...",\n'
        '  "cliente_y_preocupaciones": "...",\n'
        '  "plan_accion": "..."\n'
        "}\n"
        "Cada valor: 4-7 frases cortas, directas, útiles para decidir.\n"
        "PROMPT DE USUARIO — Template con datos del reporte\n"
        "Analiza el siguiente negocio y genera el diagnóstico estructurado según tus instrucciones.\n"
        f"json\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
    try:
        text, _model_used = await asyncio.to_thread(llm_generate_text, prompt)
        extracted = extract_json_object(text)
        parsed = json.loads(extracted)
        if not isinstance(parsed, dict):
            return fallback
        merged: dict[str, str] = {}
        for key, fallback_text in fallback.items():
            value = str(parsed.get(key, "") or "").strip()
            merged[key] = sanitize_llm_text(plainify_business_text(value or fallback_text))
        return merged
    except Exception:
        return fallback


async def build_llm_clustering_insights(
    *,
    business_name: str,
    customer_clusters: dict[str, Any],
    problem_clusters: dict[str, Any],
    quick_wins: dict[str, Any],
    can_use_llm: Callable[[], bool],
    llm_generate_text: Callable[[str], tuple[str, str | None]],
    sanitize_llm_text: Callable[[str], str],
    plainify_business_text: Callable[[str], str],
    fallback_text: str,
    generic_comment_problem: str,
) -> dict[str, Any]:
    if not can_use_llm():
        return {
            "generated": False,
            "model": None,
            "text": fallback_text,
            "reason": "llm_unavailable",
        }

    payload = {
        "business_name": business_name,
        "customer_clusters": customer_clusters.get("clusters"),
        "problem_clusters": problem_clusters.get("clusters"),
        "quick_wins": quick_wins.get("items"),
        "generic_comment_problem": generic_comment_problem,
    }
    prompt = (
        "Eres analista de experiencia cliente. Con estos datos, escribe SOLO texto plano "
        "en español cercano y muy claro (máximo 8 líneas). "
        "Evita tecnicismos y palabras en inglés. "
        "Explica: 1) tipos de cliente que aparecen, 2) problemas críticos, "
        "3) qué hacer primero en lenguaje sencillo.\n"
        f"Datos: {payload}"
    )
    try:
        text, model_used = await asyncio.to_thread(llm_generate_text, prompt)
        clean_text = str(text or "").strip()
        if not clean_text:
            raise RuntimeError("Empty LLM clustering output.")
        return {
            "generated": True,
            "model": model_used,
            "text": sanitize_llm_text(plainify_business_text(clean_text)),
        }
    except Exception:
        return {
            "generated": False,
            "model": None,
            "text": sanitize_llm_text(plainify_business_text(fallback_text)),
            "reason": "llm_failed",
        }


def fallback_clustering_text(
    *,
    customer_clusters: dict[str, Any],
    problem_clusters: dict[str, Any],
    quick_wins: dict[str, Any],
    generic_comment_problem: str,
) -> str:
    clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
    problems = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
    wins = quick_wins.get("items") if isinstance(quick_wins, dict) else []
    cluster_label = (
        str((clusters or [{}])[0].get("label", "segmentos mixtos"))
        if isinstance(clusters, list) and clusters
        else "segmentos mixtos"
    )
    problem_label = (
        str((problems or [{}])[0].get("problem", generic_comment_problem))
        if isinstance(problems, list) and problems
        else generic_comment_problem
    )
    return (
        f"El tipo de cliente que más pesa es '{cluster_label}', y el problema que más se repite es '{problem_label}'. "
        f"Hay {len(wins) if isinstance(wins, list) else 0} acciones rápidas ya detectadas para mejorar en el corto plazo."
    )
