from __future__ import annotations

import asyncio
import json
from typing import Any, Callable


async def build_llm_action_plan(
    *,
    business_name: str,
    business_context: dict[str, Any],
    top_problems: list[dict[str, Any]],
    friendly_problem_label: Callable[[str], str],
    safe_float: Callable[[Any, float], float],
    safe_int: Callable[[Any, int], int],
    can_use_llm: Callable[[], bool],
    llm_generate_text: Callable[[str], tuple[str, str | None]],
    extract_json_object: Callable[[str], str],
    sanitize_llm_text: Callable[[str], str],
    plainify_business_text: Callable[[str], str],
    normalize_action_type: Callable[[str], str],
    infer_action_type: Callable[[str], str],
    infer_action_tool: Callable[[str], str],
) -> dict[str, list[dict[str, Any]]] | None:
    problems_payload: list[dict[str, Any]] = []
    for problem in top_problems:
        problem_label = friendly_problem_label(str(problem.get("problem", "") or ""))
        sample_quotes: list[dict[str, Any]] = []
        for quote_item in (problem.get("sample_quotes") or [])[:3]:
            if not isinstance(quote_item, dict):
                continue
            text = str(quote_item.get("quote", "") or "").strip()
            if not text:
                continue
            sample_quotes.append(
                {
                    "rating": round(safe_float(quote_item.get("rating"), 0.0), 1),
                    "texto": text[:220],
                }
            )
        problems_payload.append(
            {
                "problema": problem_label,
                "severidad": round(safe_float(problem.get("severity"), 0.0), 2),
                "num_menciones": safe_int(problem.get("count"), 0),
                "valoracion_media_afectados": round(safe_float(problem.get("avg_rating"), 0.0), 2),
                "ejemplos_reales": sample_quotes,
            }
        )

    tipo_negocio = str((business_context or {}).get("tipo_negocio", "negocio local") or "negocio local").strip()
    cliente_espera = (business_context or {}).get("cliente_espera")
    if not isinstance(cliente_espera, list):
        cliente_espera = []
    fricciones_habituales = (business_context or {}).get("fricciones_habituales")
    if not isinstance(fricciones_habituales, list):
        fricciones_habituales = []

    prompt = (
        "Eres consultor de operaciones para negocios locales en España. "
        "Tu cliente es el dueño del negocio y necesita un plan útil, práctico y directo.\n\n"
        f"Negocio: {business_name or 'negocio local'}\n"
        f"Tipo de negocio: {tipo_negocio}\n"
        f"Qué espera su cliente: {', '.join(str(item) for item in cliente_espera) or 'sin datos'}\n"
        f"Fricciones habituales del sector: {', '.join(str(item) for item in fricciones_habituales) or 'sin datos'}\n\n"
        "Problemas detectados en reseñas reales:\n"
        f"{json.dumps(problems_payload, ensure_ascii=False, indent=2)}\n\n"
        "Genera acciones MUY CONCRETAS y aplicables sin contratar personal nuevo. "
        "No uses frases genéricas como 'mejorar el servicio'. Di exactamente qué hacer, quién lo hace y en qué plazo.\n"
        "Escribe en español de España, sin anglicismos, sin jerga técnica, sin markdown.\n\n"
        "Devuelve SOLO JSON válido con esta estructura exacta:\n"
        "{\n"
        '  "inmediato": [\n'
        "    {\n"
        '      "problema": "...",\n'
        '      "accion": "...",\n'
        '      "por_que": "...",\n'
        '      "encargado": "...",\n'
        '      "plazo_dias": 14,\n'
        '      "indicador": "...",\n'
        '      "tipo": "proceso|negocio|implementacion|tecnologico",\n'
        '      "herramienta_si_aplica": ""\n'
        "    }\n"
        "  ],\n"
        '  "medio": [\n'
        "    {\n"
        '      "problema": "...",\n'
        '      "accion": "...",\n'
        '      "por_que": "...",\n'
        '      "encargado": "...",\n'
        '      "plazo_dias": 60,\n'
        '      "indicador": "...",\n'
        '      "tipo": "proceso|negocio|implementacion|tecnologico",\n'
        '      "herramienta_si_aplica": ""\n'
        "    }\n"
        "  ],\n"
        '  "largo": [\n'
        "    {\n"
        '      "problema": "...",\n'
        '      "accion": "...",\n'
        '      "por_que": "...",\n'
        '      "encargado": "...",\n'
        '      "plazo_dias": 120,\n'
        '      "indicador": "...",\n'
        '      "tipo": "proceso|negocio|implementacion|tecnologico",\n'
        '      "herramienta_si_aplica": ""\n'
        "    }\n"
        "  ]\n"
        "}\n"
    )

    if not can_use_llm():
        return None

    try:
        raw_text, _model = await asyncio.to_thread(llm_generate_text, prompt)
        extracted = extract_json_object(raw_text)
        parsed = json.loads(extracted)
    except Exception:
        return None

    if not isinstance(parsed, dict):
        return None

    defaults = {"inmediato": 14, "medio": 60, "largo": 120}
    result: dict[str, list[dict[str, Any]]] = {"inmediato": [], "medio": [], "largo": []}

    for horizon in ("inmediato", "medio", "largo"):
        raw_actions = parsed.get(horizon)
        if not isinstance(raw_actions, list):
            continue
        for item in raw_actions:
            if not isinstance(item, dict):
                continue
            problem_text = friendly_problem_label(str(item.get("problema", "") or "").strip())
            action_text = sanitize_llm_text(
                plainify_business_text(str(item.get("accion", "") or "").strip())
            )
            reason_text = sanitize_llm_text(
                plainify_business_text(str(item.get("por_que", "") or "").strip())
            )
            owner_text = sanitize_llm_text(
                plainify_business_text(str(item.get("encargado", "") or "").strip())
            )
            indicator_text = sanitize_llm_text(
                plainify_business_text(str(item.get("indicador", "") or "").strip())
            )
            action_type = normalize_action_type(str(item.get("tipo", "") or "").strip())
            if not action_type:
                action_type = infer_action_type(f"{problem_text} {action_text}")
            action_tool = sanitize_llm_text(
                plainify_business_text(str(item.get("herramienta_si_aplica", "") or "").strip())
            )
            if not action_tool:
                action_tool = infer_action_tool(f"{problem_text} {action_text}")
            deadline_days = safe_int(item.get("plazo_dias"), defaults[horizon])
            if deadline_days <= 0:
                deadline_days = defaults[horizon]
            if not action_text:
                continue
            result[horizon].append(
                {
                    "problema": problem_text or "experiencia general",
                    "accion": action_text,
                    "action": action_text,
                    "por_que": reason_text,
                    "encargado": owner_text,
                    "owner": owner_text,
                    "plazo_dias": deadline_days,
                    "horizon_days": deadline_days,
                    "indicador": indicator_text,
                    "kpi": indicator_text,
                    "objetivo": indicator_text,
                    "impact": "alto" if horizon != "medio" else "medio",
                    "tipo": action_type,
                    "herramienta_si_aplica": action_tool,
                }
            )

    total_actions = sum(len(items) for items in result.values())
    if total_actions == 0:
        return None
    return result
