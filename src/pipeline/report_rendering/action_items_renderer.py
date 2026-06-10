from __future__ import annotations

import html
from typing import Any


def render_action_items(renderer: Any, payload: Any, *, is_quick_wins: bool = False) -> str:
    if not isinstance(payload, list):
        return ""
    cards: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if is_quick_wins:
            quick_win_card = _render_quick_win_card(renderer, item)
            if quick_win_card:
                cards.append(quick_win_card)
            continue

        action_card = _render_action_plan_card(renderer, item)
        if action_card:
            cards.append(action_card)
    if not cards:
        return ""
    return f"<ul class='action-list'>{''.join(cards)}</ul>"


def _render_quick_win_card(renderer: Any, item: dict[str, Any]) -> str:
    titulo = str(item.get("title", "") or "").strip()
    por_que = str(item.get("why", "") or "").strip()
    esfuerzo = str(item.get("effort", "") or "").strip()
    impacto = str(item.get("impact", "") or "").strip()
    if not titulo:
        return ""
    titulo_h = renderer._humanize_action_text(titulo)
    por_que_h = renderer._humanize_action_text(por_que)
    esfuerzo_h = renderer._humanize_effort(effort=esfuerzo)
    impacto_h = renderer._humanize_impact(impact=impacto)
    return (
        "<li class='action-card'>"
        f"<div class='title'>{html.escape(renderer._clean_narrative_text(titulo_h))}</div>"
        f"<div>{html.escape(renderer._clean_narrative_text(por_que_h))}</div>"
        f"<div class='meta-line'>Esfuerzo: {html.escape(esfuerzo_h)} · Impacto esperado: {html.escape(impacto_h)}</div>"
        "</li>"
    )


def _render_action_plan_card(renderer: Any, item: dict[str, Any]) -> str:
    accion = str(item.get("accion") or item.get("action") or "").strip()
    if not accion:
        return ""
    por_que = str(item.get("por_que") or item.get("why") or "").strip()
    encargado = str(item.get("encargado") or item.get("owner") or "").strip()
    objetivo = str(item.get("objetivo") or item.get("kpi") or "").strip()
    action_type = str(item.get("tipo", "") or "").strip().lower()
    tool = str(item.get("herramienta_si_aplica", "") or "").strip()
    if not action_type:
        action_type = renderer._infer_action_type_from_text(f"{item.get('problema', '')} {accion}")
    if not tool:
        tool = renderer._infer_action_tool_from_text(f"{item.get('problema', '')} {accion}")
    accion_h = renderer._humanize_action_text(accion)
    por_que_h = renderer._humanize_action_text(por_que)
    encargado_h = renderer._humanize_role(encargado)
    objetivo_h = renderer._humanize_action_text(objetivo)
    tool_h = renderer._humanize_action_text(tool)
    plazo = item.get("horizon_days") or item.get("horizonte_dias")
    plazo_text = ""
    if plazo is not None:
        try:
            plazo_text = f"{int(plazo)} días"
        except (TypeError, ValueError):
            plazo_text = str(plazo)

    badge_cfg = renderer._action_type_badge(action_type)
    badge_html = (
        f"<span class='tipo-badge' style='background:{badge_cfg['bg']};"
        f"color:{badge_cfg['text']};border-color:{badge_cfg['border']}'>{html.escape(badge_cfg['label'])}</span>"
    )
    return (
        "<li class='action-card'>"
        "<div class='action-card-header'>"
        f"<div class='title'>{html.escape(renderer._clean_narrative_text(accion_h))}</div>"
        f"{badge_html}"
        "</div>"
        + (f"<div>{html.escape(renderer._clean_narrative_text(por_que_h))}</div>" if por_que_h else "")
        + (
            f"<div class='meta-line'>Encargado de resolverlo: {html.escape(encargado_h)}</div>"
            if encargado_h
            else ""
        )
        + (f"<div class='meta-line'>Plazo objetivo: {html.escape(plazo_text)}</div>" if plazo_text else "")
        + (
            f"<div class='meta-line'>Indicador de seguimiento: "
            f"{html.escape(renderer._clean_narrative_text(objetivo_h))}</div>"
            if objetivo_h
            else ""
        )
        + (
            f"<div class='meta-line'>Herramienta: {html.escape(renderer._clean_narrative_text(tool_h))}</div>"
            if tool_h
            else ""
        )
        + "</li>"
    )
