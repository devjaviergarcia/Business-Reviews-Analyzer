from __future__ import annotations

from html import escape
from typing import Any


DEFAULT_LEAD_REPORT_CTA = {
    "label": "Quiero el informe mensual completo",
    "url": "/cta/paid-report",
    "description": "Revision mensual de posicion, resenas, ficha y oportunidades frente a competidores.",
}


def build_lead_report_payload(
    *,
    business: dict[str, Any],
    deep_study_snapshot: dict[str, Any] | None = None,
    competitors: list[dict[str, Any]] | None = None,
    cta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    business_data = dict(business or {})
    snapshot = dict(deep_study_snapshot or {})
    competitor_items = [dict(item) for item in competitors or [] if isinstance(item, dict)]
    cta_payload = {**DEFAULT_LEAD_REPORT_CTA, **dict(cta or {})}

    score_breakdown = snapshot.get("score_breakdown") if isinstance(snapshot.get("score_breakdown"), dict) else {}
    health_score = _health_score(score_breakdown=score_breakdown, business=business_data)
    opportunities = _build_opportunities(snapshot=snapshot)
    comparison = _build_comparison(
        business=business_data,
        competitors=competitor_items,
    )
    data_notes = _build_data_notes(snapshot=snapshot)

    return {
        "business_name": _text(business_data.get("business_name") or snapshot.get("business_name") or "Negocio"),
        "category": _optional_text(business_data.get("category")),
        "city": _optional_text(business_data.get("city")),
        "address": _optional_text(business_data.get("address")),
        "rating": _coerce_float(business_data.get("rating")),
        "review_count": _coerce_int(business_data.get("review_count")),
        "discovery_rank": _coerce_int(business_data.get("discovery_rank")),
        "website": _optional_text(business_data.get("website")),
        "phone": _optional_text(business_data.get("phone")),
        "health_score": health_score,
        "opportunity_score": _coerce_float(score_breakdown.get("opportunity") or business_data.get("opportunity_score")),
        "executive_summary": _text(snapshot.get("executive_summary") or _fallback_summary(business_data)),
        "opportunities": opportunities,
        "immediate_action": opportunities[0] if opportunities else _fallback_action(),
        "comparison": comparison,
        "strengths": _slice_texts(snapshot.get("strengths"), limit=3),
        "data_notes": data_notes,
        "cta": cta_payload,
    }


def render_lead_report_html(
    *,
    business: dict[str, Any],
    deep_study_snapshot: dict[str, Any] | None = None,
    competitors: list[dict[str, Any]] | None = None,
    cta: dict[str, Any] | None = None,
) -> str:
    payload = build_lead_report_payload(
        business=business,
        deep_study_snapshot=deep_study_snapshot,
        competitors=competitors,
        cta=cta,
    )
    business_name = payload["business_name"]
    meta_items = [
        item
        for item in (
            payload.get("category"),
            payload.get("city"),
            payload.get("address"),
        )
        if item
    ]
    score = payload["health_score"]
    rating = payload.get("rating")
    review_count = payload.get("review_count")
    rank = payload.get("discovery_rank")

    metric_cards = [
        _metric_card("Score local", f"{score}/100", "Lectura rapida de reputacion, visibilidad y conversion."),
        _metric_card("Posicion Maps", f"#{rank}" if rank else "Sin dato", "Orden en el que aparece dentro del benchmark."),
        _metric_card(
            "Rating",
            f"{rating:.1f}" if rating is not None else "Sin dato",
            f"{review_count} resenas" if review_count is not None else "Volumen no disponible",
        ),
    ]
    if payload.get("website"):
        metric_cards.append(_metric_card("Conversion", "Web visible", payload["website"]))
    else:
        metric_cards.append(_metric_card("Conversion", "Sin web visible", "Oportunidad clara desde Google Maps."))

    opportunity_items = "\n".join(_opportunity_html(item, index=index + 1) for index, item in enumerate(payload["opportunities"]))
    comparison_html = _comparison_html(payload["comparison"])
    strengths_html = _list_section("Fortalezas actuales", payload["strengths"])
    notes_html = _list_section("Limitaciones del diagnostico", payload["data_notes"], muted=True)
    immediate_action = payload["immediate_action"]
    cta_payload = payload["cta"]

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lead report - {_e(business_name)}</title>
  <style>
    :root {{
      --ink: #172018;
      --muted: #5f6f63;
      --paper: #fbf7ed;
      --card: #ffffff;
      --accent: #1f6b4f;
      --accent-2: #f1b84b;
      --line: #e5dccb;
      --danger: #9d3a26;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top left, #fff4c7 0, transparent 34rem), var(--paper);
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      line-height: 1.5;
    }}
    main {{
      max-width: 980px;
      margin: 0 auto;
      padding: 32px 18px 48px;
    }}
    .hero {{
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(135deg, #ffffff 0%, #f8ecd2 100%);
      padding: 30px;
      box-shadow: 0 18px 50px rgba(47, 39, 20, 0.10);
    }}
    .eyebrow {{
      color: var(--accent);
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      letter-spacing: .12em;
      text-transform: uppercase;
      margin: 0 0 10px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(34px, 7vw, 68px);
      line-height: .95;
      letter-spacing: -0.05em;
    }}
    .meta {{
      color: var(--muted);
      margin-top: 14px;
      font-size: 15px;
    }}
    .summary {{
      max-width: 760px;
      margin: 24px 0 0;
      font-size: 18px;
    }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 20px;
    }}
    .metric, .section, .action {{
      border: 1px solid var(--line);
      border-radius: 22px;
      background: rgba(255, 255, 255, .86);
    }}
    .metric {{ padding: 16px; min-height: 132px; }}
    .metric strong {{
      display: block;
      font-size: 30px;
      line-height: 1;
      margin: 8px 0;
    }}
    .metric span, .muted {{ color: var(--muted); font-size: 14px; }}
    .grid {{
      display: grid;
      grid-template-columns: 1.15fr .85fr;
      gap: 16px;
      margin-top: 16px;
    }}
    .section, .action {{ padding: 22px; }}
    h2 {{ margin: 0 0 14px; font-size: 24px; letter-spacing: -0.03em; }}
    .opportunity {{
      display: grid;
      grid-template-columns: 34px 1fr;
      gap: 12px;
      padding: 14px 0;
      border-top: 1px solid var(--line);
    }}
    .opportunity:first-child {{ border-top: 0; padding-top: 0; }}
    .badge {{
      width: 34px;
      height: 34px;
      border-radius: 999px;
      display: grid;
      place-items: center;
      background: var(--accent);
      color: white;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
    }}
    .priority {{
      display: inline-block;
      color: var(--danger);
      font-size: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      text-transform: uppercase;
      letter-spacing: .08em;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{ color: var(--muted); font-weight: 400; }}
    ul {{ margin: 0; padding-left: 18px; }}
    li + li {{ margin-top: 8px; }}
    .action {{
      margin-top: 16px;
      background: var(--ink);
      color: white;
      border-color: var(--ink);
    }}
    .action .muted {{ color: rgba(255, 255, 255, .72); }}
    .cta {{
      display: inline-block;
      margin-top: 16px;
      padding: 12px 16px;
      border-radius: 999px;
      background: var(--accent-2);
      color: var(--ink);
      text-decoration: none;
      font-weight: 700;
    }}
    @media (max-width: 820px) {{
      .metrics, .grid {{ grid-template-columns: 1fr; }}
      .hero {{ padding: 22px; }}
      main {{ padding-top: 18px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Diagnostico beta de demanda local</p>
      <h1>{_e(business_name)}</h1>
      {_meta_html(meta_items)}
      <p class="summary">{_e(payload["executive_summary"])}</p>
      <div class="metrics">
        {"".join(metric_cards)}
      </div>
    </section>
    <section class="grid">
      <article class="section">
        <h2>3 oportunidades principales</h2>
        {opportunity_items}
      </article>
      <aside class="section">
        <h2>Comparativa rapida</h2>
        {comparison_html}
      </aside>
    </section>
    <section class="action">
      <p class="eyebrow">Accion inmediata recomendada</p>
      <h2>{_e(immediate_action["title"])}</h2>
      <p>{_e(immediate_action["rationale"])}</p>
      <p class="muted">{_e(immediate_action["expected_impact"])}</p>
      <a class="cta" href="{_e(str(cta_payload.get("url") or "#"))}">{_e(str(cta_payload.get("label") or DEFAULT_LEAD_REPORT_CTA["label"]))}</a>
      <p class="muted">{_e(str(cta_payload.get("description") or DEFAULT_LEAD_REPORT_CTA["description"]))}</p>
    </section>
    <section class="grid">
      {strengths_html}
      {notes_html}
    </section>
  </main>
</body>
</html>
"""


def _metric_card(label: str, value: str, description: str) -> str:
    return f"""<div class="metric">
  <span>{_e(label)}</span>
  <strong>{_e(value)}</strong>
  <span>{_e(description)}</span>
</div>"""


def _opportunity_html(item: dict[str, str], *, index: int) -> str:
    return f"""<div class="opportunity">
  <div class="badge">{index}</div>
  <div>
    <span class="priority">{_e(item.get("priority") or "media")}</span>
    <h3>{_e(item.get("title") or "Oportunidad")}</h3>
    <p>{_e(item.get("rationale") or "")}</p>
    <p class="muted">{_e(item.get("expected_impact") or "")}</p>
  </div>
</div>"""


def _comparison_html(comparison: dict[str, Any]) -> str:
    competitors = comparison.get("competitors") if isinstance(comparison.get("competitors"), list) else []
    if not competitors:
        return f"""<p>{_e(str(comparison.get("summary") or "Faltan competidores seleccionados para comparar."))}</p>"""
    rows = []
    for item in competitors:
        rank = _coerce_int(item.get("discovery_rank"))
        rating = _coerce_float(item.get("rating"))
        reviews = _coerce_int(item.get("review_count"))
        rows.append(
            "<tr>"
            f"<td>{_e(str(item.get('business_name') or 'Competidor'))}</td>"
            f"<td>{_e(f'#{rank}' if rank else '-')}</td>"
            f"<td>{_e(f'{rating:.1f}' if rating is not None else '-')}</td>"
            f"<td>{_e(str(reviews) if reviews is not None else '-')}</td>"
            "</tr>"
        )
    return f"""<p>{_e(str(comparison.get("summary") or ""))}</p>
<table>
  <thead><tr><th>Negocio</th><th>Orden</th><th>Rating</th><th>Resenas</th></tr></thead>
  <tbody>{"".join(rows)}</tbody>
</table>"""


def _list_section(title: str, items: list[str], *, muted: bool = False) -> str:
    if not items:
        return ""
    class_name = "section muted-section" if muted else "section"
    list_items = "".join(f"<li>{_e(item)}</li>" for item in items)
    return f"""<article class="{class_name}">
  <h2>{_e(title)}</h2>
  <ul>{list_items}</ul>
</article>"""


def _meta_html(items: list[str]) -> str:
    if not items:
        return ""
    return f"""<div class="meta">{_e(" - ".join(items))}</div>"""


def _build_opportunities(*, snapshot: dict[str, Any]) -> list[dict[str, str]]:
    actions = snapshot.get("monthly_actions") if isinstance(snapshot.get("monthly_actions"), list) else []
    opportunities: list[dict[str, str]] = []
    for item in actions:
        if not isinstance(item, dict):
            continue
        title = _optional_text(item.get("title"))
        if not title:
            continue
        opportunities.append(
            {
                "priority": _text(item.get("priority") or "medium"),
                "title": title,
                "rationale": _text(item.get("rationale") or "Accion recomendada por el diagnostico."),
                "expected_impact": _text(item.get("expected_impact") or "Mejorar conversion local."),
            }
        )
        if len(opportunities) == 3:
            return opportunities

    fallback_sources = []
    for key in ("risks", "competitor_gaps"):
        values = snapshot.get(key) if isinstance(snapshot.get(key), list) else []
        fallback_sources.extend(_slice_texts(values, limit=3))
    for item in fallback_sources:
        opportunities.append(
            {
                "priority": "medium",
                "title": item,
                "rationale": "Senal detectada en la ficha o comparativa local.",
                "expected_impact": "Priorizar esta mejora puede aumentar confianza y contacto.",
            }
        )
        if len(opportunities) == 3:
            return opportunities

    return [_fallback_action()]


def _build_comparison(*, business: dict[str, Any], competitors: list[dict[str, Any]]) -> dict[str, Any]:
    rank = _coerce_int(business.get("discovery_rank"))
    ranked_competitors = sorted(
        competitors,
        key=lambda item: (_coerce_int(item.get("discovery_rank")) or 9999, -(_coerce_int(item.get("review_count")) or 0)),
    )
    competitor_ranks = [_coerce_int(item.get("discovery_rank")) for item in ranked_competitors]
    competitor_ranks = [item for item in competitor_ranks if item is not None]
    if rank and competitor_ranks:
        avg_rank = sum(competitor_ranks) / len(competitor_ranks)
        if rank <= avg_rank:
            summary = f"Aparece #{rank}, mejor o similar que la media visible de competidores (#{avg_rank:.1f})."
        else:
            summary = f"Aparece #{rank}, por detras de la media visible de competidores (#{avg_rank:.1f})."
    elif rank:
        summary = f"Aparece #{rank}. Falta una muestra competitiva suficiente para contextualizarlo."
    else:
        summary = "Todavia no hay posicion de aparicion para ponderar visibilidad en Maps."

    return {
        "summary": summary,
        "competitors": ranked_competitors[:3],
    }


def _build_data_notes(*, snapshot: dict[str, Any]) -> list[str]:
    warnings = snapshot.get("warnings") if isinstance(snapshot.get("warnings"), list) else []
    notes = []
    mapping = {
        "missing_reviews": "Faltan resenas textuales: el informe usa ficha y benchmark.",
        "missing_competitors": "Faltan competidores seleccionados: comparativa limitada.",
        "missing_rating": "No hay rating fiable en el listing.",
        "missing_review_count": "No hay volumen de resenas fiable.",
        "missing_discovery_rank": "No hay orden de aparicion en el benchmark.",
        "missing_website": "No hay web visible en la ficha.",
        "missing_phone": "No hay telefono visible en la ficha.",
    }
    for warning in warnings:
        raw = str(warning or "")
        key = raw.split(":", 1)[0]
        notes.append(mapping.get(key, raw))
    return list(dict.fromkeys([item for item in notes if item]))[:4]


def _health_score(*, score_breakdown: dict[str, Any], business: dict[str, Any]) -> int:
    opportunity = _coerce_float(score_breakdown.get("opportunity") or business.get("opportunity_score"))
    if opportunity is not None:
        return _bound_int(100.0 - opportunity)
    rating = _coerce_float(business.get("rating"))
    review_count = _coerce_int(business.get("review_count")) or 0
    rank = _coerce_int(business.get("discovery_rank"))
    rating_score = (rating or 0) / 5 * 55
    review_score = min(review_count / 300, 1) * 25
    rank_score = max(0, 20 - ((rank or 12) - 1) * 2)
    return _bound_int(rating_score + review_score + rank_score)


def _fallback_summary(business: dict[str, Any]) -> str:
    name = _text(business.get("business_name") or "Este negocio")
    rank = _coerce_int(business.get("discovery_rank"))
    rating = _coerce_float(business.get("rating"))
    pieces = [f"{name} tiene una ficha suficiente para iniciar un diagnostico local."]
    if rank:
        pieces.append(f"Aparece en posicion #{rank} dentro del benchmark.")
    if rating is not None:
        pieces.append(f"Su rating visible es {rating:.1f}.")
    return " ".join(pieces)


def _fallback_action() -> dict[str, str]:
    return {
        "priority": "medium",
        "title": "Completar datos criticos de la ficha",
        "rationale": "El diagnostico necesita rating, resenas, posicion y datos de contacto fiables.",
        "expected_impact": "Base mas solida para decidir la siguiente mejora comercial.",
    }


def _slice_texts(value: Any, *, limit: int) -> list[str]:
    if not isinstance(value, list):
        return []
    output = []
    for item in value:
        text = _optional_text(item)
        if text:
            output.append(text)
        if len(output) == limit:
            break
    return output


def _text(value: Any) -> str:
    return str(value or "").strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


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


def _bound_int(value: float) -> int:
    return int(round(min(max(float(value), 0.0), 100.0)))


def _e(value: str) -> str:
    return escape(str(value), quote=True)
