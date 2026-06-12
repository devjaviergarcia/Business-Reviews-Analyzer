from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any

from src.crm.reports.lead_report import build_lead_report_payload


DEFAULT_PAID_REPORT_CTA = {
    "label": "Agendar revision mensual",
    "url": "/cta/monthly-review",
    "description": "Revisar cambios, resolver bloqueos y decidir las acciones del proximo mes.",
}


def build_paid_report_payload(
    *,
    business: dict[str, Any],
    deep_study_snapshot: dict[str, Any] | None = None,
    competitors: list[dict[str, Any]] | None = None,
    history: list[dict[str, Any]] | None = None,
    report_month: str | None = None,
    cta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lead_payload = build_lead_report_payload(
        business=business,
        deep_study_snapshot=deep_study_snapshot,
        competitors=competitors,
        cta=cta,
    )
    snapshot = dict(deep_study_snapshot or {})
    history_items = [dict(item) for item in history or [] if isinstance(item, dict)]
    actions = _monthly_plan(snapshot=snapshot)
    templates = _response_templates(snapshot=snapshot, business_name=lead_payload["business_name"])
    cta_payload = {**DEFAULT_PAID_REPORT_CTA, **dict(cta or {})}
    return {
        **lead_payload,
        "report_month": str(report_month or datetime.now(timezone.utc).strftime("%Y-%m")),
        "history": history_items,
        "monthly_plan": actions,
        "response_templates": templates,
        "cta": cta_payload,
    }


def render_paid_report_html(
    *,
    business: dict[str, Any],
    deep_study_snapshot: dict[str, Any] | None = None,
    competitors: list[dict[str, Any]] | None = None,
    history: list[dict[str, Any]] | None = None,
    report_month: str | None = None,
    cta: dict[str, Any] | None = None,
) -> str:
    payload = build_paid_report_payload(
        business=business,
        deep_study_snapshot=deep_study_snapshot,
        competitors=competitors,
        history=history,
        report_month=report_month,
        cta=cta,
    )
    competitors_html = _competitors_html(payload.get("comparison", {}))
    plan_html = "".join(_plan_item_html(item, index=index + 1) for index, item in enumerate(payload["monthly_plan"]))
    templates_html = "".join(_template_html(item) for item in payload["response_templates"])
    history_html = _history_html(payload["history"])
    notes_html = _notes_html(payload.get("data_notes") or [])
    score_explanation_html = _score_explanation_html(payload.get("score_explanation"))
    cta_payload = payload["cta"]

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Paid report mensual - {_e(payload["business_name"])}</title>
  <style>
    :root {{
      --ink: #101716;
      --muted: #60706b;
      --bg: #eef4ed;
      --card: #ffffff;
      --line: #d7e1d6;
      --accent: #0f6b55;
      --accent-2: #d8ff7a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        linear-gradient(135deg, rgba(216,255,122,.38), transparent 30rem),
        linear-gradient(315deg, rgba(15,107,85,.16), transparent 28rem),
        var(--bg);
      color: var(--ink);
      font-family: "Aptos", "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 32px 18px 56px; }}
    .hero, .card {{
      background: rgba(255,255,255,.92);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: 0 18px 48px rgba(16,23,22,.08);
    }}
    .hero {{ padding: 32px; }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent);
      font-size: 12px;
      letter-spacing: .12em;
      text-transform: uppercase;
      font-weight: 800;
    }}
    h1 {{ margin: 0; font-size: clamp(36px, 7vw, 74px); line-height: .92; letter-spacing: -.06em; }}
    h2 {{ margin: 0 0 16px; font-size: 25px; letter-spacing: -.03em; }}
    h3 {{ margin: 0 0 6px; }}
    .summary {{ max-width: 820px; font-size: 18px; color: #263532; }}
    .dash {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-top: 22px;
    }}
    .metric {{
      border-radius: 22px;
      padding: 18px;
      background: #f8fbf5;
      border: 1px solid var(--line);
    }}
    .metric strong {{ display: block; font-size: 34px; line-height: 1; margin: 8px 0; }}
    .muted, .metric span {{ color: var(--muted); font-size: 14px; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }}
    .card {{ padding: 24px; }}
    .score-explanation {{ margin-top: 16px; }}
    .score-parts {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .score-part {{
      border-radius: 18px;
      padding: 16px;
      background: #f8fbf5;
      border: 1px solid var(--line);
    }}
    .score-part strong {{ display: block; margin-top: 6px; font-size: 28px; line-height: 1; }}
    .reason-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 16px;
    }}
    .reason-box {{
      border-radius: 20px;
      padding: 18px;
      border: 1px solid var(--line);
      background: #f8fbf5;
    }}
    .reason-box h3 {{ margin: 0 0 10px; }}
    .reason-box ul {{ margin: 0; padding-left: 18px; }}
    .reason-box--good h3 {{ color: var(--accent); }}
    .reason-box--bad h3 {{ color: #9d3a26; }}
    .plan-item {{
      display: grid;
      grid-template-columns: 42px 1fr;
      gap: 14px;
      padding: 15px 0;
      border-top: 1px solid var(--line);
    }}
    .plan-item:first-of-type {{ border-top: 0; padding-top: 0; }}
    .week {{
      height: 42px;
      border-radius: 14px;
      background: var(--ink);
      color: white;
      display: grid;
      place-items: center;
      font-weight: 800;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 11px 8px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .template {{
      background: #f8fbf5;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      margin-top: 10px;
    }}
    .cta {{
      display: inline-block;
      margin-top: 16px;
      padding: 13px 17px;
      border-radius: 999px;
      background: var(--accent-2);
      color: var(--ink);
      font-weight: 800;
      text-decoration: none;
    }}
    @media (max-width: 860px) {{
      .dash, .grid, .score-parts, .reason-grid {{ grid-template-columns: 1fr; }}
      .hero {{ padding: 22px; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Paid report mensual - {_e(payload["report_month"])}</p>
      <h1>{_e(payload["business_name"])}</h1>
      <p class="summary">{_e(payload["executive_summary"])}</p>
      <div class="dash">
        {_metric("Score local", f'{payload["health_score"]}/100', "Estado mensual de reputacion, visibilidad y conversion.")}
        {_metric("Orden Maps", f'#{payload["discovery_rank"]}' if payload.get("discovery_rank") else "Sin dato", "Posicion capturada en el benchmark.")}
        {_metric("Rating", _rating_text(payload), _reviews_text(payload))}
        {_metric("Oportunidad", _opportunity_text(payload), "Cuanto margen accionable queda este mes.")}
      </div>
    </section>
    {score_explanation_html}
    <section class="grid">
      <article class="card">
        <h2>Comparativa con competidores</h2>
        {competitors_html}
      </article>
      <article class="card">
        <h2>Historico y seguimiento</h2>
        {history_html}
      </article>
    </section>
    <section class="card" style="margin-top:16px">
      <h2>Plan mensual de accion</h2>
      {plan_html}
    </section>
    <section class="grid">
      <article class="card">
        <h2>Plantillas de respuesta</h2>
        {templates_html}
      </article>
      <article class="card">
        <h2>Gestion del siguiente mes</h2>
        <p>Este informe se usa como base de seguimiento: revisar cambios de ficha, capturar nuevas resenas y comparar la posicion contra el mismo set competitivo.</p>
        {notes_html}
        <a class="cta" href="{_e(str(cta_payload.get("url") or "#"))}">{_e(str(cta_payload.get("label") or DEFAULT_PAID_REPORT_CTA["label"]))}</a>
        <p class="muted">{_e(str(cta_payload.get("description") or DEFAULT_PAID_REPORT_CTA["description"]))}</p>
      </article>
    </section>
  </main>
</body>
</html>
"""


def _metric(label: str, value: str, description: str) -> str:
    return f"""<div class="metric"><span>{_e(label)}</span><strong>{_e(value)}</strong><span>{_e(description)}</span></div>"""


def _competitors_html(comparison: dict[str, Any]) -> str:
    competitors = comparison.get("competitors") if isinstance(comparison.get("competitors"), list) else []
    if not competitors:
        return "<p>No hay competidores seleccionados. El primer mes debe fijar el set competitivo estable.</p>"
    rows = []
    for item in competitors:
        rank = _coerce_int(item.get("discovery_rank"))
        rows.append(
            "<tr>"
            f"<td>{_e(str(item.get('business_name') or 'Competidor'))}</td>"
            f"<td>{_e(f'#{rank}' if rank else '-')}</td>"
            f"<td>{_e(_float_text(item.get('rating')))}</td>"
            f"<td>{_e(str(_coerce_int(item.get('review_count')) or '-'))}</td>"
            f"<td>{_e('Si' if item.get('website') else 'No')}</td>"
            "</tr>"
        )
    return f"""<p>{_e(str(comparison.get("summary") or ""))}</p>
<table>
  <thead><tr><th>Negocio</th><th>Orden</th><th>Rating</th><th>Resenas</th><th>Web</th></tr></thead>
  <tbody>{"".join(rows)}</tbody>
</table>"""


def _history_html(history: list[dict[str, Any]]) -> str:
    if not history:
        return "<p>Sin historico todavia. Este informe queda como mes base para medir rating, resenas, orden de aparicion y acciones ejecutadas.</p>"
    rows = []
    for item in history[-6:]:
        rank = _coerce_int(item.get("discovery_rank"))
        rows.append(
            "<tr>"
            f"<td>{_e(str(item.get('month') or item.get('report_month') or '-'))}</td>"
            f"<td>{_e(str(item.get('health_score') or '-'))}</td>"
            f"<td>{_e(f'#{rank}' if rank else '-')}</td>"
            f"<td>{_e(_float_text(item.get('rating')))}</td>"
            f"<td>{_e(str(_coerce_int(item.get('review_count')) or '-'))}</td>"
            "</tr>"
        )
    return f"""<table>
  <thead><tr><th>Mes</th><th>Score</th><th>Orden</th><th>Rating</th><th>Resenas</th></tr></thead>
  <tbody>{"".join(rows)}</tbody>
</table>"""


def _score_explanation_html(score_explanation: dict[str, Any] | None) -> str:
    if not isinstance(score_explanation, dict):
        return ""
    positives = score_explanation.get("positives") if isinstance(score_explanation.get("positives"), list) else []
    negatives = score_explanation.get("negatives") if isinstance(score_explanation.get("negatives"), list) else []
    component_scores = (
        score_explanation.get("component_scores")
        if isinstance(score_explanation.get("component_scores"), dict)
        else {}
    )
    if not positives and not negatives and not component_scores:
        return ""

    summary = str(score_explanation.get("summary") or "").strip()
    component_cards = []
    labels = {
        "reputation": "Reputacion",
        "visibility": "Visibilidad",
        "conversion": "Conversion",
        "response": "Respuesta",
    }
    for key in ("reputation", "visibility", "conversion", "response"):
        value = _coerce_float(component_scores.get(key))
        if value is None:
            continue
        component_cards.append(
            f"""<div class="score-part">
  <span>{_e(labels[key])}</span>
  <strong>{_e(f"{value:.0f}/100")}</strong>
</div>"""
        )

    positives_html = (
        "<ul>" + "".join(f"<li>{_e(str(item))}</li>" for item in positives[:4] if str(item).strip()) + "</ul>"
        if positives
        else "<p class=\"muted\">No destaca una palanca positiva clara en este corte.</p>"
    )
    negatives_html = (
        "<ul>" + "".join(f"<li>{_e(str(item))}</li>" for item in negatives[:4] if str(item).strip()) + "</ul>"
        if negatives
        else "<p class=\"muted\">No destaca un freno principal con los datos disponibles.</p>"
    )

    return f"""<section class="card score-explanation">
  <h2>Por qué sale este score</h2>
  {f"<p>{_e(summary)}</p>" if summary else ""}
  <div class="score-parts">{"".join(component_cards)}</div>
  <div class="reason-grid">
    <div class="reason-box reason-box--good">
      <h3>Lo que suma</h3>
      {positives_html}
    </div>
    <div class="reason-box reason-box--bad">
      <h3>Lo que resta</h3>
      {negatives_html}
    </div>
  </div>
</section>"""


def _monthly_plan(*, snapshot: dict[str, Any]) -> list[dict[str, str]]:
    actions = snapshot.get("monthly_actions") if isinstance(snapshot.get("monthly_actions"), list) else []
    output = []
    for index, action in enumerate(actions[:4]):
        if not isinstance(action, dict):
            continue
        output.append(
            {
                "week": f"S{index + 1}",
                "title": str(action.get("title") or "Accion mensual").strip(),
                "rationale": str(action.get("rationale") or "Prioridad detectada en el diagnostico.").strip(),
                "expected_impact": str(action.get("expected_impact") or "Mejora medible en visibilidad o conversion.").strip(),
            }
        )
    while len(output) < 4:
        index = len(output)
        fallback = [
            ("Auditar ficha", "Revisar categoria, atributos, telefono, web y fotos.", "Base de conversion sin friccion."),
            ("Captar resenas", "Activar una rutina sencilla de solicitud a clientes reales.", "Mas prueba social mensual."),
            ("Responder resenas", "Responder positivas y criticas con tono estable.", "Mejor percepcion publica."),
            ("Publicar contenido local", "Convertir platos, servicios o casos reales en contenido local.", "Mas senales de confianza."),
        ][index]
        output.append({"week": f"S{index + 1}", "title": fallback[0], "rationale": fallback[1], "expected_impact": fallback[2]})
    return output


def _plan_item_html(item: dict[str, str], *, index: int) -> str:
    week = item.get("week") or f"S{index}"
    return f"""<div class="plan-item">
  <div class="week">{_e(week)}</div>
  <div>
    <h3>{_e(item.get("title") or "Accion")}</h3>
    <p>{_e(item.get("rationale") or "")}</p>
    <p class="muted">{_e(item.get("expected_impact") or "")}</p>
  </div>
</div>"""


def _response_templates(*, snapshot: dict[str, Any], business_name: str) -> list[dict[str, str]]:
    templates = snapshot.get("response_templates") if isinstance(snapshot.get("response_templates"), list) else []
    output = []
    for item in templates[:3]:
        if not isinstance(item, dict):
            continue
        output.append(
            {
                "scenario": str(item.get("scenario") or "escenario").strip(),
                "template": str(item.get("template") or "").strip(),
            }
        )
    if output:
        return output
    return [
        {
            "scenario": "resena positiva",
            "template": f"Gracias por valorar {business_name}. Nos ayuda mucho saber que la experiencia estuvo a la altura.",
        },
        {
            "scenario": "resena critica",
            "template": "Gracias por contarnos lo ocurrido. Revisaremos el caso con el equipo para mejorar la experiencia.",
        },
    ]


def _template_html(item: dict[str, str]) -> str:
    return f"""<div class="template">
  <strong>{_e(item.get("scenario") or "escenario")}</strong>
  <p>{_e(item.get("template") or "")}</p>
</div>"""


def _notes_html(notes: list[str]) -> str:
    if not notes:
        return "<p class=\"muted\">No hay limitaciones criticas adicionales con los datos actuales.</p>"
    return "<ul>" + "".join(f"<li>{_e(note)}</li>" for note in notes[:4]) + "</ul>"


def _rating_text(payload: dict[str, Any]) -> str:
    rating = _coerce_float(payload.get("rating"))
    return f"{rating:.1f}" if rating is not None else "Sin dato"


def _reviews_text(payload: dict[str, Any]) -> str:
    reviews = _coerce_int(payload.get("review_count"))
    return f"{reviews} resenas" if reviews is not None else "Volumen no disponible"


def _opportunity_text(payload: dict[str, Any]) -> str:
    opportunity = _coerce_float(payload.get("opportunity_score"))
    return f"{opportunity:.0f}/100" if opportunity is not None else "Sin dato"


def _float_text(value: Any) -> str:
    parsed = _coerce_float(value)
    return f"{parsed:.1f}" if parsed is not None else "-"


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


def _e(value: str) -> str:
    return escape(str(value), quote=True)
