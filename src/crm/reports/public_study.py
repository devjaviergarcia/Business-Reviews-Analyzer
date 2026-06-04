from __future__ import annotations

from collections import Counter
from html import escape
from statistics import mean
from typing import Any


DEFAULT_PUBLIC_STUDY_CTA = {
    "label": "Pedir mi diagnostico gratuito",
    "url": "https://repiq.es/?utm_source=public_study&utm_medium=owned&utm_campaign=benchmark_public&utm_content=cta#pre-report-form",
    "description": "Recibe una lectura concreta de posicion, ficha y oportunidades frente a negocios similares.",
}


SENSITIVE_KEYS = {
    "email",
    "email_normalized",
    "phone",
    "website",
    "address",
    "maps_url",
    "maps_url_canonical",
    "source_ref",
    "raw_snapshot",
}


def build_public_study_payload(
    *,
    benchmark_run: dict[str, Any],
    businesses: list[dict[str, Any]],
    cta: dict[str, Any] | None = None,
    geo_grid_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run = dict(benchmark_run or {})
    business_items = [dict(item) for item in businesses or [] if isinstance(item, dict)]
    cta_payload = {**DEFAULT_PUBLIC_STUDY_CTA, **dict(cta or {})}
    benchmark_id = str(run.get("benchmark_run_id") or run.get("id") or "").strip()
    if benchmark_id and "utm_content=" not in str(cta_payload.get("url") or ""):
        separator = "&" if "?" in str(cta_payload.get("url") or "") else "?"
        cta_payload["url"] = f"{cta_payload['url']}{separator}utm_content={benchmark_id}"

    ratings = [_coerce_float(item.get("rating")) for item in business_items]
    ratings = [item for item in ratings if item is not None]
    reviews = [_coerce_int(item.get("review_count")) for item in business_items]
    reviews = [item for item in reviews if item is not None]
    ranks = [_coerce_int(item.get("discovery_rank")) for item in business_items]
    ranks = [item for item in ranks if item is not None]
    website_count = sum(1 for item in business_items if _has_text(item.get("website")))
    phone_count = sum(1 for item in business_items if _has_text(item.get("phone")))
    enriched_count = sum(1 for item in business_items if bool(item.get("listing_enriched")))

    top_visible = sorted(
        business_items,
        key=lambda item: (_coerce_int(item.get("discovery_rank")) or 9999, -(_coerce_int(item.get("review_count")) or 0)),
    )[:10]
    opportunity_items = sorted(
        business_items,
        key=lambda item: (_coerce_float(item.get("opportunity_score")) or 0.0, _coerce_int(item.get("discovery_rank")) or 9999),
        reverse=True,
    )[:8]

    return {
        "benchmark_run_id": benchmark_id or None,
        "title": str(run.get("title") or _fallback_title(run)).strip(),
        "query": str(run.get("query") or "").strip(),
        "city": str(run.get("city") or "").strip() or None,
        "category": str(run.get("category") or "").strip() or None,
        "status": str(run.get("status") or "").strip() or None,
        "metrics": {
            "businesses": len(business_items),
            "avg_rating": round(mean(ratings), 2) if ratings else None,
            "avg_review_count": round(mean(reviews), 1) if reviews else None,
            "best_visible_rank": min(ranks) if ranks else None,
            "website_share": _share(website_count, len(business_items)),
            "phone_share": _share(phone_count, len(business_items)),
            "enriched_share": _share(enriched_count, len(business_items)),
        },
        "category_distribution": _category_distribution(business_items),
        "public_insights": _public_insights(business_items),
        "top_visible_examples": [_anonymize_business(item, index=index + 1) for index, item in enumerate(top_visible)],
        "opportunity_examples": [_anonymize_business(item, index=index + 1) for index, item in enumerate(opportunity_items)],
        "geo_visibility": _build_geo_visibility_payload(geo_grid_stats),
        "cta": cta_payload,
        "data_notes": _data_notes(run, business_items),
    }


def render_public_study_html(
    *,
    benchmark_run: dict[str, Any],
    businesses: list[dict[str, Any]],
    cta: dict[str, Any] | None = None,
    geo_grid_stats: dict[str, Any] | None = None,
) -> str:
    payload = build_public_study_payload(
        benchmark_run=benchmark_run,
        businesses=businesses,
        cta=cta,
        geo_grid_stats=geo_grid_stats,
    )
    metrics = payload["metrics"]
    cta_payload = payload["cta"]
    category_html = _category_html(payload["category_distribution"])
    visible_html = _examples_table(payload["top_visible_examples"], title="Negocios mejor posicionados")
    opportunity_html = _examples_table(payload["opportunity_examples"], title="Negocios con mas oportunidad")
    geo_visibility_html = _geo_visibility_html(payload.get("geo_visibility"))
    insights_html = "".join(f"<li>{_e(item)}</li>" for item in payload["public_insights"])
    notes_html = "".join(f"<li>{_e(item)}</li>" for item in payload["data_notes"])

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_e(payload["title"])}</title>
  <style>
    :root {{
      --ink: #15120c;
      --muted: #6a6255;
      --paper: #f7f0df;
      --card: #fffaf0;
      --line: #ddcfb2;
      --accent: #b94f2a;
      --accent-2: #143d35;
      --gold: #e6b85c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 0%, rgba(230,184,92,.42), transparent 30rem),
        radial-gradient(circle at 100% 20%, rgba(20,61,53,.18), transparent 28rem),
        var(--paper);
      font-family: "Georgia", "Times New Roman", serif;
      line-height: 1.55;
    }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 34px 18px 56px; }}
    .hero, .card {{
      border: 1px solid var(--line);
      border-radius: 30px;
      background: rgba(255,250,240,.88);
      box-shadow: 0 18px 52px rgba(50,37,18,.10);
    }}
    .hero {{ padding: 34px; }}
    .eyebrow {{
      margin: 0 0 10px;
      color: var(--accent);
      font: 800 12px/1 ui-monospace, SFMono-Regular, Menlo, monospace;
      letter-spacing: .14em;
      text-transform: uppercase;
    }}
    h1 {{ margin: 0; font-size: clamp(38px, 7vw, 78px); line-height: .92; letter-spacing: -.06em; }}
    h2 {{ margin: 0 0 14px; font-size: 25px; letter-spacing: -.03em; }}
    .summary {{ max-width: 780px; color: #332a1b; font-size: 18px; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 22px; }}
    .metric {{ border: 1px solid var(--line); border-radius: 22px; padding: 18px; background: #fffdf8; }}
    .metric strong {{ display: block; font-size: 34px; line-height: 1; margin: 8px 0; }}
    .muted, .metric span {{ color: var(--muted); font-size: 14px; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }}
    .card {{ padding: 24px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    ul {{ margin: 0; padding-left: 20px; }}
    li + li {{ margin-top: 8px; }}
    .cta-card {{ background: var(--accent-2); color: white; border-color: var(--accent-2); }}
    .cta-card .muted {{ color: rgba(255,255,255,.76); }}
    .cta {{
      display: inline-block;
      margin-top: 16px;
      padding: 13px 18px;
      border-radius: 999px;
      background: var(--gold);
      color: var(--ink);
      text-decoration: none;
      font-weight: 800;
    }}
    @media (max-width: 860px) {{ .metrics, .grid {{ grid-template-columns: 1fr; }} .hero {{ padding: 22px; }} }}
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Estudio local anonimo</p>
      <h1>{_e(payload["title"])}</h1>
      <p class="summary">Analisis agregado de {int(metrics["businesses"])} negocios para detectar patrones de visibilidad, reputacion y conversion local. No se publican emails, telefonos, direcciones ni URLs de negocio.</p>
      <div class="metrics">
        {_metric("Negocios", str(metrics["businesses"]), "Muestra capturada del benchmark.")}
        {_metric("Rating medio", _float_or_dash(metrics["avg_rating"]), "Promedio de fichas con rating disponible.")}
        {_metric("Resenas medias", _float_or_dash(metrics["avg_review_count"]), "Volumen medio de prueba social.")}
        {_metric("Con web", _percent_or_dash(metrics["website_share"]), "Porcentaje con enlace externo visible.")}
      </div>
    </section>
    <section class="grid">
      <article class="card">
        <h2>Lecturas principales</h2>
        <ul>{insights_html}</ul>
      </article>
      <article class="card">
        <h2>Distribucion por categoria</h2>
        {category_html}
      </article>
    </section>
    <section class="grid">
      {visible_html}
      {opportunity_html}
    </section>
    {geo_visibility_html}
    <section class="grid">
      <article class="card">
        <h2>Notas de datos</h2>
        <ul>{notes_html}</ul>
      </article>
      <article class="card cta-card">
        <p class="eyebrow">CTA trackeable</p>
        <h2>Quieres ver tu negocio dentro de esta lectura?</h2>
        <p>El informe individual baja estos patrones a tu ficha: orden de aparicion, rating, resenas, competidores y primeras acciones.</p>
        <a class="cta" href="{_e(str(cta_payload.get('url') or '#'))}">{_e(str(cta_payload.get('label') or DEFAULT_PUBLIC_STUDY_CTA['label']))}</a>
        <p class="muted">{_e(str(cta_payload.get('description') or DEFAULT_PUBLIC_STUDY_CTA['description']))}</p>
      </article>
    </section>
  </main>
</body>
</html>
"""


def public_study_payload_is_safe(payload: dict[str, Any]) -> bool:
    text = str(payload).lower()
    return not any(key in text for key in SENSITIVE_KEYS)


def _anonymize_business(item: dict[str, Any], *, index: int) -> dict[str, Any]:
    business_name = str(item.get("business_name") or "").strip()
    label = business_name or f"Negocio #{index}"
    return {
        "label": label,
        "category": str(item.get("category") or "Sin categoria").strip() or "Sin categoria",
        "discovery_rank": _coerce_int(item.get("discovery_rank")),
        "rating": _coerce_float(item.get("rating")),
        "review_count": _coerce_int(item.get("review_count")),
        "opportunity_score": _coerce_float(item.get("opportunity_score")),
        "has_website": _has_text(item.get("website")),
    }


def _public_insights(businesses: list[dict[str, Any]]) -> list[str]:
    total = len(businesses)
    if total == 0:
        return ["La muestra aun no tiene negocios suficientes para publicar conclusiones."]
    no_web = sum(1 for item in businesses if not _has_text(item.get("website")))
    high_rating_low_visibility = sum(
        1
        for item in businesses
        if (_coerce_float(item.get("rating")) or 0) >= 4.4 and (_coerce_int(item.get("discovery_rank")) or 9999) > 10
    )
    low_reviews = sum(1 for item in businesses if (_coerce_int(item.get("review_count")) or 0) < 50)
    not_enriched = sum(1 for item in businesses if not bool(item.get("listing_enriched")))
    return [
        f"{_percent_text(no_web, total)} de la muestra no tiene web visible en la ficha: oportunidad clara de conversion.",
        f"{_percent_text(high_rating_low_visibility, total)} combina buen rating con baja visibilidad aparente: hay demanda que no se esta capturando.",
        f"{_percent_text(low_reviews, total)} tiene menos de 50 resenas: la prueba social sigue siendo una palanca basica.",
        f"{_percent_text(not_enriched, total)} quedo con datos incompletos de listing: conviene revisar esos casos antes de sacar conclusiones duras.",
    ]


def _category_distribution(businesses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for item in businesses:
        category = str(item.get("category") or "Sin categoria").strip() or "Sin categoria"
        counter[category] += 1
    total = len(businesses)
    return [
        {"category": category, "count": count, "share": _share(count, total)}
        for category, count in counter.most_common(8)
    ]


def _category_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<p class='muted'>Sin categorias suficientes.</p>"
    rows = "".join(
        "<tr>"
        f"<td>{_e(str(item.get('category') or '-'))}</td>"
        f"<td>{int(item.get('count') or 0)}</td>"
        f"<td>{_percent_or_dash(item.get('share'))}</td>"
        "</tr>"
        for item in items
    )
    return f"<table><thead><tr><th>Categoria</th><th>Negocios</th><th>Peso</th></tr></thead><tbody>{rows}</tbody></table>"


def _examples_table(items: list[dict[str, Any]], *, title: str) -> str:
    if not items:
        body = "<p class='muted'>Sin ejemplos suficientes.</p>"
    else:
        rows = "".join(
            "<tr>"
            f"<td>{_e(str(item.get('label') or '-'))}</td>"
            f"<td>{_e(str(item.get('category') or '-'))}</td>"
            f"<td>{_e('#' + str(item.get('discovery_rank')) if item.get('discovery_rank') else '-')}</td>"
            f"<td>{_float_or_dash(item.get('rating'))}</td>"
            f"<td>{_e(str(item.get('review_count') or '-'))}</td>"
            f"<td>{_float_or_dash(item.get('opportunity_score'))}</td>"
            "</tr>"
            for item in items
        )
        body = (
            "<table><thead><tr><th>Ejemplo</th><th>Categoria</th><th>Orden</th><th>Rating</th>"
            f"<th>Resenas</th><th>Oportunidad</th></tr></thead><tbody>{rows}</tbody></table>"
        )
    return f"<article class='card'><h2>{_e(title)}</h2>{body}</article>"


def _data_notes(run: dict[str, Any], businesses: list[dict[str, Any]]) -> list[str]:
    notes = [
        "El estudio no expone emails, telefonos ni direcciones exactas en esta vista publica.",
        "El orden de aparicion depende de la busqueda, localizacion, momento y estado de Google Maps.",
    ]
    if str(run.get("status") or "") not in {"completed", "partial"}:
        notes.append("El benchmark no esta marcado como completado; los datos pueden ser provisionales.")
    if len(businesses) < int(run.get("limit") or len(businesses) or 0):
        notes.append("La muestra capturada es menor que el limite solicitado; conviene ampliarla antes de una publicacion grande.")
    return notes


def _build_geo_visibility_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    points = payload.get("points") if isinstance(payload.get("points"), list) else []
    sanitized_points: list[dict[str, Any]] = []
    for point in points:
        if not isinstance(point, dict):
            continue
        try:
            lat = float(point.get("lat"))
            lng = float(point.get("lng"))
            order = int(point.get("point_order") or 0)
        except (TypeError, ValueError):
            continue
        if order <= 0:
            continue
        top_results = point.get("top_results") if isinstance(point.get("top_results"), list) else []
        first_rank = None
        for item in top_results:
            if not isinstance(item, dict):
                continue
            try:
                maybe_rank = int(item.get("rank") or 0)
            except (TypeError, ValueError):
                continue
            if maybe_rank > 0:
                first_rank = maybe_rank
                break
        sanitized_points.append(
            {
                "point_order": order,
                "point_label": str(point.get("point_label") or f"Punto {order}").strip(),
                "lat": lat,
                "lng": lng,
                "rank": first_rank,
            }
        )

    if not sanitized_points:
        return None

    visibility_score = _coerce_float(summary.get("visibility_score"))
    share_top3 = _coerce_float(summary.get("share_top3"))
    share_top10 = _coerce_float(summary.get("share_top10"))
    share_not_found = _coerce_float(summary.get("share_not_found"))
    return {
        "provider_mode": str(summary.get("provider_mode") or "maps_live").strip(),
        "visibility_score": visibility_score,
        "share_top3": share_top3,
        "share_top10": share_top10,
        "share_not_found": share_not_found,
        "points": sanitized_points,
    }


def _geo_visibility_html(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    points = payload.get("points") if isinstance(payload.get("points"), list) else []
    if not points:
        return ""

    lats = [float(item["lat"]) for item in points]
    lngs = [float(item["lng"]) for item in points]
    min_lat = min(lats)
    max_lat = max(lats)
    min_lng = min(lngs)
    max_lng = max(lngs)
    dx = max(max_lng - min_lng, 0.0001)
    dy = max(max_lat - min_lat, 0.0001)
    width = 860
    height = 420
    pad = 28

    dots: list[str] = []
    for point in points:
        x = pad + ((float(point["lng"]) - min_lng) / dx) * (width - (pad * 2))
        y = height - pad - ((float(point["lat"]) - min_lat) / dy) * (height - (pad * 2))
        rank = _coerce_int(point.get("rank"))
        color = "#8b8178"
        if rank is not None and rank <= 3:
            color = "#2f9e44"
        elif rank is not None and rank <= 10:
            color = "#d9a322"
        elif rank is not None:
            color = "#c44c2f"
        point_label = str(point.get("point_label") or f"Punto {point.get('point_order')}")
        rank_label = f"#{rank}" if rank else "sin aparicion"
        title = f"{point_label} · {rank_label}"
        dots.append(
            "<g>"
            f"<circle cx='{x:.1f}' cy='{y:.1f}' r='8' fill='{color}' stroke='#1b130c' stroke-width='1.2' />"
            f"<text x='{x + 10:.1f}' y='{y + 4:.1f}' font-size='10' fill='#2b1b12'>{int(point.get('point_order') or 0)}</text>"
            f"<title>{_e(title)}</title>"
            "</g>"
        )

    visibility_score = _coerce_float(payload.get("visibility_score"))
    share_top3 = _coerce_float(payload.get("share_top3"))
    share_top10 = _coerce_float(payload.get("share_top10"))
    share_not_found = _coerce_float(payload.get("share_not_found"))
    provider_mode = str(payload.get("provider_mode") or "maps_live").strip()
    insights = _geo_visibility_insights(
        visibility_score=visibility_score,
        share_top3=share_top3,
        share_top10=share_top10,
        share_not_found=share_not_found,
    )

    return f"""
    <section class="card" style="margin-top:16px;">
      <h2>Visibilidad geográfica</h2>
      <p class="muted">Mapa de posiciones por punto de búsqueda (modo: {_e(provider_mode)}).</p>
      <div class="metrics" style="margin-top:10px;">
        {_metric("Visibility score", _float_or_dash(visibility_score), "0-100, ponderado por cobertura de posiciones.")}
        {_metric("Top 3", _percent_or_dash(share_top3), "Porcentaje de nodos donde aparece en top 3.")}
        {_metric("Top 10", _percent_or_dash(share_top10), "Porcentaje de nodos donde aparece en top 10.")}
        {_metric("Sin aparición", _percent_or_dash(share_not_found), "Nodos donde no aparece en resultados capturados.")}
      </div>
      <div style="margin-top:12px; border:1px solid var(--line); border-radius:16px; overflow:hidden; background:#f9f4e7;">
        <svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="Heatmap geográfico">
          <rect x="0" y="0" width="{width}" height="{height}" fill="#f9f4e7"></rect>
          {''.join(dots)}
        </svg>
      </div>
      <p class="muted" style="margin-top:8px;">Leyenda: verde top 1-3 · amarillo top 4-10 · rojo top &gt;10 · gris sin aparición.</p>
      <ul>{"".join(f"<li>{_e(item)}</li>" for item in insights)}</ul>
    </section>
    """


def _geo_visibility_insights(
    *,
    visibility_score: float | None,
    share_top3: float | None,
    share_top10: float | None,
    share_not_found: float | None,
) -> list[str]:
    notes: list[str] = []
    score = float(visibility_score or 0.0)
    if score >= 70:
        notes.append("La cobertura geográfica es alta: conviene defender posiciones con constancia de reseñas y ficha viva.")
    elif score >= 45:
        notes.append("La visibilidad es intermedia: hay zonas donde compites bien y otras donde la presencia cae.")
    else:
        notes.append("La visibilidad es baja: existe margen claro para trabajar proximidad, reseñas y señales locales.")

    if share_not_found is not None and share_not_found >= 0.35:
        notes.append("Más de un tercio de nodos no muestran presencia: prioriza acciones localizadas por barrios con peor cobertura.")
    if share_top10 is not None and share_top10 < 0.5:
        notes.append("Menos del 50% de nodos cae en top 10: revisa categoría principal, contenido de ficha y autoridad local.")
    if share_top3 is not None and share_top3 < 0.2:
        notes.append("La presencia en top 3 es limitada: conviene reforzar señales competitivas donde ya rozas primera página local.")
    if not notes:
        notes.append("Cobertura geográfica estable. Mantén ritmo de reseñas y contenido para proteger cuota local.")
    return notes


def _fallback_title(run: dict[str, Any]) -> str:
    query = str(run.get("query") or "benchmark local").strip()
    city = str(run.get("city") or "").strip()
    return f"Estudio local: {query}{' en ' + city if city else ''}"


def _metric(label: str, value: str, description: str) -> str:
    return f"<div class='metric'><span>{_e(label)}</span><strong>{_e(value)}</strong><span>{_e(description)}</span></div>"


def _percent_text(value: int, total: int) -> str:
    return _percent_or_dash(_share(value, total))


def _share(value: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round(float(value) / float(total), 4)


def _percent_or_dash(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "-"
    return f"{parsed * 100:.0f}%"


def _float_or_dash(value: Any) -> str:
    parsed = _coerce_float(value)
    if parsed is None:
        return "-"
    if abs(parsed - round(parsed)) < 0.001:
        return str(int(round(parsed)))
    return f"{parsed:.1f}"


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


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


def _e(value: Any) -> str:
    return escape(str(value), quote=True)
