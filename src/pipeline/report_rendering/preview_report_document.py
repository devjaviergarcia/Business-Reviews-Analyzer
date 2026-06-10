from __future__ import annotations

import html
from typing import Any

from .font_embedding import load_embedded_font_css



def build_preview_report_html(*, renderer: Any, preview_payload: dict[str, Any]) -> str:
    business_name = str(preview_payload.get("business_name", "") or "").strip() or "Negocio"
    generated_at = str(preview_payload.get("generated_at", "") or "")
    sections = preview_payload.get("sections")
    if not isinstance(sections, dict):
        sections = {}

    resumen = sections.get("1_resumen_ejecutivo_preview")
    if not isinstance(resumen, dict):
        resumen = {}
    tipos = sections.get("2_tipos_cliente_y_comentarios_relevantes")
    if not isinstance(tipos, dict):
        tipos = {}
    cta = sections.get("3_llamada_a_la_accion")
    if not isinstance(cta, dict):
        cta = {}

    types_payload = tipos.get("tipos_cliente")
    if not isinstance(types_payload, list):
        types_payload = []

    type_cards: list[str] = []
    for item in types_payload[:3]:
        if not isinstance(item, dict):
            continue
        comment = item.get("comentario_representativo")
        if not isinstance(comment, dict):
            comment = {}
        type_cards.append(
            "<article class='type-card'>"
            f"<h3>{html.escape(str(item.get('label', '') or 'Tipo de cliente'))}</h3>"
            f"<p><strong>Estado:</strong> {html.escape(str(item.get('estado_emocional', '') or ''))}</p>"
            f"<p><strong>Intención:</strong> {html.escape(str(item.get('intencion_detectada', '') or ''))}</p>"
            f"<p><strong>Expectativas:</strong> {html.escape(str(item.get('expectativas', '') or ''))}</p>"
            "<div class='quote'>"
            f"<div class='quote-meta'>{html.escape(str(comment.get('author_name', '') or 'Cliente'))} · "
            f"Rating {html.escape(str(comment.get('rating', '') or '-'))} · "
            f"{html.escape(str(comment.get('source', '') or 'unknown'))}</div>"
            f"<div class='quote-text'>“{html.escape(str(comment.get('quote', '') or 'Sin comentario representativo.'))}”</div>"
            f"<div class='quote-why'>{html.escape(str(comment.get('relevance_reason', '') or ''))}</div>"
            "</div>"
            "</article>"
        )

    font_face_css = load_embedded_font_css()

    return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Preview de reputación - {html.escape(business_name)}</title>
    <style>
      {font_face_css}
      :root {{
        --bg: #F4F2EC;
        --panel: #FFFFFF;
        --line: rgba(0, 0, 0, 0.08);
        --text: #161616;
        --muted: #64748B;
        --a1: {renderer._PALETTE[0]};
        --a2: {renderer._PALETTE[1]};
        --a3: {renderer._PALETTE[2]};
        --a4: {renderer._PALETTE[3]};
        --font-display: "Syne", sans-serif;
        --font-body: "Plus Jakarta Sans", sans-serif;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font-family: var(--font-body);
      }}
      .wrap {{ max-width: 940px; margin: 0 auto; padding: 20px; }}
      .header, .section {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px 16px;
        margin-bottom: 12px;
      }}
      .header h1 {{ margin: 0 0 4px 0; font-size: 24px; font-family: var(--font-display); font-weight: 700; }}
      h2, h3 {{ font-family: var(--font-display); font-weight: 700; }}
      .meta {{ color: var(--muted); font-size: 12px; }}
      h2 {{ margin: 0 0 10px 0; font-size: 18px; }}
      h3 {{ margin: 0 0 6px 0; font-size: 14px; }}
      p {{ margin: 6px 0; }}
      .score-pill {{
        display: inline-block;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: var(--a1);
        padding: 4px 10px;
        font-size: 12px;
        margin-right: 6px;
      }}
      .type-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 8px;
      }}
      .type-card {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
        background: var(--panel);
      }}
      .quote {{
        margin-top: 8px;
        border-left: 3px solid var(--a2);
        padding-left: 8px;
      }}
      .quote-meta {{ color: var(--muted); font-size: 11px; margin-bottom: 4px; }}
      .quote-text {{ font-size: 12px; }}
      .quote-why {{ color: var(--muted); font-size: 11px; margin-top: 4px; }}
      .cta {{
        background: rgba(212, 149, 10, 0.12);
        border-radius: 12px;
        padding: 12px;
        border: 1px solid var(--line);
      }}
      .cta strong {{ display: block; margin-bottom: 6px; }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <header class="header">
        <h1>Avance de reputación - {html.escape(business_name)}</h1>
        <div class="meta">Generado: {html.escape(generated_at)}</div>
      </header>
      <section class="section">
        <h2>Resumen ejecutivo (avance)</h2>
        <p>
          <span class="score-pill">{html.escape(str(resumen.get('score', '') or 'Puntuación no disponible'))}</span>
          <span class="score-pill">{html.escape(str(resumen.get('nivel_reputacion', '') or 'Nivel no disponible'))}</span>
        </p>
        <p>{html.escape(str(resumen.get('texto', '') or 'Sin resumen disponible.'))}</p>
      </section>
      <section class="section">
        <h2>3 tipos de cliente y evidencia</h2>
        <div class="type-grid">
          {''.join(type_cards) if type_cards else '<p>No hay segmentos disponibles todavía.</p>'}
        </div>
      </section>
      <section class="section">
        <div class="cta">
          <strong>¿Quieres el análisis completo y plan de acción detallado?</strong>
          <p>{html.escape(str(cta.get('texto', '') or 'Rellena el formulario para recibir el informe completo.'))}</p>
          <p>{html.escape(str(cta.get('accion_recomendada', '') or 'Completa el formulario para continuar.'))}</p>
        </div>
      </section>
    </main>
  </body>
</html>
"""
