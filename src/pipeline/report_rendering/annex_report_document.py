from __future__ import annotations

import html
from typing import Any

from .font_embedding import load_embedded_font_css



def build_annex_report_html(
    *,
    renderer: Any,
    report_payload: dict[str, Any],
    annexes_payload: dict[str, Any],
) -> str:
    business_name = str(report_payload.get("business_name", "") or "").strip() or "Business"
    generated_at = str(report_payload.get("generated_at", "") or "")

    full_data = annexes_payload.get("full_data")
    if not isinstance(full_data, dict):
        full_data = {}
    benchmarking = annexes_payload.get("benchmarking_full")
    if not isinstance(benchmarking, dict):
        benchmarking = {}
    voices = annexes_payload.get("voice_of_customer")
    if not isinstance(voices, dict):
        voices = {}

    body_parts: list[str] = []
    dataset_summary_html = renderer._render_dataset_summary_spanish(full_data.get("dataset_summary"))
    dimension_guide_html = renderer._render_dimension_guide(full_data.get("dataset_summary"))
    rows_table_html = renderer._render_review_rows_table(full_data.get("review_rows"))
    benchmark_html = renderer._render_payload(benchmarking) if not renderer._is_empty_payload(benchmarking) else ""
    voices_html = renderer._render_voice_quotes(voices)

    if dataset_summary_html:
        body_parts.extend(
            [
                "<section class='section'>",
                "<h2>Resumen del conjunto de datos</h2>",
                dataset_summary_html,
                "</section>",
            ]
        )
    if dimension_guide_html:
        body_parts.extend(
            [
                "<section class='section'>",
                "<h2>Guía para interpretar las métricas</h2>",
                "<p>Estas métricas ayudan a leer mejor el estado del negocio. No son solo números: indican riesgos y oportunidades reales.</p>",
                dimension_guide_html,
                "</section>",
            ]
        )
    if rows_table_html:
        body_parts.extend(
            [
                "<section class='section'>",
                "<h2>Detalle de reseñas (muestra tabular)</h2>",
                rows_table_html,
                "</section>",
            ]
        )
    if benchmark_html:
        body_parts.extend(
            [
                "<section class='section'>",
                "<h2>Comparativa con competidores</h2>",
                benchmark_html,
                "</section>",
            ]
        )
    if voices_html:
        body_parts.extend(
            [
                "<section class='section'>",
                "<h2>Voz literal del cliente (anonimizada)</h2>",
                voices_html,
                "</section>",
            ]
        )

    font_face_css = load_embedded_font_css()

    return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Anexos del reporte - {html.escape(business_name)}</title>
    <style>
      {font_face_css}
      :root {{
        --bg: #F4F2EC;
        --text: #161616;
        --muted: #64748B;
        --line: rgba(0, 0, 0, 0.08);
        --panel: #FFFFFF;
        --accent: {renderer._PALETTE[2]};
        --font-display: "Syne", sans-serif;
        --font-body: "Plus Jakarta Sans", sans-serif;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font-family: var(--font-body);
        line-height: 1.4;
      }}
      .wrap {{ max-width: 1120px; margin: 0 auto; padding: 18px; }}
      .header {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 10px;
        background: var(--panel);
      }}
      .section {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 12px;
        margin-bottom: 10px;
        background: var(--panel);
      }}
      h1 {{ margin: 0; font-size: 22px; font-family: var(--font-display); font-weight: 700; }}
      h2 {{ margin: 0 0 8px 0; font-size: 16px; font-family: var(--font-display); font-weight: 700; }}
      table {{
        width: 100%;
        border-collapse: collapse;
      }}
      th, td {{
        border: 1px solid var(--line);
        padding: 6px 7px;
        font-size: 11px;
        vertical-align: top;
      }}
      th {{ background: var(--accent); text-align: left; }}
      .muted {{ color: var(--muted); font-size: 12px; }}
      ul {{ margin: 6px 0 6px 16px; }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <header class="header">
        <h1>Anexos completos de {html.escape(business_name)}</h1>
        <div class="muted">Generado: {html.escape(generated_at)}</div>
      </header>
      {''.join(body_parts)}
    </main>
  </body>
</html>
"""
