from __future__ import annotations

import html
from typing import Any

from .final_report_intro_block import build_final_report_intro_block
from .final_report_render_context import build_final_report_render_context
from .final_report_stylesheet import build_final_report_stylesheet
from .font_embedding import load_embedded_font_css


def build_final_report_html(
    *,
    renderer: Any,
    report_payload: dict[str, Any],
    intro_context_text: str,
) -> str:
    business_name = str(report_payload.get("business_name", "") or "").strip() or "Business"
    generated_at = str(report_payload.get("generated_at", "") or "")
    generated_human = renderer._format_human_date(generated_at)

    render_context = build_final_report_render_context(
        renderer=renderer,
        report_payload=report_payload,
    )
    render_sections = render_context["render_sections"]
    ordered_keys = render_context["ordered_keys"]
    total_reviews = render_context["total_reviews"]
    fuentes_label = render_context["fuentes_label"]

    body_parts: list[str] = [
        build_final_report_intro_block(
            renderer=renderer,
            intro_context_text=intro_context_text,
            total_reviews=total_reviews,
            fuentes_label=fuentes_label,
            generated_human=generated_human,
        )
    ]

    for key in ordered_keys:
        payload = render_sections.get(key) if isinstance(render_sections, dict) else None
        rendered = renderer._render_section_by_key(section_key=str(key), section_payload=payload)
        if rendered.strip():
            body_parts.append(rendered)

    stylesheet = build_final_report_stylesheet(
        renderer=renderer,
        font_face_css=load_embedded_font_css(),
    )

    return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Reporte reputación - {html.escape(business_name)}</title>
    <style>
{stylesheet}
    </style>
  </head>
  <body>
    <main class="wrap">
      <header class="header">
        <h1>Reporte de reputación de {html.escape(business_name)}</h1>
        <div class="meta">Generado: {html.escape(generated_human)}</div>
      </header>
      {''.join(body_parts)}
      <div class="footer">Análisis elaborado por Repiq · {html.escape(generated_human)}</div>
    </main>
  </body>
</html>
"""
