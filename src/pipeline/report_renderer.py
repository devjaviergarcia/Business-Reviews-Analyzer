from __future__ import annotations

import asyncio
import csv
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class StructuredReportRenderer:
    """Render structured report data to JSON/HTML/PDF artifacts."""

    _PALETTE = (
        "#0A7567",
        "#12B08A",
        "#D4F0E8",
        "#D4950A",
        "#C23B18",
        "#64748B",
    )

    def __init__(self, *, artifacts_root: str | Path = "artifacts/reports") -> None:
        self.artifacts_root = Path(artifacts_root)

    async def render(
        self,
        *,
        report_payload: dict[str, Any],
        intro_context_text: str,
        business_id: str,
        analysis_id: str,
        output_format: str = "pdf",
    ) -> dict[str, Any]:
        normalized_format = str(output_format or "pdf").strip().lower() or "pdf"
        business_name = str(report_payload.get("business_name", "") or "").strip() or "negocio"
        slug_business_name = self._safe_name_slug(business_name)
        slug_business_id = self._safe_identifier_slug(str(business_id))
        slug_analysis = self._safe_identifier_slug(str(analysis_id))

        business_dir = self.artifacts_root / f"{slug_business_name}__{slug_business_id}" / f"analisis_{slug_analysis}"
        reports_dir = business_dir / "reportes"
        annexes_dir = business_dir / "anexos"
        reports_dir.mkdir(parents=True, exist_ok=True)
        annexes_dir.mkdir(parents=True, exist_ok=True)

        final_report_stem = f"reporte_final_{slug_business_name}_{slug_analysis}"
        annex_stem = f"anexo_completo_{slug_business_name}_{slug_analysis}"
        annex_data_stem = f"anexo_datos_{slug_business_name}_{slug_analysis}"

        json_path = reports_dir / f"{final_report_stem}.json"
        html_path = reports_dir / f"{final_report_stem}.html"
        pdf_path = reports_dir / f"{final_report_stem}.pdf"
        annex_csv_path = annexes_dir / f"{annex_data_stem}.csv"
        annex_html_path = annexes_dir / f"{annex_stem}.html"
        annex_pdf_path = annexes_dir / f"{annex_stem}.pdf"

        json_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2, default=self._json_default),
            encoding="utf-8",
        )

        html_content = self._build_html(report_payload=report_payload, intro_context_text=intro_context_text)
        html_path.write_text(html_content, encoding="utf-8")

        annexes_payload = report_payload.get("annexes")
        if not isinstance(annexes_payload, dict):
            annexes_payload = {}
        self._write_annex_csv(annexes_payload=annexes_payload, csv_path=annex_csv_path)
        annex_html = self._build_annex_html(report_payload=report_payload, annexes_payload=annexes_payload)
        annex_html_path.write_text(annex_html, encoding="utf-8")

        pdf_generated = False
        pdf_error = None
        annex_pdf_generated = False
        annex_pdf_error = None
        if normalized_format == "pdf":
            try:
                await self._render_pdf_from_html(html_content=html_content, pdf_path=pdf_path)
                pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                pdf_error = str(exc)
            try:
                await self._render_pdf_from_html(html_content=annex_html, pdf_path=annex_pdf_path)
                annex_pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                annex_pdf_error = str(exc)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "output_format": normalized_format,
            "display_name": f"Reporte final - {business_name}",
            "json": {
                "path": str(json_path.resolve()),
                "filename": json_path.name,
                "exists": json_path.exists(),
            },
            "html": {
                "path": str(html_path.resolve()),
                "filename": html_path.name,
                "exists": html_path.exists(),
            },
            "pdf": {
                "path": str(pdf_path.resolve()),
                "filename": pdf_path.name,
                "exists": pdf_path.exists() if normalized_format == "pdf" else False,
                "generated": pdf_generated,
                "error": pdf_error,
            },
            "annex": {
                "csv": {
                    "path": str(annex_csv_path.resolve()),
                    "filename": annex_csv_path.name,
                    "exists": annex_csv_path.exists(),
                },
                "html": {
                    "path": str(annex_html_path.resolve()),
                    "filename": annex_html_path.name,
                    "exists": annex_html_path.exists(),
                },
                "pdf": {
                    "path": str(annex_pdf_path.resolve()),
                    "filename": annex_pdf_path.name,
                    "exists": annex_pdf_path.exists() if normalized_format == "pdf" else False,
                    "generated": annex_pdf_generated,
                    "error": annex_pdf_error,
                },
            },
        }

    async def render_preview(
        self,
        *,
        preview_payload: dict[str, Any],
        business_id: str,
        analysis_id: str,
        output_format: str = "pdf",
    ) -> dict[str, Any]:
        normalized_format = str(output_format or "pdf").strip().lower() or "pdf"
        business_name = str(preview_payload.get("business_name", "") or "").strip() or "negocio"
        slug_business_name = self._safe_name_slug(business_name)
        slug_business_id = self._safe_identifier_slug(str(business_id))
        slug_analysis = self._safe_identifier_slug(str(analysis_id))

        business_dir = self.artifacts_root / f"{slug_business_name}__{slug_business_id}" / f"analisis_{slug_analysis}"
        reports_dir = business_dir / "reportes"
        reports_dir.mkdir(parents=True, exist_ok=True)

        welcome_report_stem = f"reporte_bienvenida_{slug_business_name}_{slug_analysis}"
        json_path = reports_dir / f"{welcome_report_stem}.json"
        html_path = reports_dir / f"{welcome_report_stem}.html"
        pdf_path = reports_dir / f"{welcome_report_stem}.pdf"

        json_path.write_text(
            json.dumps(preview_payload, ensure_ascii=False, indent=2, default=self._json_default),
            encoding="utf-8",
        )
        html_content = self._build_preview_html(preview_payload=preview_payload)
        html_path.write_text(html_content, encoding="utf-8")

        pdf_generated = False
        pdf_error = None
        if normalized_format == "pdf":
            try:
                await self._render_pdf_from_html(html_content=html_content, pdf_path=pdf_path)
                pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                pdf_error = str(exc)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "output_format": normalized_format,
            "display_name": f"Reporte de bienvenida - {business_name}",
            "json": {
                "path": str(json_path.resolve()),
                "filename": json_path.name,
                "exists": json_path.exists(),
            },
            "html": {
                "path": str(html_path.resolve()),
                "filename": html_path.name,
                "exists": html_path.exists(),
            },
            "pdf": {
                "path": str(pdf_path.resolve()),
                "filename": pdf_path.name,
                "exists": pdf_path.exists() if normalized_format == "pdf" else False,
                "generated": pdf_generated,
                "error": pdf_error,
            },
        }

    async def _render_pdf_from_html(self, *, html_content: str, pdf_path: Path) -> None:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.set_content(html_content, wait_until="networkidle")
                await page.emulate_media(media="screen")
                await page.pdf(
                    path=str(pdf_path),
                    format="A4",
                    print_background=True,
                    margin={
                        "top": "12mm",
                        "bottom": "12mm",
                        "left": "10mm",
                        "right": "10mm",
                    },
                )
            finally:
                await browser.close()

    def _build_html(self, *, report_payload: dict[str, Any], intro_context_text: str) -> str:
        business_name = str(report_payload.get("business_name", "") or "").strip() or "Business"
        generated_at = str(report_payload.get("generated_at", "") or "")
        sections = report_payload.get("sections")
        if not isinstance(sections, dict):
            sections = {}

        section_order = report_payload.get("section_order")
        ordered_keys = section_order if isinstance(section_order, list) else list(sections.keys())

        anexo_resumen = sections.get("5_anexos_resumen") if isinstance(sections.get("5_anexos_resumen"), dict) else {}
        resumen_dataset = (
            anexo_resumen.get("resumen_dataset")
            if isinstance(anexo_resumen.get("resumen_dataset"), dict)
            else {}
        )
        total_reviews = self._safe_int(resumen_dataset.get("total_reviews"))
        fuentes = resumen_dataset.get("by_source") if isinstance(resumen_dataset.get("by_source"), dict) else {}
        fuentes_label = ", ".join(
            f"{self._source_name_spanish(str(source))} ({self._safe_int(count)})"
            for source, count in list(fuentes.items())[:4]
            if str(source).strip()
        )

        body_parts: list[str] = []
        generated_human = self._format_human_date(generated_at)
        intro_text = self._clean_narrative_text(str(intro_context_text or "").strip())
        body_parts.append("<section class='intro context-banner'>")
        body_parts.append("<div class='context-row'>")
        body_parts.append(
            f"<span class='context-item'>{self._icon_slot('reviews')}<strong>{total_reviews}</strong> opiniones analizadas</span>"
        )
        if fuentes_label:
            body_parts.append(
                f"<span class='context-item'>{self._icon_slot('sources')}Fuentes: <strong>{html.escape(fuentes_label)}</strong></span>"
            )
        body_parts.append(
            f"<span class='context-item'>{self._icon_slot('updated')}Actualizado: <strong>{html.escape(generated_human)}</strong></span>"
        )
        body_parts.append("</div>")
        if intro_text:
            body_parts.append(f"<p class='muted'>{html.escape(intro_text)}</p>")
        body_parts.append("</section>")

        for key in ordered_keys:
            payload = sections.get(key) if isinstance(sections, dict) else None
            rendered = self._render_section_by_key(section_key=str(key), section_payload=payload)
            if rendered.strip():
                body_parts.append(rendered)

        return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Reporte reputación - {html.escape(business_name)}</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
      :root {{
        --bg: #F4F2EC;
        --text: #161616;
        --muted: #64748B;
        --panel: #FFFFFF;
        --line: rgba(0, 0, 0, 0.08);
        --accent-1: {self._PALETTE[0]};
        --accent-2: {self._PALETTE[1]};
        --accent-3: {self._PALETTE[2]};
        --accent-4: {self._PALETTE[3]};
        --accent-5: {self._PALETTE[4]};
        --accent-6: {self._PALETTE[5]};
        --good: #12B08A;
        --warn: #D4950A;
        --bad: #C23B18;
        --font-display: "Syne", sans-serif;
        --font-body: "Plus Jakarta Sans", sans-serif;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        color: var(--text);
        background: var(--bg);
        font-family: var(--font-body);
        line-height: 1.45;
      }}
      .wrap {{
        max-width: 1040px;
        margin: 0 auto;
        padding: 24px;
      }}
      .header {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 20px;
        margin-bottom: 14px;
      }}
      .header h1 {{
        margin: 0 0 6px 0;
        font-size: 26px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      .meta {{
        color: var(--muted);
        font-size: 13px;
      }}
      .intro, .section {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 14px 16px;
        margin-bottom: 12px;
      }}
      .context-banner {{
        border-left: 3px solid var(--accent-1);
        border-top-left-radius: 0;
        border-bottom-left-radius: 0;
      }}
      .context-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 18px;
        align-items: center;
        margin-bottom: 4px;
      }}
      .context-item {{
        font-size: 13px;
        color: var(--muted);
      }}
      .icon-slot {{
        display: inline-flex;
        width: 14px;
        height: 14px;
        border-radius: 4px;
        border: 1px solid rgba(10, 117, 103, 0.35);
        background: rgba(212, 240, 232, 0.45);
        margin-right: 6px;
        vertical-align: -2px;
      }}
      .context-item strong {{
        color: var(--text);
      }}
      .section--diagnostico {{ border-left: 3px solid var(--accent-1); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--puntuacion {{ border-left: 3px solid var(--accent-2); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--cliente {{ border-left: 3px solid var(--accent-3); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--accion {{ border-left: 3px solid var(--accent-5); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--anexo {{ border-left: 3px solid var(--accent-6); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      h2 {{
        margin: 0 0 10px 0;
        font-size: 18px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      h3 {{
        margin: 10px 0 6px 0;
        font-size: 15px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      p {{
        margin: 6px 0;
      }}
      ul {{
        margin: 6px 0 6px 18px;
        padding: 0;
      }}
      li {{
        margin: 3px 0;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 8px;
      }}
      th, td {{
        border: 1px solid var(--line);
        padding: 7px 8px;
        font-size: 12px;
        vertical-align: top;
      }}
      th {{
        background: var(--accent-3);
        text-align: left;
      }}
      .muted {{ color: var(--muted); }}
      .pill-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .pill {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 8px 10px;
        font-size: 12px;
      }}
      .score-hero {{
        display: grid;
        grid-template-columns: 240px 1fr;
        gap: 12px;
      }}
      .score-card {{
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px;
        background: var(--panel);
        border-left: 3px solid var(--accent-2);
        border-top-left-radius: 0;
        border-bottom-left-radius: 0;
      }}
      .score-value {{
        font-size: 48px;
        line-height: 1;
        font-weight: 800;
      }}
      .score-label {{
        margin-top: 6px;
        font-size: 13px;
        font-weight: 600;
      }}
      .score-bar-wrap {{
        margin-top: 12px;
      }}
      .score-bar-track {{
        position: relative;
        height: 8px;
        border-radius: 4px;
        border: 1px solid var(--line);
      }}
      .score-bar-zones {{
        display: flex;
        height: 100%;
        border-radius: 4px;
        overflow: hidden;
      }}
      .zone {{
        flex: 1;
      }}
      .zone-red {{ background: var(--bad); flex: 0.55; }}
      .zone-orange {{ background: var(--warn); flex: 0.15; }}
      .zone-yellow {{ background: var(--accent-3); flex: 0.15; }}
      .zone-green {{ background: var(--good); flex: 0.15; }}
      .score-bar-marker {{
        position: absolute;
        top: -4px;
        width: 16px;
        height: 16px;
        border-radius: 50%;
        border: 2px solid #fff;
        transform: translateX(-50%);
      }}
      .score-bar-labels {{
        display: flex;
        justify-content: space-between;
        font-size: 10px;
        color: var(--muted);
        margin-top: 4px;
      }}
      .cluster-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .cluster-card {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
        background: var(--panel);
      }}
      .timeline {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .timeline-col {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
      }}
      .timeline-col h4 {{
        margin: 0 0 6px 0;
        font-size: 13px;
      }}
      .timeline-col:nth-child(1) .action-card {{ border-left: 3px solid var(--warn); }}
      .timeline-col:nth-child(2) .action-card {{ border-left: 3px solid var(--accent-2); }}
      .timeline-col:nth-child(3) .action-card {{ border-left: 3px solid var(--accent-6); }}
      .timeline-col:nth-child(1) h4 {{ color: var(--warn); }}
      .timeline-col:nth-child(2) h4 {{ color: var(--accent-2); }}
      .timeline-col:nth-child(3) h4 {{ color: var(--accent-6); }}
      .scatter {{
        margin-top: 10px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--panel);
        padding: 6px;
      }}
      .scatter-note {{
        font-size: 11px;
        color: var(--muted);
        margin-top: 4px;
      }}
      .bar-chart-wrap {{
        margin-top: 8px;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 8px;
        background: var(--panel);
      }}
      .action-list {{
        list-style: none;
        margin: 0;
        padding: 0;
        display: grid;
        gap: 8px;
      }}
      .action-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .action-card .title {{
        font-weight: 600;
        margin-bottom: 4px;
      }}
      .action-card-header {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 8px;
        margin-bottom: 4px;
      }}
      .tipo-badge {{
        display: inline-block;
        font-size: 10px;
        font-weight: 600;
        border-radius: 999px;
        border: 1px solid;
        padding: 2px 8px;
        white-space: nowrap;
        flex-shrink: 0;
      }}
      .urgent-block {{
        background: rgba(194, 59, 24, 0.08);
        border: 1px solid rgba(194, 59, 24, 0.26);
        border-left: 4px solid var(--warn);
        border-radius: 0 12px 12px 0;
        padding: 12px 14px;
        margin: 12px 0;
      }}
      .urgent-title {{
        color: var(--bad);
        font-size: 14px;
        margin: 0 0 8px 0;
      }}
      .fw-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        margin-top: 10px;
      }}
      .fw-col-title {{
        font-size: 14px;
        font-weight: 700;
        margin: 0 0 10px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid currentColor;
      }}
      .fw-col-strong {{ color: var(--good); }}
      .fw-col-weak {{ color: var(--warn); }}
      .fw-card {{
        display: flex;
        gap: 10px;
        border-radius: 10px;
        padding: 10px 12px;
        margin-bottom: 8px;
      }}
      .fw-strong {{
        background: rgba(18, 176, 138, 0.10);
        border: 1px solid rgba(18, 176, 138, 0.28);
      }}
      .fw-weak {{
        background: rgba(212, 149, 10, 0.12);
        border: 1px solid rgba(212, 149, 10, 0.28);
      }}
      .fw-icon {{
        display: inline-flex;
        width: 16px;
        height: 16px;
        flex-shrink: 0;
        margin-top: 2px;
      }}
      .fw-strong .fw-icon {{ color: var(--good); }}
      .fw-weak .fw-icon {{ color: var(--warn); }}
      .fw-title {{
        font-weight: 600;
        font-size: 13px;
        margin-bottom: 3px;
      }}
      .fw-desc {{
        font-size: 12px;
        color: var(--muted);
        margin-bottom: 4px;
      }}
      .fw-action {{
        font-size: 12px;
      }}
      .fw-tipo-badge {{
        display: inline-block;
        font-size: 11px;
        background: rgba(212, 149, 10, 0.14);
        color: #8a6209;
        border: 1px solid rgba(212, 149, 10, 0.30);
        border-radius: 999px;
        padding: 2px 8px;
      }}
      .annex-details {{
        cursor: pointer;
      }}
      .annex-summary {{
        font-weight: 600;
        font-size: 14px;
        color: var(--muted);
        list-style: none;
      }}
      .annex-summary::-webkit-details-marker {{ display: none; }}
      .annex-hint {{
        font-weight: 400;
        font-size: 12px;
      }}
      .annex-body {{
        border-top: 1px solid var(--line);
        padding-top: 12px;
        margin-top: 10px;
      }}
      .meta-line {{
        color: var(--muted);
        font-size: 12px;
      }}
      .voice-list {{
        list-style: none;
        margin: 0;
        padding: 0;
        display: grid;
        gap: 8px;
      }}
      .voice-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .voice-meta {{
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 4px;
      }}
      .metric-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .metric-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .metric-title {{
        font-weight: 600;
        margin-bottom: 4px;
      }}
      .metric-value {{
        font-size: 20px;
        font-weight: 700;
        line-height: 1.1;
      }}
      .metric-explain {{
        color: var(--muted);
        font-size: 12px;
      }}
      .badge {{
        display: inline-block;
        border-radius: 999px;
        font-size: 11px;
        padding: 2px 8px;
        border: 1px solid transparent;
      }}
      .badge.good {{ background: #e7fbef; color: var(--good); border-color: #c5f1d7; }}
      .badge.warn {{ background: rgba(212, 149, 10, 0.12); color: #8a6209; border-color: rgba(212, 149, 10, 0.30); }}
      .badge.bad {{ background: #ffe8e8; color: #ab2329; border-color: #ffc9cb; }}
      .footer {{
        color: var(--muted);
        text-align: center;
        margin-top: 16px;
        font-size: 12px;
      }}
      @media (max-width: 820px) {{
        .score-hero {{ grid-template-columns: 1fr; }}
        .timeline {{ grid-template-columns: 1fr; }}
        .fw-grid {{ grid-template-columns: 1fr; }}
      }}
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

    def _build_preview_html(self, *, preview_payload: dict[str, Any]) -> str:
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

        return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Preview de reputación - {html.escape(business_name)}</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
      :root {{
        --bg: #F4F2EC;
        --panel: #FFFFFF;
        --line: rgba(0, 0, 0, 0.08);
        --text: #161616;
        --muted: #64748B;
        --a1: {self._PALETTE[0]};
        --a2: {self._PALETTE[1]};
        --a3: {self._PALETTE[2]};
        --a4: {self._PALETTE[3]};
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

    def _build_annex_html(self, *, report_payload: dict[str, Any], annexes_payload: dict[str, Any]) -> str:
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
        dataset_summary_html = self._render_dataset_summary_spanish(full_data.get("dataset_summary"))
        dimension_guide_html = self._render_dimension_guide(full_data.get("dataset_summary"))
        rows_table_html = self._render_review_rows_table(full_data.get("review_rows"))
        benchmark_html = self._render_payload(benchmarking) if not self._is_empty_payload(benchmarking) else ""
        voices_html = self._render_voice_quotes(voices)

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

        return f"""<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Anexos del reporte - {html.escape(business_name)}</title>
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');
      :root {{
        --bg: #F4F2EC;
        --text: #161616;
        --muted: #64748B;
        --line: rgba(0, 0, 0, 0.08);
        --panel: #FFFFFF;
        --accent: {self._PALETTE[2]};
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

    def _write_annex_csv(self, *, annexes_payload: dict[str, Any], csv_path: Path) -> None:
        full_data = annexes_payload.get("full_data")
        if not isinstance(full_data, dict):
            full_data = {}
        review_rows = full_data.get("review_rows")
        if not isinstance(review_rows, list):
            review_rows = []

        fieldnames = [
            "review_index",
            "customer_key",
            "cluster_id",
            "cluster_label",
            "source",
            "author_name",
            "rating",
            "sentiment",
            "expectation_gap",
            "satisfaction",
            "tranquility_aggressiveness",
            "improvement_intent",
            "dominant_problem",
            "has_owner_reply",
            "owner_reply_excerpt",
            "review_excerpt",
        ]

        with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in review_rows:
                if not isinstance(row, dict):
                    continue
                safe_row = {key: row.get(key) for key in fieldnames}
                writer.writerow(safe_row)

    def _render_section_by_key(self, *, section_key: str, section_payload: Any) -> str:
        title = self._humanize_section_key(section_key)
        section_class_map = {
            "1_resumen_ejecutivo": "section section--diagnostico",
            "2_score_reputacion": "section section--puntuacion",
            "3_quien_es_tu_cliente_y_que_le_preocupa": "section section--cliente",
            "4_plan_de_accion": "section section--accion",
            "5_anexos_resumen": "section section--anexo",
        }
        section_class = section_class_map.get(section_key, "section")
        if not isinstance(section_payload, dict):
            content = self._render_payload(section_payload)
            if not content.strip():
                return ""
            return f"<section class='{section_class}'><h2>{html.escape(title)}</h2>{content}</section>"

        if section_key == "1_resumen_ejecutivo":
            content = self._render_section_resumen(section_payload)
        elif section_key == "2_score_reputacion":
            content = self._render_section_score(section_payload)
        elif section_key == "3_quien_es_tu_cliente_y_que_le_preocupa":
            content = self._render_section_clientes(section_payload)
        elif section_key == "4_plan_de_accion":
            content = self._render_section_plan(section_payload)
        elif section_key == "5_anexos_resumen":
            content = self._render_section_anexos(section_payload)
        else:
            content = self._render_payload(section_payload)

        if not content.strip():
            return ""
        return f"<section class='{section_class}'><h2>{html.escape(title)}</h2>{content}</section>"

    def _render_section_resumen(self, payload: dict[str, Any]) -> str:
        diagnostico = self._clean_narrative_text(str(payload.get("diagnostico", "") or "").strip())
        estado = payload.get("estado_actual") if isinstance(payload.get("estado_actual"), dict) else {}
        aciertos = payload.get("aciertos_notorios") if isinstance(payload.get("aciertos_notorios"), list) else []
        aciertos_estructurados = (
            payload.get("aciertos_estructurados")
            if isinstance(payload.get("aciertos_estructurados"), list)
            else []
        )
        score = self._safe_float(estado.get("score_reputacion"))
        score_badge = self._score_badge(score)
        pills = [
            f"<div class='pill'><strong>Puntuación:</strong> {round(score, 1)}/100</div>",
            f"<div class='pill'><strong>Nivel:</strong> {html.escape(str(estado.get('nivel_reputacion', '') or ''))}</div>",
            f"<div class='pill'><strong>Tipos de cliente detectados:</strong> {self._safe_int(estado.get('cluster_count'))}</div>",
            f"<div class='pill'><strong>Problemas principales:</strong> {len(estado.get('problemas_principales') or []) if isinstance(estado.get('problemas_principales'), list) else 0}</div>",
        ]
        parts = [
            f"<p>{html.escape(diagnostico)}</p>" if diagnostico else "",
            f"<p>{score_badge}</p>",
            f"<div class='pill-grid'>{''.join(pills)}</div>",
        ]
        if aciertos_estructurados:
            cards: list[str] = []
            for item in aciertos_estructurados[:3]:
                if not isinstance(item, dict):
                    continue
                concepto = str(item.get("concepto", "") or "").strip()
                cita = str(item.get("cita", "") or "").strip()
                if not concepto:
                    continue
                cards.append(
                    "<article class='fw-card fw-strong'>"
                    f"<div class='fw-icon'>{self._icon_slot('strength')}</div>"
                    "<div>"
                    + f"<div class='fw-title'>{html.escape(concepto)}</div>"
                    + (f"<div class='fw-desc'>“{html.escape(cita)}”</div>" if cita else "")
                    + "</div>"
                    + "</article>"
                )
            if cards:
                parts.extend(
                    [
                        "<h3>Aciertos que más valoran tus clientes satisfechos</h3>",
                        f"<div class='cluster-grid'>{''.join(cards)}</div>",
                    ]
                )
        else:
            aciertos_items = [str(item or "").strip() for item in aciertos[:3] if str(item or "").strip()]
            if aciertos_items:
                aciertos_html = "<ul>" + "".join(f"<li>{html.escape(item)}</li>" for item in aciertos_items) + "</ul>"
                parts.extend(["<h3>Aciertos que más valoran tus clientes satisfechos</h3>", aciertos_html])
        return "".join(parts)

    def _render_section_score(self, payload: dict[str, Any]) -> str:
        display = str(payload.get("score_display", "") or "").strip() or "0/100"
        score_value = self._safe_float(payload.get("score_value"))
        label = str(payload.get("nivel_reputacion", "") or "").strip()
        explicacion = self._clean_narrative_text(str(payload.get("explicacion", "") or "").strip())
        componentes = payload.get("componentes_numericos")
        evolucion = payload.get("evolucion")
        components_html = self._render_score_components(componentes)
        evolucion_html = self._render_payload(evolucion) if not self._is_empty_payload(evolucion) else ""
        marker_color = "#C23B18"
        if score_value >= 85.0:
            marker_color = "#12B08A"
        elif score_value >= 70.0:
            marker_color = "#0A7567"
        elif score_value >= 55.0:
            marker_color = "#D4950A"
        marker_pos = max(0.0, min(100.0, score_value))
        score_scale_html = (
            "<div class='score-bar-wrap'>"
            "<div class='score-bar-track'>"
            "<div class='score-bar-zones'>"
            "<div class='zone zone-red'></div>"
            "<div class='zone zone-orange'></div>"
            "<div class='zone zone-yellow'></div>"
            "<div class='zone zone-green'></div>"
            "</div>"
            f"<div class='score-bar-marker' style='left:{marker_pos:.1f}%;background:{marker_color}'></div>"
            "</div>"
            "<div class='score-bar-labels'><span>0</span><span>55</span><span>70</span><span>85</span><span>100</span></div>"
            "</div>"
        )
        return (
            "<div class='score-hero'>"
            "<div class='score-card'>"
            f"<div class='score-value'>{html.escape(display)}</div>"
            f"<div class='score-label'>{html.escape(label)}</div>"
            f"{score_scale_html}"
            "</div>"
            "<div>"
            f"<p>{html.escape(explicacion)}</p>"
            f"{components_html}"
            "</div>"
            "</div>"
            + ("<h3>Evolución y tendencia</h3>" + evolucion_html if evolucion_html else "")
        )

    def _render_section_clientes(self, payload: dict[str, Any]) -> str:
        lectura = self._clean_narrative_text(str(payload.get("lectura_ejecutiva", "") or "").strip())
        clientes = payload.get("tipologias_cliente_top3")
        if not isinstance(clientes, list):
            clientes = []
        preocupaciones = payload.get("preocupaciones_top3")
        if not isinstance(preocupaciones, list):
            preocupaciones = []
        scatter = payload.get("scatter_clientes")
        bar_chart = payload.get("bar_chart_clientes")
        fortalezas_debilidades = (
            payload.get("fortalezas_debilidades")
            if isinstance(payload.get("fortalezas_debilidades"), dict)
            else {}
        )
        strengths_weaknesses_html = self._render_strengths_weaknesses_section(fortalezas_debilidades)
        bar_chart_html = ""
        if isinstance(bar_chart, dict):
            bar_chart_html = self._render_bar_chart_vista_c(bar_chart)
        if not bar_chart_html and isinstance(scatter, dict):
            bar_chart_html = self._render_customer_bar_chart(scatter)

        customer_cards: list[str] = []
        for item in clientes[:3]:
            if not isinstance(item, dict):
                continue
            customer_cards.append(
                "<article class='cluster-card'>"
                f"<h3>{html.escape(str(item.get('label', '') or 'Tipo de cliente'))}</h3>"
                f"<p><strong>Descripción:</strong> {html.escape(str(item.get('descripcion_segmento', '') or ''))}</p>"
                f"<p><strong>Estado emocional:</strong> {html.escape(str(item.get('estado_emocional', '') or ''))}</p>"
                f"<p><strong>Intención:</strong> {html.escape(str(item.get('intencion_detectada', '') or ''))}</p>"
                f"<p><strong>Expectativas:</strong> {html.escape(str(item.get('expectativas', '') or ''))}</p>"
                "</article>"
            )

        problem_cards: list[str] = []
        for item in preocupaciones[:3]:
            if not isinstance(item, dict):
                continue
            problema = self._humanize_action_text(str(item.get("problema", "") or "Tema"))
            severity_value = self._safe_float(item.get("severidad"))
            severity_label = self._severity_band(severity_value)
            problem_cards.append(
                "<article class='cluster-card'>"
                f"<h3>{html.escape(problema)}</h3>"
                f"<p><strong>Volumen:</strong> {self._safe_int(item.get('volumen'))}</p>"
                f"<p><strong>Severidad:</strong> {html.escape(severity_label)} ({severity_value:.3f})</p>"
                f"<p><strong>Valoración asociada:</strong> {round(self._safe_float(item.get('rating_medio_asociado')), 2)}</p>"
                f"<p><strong>Ejemplo:</strong> {html.escape(str(item.get('ejemplo_literal', '') or ''))}</p>"
                "</article>"
            )

        parts = [f"<p>{html.escape(lectura)}</p>" if lectura else ""]
        if strengths_weaknesses_html:
            parts.append(strengths_weaknesses_html)
        if customer_cards:
            parts.extend(
                [
                    "<h3>Tipos de cliente más relevantes</h3>",
                    f"<div class='cluster-grid'>{''.join(customer_cards)}</div>",
                ]
            )
        if problem_cards:
            parts.extend(
                [
                    "<h3>Qué le preocupa a cada tipo de cliente</h3>",
                    f"<div class='cluster-grid'>{''.join(problem_cards)}</div>",
                ]
            )
        if bar_chart_html:
            parts.extend(["<h3>Peso de cada tipo de cliente</h3>", bar_chart_html])
        scatter_html = ""
        if isinstance(scatter, dict):
            scatter_html = self._render_scatter_vista_d(scatter)
            if not scatter_html:
                scatter_html = self._render_payload(scatter) if not self._is_empty_payload(scatter) else ""
        if scatter_html:
            parts.extend(["<h3>Visualización de tipos de clientes</h3>", scatter_html])
        return "".join(parts)

    def _render_strengths_weaknesses_section(self, payload: dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        strengths = payload.get("fortalezas") if isinstance(payload.get("fortalezas"), list) else []
        weaknesses = payload.get("debilidades") if isinstance(payload.get("debilidades"), list) else []
        if not strengths and not weaknesses:
            return ""

        strong_cards: list[str] = []
        seen_strength_titles: set[str] = set()
        for item in strengths[:4]:
            if not isinstance(item, dict):
                continue
            title = self._clean_narrative_text(str(item.get("titulo", "") or "").strip())
            description = self._clean_narrative_text(str(item.get("descripcion", "") or "").strip())
            keep = self._clean_narrative_text(str(item.get("como_mantener", "") or "").strip())
            normalized_title = self._normalize_text(title)
            if not title or not normalized_title or normalized_title in seen_strength_titles:
                continue
            seen_strength_titles.add(normalized_title)
            strong_cards.append(
                "<article class='fw-card fw-strong'>"
                f"<div class='fw-icon'>{self._icon_slot('strength')}</div>"
                "<div>"
                f"<div class='fw-title'>{html.escape(title)}</div>"
                + (f"<div class='fw-desc'>{html.escape(description)}</div>" if description else "")
                + (f"<div class='fw-action'><strong>Cómo mantenerlo:</strong> {html.escape(keep)}</div>" if keep else "")
                + "</div>"
                "</article>"
            )

        weak_cards: list[str] = []
        seen_weak_titles: set[str] = set()
        for item in weaknesses[:4]:
            if not isinstance(item, dict):
                continue
            title = self._clean_narrative_text(str(item.get("titulo", "") or "").strip())
            description = self._clean_narrative_text(str(item.get("descripcion", "") or "").strip())
            w_type = str(item.get("tipo", "") or "").strip().lower() or "proceso"
            normalized_title = self._normalize_text(title)
            if not title or not normalized_title or normalized_title in seen_weak_titles:
                continue
            seen_weak_titles.add(normalized_title)
            weak_cards.append(
                "<article class='fw-card fw-weak'>"
                f"<div class='fw-icon'>{self._icon_slot('improvement')}</div>"
                "<div>"
                f"<div class='fw-title'>{html.escape(title)}</div>"
                + (f"<div class='fw-desc'>{html.escape(description)}</div>" if description else "")
                + f"<div><span class='fw-tipo-badge'>{html.escape(self._humanize_action_type_label(w_type))}</span></div>"
                + "</div>"
                "</article>"
            )

        if not strong_cards and not weak_cards:
            return ""
        strong_html = "".join(strong_cards) if strong_cards else "<p class='muted'>Sin fortalezas destacadas en esta muestra.</p>"
        weak_html = "".join(weak_cards) if weak_cards else "<p class='muted'>Sin debilidades críticas en esta muestra.</p>"
        return (
            "<h3>Qué funciona bien y qué hay que mejorar</h3>"
            "<div class='fw-grid'>"
            "<div class='fw-col'>"
            f"<h4 class='fw-col-title fw-col-strong'>{self._icon_slot('strength')}Puntos fuertes</h4>"
            f"{strong_html}"
            "</div>"
            "<div class='fw-col'>"
            f"<h4 class='fw-col-title fw-col-weak'>{self._icon_slot('improvement')}Puntos a mejorar</h4>"
            f"{weak_html}"
            "</div>"
            "</div>"
        )

    def _render_bar_chart_vista_c(self, bar_chart_data: dict[str, Any]) -> str:
        """
        Vista C — SVG de barras horizontales por tipo de cliente.
        """
        rows = bar_chart_data.get("rows") if isinstance(bar_chart_data, dict) else []
        if not isinstance(rows, list) or not rows:
            return ""

        svg_w = 860
        row_h = 52
        header_h = 28
        pad_l = 10
        bar_max_w = 440
        col_sat = 560
        col_sent = 680
        col_pct = 780
        total_h = header_h + len(rows) * row_h + 10
        font = "Plus Jakarta Sans, sans-serif"

        svg_parts: list[str] = [
            f'<svg viewBox="0 0 {svg_w} {total_h}" width="100%" style="display:block;">',
            f'<text x="{pad_l}" y="20" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Segmento de cliente</text>',
            f'<text x="{col_sat}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Satisfacción</text>',
            f'<text x="{col_sent}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Sentimiento</text>',
            f'<text x="{col_pct}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Peso</text>',
            f'<line x1="{pad_l}" y1="25" x2="{svg_w - 10}" y2="25" stroke="#E2DFD6" stroke-width="0.8"/>',
        ]

        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            y_base = header_h + idx * row_h
            color = str(row.get("color", "#0A7567") or "#0A7567")
            label = html.escape(str(row.get("label", "") or "Segmento"))
            count = self._safe_int(row.get("count_reviews"))
            weight_pct = self._safe_float(row.get("weight_pct"))
            bar_w = max(4, round((weight_pct / 100.0) * bar_max_w))
            sat_label = html.escape(str(row.get("satisfaction_label", "") or ""))
            sat_pct = self._safe_float(row.get("satisfaction_pct"))
            sent_label = html.escape(str(row.get("sentiment_label", "") or ""))
            sentiment = self._safe_float(row.get("sentiment"))
            sent_sign = "+" if sentiment >= 0 else ""

            if idx > 0:
                svg_parts.append(
                    f'<line x1="{pad_l}" y1="{y_base}" x2="{svg_w - 10}" y2="{y_base}" stroke="#E2DFD6" stroke-width="0.5"/>'
                )

            svg_parts.append(
                f'<text x="{pad_l}" y="{y_base + 18}" font-family="{font}" font-size="13" font-weight="700" fill="{color}">{label}</text>'
            )
            svg_parts.append(
                f'<text x="{pad_l}" y="{y_base + 33}" font-family="{font}" font-size="11" fill="#64748B">{count} reseñas</text>'
            )
            svg_parts.append(
                f'<rect x="{pad_l}" y="{y_base + 37}" width="{bar_w}" height="10" rx="5" fill="{color}" fill-opacity="0.85"/>'
            )
            svg_parts.append(
                f'<text x="{pad_l + bar_w + 6}" y="{y_base + 47}" font-family="{font}" font-size="10" fill="{color}" font-weight="600">{weight_pct:.1f}%</text>'
            )

            svg_parts.append(
                f'<circle cx="{col_sat}" cy="{y_base + 25}" r="21" fill="{color}" fill-opacity="0.12" stroke="{color}" stroke-width="1.5"/>'
            )
            svg_parts.append(
                f'<text x="{col_sat}" y="{y_base + 22}" text-anchor="middle" font-family="{font}" font-size="10" font-weight="700" fill="{color}">{sat_pct:.1f}%</text>'
            )
            svg_parts.append(
                f'<text x="{col_sat}" y="{y_base + 33}" text-anchor="middle" font-family="{font}" font-size="9" fill="{color}" opacity="0.8">{sat_label}</text>'
            )

            svg_parts.append(
                f'<text x="{col_sent}" y="{y_base + 23}" text-anchor="middle" font-family="{font}" font-size="14" font-weight="700" fill="{color}">{sent_sign}{sentiment:.2f}</text>'
            )
            svg_parts.append(
                f'<text x="{col_sent}" y="{y_base + 36}" text-anchor="middle" font-family="{font}" font-size="10" fill="{color}" opacity="0.8">{sent_label}</text>'
            )

            bubble_r = max(4, round(4 + (weight_pct / 100.0) * 22))
            svg_parts.append(
                f'<circle cx="{col_pct}" cy="{y_base + 25}" r="{bubble_r}" fill="{color}" fill-opacity="0.85"/>'
            )
            if weight_pct >= 5:
                svg_parts.append(
                    f'<text x="{col_pct}" y="{y_base + 29}" text-anchor="middle" font-family="{font}" font-size="9" fill="#FFFFFF" font-weight="700">{weight_pct:.1f}%</text>'
                )

        svg_parts.append("</svg>")
        return "<div class='bar-chart-wrap'>" + "\n".join(svg_parts) + "</div>"

    def _render_customer_bar_chart(self, scatter_payload: dict[str, Any]) -> str:
        circles = scatter_payload.get("circles") if isinstance(scatter_payload, dict) else []
        if not isinstance(circles, list) or not circles:
            return ""
        total = sum(self._safe_int(item.get("count")) for item in circles if isinstance(item, dict))
        if total <= 0:
            return ""

        width = 640
        label_width = 180
        bar_max = width - label_width - 90
        row_h = 32
        gap = 10
        colors = ["#0A7567", "#12B08A", "#D4F0E8", "#D4950A", "#C23B18"]
        rows: list[str] = []
        visible = circles[:5]
        for idx, item in enumerate(visible):
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "") or f"Segmento {idx + 1}").strip()[:30]
            count = self._safe_int(item.get("count"))
            pct = count / max(1, total)
            bar_w = max(1, int(round(pct * bar_max)))
            y = idx * (row_h + gap)
            color = colors[idx % len(colors)]
            rows.append(
                f"<text x='0' y='{y + 20}' fill='#161616' font-size='12' font-family='Plus Jakarta Sans,sans-serif'>{html.escape(label)}</text>"
                f"<rect x='{label_width}' y='{y}' width='{bar_w}' height='{row_h}' rx='6' fill='{color}' opacity='0.85'/>"
                f"<text x='{label_width + bar_w + 6}' y='{y + 20}' fill='#64748B' font-size='11' font-family='Plus Jakarta Sans,sans-serif'>{int(round(pct * 100))}% ({count})</text>"
            )
        total_h = max(44, len(visible) * (row_h + gap))
        return (
            "<div class='bar-chart-wrap'>"
            f"<svg viewBox='0 0 {width} {total_h}' width='100%' height='{total_h}'>"
            f"{''.join(rows)}"
            "</svg>"
            "</div>"
        )

    def _render_section_plan(self, payload: dict[str, Any]) -> str:
        lectura = self._clean_narrative_text(str(payload.get("lectura_ejecutiva", "") or "").strip())
        invisibles = payload.get("problemas_invisibles")
        if not isinstance(invisibles, list):
            invisibles = []
        corto = payload.get("corto_plazo_0_30_dias")
        medio = payload.get("medio_plazo_30_90_dias")
        largo = payload.get("largo_plazo_90_mas_dias")
        quick_wins = payload.get("quick_wins_esta_semana")
        if not isinstance(corto, list):
            corto = []
        if not isinstance(medio, list):
            medio = []
        if not isinstance(largo, list):
            largo = []
        if not isinstance(quick_wins, list):
            quick_wins = []

        quick_wins_filtered = self._dedupe_quick_wins_against_plan(
            quick_wins=quick_wins,
            plan_actions=[*corto, *medio, *largo],
        )

        invisible_items = "".join(
            "<li>"
            f"<strong>{html.escape(str(item.get('risk', '') or 'Riesgo detectado'))}:</strong> "
            f"{html.escape(str(item.get('detail', '') or ''))}"
            "</li>"
            for item in invisibles[:6]
            if isinstance(item, dict)
        )
        corto_html = self._render_action_items(corto)
        medio_html = self._render_action_items(medio)
        largo_html = self._render_action_items(largo)
        quick_html = self._render_action_items(quick_wins_filtered, is_quick_wins=True)

        parts = [f"<p>{html.escape(lectura)}</p>" if lectura else ""]
        if quick_html:
            parts.extend(
                [
                    "<div class='urgent-block'>",
                    f"<h3 class='urgent-title'>{self._icon_slot('urgent')}Esta semana — acciones de impacto inmediato</h3>",
                    quick_html,
                    "</div>",
                ]
            )
        if invisible_items:
            parts.extend(["<h3>Problemas invisibles (antes de que escalen)</h3>", f"<ul>{invisible_items}</ul>"])

        if corto_html or medio_html or largo_html:
            parts.extend(
                [
                    "<h3>Plan de acción por plazos</h3>",
                    "<div class='timeline'>",
                    f"<div class='timeline-col'><h4>Corto plazo (0-30 días)</h4>{corto_html}</div>",
                    f"<div class='timeline-col'><h4>Medio plazo (30-90 días)</h4>{medio_html}</div>",
                    f"<div class='timeline-col'><h4>Largo plazo (+90 días)</h4>{largo_html}</div>",
                    "</div>",
                    "<p class='muted'>En el anexo tienes el detalle completo de cada medida para llevarla a la práctica.</p>",
                ]
            )
        return "".join(parts)

    def _dedupe_quick_wins_against_plan(
        self,
        *,
        quick_wins: list[dict[str, Any]],
        plan_actions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not quick_wins:
            return []
        action_keys: set[str] = set()
        for item in plan_actions:
            if not isinstance(item, dict):
                continue
            action_text = str(item.get("accion") or item.get("action") or "").strip()
            if not action_text:
                continue
            action_keys.add(self._normalize_text(action_text))

        filtered: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        for item in quick_wins:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "") or "").strip()
            if not title:
                continue
            normalized_title = self._normalize_text(title)
            if not normalized_title or normalized_title in seen_titles:
                continue
            if any(normalized_title in key or key in normalized_title for key in action_keys if key):
                continue
            seen_titles.add(normalized_title)
            filtered.append(item)
        return filtered

    def _render_section_anexos(self, payload: dict[str, Any]) -> str:
        note = str(payload.get("nota", "") or "").strip()
        dataset = payload.get("resumen_dataset")
        benchmarking = payload.get("benchmarking_resumen")
        voces = payload.get("voz_literal_muestra")
        dataset_html = self._render_dataset_summary_spanish(dataset)
        benchmark_html = self._render_payload(benchmarking) if not self._is_empty_payload(benchmarking) else ""
        voces_html = self._render_voice_quotes(voces)
        parts = [f"<p>{html.escape(note)}</p>" if note else ""]
        if dataset_html:
            parts.extend(["<h3>Resumen del conjunto de datos</h3>", dataset_html, "<h3>Cómo leer estos indicadores</h3>", self._render_dimension_guide(dataset)])
        if benchmark_html:
            parts.extend(["<h3>Resumen frente a competidores</h3>", benchmark_html])
        if voces_html:
            parts.extend(["<h3>Voz literal del cliente (muestra anonimizada)</h3>", voces_html])
        if not parts:
            return ""
        return (
            "<details class='annex-details'>"
            f"<summary class='annex-summary'>{self._icon_slot('annex')}Datos técnicos del análisis "
            "<span class='annex-hint'>(despliega para ver)</span></summary>"
            f"<div class='annex-body'>{''.join(parts)}</div>"
            "</details>"
        )

    def _render_review_rows_table(self, payload: Any) -> str:
        if not isinstance(payload, list) or not payload:
            return ""
        rows = payload[:2000]
        headers = [
            "Índice",
            "Fuente",
            "Autor",
            "Valoración",
            "Sentimiento",
            "Brecha de expectativas",
            "Satisfacción",
            "Tema principal",
            "Tiene respuesta del negocio",
            "Resumen de reseña",
        ]
        map_header = {
            "Índice": "review_index",
            "Fuente": "source",
            "Autor": "author_name",
            "Valoración": "rating",
            "Sentimiento": "sentiment",
            "Brecha de expectativas": "expectation_gap",
            "Satisfacción": "satisfaction",
            "Tema principal": "dominant_problem",
            "Tiene respuesta del negocio": "has_owner_reply",
            "Resumen de reseña": "review_excerpt",
        }
        head_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
        body_html_rows = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            tds = []
            for header in headers:
                value = item.get(map_header[header])
                tds.append(f"<td>{html.escape(str(value if value is not None else ''))}</td>")
            body_html_rows.append(f"<tr>{''.join(tds)}</tr>")
        return f"<table><thead><tr>{head_html}</tr></thead><tbody>{''.join(body_html_rows)}</tbody></table>"

    def _humanize_section_key(self, key: str) -> str:
        mapped_titles = {
            "1_resumen_ejecutivo": "Diagnóstico temprano",
            "2_score_reputacion": "Puntuación de tu reputación",
            "3_quien_es_tu_cliente_y_que_le_preocupa": "Quién es tu cliente y qué le preocupa",
            "4_plan_de_accion": "Plan de acción",
            "5_anexos_resumen": "Anexo resumen",
        }
        if key in mapped_titles:
            return mapped_titles[key]
        clean = str(key or "").strip()
        clean = re.sub(r"^\d+[_\-.]?", "", clean)
        clean = clean.replace("_", " ").replace("-", " ").strip()
        if not clean:
            return "Sección"
        return clean[:1].upper() + clean[1:]

    def _render_payload(self, payload: Any, *, depth: int = 0) -> str:
        if payload is None:
            return ""

        if isinstance(payload, (str, int, float, bool)):
            text = self._clean_narrative_text(str(payload))
            return f"<p>{html.escape(text)}</p>" if text else ""

        if isinstance(payload, list):
            if not payload:
                return ""
            if all(isinstance(item, (str, int, float, bool)) for item in payload):
                items = "".join(
                    f"<li>{html.escape(self._clean_narrative_text(str(item)))}</li>"
                    for item in payload
                    if self._clean_narrative_text(str(item))
                )
                if not items:
                    return ""
                return f"<ul>{items}</ul>"
            rows = []
            for item in payload:
                rendered_item = self._render_payload(item, depth=depth + 1)
                if rendered_item.strip():
                    rows.append(f"<li>{rendered_item}</li>")
            if not rows:
                return ""
            return f"<ul>{''.join(rows)}</ul>"

        if isinstance(payload, dict):
            scatter_html = self._maybe_render_scatter_svg(payload)
            if scatter_html:
                return scatter_html

            payload_to_render = dict(payload)
            rank_value = self._safe_int(payload_to_render.get("target_rank"))
            competitors_compared = self._safe_int(payload_to_render.get("total_competitors_compared"))
            total_businesses_compared = self._safe_int(payload_to_render.get("total_businesses_compared"))
            if rank_value > 0 and competitors_compared > 0 and total_businesses_compared <= 0:
                total_businesses_compared = competitors_compared + 1
            if rank_value > 0 and total_businesses_compared > 0:
                payload_to_render["target_rank"] = (
                    f"{rank_value} de {total_businesses_compared} negocios similares analizados"
                )
                payload_to_render.pop("total_competitors_compared", None)
                payload_to_render.pop("total_businesses_compared", None)

            scalar_rows = []
            nested_rows = []
            hidden_keys = {"analysis_id", "dataset_id", "trend_slope", "sentiment_score"}
            for key, value in payload_to_render.items():
                if str(key).strip().lower() in hidden_keys:
                    continue
                key_label = html.escape(self._labelize_key_spanish(str(key)))
                if isinstance(value, (str, int, float, bool)) or value is None:
                    if isinstance(value, bool):
                        rendered_value = "Sí" if value else "No"
                    elif value is None:
                        rendered_value = "—"
                    else:
                        rendered_raw = str(value)
                        lower_key = str(key).strip().lower()
                        if lower_key in {"created_at", "generated_at", "report_generated_at", "preview_report_generated_at"}:
                            rendered_raw = self._format_human_date(rendered_raw)
                        elif lower_key == "target_reputation_score":
                            try:
                                rendered_raw = f"{float(rendered_raw):.1f}/100"
                            except (TypeError, ValueError):
                                rendered_raw = "—"
                        elif lower_key == "overall_sentiment":
                            rendered_raw = self._humanize_sentiment_value(rendered_raw)
                        elif lower_key == "trend":
                            rendered_raw = self._humanize_trend_value(rendered_raw)
                        rendered_value = html.escape(self._clean_narrative_text(rendered_raw))
                        if not rendered_value:
                            rendered_value = "—"
                    scalar_rows.append(
                        f"<tr><th>{key_label}</th><td>{rendered_value}</td></tr>"
                    )
                else:
                    rendered_nested = self._render_payload(value, depth=depth + 1)
                    if rendered_nested.strip():
                        nested_rows.append(f"<h3>{key_label}</h3>{rendered_nested}")

            parts = []
            if scalar_rows:
                parts.append(f"<table><tbody>{''.join(scalar_rows)}</tbody></table>")
            if nested_rows:
                parts.append("".join(nested_rows))
            if not parts:
                return ""
            return "".join(parts)

        text = self._clean_narrative_text(str(payload))
        return f"<p>{html.escape(text)}</p>" if text else ""

    def _render_scatter_vista_d(self, scatter_data: dict[str, Any]) -> str:
        """
        Vista D — SVG por zonas semánticas (layout fijo).
        No usa coordenadas reales de scatter: cada burbuja ocupa una celda lógica.
        """
        bubbles = scatter_data.get("bubbles") if isinstance(scatter_data, dict) else []
        if not isinstance(bubbles, list) or not bubbles:
            return ""

        svg_w, svg_h = 900, 390
        pad_l, pad_r, pad_t, pad_b = 72, 40, 28, 64
        plot_w = svg_w - pad_l - pad_r
        plot_h = svg_h - pad_t - pad_b
        half_w = plot_w / 2.0
        half_h = plot_h / 2.0
        x_mid = pad_l + half_w
        y_mid = pad_t + half_h
        font = "Plus Jakarta Sans, sans-serif"
        axes = scatter_data.get("axes") if isinstance(scatter_data.get("axes"), dict) else {}
        quadrant_labels = (
            scatter_data.get("quadrant_labels")
            if isinstance(scatter_data.get("quadrant_labels"), dict)
            else {}
        )

        zone_rects: dict[str, dict[str, float]] = {
            "top_left": {"x": pad_l, "y": pad_t, "w": half_w, "h": half_h},
            "top_right": {"x": x_mid, "y": pad_t, "w": half_w, "h": half_h},
            "bottom_left": {"x": pad_l, "y": y_mid, "w": half_w, "h": half_h},
            "bottom_right": {"x": x_mid, "y": y_mid, "w": half_w, "h": half_h},
        }
        zone_bg: dict[str, str] = {
            "top_left": "#0A7567",
            "top_right": "#D4950A",
            "bottom_left": "#8B95A5",
            "bottom_right": "#C23B18",
        }
        zone_order = ["top_left", "top_right", "bottom_left", "bottom_right"]

        def _fallback_zone_from_xy(item: dict[str, Any]) -> str:
            x = self._safe_float(item.get("x"))
            y = self._safe_float(item.get("y"))
            if y >= 50.0 and x < 50.0:
                return "top_left"
            if y >= 50.0 and x >= 50.0:
                return "top_right"
            if y < 50.0 and x < 50.0:
                return "bottom_left"
            return "bottom_right"

        grouped: dict[str, list[dict[str, Any]]] = {zone: [] for zone in zone_order}
        for item in bubbles:
            if not isinstance(item, dict):
                continue
            zone = str(item.get("zone", "") or "").strip().lower()
            if zone not in grouped:
                zone = _fallback_zone_from_xy(item)
            grouped[zone].append(item)

        for zone in zone_order:
            grouped[zone] = sorted(
                grouped[zone],
                key=lambda bubble: self._safe_int(bubble.get("count_reviews")),
                reverse=True,
            )

        def _layout_zone(
            zone_rect: dict[str, float],
            zone_bubbles: list[dict[str, Any]],
        ) -> list[tuple[dict[str, Any], float, float, float]]:
            n = len(zone_bubbles)
            if n <= 0:
                return []
            x0 = zone_rect["x"]
            y0 = zone_rect["y"]
            w = zone_rect["w"]
            h = zone_rect["h"]
            cx_center = x0 + (w / 2.0)
            cy_center = y0 + (h / 2.0)
            placements: list[tuple[dict[str, Any], float, float, float]] = []

            if n == 1:
                diameter = min(w, h) * 0.80
                placements.append((zone_bubbles[0], cx_center, cy_center, diameter / 2.0))
                return placements

            if n == 2:
                diameter = min(w * 0.42, h * 0.78)
                left_x = x0 + (w * 0.30)
                right_x = x0 + (w * 0.70)
                placements.append((zone_bubbles[0], left_x, cy_center, diameter / 2.0))
                placements.append((zone_bubbles[1], right_x, cy_center, diameter / 2.0))
                return placements

            diameter = min(w * 0.38, h * 0.38)
            grid_positions = [
                (0.30, 0.33),
                (0.70, 0.33),
                (0.30, 0.72),
                (0.70, 0.72),
            ]
            for idx, bubble in enumerate(zone_bubbles[:4]):
                rel_x, rel_y = grid_positions[idx]
                bubble_cx = x0 + (w * rel_x)
                bubble_cy = y0 + (h * rel_y)
                placements.append((bubble, bubble_cx, bubble_cy, diameter / 2.0))
            return placements

        placed_bubbles: list[tuple[dict[str, Any], float, float, float]] = []
        for zone in zone_order:
            placed_bubbles.extend(_layout_zone(zone_rects[zone], grouped[zone]))

        svg_parts: list[str] = [
            f'<svg viewBox="0 0 {svg_w} {svg_h}" width="100%" style="display:block;">',
        ]

        for zone in zone_order:
            rect = zone_rects[zone]
            bg_color = zone_bg.get(zone, "#64748B")
            svg_parts.append(
                f'<rect x="{rect["x"]}" y="{rect["y"]}" width="{rect["w"]}" height="{rect["h"]}" fill="{bg_color}" fill-opacity="0.045"/>'
            )
            label_text = str(quadrant_labels.get(zone, "") or "").strip()
            if label_text:
                label_center_x = rect["x"] + (rect["w"] / 2.0)
                label_y = rect["y"] + 14.0
                svg_parts.append(
                    f'<text x="{label_center_x}" y="{label_y}" text-anchor="middle" font-family="{font}" font-size="10" font-weight="600" fill="{bg_color}" opacity="0.85">{html.escape(label_text)}</text>'
                )

        svg_parts.extend(
            [
                f'<line x1="{x_mid}" y1="{pad_t}" x2="{x_mid}" y2="{pad_t + plot_h}" stroke="#D9D5CA" stroke-width="1.2"/>',
                f'<line x1="{pad_l}" y1="{y_mid}" x2="{pad_l + plot_w}" y2="{y_mid}" stroke="#D9D5CA" stroke-width="1.2"/>',
            ]
        )

        for bubble, cx_raw, cy_raw, radius_raw in placed_bubbles:
            color = str(bubble.get("color", "#0A7567") or "#0A7567")
            label_value = str(bubble.get("label", "") or "Segmento").strip()
            label_text = html.escape(label_value[:22] + "..." if len(label_value) > 22 else label_value)
            count = self._safe_int(bubble.get("count_reviews"))
            weight_pct = self._safe_float(bubble.get("weight_pct"))
            cx = round(cx_raw, 1)
            cy = round(cy_raw, 1)
            r = round(max(16.0, min(76.0, radius_raw)), 1)

            if r >= 56:
                label_font = 12
                meta_font = 11
                line1_y = cy - 8
                line2_y = cy + 10
            elif r >= 44:
                label_font = 11
                meta_font = 10
                line1_y = cy - 6
                line2_y = cy + 8
            else:
                label_font = 10
                meta_font = 9
                line1_y = cy - 5
                line2_y = cy + 7

            svg_parts.append(
                f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>'
            )
            svg_parts.append(
                f'<text x="{cx}" y="{line1_y}" text-anchor="middle" font-family="{font}" font-size="{label_font}" font-weight="700" fill="{color}">{label_text}</text>'
            )
            svg_parts.append(
                f'<text x="{cx}" y="{line2_y}" text-anchor="middle" font-family="{font}" font-size="{meta_font}" fill="{color}" opacity="0.9">{count} · {weight_pct:.1f}%</text>'
            )

        svg_parts.extend(
            [
                f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" y2="{pad_t + plot_h}" stroke="#C5C1B8" stroke-width="0.8"/>',
                f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" stroke="#C5C1B8" stroke-width="0.8"/>',
                f'<text x="{pad_l + (plot_w / 2.0)}" y="{svg_h - 8}" text-anchor="middle" font-family="{font}" font-size="12" fill="#64748B">{html.escape(str(axes.get("x_label", "Brecha de expectativa")))}</text>',
                f'<text x="18" y="{pad_t + (plot_h / 2.0)}" text-anchor="middle" font-family="{font}" font-size="12" fill="#64748B" transform="rotate(-90,18,{pad_t + (plot_h / 2.0)})">{html.escape(str(axes.get("y_label", "Satisfacción")))}</text>',
                f'<text x="{pad_l + 4}" y="{pad_t + plot_h + 15}" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("x_low", "Expectativas cumplidas")))}</text>',
                f'<text x="{pad_l + plot_w - 4}" y="{pad_t + plot_h + 15}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("x_high", "Expectativas no cumplidas")))}</text>',
                f'<text x="{pad_l - 8}" y="{pad_t + plot_h}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("y_low", "Baja satisfacción")))}</text>',
                f'<text x="{pad_l - 8}" y="{pad_t + 8}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("y_high", "Alta satisfacción")))}</text>',
                "</svg>",
            ]
        )
        return "<div class='scatter'>" + "\n".join(svg_parts) + "</div>"

    def _maybe_render_scatter_svg(self, payload: dict[str, Any]) -> str | None:
        if payload.get("type") == "scatter_d" or isinstance(payload.get("bubbles"), list):
            rendered = self._render_scatter_vista_d(payload)
            if rendered:
                return rendered

        axes = payload.get("axes")
        circles = payload.get("circles")
        points = payload.get("points")
        if not isinstance(axes, dict):
            return None
        if not isinstance(circles, list):
            return None

        width = 920.0
        height = 400.0
        pad_left = 96.0
        pad_right = 46.0
        pad_top = 48.0
        pad_bottom = 80.0
        inner_w = width - (pad_left + pad_right)
        inner_h = height - (pad_top + pad_bottom)

        def sx(x: float) -> float:
            x = max(0.0, min(100.0, x))
            return pad_left + ((x / 100.0) * inner_w)

        def sy(y: float) -> float:
            y = max(0.0, min(100.0, y))
            return height - pad_bottom - ((y / 100.0) * inner_h)

        svg_parts = []
        svg_parts.append(
            f"<line x1='{pad_left}' y1='{height - pad_bottom}' x2='{width - pad_right}' y2='{height - pad_bottom}' stroke='#8fd6c5' stroke-width='1'/>"
        )
        svg_parts.append(
            f"<line x1='{pad_left}' y1='{height - pad_bottom}' x2='{pad_left}' y2='{pad_top}' stroke='#8fd6c5' stroke-width='1'/>"
        )

        for tick in (0, 25, 50, 75, 100):
            x = sx(float(tick))
            y = sy(float(tick))
            svg_parts.append(
                f"<line x1='{x}' y1='{height - pad_bottom}' x2='{x}' y2='{height - pad_bottom + 6}' stroke='#8fd6c5' stroke-width='1'/>"
            )
            svg_parts.append(
                f"<text x='{x - 9}' y='{height - pad_bottom + 22}' fill='#64748B' font-size='11'>{tick}</text>"
            )
            svg_parts.append(
                f"<line x1='{pad_left - 6}' y1='{y}' x2='{pad_left}' y2='{y}' stroke='#8fd6c5' stroke-width='1'/>"
            )
            svg_parts.append(f"<text x='26' y='{y + 4}' fill='#64748B' font-size='11'>{tick}</text>")

        palette = list(self._PALETTE)
        for index, circle in enumerate(circles):
            if not isinstance(circle, dict):
                continue
            center = circle.get("center") if isinstance(circle.get("center"), dict) else {}
            cx = sx(float(center.get("x", 0.0)))
            cy = sy(float(center.get("y", 0.0)))
            radius_raw = float(circle.get("radius", 4.0))
            radius = max(4.0, min(36.0, radius_raw))
            color = palette[index % len(palette)]
            label = html.escape(str(circle.get("label", f"cluster_{index}") or f"cluster_{index}"))
            svg_parts.append(
                f"<circle cx='{cx}' cy='{cy}' r='{radius}' fill='{color}66' stroke='{color}' stroke-width='1.5'/>"
            )
            svg_parts.append(f"<text x='{cx + 4}' y='{cy - 4}' fill='#1b3d36' font-size='10'>{label}</text>")

        if isinstance(points, list):
            for point in points[:500]:
                if not isinstance(point, dict):
                    continue
                x = sx(float(point.get("x", 0.0)))
                y = sy(float(point.get("y", 0.0)))
                size = max(2.0, min(6.0, float(point.get("size", 1.0))))
                svg_parts.append(
                    f"<circle cx='{x}' cy='{y}' r='{size}' fill='#1b3d36cc' stroke='#ffffff' stroke-width='0.6'/>"
                )

        x_label = html.escape(str(axes.get("x_label", axes.get("x", "X"))))
        y_label = html.escape(str(axes.get("y_label", axes.get("y", "Y"))))
        svg_parts.append(
            f"<text x='{(pad_left + inner_w / 2) - 120}' y='{height - 16}' fill='#64748B' font-size='13'>{x_label}</text>"
        )
        svg_parts.append(
            f"<text x='18' y='{(pad_top + inner_h / 2)}' transform='rotate(-90, 24, {pad_top + inner_h / 2})' fill='#64748B' font-size='13'>{y_label}</text>"
        )

        return (
            "<div class='scatter'>"
            f"<svg viewBox='0 0 {width} {height}' width='100%' height='{height}'>{''.join(svg_parts)}</svg>"
            "</div>"
        )

    def _render_score_components(self, components: Any) -> str:
        if not isinstance(components, dict):
            return ""
        labels = [
            (
                "avg_rating",
                "Valoración media",
                "Media de estrellas. Cuanto más cerca de 5, mejor percepción global.",
                lambda v: f"{self._safe_float(v):.2f} / 5",
            ),
            (
                "response_rate",
                "Tasa de respuesta a comentarios",
                "Porcentaje de reseñas respondidas por el negocio.",
                lambda v: f"{self._safe_float(v) * 100:.1f}%",
            ),
            (
                "negative_ratio",
                "Proporción de reseñas negativas",
                "Parte de reseñas con experiencia negativa. Cuanto más baja, mejor.",
                lambda v: f"{self._safe_float(v) * 100:.1f}%",
            ),
            (
                "sentiment_avg",
                "Sentimiento medio",
                "Mide el tono global de las reseñas (de negativo a positivo).",
                lambda v: f"{self._safe_float(v):.2f}",
            ),
            (
                "tranquility_avg",
                "Calma percibida",
                "Mide si el tono es tranquilo o agresivo. Más alto suele ser mejor.",
                lambda v: f"{self._safe_float(v):.2f}",
            ),
        ]
        cards: list[str] = []
        for key, title, explain, formatter in labels:
            if key not in components:
                continue
            raw = components.get(key)
            value_num = self._safe_float(raw)
            context_label = self._metric_context_label(key, value_num)
            display_value = formatter(raw)
            if context_label:
                display_value = f"{display_value} · {context_label}"
            cards.append(
                "<article class='metric-card'>"
                f"<div class='metric-title'>{html.escape(title)}</div>"
                f"<div class='metric-value'>{html.escape(display_value)}</div>"
                f"<div class='metric-explain'>{html.escape(explain)}</div>"
                "</article>"
            )
        return f"<div class='metric-grid'>{''.join(cards)}</div>" if cards else ""

    def _render_action_items(self, payload: Any, *, is_quick_wins: bool = False) -> str:
        if not isinstance(payload, list):
            return ""
        cards: list[str] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            if is_quick_wins:
                titulo = str(item.get("title", "") or "").strip()
                por_que = str(item.get("why", "") or "").strip()
                esfuerzo = str(item.get("effort", "") or "").strip()
                impacto = str(item.get("impact", "") or "").strip()
                if not titulo:
                    continue
                titulo_h = self._humanize_action_text(titulo)
                por_que_h = self._humanize_action_text(por_que)
                esfuerzo_h = self._humanize_effort(effort=esfuerzo)
                impacto_h = self._humanize_impact(impact=impacto)
                cards.append(
                    "<li class='action-card'>"
                    f"<div class='title'>{html.escape(self._clean_narrative_text(titulo_h))}</div>"
                    f"<div>{html.escape(self._clean_narrative_text(por_que_h))}</div>"
                    f"<div class='meta-line'>Esfuerzo: {html.escape(esfuerzo_h)} · Impacto esperado: {html.escape(impacto_h)}</div>"
                    "</li>"
                )
                continue

            accion = str(item.get("accion") or item.get("action") or "").strip()
            if not accion:
                continue
            por_que = str(item.get("por_que") or item.get("why") or "").strip()
            encargado = str(item.get("encargado") or item.get("owner") or "").strip()
            objetivo = str(item.get("objetivo") or item.get("kpi") or "").strip()
            action_type = str(item.get("tipo", "") or "").strip().lower()
            tool = str(item.get("herramienta_si_aplica", "") or "").strip()
            if not action_type:
                action_type = self._infer_action_type_from_text(
                    f"{item.get('problema', '')} {accion}"
                )
            if not tool:
                tool = self._infer_action_tool_from_text(f"{item.get('problema', '')} {accion}")
            accion_h = self._humanize_action_text(accion)
            por_que_h = self._humanize_action_text(por_que)
            encargado_h = self._humanize_role(encargado)
            objetivo_h = self._humanize_action_text(objetivo)
            tool_h = self._humanize_action_text(tool)
            plazo = item.get("horizon_days") or item.get("horizonte_dias")
            plazo_text = ""
            if plazo is not None:
                try:
                    plazo_text = f"{int(plazo)} días"
                except (TypeError, ValueError):
                    plazo_text = str(plazo)

            badge_cfg = self._action_type_badge(action_type)
            badge_html = (
                f"<span class='tipo-badge' style='background:{badge_cfg['bg']};"
                f"color:{badge_cfg['text']};border-color:{badge_cfg['border']}'>{html.escape(badge_cfg['label'])}</span>"
            )
            cards.append(
                "<li class='action-card'>"
                "<div class='action-card-header'>"
                f"<div class='title'>{html.escape(self._clean_narrative_text(accion_h))}</div>"
                f"{badge_html}"
                "</div>"
                + (f"<div>{html.escape(self._clean_narrative_text(por_que_h))}</div>" if por_que_h else "")
                + (
                    f"<div class='meta-line'>Encargado de resolverlo: {html.escape(encargado_h)}</div>"
                    if encargado_h
                    else ""
                )
                + (f"<div class='meta-line'>Plazo objetivo: {html.escape(plazo_text)}</div>" if plazo_text else "")
                + (f"<div class='meta-line'>Indicador de seguimiento: {html.escape(self._clean_narrative_text(objetivo_h))}</div>" if objetivo_h else "")
                + (f"<div class='meta-line'>Herramienta: {html.escape(self._clean_narrative_text(tool_h))}</div>" if tool_h else "")
                + "</li>"
            )
        if not cards:
            return ""
        return f"<ul class='action-list'>{''.join(cards)}</ul>"

    def _render_dataset_summary_spanish(self, dataset: Any) -> str:
        if not isinstance(dataset, dict):
            return ""
        total = self._safe_int(dataset.get("total_reviews"))
        avg_rating = self._safe_float(dataset.get("avg_rating"))
        response_rate = self._safe_float(dataset.get("response_rate"))
        by_source = dataset.get("by_source") if isinstance(dataset.get("by_source"), dict) else {}
        by_problem = dataset.get("by_problem") if isinstance(dataset.get("by_problem"), dict) else {}

        cards = [
            "<article class='metric-card'>"
            "<div class='metric-title'>Reseñas analizadas</div>"
            f"<div class='metric-value'>{total}</div>"
            "<div class='metric-explain'>Cantidad total de opiniones incluidas en este informe.</div>"
            "</article>",
            "<article class='metric-card'>"
            "<div class='metric-title'>Valoración media</div>"
            f"<div class='metric-value'>{avg_rating:.2f} / 5</div>"
            "<div class='metric-explain'>Media de puntuación. Por encima de 4 suele indicar buena percepción.</div>"
            "</article>",
            "<article class='metric-card'>"
            "<div class='metric-title'>Tasa de respuesta a comentarios</div>"
            f"<div class='metric-value'>{response_rate * 100:.1f}%</div>"
            "<div class='metric-explain'>Porcentaje de reseñas que reciben respuesta del negocio.</div>"
            "</article>",
        ]
        source_text = ", ".join(
            f"{self._source_name_spanish(str(k))}: {self._safe_int(v)}"
            for k, v in by_source.items()
            if str(k).strip()
        )
        problem_text = ", ".join(
            f"{self._clean_narrative_text(self._humanize_action_text(str(k).replace('_', ' ')))}: {self._safe_int(v)}"
            for k, v in list(by_problem.items())[:6]
            if str(k).strip()
        )
        extra = []
        if source_text:
            extra.append(f"<p><strong>Distribución por fuente:</strong> {html.escape(source_text)}</p>")
        if problem_text:
            extra.append(f"<p><strong>Temas más repetidos:</strong> {html.escape(problem_text)}</p>")
        return f"<div class='metric-grid'>{''.join(cards)}</div>{''.join(extra)}"

    def _render_dimension_guide(self, dataset: Any) -> str:
        if not isinstance(dataset, dict):
            return ""
        dims = dataset.get("dimension_averages") if isinstance(dataset.get("dimension_averages"), dict) else {}
        if not dims:
            return ""
        guide = [
            (
                "sentiment",
                "Sentimiento",
                "Resume el tono general de las reseñas. Valores más altos suelen ser mejor.",
                "Una señal saludable suele estar claramente por encima de 0.",
            ),
            (
                "expectation_gap",
                "Brecha de expectativas",
                "Mide cuánto se aleja la experiencia de lo que esperaba el cliente.",
                "Cuanto más cerca de 0, mejor alineación con lo prometido.",
            ),
            (
                "satisfaction",
                "Satisfacción",
                "Nivel de satisfacción global detectado en opiniones y valoración.",
                "Valores altos indican más probabilidad de repetición o recomendación.",
            ),
            (
                "tranquility_aggressiveness",
                "Tranquilidad vs agresividad",
                "Captura si el lenguaje es calmado o tenso/agresivo.",
                "Más alto suele reflejar una conversación más sana con el cliente.",
            ),
            (
                "improvement_intent",
                "Intención de mejora",
                "Cuánto piden cambios concretos los clientes.",
                "Alto no es malo por sí mismo: puede señalar oportunidades claras de mejora.",
            ),
        ]
        rows = []
        for key, title, meaning, reading in guide:
            if key not in dims:
                continue
            value = self._safe_float(dims.get(key))
            context_label = self._metric_context_label(key, value)
            display_value = f"{value:.2f}"
            if context_label:
                display_value = f"{display_value} · {context_label}"
            rows.append(
                "<article class='metric-card'>"
                f"<div class='metric-title'>{html.escape(title)}</div>"
                f"<div class='metric-value'>{html.escape(display_value)}</div>"
                f"<div class='metric-explain'>{html.escape(meaning)}</div>"
                f"<div class='metric-explain'>{html.escape(reading)}</div>"
                "</article>"
            )
        return f"<div class='metric-grid'>{''.join(rows)}</div>" if rows else ""

    def _render_voice_quotes(self, voces: Any) -> str:
        if not isinstance(voces, dict):
            return ""
        positive = voces.get("positive_quotes") if isinstance(voces.get("positive_quotes"), list) else []
        negative = voces.get("negative_quotes") if isinstance(voces.get("negative_quotes"), list) else []
        improvement = voces.get("improvement_quotes") if isinstance(voces.get("improvement_quotes"), list) else []
        selected = [*positive[:2], *negative[:2], *improvement[:2]]
        cards: list[str] = []
        for item in selected:
            if not isinstance(item, dict):
                continue
            quote = str(item.get("quote", "") or "").strip()
            if not quote:
                continue
            source_label = self._source_name_spanish(str(item.get("source", "") or "desconocida"))
            cards.append(
                "<li class='voice-card'>"
                f"<div class='voice-meta'>{html.escape(self._anonymize_person_name(str(item.get('author_name', '') or 'Cliente')))} · "
                f"Valoración {self._safe_float(item.get('rating')):.1f} · "
                f"Fuente {html.escape(source_label)}</div>"
                f"<div>{html.escape(self._clean_narrative_text(quote))}</div>"
                "</li>"
            )
        if not cards:
            return ""
        return f"<ul class='voice-list'>{''.join(cards)}</ul>"

    def _clean_narrative_text(self, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = text.replace("**", "")
        text = self._humanize_action_text(text)
        text = re.sub(r"\bimpactoo\b", "impacto", text, flags=re.IGNORECASE)
        text = re.sub(
            r"\b([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{4,})([aeiouáéíóúüAEIOUÁÉÍÓÚÜ])\2\b",
            r"\1\2",
            text,
        )
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _is_empty_payload(self, payload: Any) -> bool:
        if payload is None:
            return True
        if isinstance(payload, str):
            return not str(payload).strip()
        if isinstance(payload, (list, tuple, set)):
            return len(payload) == 0
        if isinstance(payload, dict):
            return len(payload) == 0
        return False

    def _anonymize_person_name(self, name: str) -> str:
        clean = str(name or "").strip()
        if not clean:
            return "C********"
        first = clean[0].upper()
        return f"{first}{'*' * 7}"

    def _source_name_spanish(self, source: str) -> str:
        normalized = str(source or "").strip().lower()
        if not normalized:
            return "fuente no identificada"
        mapping = {
            "google_maps": "Google Maps",
            "tripadvisor": "Tripadvisor",
            "trustpilot": "Trustpilot",
            "booking": "Booking",
            "reddit": "Reddit",
            "unknown": "fuente no identificada",
        }
        if normalized in mapping:
            return mapping[normalized]
        return normalized.replace("_", " ")

    def _icon_slot(self, icon_name: str) -> str:
        safe_name = html.escape(str(icon_name or "").strip().lower(), quote=True)
        return f"<span class='icon-slot' data-icon='{safe_name}' aria-hidden='true'></span>"

    def _labelize_key_spanish(self, key: str) -> str:
        normalized = str(key or "").strip()
        if not normalized:
            return "Dato"
        mapping = {
            "avg_rating": "Valoración media",
            "response_rate": "Tasa de respuesta a comentarios",
            "negative_ratio": "Proporción de reseñas negativas",
            "sentiment_avg": "Sentimiento medio",
            "tranquility_avg": "Calma percibida",
            "trend": "Evolución general",
            "trend_slope": "Ritmo de cambio",
            "analyses_history": "Histórico de análisis",
            "satisfaction_by_relative_time_bucket": "Satisfacción por antigüedad de reseña",
            "satisfaccion_por_antiguedad_resena": "Satisfacción por antigüedad de reseña",
            "score_scale": "Escala de puntuación",
            "target_rank": "Posición del negocio",
            "total_competitors_compared": "Competidores comparados",
            "total_businesses_compared": "Negocios analizados en la comparativa",
            "target_reputation_score": "Puntuación del negocio",
            "top_competitors": "Competidores destacados",
            "total_reviews": "Reseñas totales",
            "by_source": "Distribución por fuente",
            "by_problem": "Distribución por tema",
            "dimension_averages": "Promedio de dimensiones",
            "overall_sentiment": "Sentimiento del periodo",
            "review_count": "Número de reseñas",
            "cluster_count": "Número de tipos de cliente",
            "cluster_id": "Tipo de cliente",
            "review_rows": "Reseñas",
            "dominant_problem": "Tema principal",
            "has_owner_reply": "Tiene respuesta del negocio",
            "owner_reply_excerpt": "Respuesta del negocio (extracto)",
            "review_excerpt": "Extracto de reseña",
            "created_at": "Fecha",
            "source": "Fuente",
            "author_name": "Cliente",
            "rating": "Valoración",
            "score_display": "Puntuación mostrada",
            "nivel_reputacion": "Nivel de reputación",
            "problema": "Problema",
            "severidad": "Severidad",
            "volumen": "Volumen",
            "ejemplo_literal": "Ejemplo literal",
            "impact": "Impacto",
            "owner": "Encargado",
            "horizon_days": "Plazo (días)",
            "kpi": "Indicador de seguimiento",
            "old": "Antiguas",
            "medium": "Intermedias",
            "recent": "Recientes",
            "unknown": "Sin fecha clara",
        }
        if normalized in mapping:
            return mapping[normalized]
        prettified = normalized.replace("_", " ").replace("-", " ").strip()
        if not prettified:
            return "Dato"
        return prettified[:1].upper() + prettified[1:]

    def _humanize_action_text(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        output = value
        output = re.sub(
            r"satisfaction by relative time bucket",
            "Satisfacción por antigüedad de reseña",
            output,
            flags=re.IGNORECASE,
        )
        output = re.sub(
            r"^corregir incidencias de ['\"]?([^'\"]+)['\"]? con checklist operativo diario\.?$",
            r"Mejorar de inmediato '\1' con una rutina diaria de revisión.",
            output,
            flags=re.IGNORECASE,
        )
        output = re.sub(
            r"^estandarizar proceso y formación sobre ['\"]?([^'\"]+)['\"]?\.?$",
            r"Ordenar el proceso y formar al equipo para evitar fallos en '\1'.",
            output,
            flags=re.IGNORECASE,
        )
        output = re.sub(
            r"^automatizar seguimiento de señales tempranas de ['\"]?([^'\"]+)['\"]?\.?$",
            r"Crear un seguimiento continuo para detectar pronto fallos en '\1'.",
            output,
            flags=re.IGNORECASE,
        )
        output = re.sub(
            r"^micro-acción sobre ['\"]?([^'\"]+)['\"]?\.?$",
            r"Acción rápida sobre '\1'.",
            output,
            flags=re.IGNORECASE,
        )
        replacements = (
            ("checklist operativo diario", "rutina diaria de revisión"),
            ("checklist", "guía de tareas"),
            ("checklists", "guías de tareas"),
            ("micro-acción", "acción rápida"),
            ("quick wins", "acciones rápidas"),
            ("Data/Producto", "Dirección y mejora de procesos"),
            ("Gerencia + Calidad", "Gerencia y calidad"),
            ("Responsable de operación", "Encargado de operaciones"),
            ("precio_valor", "relación calidad-precio"),
            ("calidad_comida", "calidad de la comida"),
            ("tiempo_espera", "tiempo de espera"),
            ("gestion_reservas", "gestión de reservas"),
            ("ambiente_ruido", "ambiente y ruido"),
            ("<24h", "menos de 24 horas"),
            ("KPI", "indicador de seguimiento"),
            ("KPIs", "indicadores de seguimiento"),
            ("owner", "encargado"),
            ("impact", "impacto"),
            ("score", "puntuación de reputación"),
            ("trend", "tendencia"),
            ("response rate", "tasa de respuesta a comentarios"),
            ("bucket", "tramo temporal"),
            ("dataset", "conjunto de reseñas"),
            ("old", "antiguas"),
            ("medium", "intermedias"),
            ("recent", "recientes"),
        )
        for src, dst in replacements:
            output = re.sub(re.escape(src), dst, output, flags=re.IGNORECASE)
        output = re.sub(r"\bel tendencia\b", "la tendencia", output, flags=re.IGNORECASE)
        output = re.sub(r"\bservicio en reseñas negativas un 25%\b", "las menciones negativas sobre el servicio en un 25%", output, flags=re.IGNORECASE)
        output = re.sub(r"\bcalidad de la comida en reseñas negativas un 25%\b", "las menciones negativas sobre la calidad de la comida en un 25%", output, flags=re.IGNORECASE)
        output = re.sub(r"\brelación calidad-precio en reseñas negativas un 25%\b", "las menciones negativas sobre la relación calidad-precio en un 25%", output, flags=re.IGNORECASE)
        return output

    def _humanize_sentiment_value(self, value: str) -> str:
        normalized = self._normalize_text(value)
        mapping = {
            "positive": "Positivo",
            "mixed": "Mixto",
            "negative": "Negativo",
            "positivo": "Positivo",
            "mixto": "Mixto",
            "negativo": "Negativo",
        }
        return mapping.get(normalized, value)

    def _humanize_trend_value(self, value: str) -> str:
        normalized = self._normalize_text(value)
        mapping = {
            "al alza": "Al alza",
            "al_alza": "Al alza",
            "a la baja": "A la baja",
            "a_la_baja": "A la baja",
            "estable": "Estable",
        }
        return mapping.get(normalized, value)

    def _metric_context_label(self, key: str, value: float) -> str:
        normalized = self._normalize_text(key)
        if normalized in {"sentiment avg", "sentiment", "overall sentiment"}:
            if value >= 0.6:
                return "Tono positivo"
            if value >= 0.2:
                return "Tono favorable"
            if value > -0.2:
                return "Tono mixto"
            return "Tono negativo"
        if normalized in {"tranquility avg", "tranquility aggressiveness"}:
            if value >= 0.85:
                return "Muy tranquilo"
            if value >= 0.65:
                return "Tranquilo"
            if value >= 0.45:
                return "Con algo de tensión"
            return "Tenso"
        if normalized == "satisfaction":
            if value >= 0.8:
                return "Alta"
            if value >= 0.6:
                return "Media"
            return "Baja"
        if normalized == "expectation gap":
            if value <= 0.12:
                return "Expectativas bien gestionadas"
            if value <= 0.3:
                return "Hay margen de ajuste"
            return "Brecha relevante"
        if normalized == "improvement intent":
            if value <= 0.15:
                return "Baja - clientes satisfechos"
            if value <= 0.35:
                return "Moderada"
            return "Alta — piden cambios"
        if normalized == "negative ratio":
            if value <= 0.08:
                return "Bajo"
            if value <= 0.18:
                return "Medio"
            return "Alto"
        if normalized == "avg rating":
            if value >= 4.5:
                return "Excelente"
            if value >= 4.0:
                return "Buena"
            if value >= 3.5:
                return "Aceptable"
            return "Mejorable"
        if normalized == "response rate":
            if value >= 0.7:
                return "Muy activa"
            if value >= 0.4:
                return "Aceptable"
            if value > 0.0:
                return "Baja"
            return "Sin respuestas"
        return ""

    def _severity_band(self, value: float) -> str:
        if value >= 0.7:
            return "Alta"
        if value >= 0.4:
            return "Media"
        return "Baja"

    def _humanize_effort(self, *, effort: str) -> str:
        value = str(effort or "").strip().lower()
        mapping = {"low": "bajo", "medium": "medio", "high": "alto", "bajo": "bajo", "medio": "medio", "alto": "alto"}
        return mapping.get(value, "medio")

    def _humanize_impact(self, *, impact: str) -> str:
        value = str(impact or "").strip().lower()
        mapping = {"low": "bajo", "medium": "medio", "high": "alto", "bajo": "bajo", "medio": "medio", "alto": "alto"}
        return mapping.get(value, "medio")

    def _humanize_role(self, role: str) -> str:
        value = str(role or "").strip()
        if not value:
            return ""
        return self._humanize_action_text(value)

    def _humanize_action_type_label(self, action_type: str) -> str:
        value = str(action_type or "").strip().lower()
        mapping = {
            "proceso": "Proceso interno",
            "negocio": "Decisión de negocio",
            "implementacion": "Implementación",
            "tecnologico": "Solución tecnológica",
        }
        return mapping.get(value, "Proceso interno")

    def _action_type_badge(self, action_type: str) -> dict[str, str]:
        value = str(action_type or "").strip().lower()
        mapping = {
            "proceso": {
                "label": "Proceso interno",
                "bg": "#e3f0ff",
                "text": "#1a5fa8",
                "border": "#b3d1f5",
            },
            "negocio": {
                "label": "Decisión de negocio",
                "bg": "#fdf0e3",
                "text": "#a85f1a",
                "border": "#f5d1b3",
            },
            "implementacion": {
                "label": "Implementación",
                "bg": "#f3e3ff",
                "text": "#7a1aa8",
                "border": "#d9b3f5",
            },
            "tecnologico": {
                "label": "Solución tecnológica",
                "bg": "#e3fff0",
                "text": "#1aa85f",
                "border": "#b3f5d1",
            },
        }
        return mapping.get(value, mapping["proceso"])

    def _normalize_text(self, value: str) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        normalized = re.sub(r"\s+", " ", raw)
        normalized = normalized.replace("_", " ")
        normalized = re.sub(r"[^a-z0-9áéíóúüñ ]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _format_human_date(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "fecha no disponible"
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return raw[:10] if len(raw) >= 10 else raw
        months = [
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        ]
        return f"{dt.day} de {months[dt.month - 1]} de {dt.year}"

    def _infer_action_type_from_text(self, text: str) -> str:
        normalized = self._normalize_text(text)
        if any(token in normalized for token in ("crm", "software", "automat", "dashboard", "alerta")):
            return "tecnologico"
        if any(token in normalized for token in ("implementar", "integrar", "desarrollar")):
            return "implementacion"
        if any(token in normalized for token in ("precio", "margen", "carta", "menu", "estrategia")):
            return "negocio"
        return "proceso"

    def _infer_action_tool_from_text(self, text: str) -> str:
        normalized = self._normalize_text(text)
        if any(token in normalized for token in ("resena", "reseña", "responder")):
            return "Panel de reseñas y plantilla de respuesta"
        if any(token in normalized for token in ("tiempo", "espera", "comanda")):
            return "Registro de tiempos por turno"
        if any(token in normalized for token in ("formacion", "formación", "protocolo", "equipo")):
            return "Guía operativa y sesión interna"
        if any(token in normalized for token in ("precio", "menu", "carta")):
            return "Revisión de carta y costes"
        return ""

    def _score_badge(self, score: float) -> str:
        if score >= 85.0:
            return "<span class='badge good'>Excelente reputación</span>"
        if score >= 70.0:
            return "<span class='badge good'>Reputación sólida</span>"
        if score >= 55.0:
            return "<span class='badge warn'>Reputación media mejorable</span>"
        if score >= 40.0:
            return "<span class='badge warn'>Reputación mejorable</span>"
        return "<span class='badge bad'>Reputación crítica</span>"

    def _safe_float(self, value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _safe_int(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _slugify(self, value: str) -> str:
        raw = str(value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9._-]+", "-", raw)
        raw = raw.strip("-")
        return raw or "item"

    def _safe_identifier_slug(self, value: str) -> str:
        slug = self._slugify(value)
        return slug[:64] if slug else "id"

    def _safe_name_slug(self, value: str) -> str:
        slug = self._slugify(value)
        if not slug:
            return "negocio"
        return slug[:60]

    def _json_default(self, value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
