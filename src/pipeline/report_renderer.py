from __future__ import annotations

import asyncio
import csv
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.pipeline.report_rendering import ReportRenderingSectionsMixin


class StructuredReportRenderer(ReportRenderingSectionsMixin):
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
        source_analysis = report_payload.get("source_analysis")
        if not isinstance(source_analysis, dict):
            source_analysis = {}
        source_comparison = report_payload.get("source_comparison")
        if not isinstance(source_comparison, dict):
            source_comparison = None

        render_sections = dict(sections)
        google_source_payload = (
            source_analysis.get("google_maps")
            if isinstance(source_analysis.get("google_maps"), dict)
            else None
        )
        tripadvisor_source_payload = (
            source_analysis.get("tripadvisor")
            if isinstance(source_analysis.get("tripadvisor"), dict)
            else None
        )
        has_both_sources = bool(google_source_payload and tripadvisor_source_payload)
        if has_both_sources:
            render_sections["4_lectura_fuente_google_maps"] = google_source_payload
            render_sections["5_lectura_fuente_tripadvisor"] = tripadvisor_source_payload
        if has_both_sources and source_comparison:
            render_sections["7_comparativa_fuentes"] = {
                **source_comparison,
                "google_data": google_source_payload,
                "tripadvisor_data": tripadvisor_source_payload,
            }

        preferred_order = [
            "1_resumen_ejecutivo",
            "2_score_reputacion",
            "3_quien_es_tu_cliente_y_que_le_preocupa",
            "4_lectura_fuente_google_maps",
            "5_lectura_fuente_tripadvisor",
            "4_plan_de_accion",
            "7_comparativa_fuentes",
            "5_anexos_resumen",
        ]
        ordered_keys = [key for key in preferred_order if key in render_sections]
        section_order = report_payload.get("section_order")
        if isinstance(section_order, list):
            for key in section_order:
                normalized_key = str(key or "").strip()
                if normalized_key and normalized_key in render_sections and normalized_key not in ordered_keys:
                    ordered_keys.append(normalized_key)
        for key in render_sections.keys():
            if key not in ordered_keys:
                ordered_keys.append(key)

        anexo_resumen = sections.get("5_anexos_resumen") if isinstance(sections.get("5_anexos_resumen"), dict) else {}
        resumen_dataset = (
            anexo_resumen.get("resumen_dataset")
            if isinstance(anexo_resumen.get("resumen_dataset"), dict)
            else {}
        )
        report_metadata = report_payload.get("report_metadata")
        if not isinstance(report_metadata, dict):
            report_metadata = {}
        metadata_source_counts = (
            report_metadata.get("source_counts")
            if isinstance(report_metadata.get("source_counts"), dict)
            else {}
        )
        if not isinstance(metadata_source_counts, dict):
            metadata_source_counts = {}
        fuentes = (
            {
                str(source).strip().lower(): self._safe_int(count)
                for source, count in metadata_source_counts.items()
                if str(source).strip() and self._safe_int(count) > 0
            }
            if metadata_source_counts
            else (
                resumen_dataset.get("by_source")
                if isinstance(resumen_dataset.get("by_source"), dict)
                else {}
            )
        )
        total_reviews = self._safe_int(sum(fuentes.values())) if fuentes else self._safe_int(resumen_dataset.get("total_reviews"))
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
            payload = render_sections.get(key) if isinstance(render_sections, dict) else None
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
