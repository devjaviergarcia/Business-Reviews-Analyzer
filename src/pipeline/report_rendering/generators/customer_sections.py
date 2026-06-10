from __future__ import annotations

import html
from typing import Any

from .base_section_generator import _BaseSectionGenerator


class CustomerProfileSectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        lectura = r._clean_narrative_text(str(payload.get("lectura_ejecutiva", "") or "").strip())
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
            bar_chart_html = r._render_bar_chart_vista_c(bar_chart)
        if not bar_chart_html and isinstance(scatter, dict):
            bar_chart_html = r._render_customer_bar_chart(scatter)

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
            problema = r._humanize_action_text(str(item.get("problema", "") or "Tema"))
            severity_value = r._safe_float(item.get("severidad"))
            severity_label = r._severity_band(severity_value)
            problem_cards.append(
                "<article class='cluster-card'>"
                f"<h3>{html.escape(problema)}</h3>"
                f"<p><strong>Volumen:</strong> {r._safe_int(item.get('volumen'))}</p>"
                f"<p><strong>Severidad:</strong> {html.escape(severity_label)} ({severity_value:.3f})</p>"
                f"<p><strong>Valoración asociada:</strong> {round(r._safe_float(item.get('rating_medio_asociado')), 2)}</p>"
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
            scatter_html = r._render_scatter_vista_d(scatter)
            if not scatter_html:
                scatter_html = r._render_payload(scatter) if not r._is_empty_payload(scatter) else ""
        if scatter_html:
            parts.extend(["<h3>Visualización de tipos de clientes</h3>", scatter_html])
        return "".join(parts)

    def _render_strengths_weaknesses_section(self, payload: dict[str, Any]) -> str:
        r = self.renderer
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
            title = r._clean_narrative_text(str(item.get("titulo", "") or "").strip())
            description = r._clean_narrative_text(str(item.get("descripcion", "") or "").strip())
            keep = r._clean_narrative_text(str(item.get("como_mantener", "") or "").strip())
            normalized_title = r._normalize_text(title)
            if not title or not normalized_title or normalized_title in seen_strength_titles:
                continue
            seen_strength_titles.add(normalized_title)
            strong_cards.append(
                "<article class='fw-card fw-strong'>"
                f"<div class='fw-icon'>{r._icon_slot('strength')}</div>"
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
            title = r._clean_narrative_text(str(item.get("titulo", "") or "").strip())
            description = r._clean_narrative_text(str(item.get("descripcion", "") or "").strip())
            w_type = str(item.get("tipo", "") or "").strip().lower() or "proceso"
            normalized_title = r._normalize_text(title)
            if not title or not normalized_title or normalized_title in seen_weak_titles:
                continue
            seen_weak_titles.add(normalized_title)
            weak_cards.append(
                "<article class='fw-card fw-weak'>"
                f"<div class='fw-icon'>{r._icon_slot('improvement')}</div>"
                "<div>"
                f"<div class='fw-title'>{html.escape(title)}</div>"
                + (f"<div class='fw-desc'>{html.escape(description)}</div>" if description else "")
                + f"<div><span class='fw-tipo-badge'>{html.escape(r._humanize_action_type_label(w_type))}</span></div>"
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
            f"<h4 class='fw-col-title fw-col-strong'>{r._icon_slot('strength')}Puntos fuertes</h4>"
            f"{strong_html}"
            "</div>"
            "<div class='fw-col'>"
            f"<h4 class='fw-col-title fw-col-weak'>{r._icon_slot('improvement')}Puntos a mejorar</h4>"
            f"{weak_html}"
            "</div>"
            "</div>"
        )
