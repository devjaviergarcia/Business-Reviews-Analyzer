from __future__ import annotations

import html
from typing import Any

from .base_section_generator import _BaseSectionGenerator


class ActionPlanSectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        lectura = r._clean_narrative_text(str(payload.get("lectura_ejecutiva", "") or "").strip())
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
        corto_html = r._render_action_items(corto)
        medio_html = r._render_action_items(medio)
        largo_html = r._render_action_items(largo)
        quick_html = r._render_action_items(quick_wins_filtered, is_quick_wins=True)

        parts = [f"<p>{html.escape(lectura)}</p>" if lectura else ""]
        if quick_html:
            parts.extend(
                [
                    "<div class='urgent-block'>",
                    f"<h3 class='urgent-title'>{r._icon_slot('urgent')}Esta semana — acciones de impacto inmediato</h3>",
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
        r = self.renderer
        if not quick_wins:
            return []
        action_keys: set[str] = set()
        for item in plan_actions:
            if not isinstance(item, dict):
                continue
            action_text = str(item.get("accion") or item.get("action") or "").strip()
            if not action_text:
                continue
            action_keys.add(r._normalize_text(action_text))

        filtered: list[dict[str, Any]] = []
        seen_titles: set[str] = set()
        for item in quick_wins:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "") or "").strip()
            if not title:
                continue
            normalized_title = r._normalize_text(title)
            if not normalized_title or normalized_title in seen_titles:
                continue
            if any(normalized_title in key or key in normalized_title for key in action_keys if key):
                continue
            seen_titles.add(normalized_title)
            filtered.append(item)
        return filtered


class AnnexSummarySectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        note = str(payload.get("nota", "") or "").strip()
        dataset = payload.get("resumen_dataset")
        benchmarking = payload.get("benchmarking_resumen")
        voces = payload.get("voz_literal_muestra")
        dataset_html = r._render_dataset_summary_spanish(dataset)
        benchmark_html = r._render_payload(benchmarking) if not r._is_empty_payload(benchmarking) else ""
        voces_html = r._render_voice_quotes(voces)
        parts = [f"<p>{html.escape(note)}</p>" if note else ""]
        if dataset_html:
            parts.extend([
                "<h3>Resumen del conjunto de datos</h3>",
                dataset_html,
                "<h3>Cómo leer estos indicadores</h3>",
                r._render_dimension_guide(dataset),
            ])
        if benchmark_html:
            parts.extend(["<h3>Resumen frente a competidores</h3>", benchmark_html])
        if voces_html:
            parts.extend(["<h3>Voz literal del cliente (muestra anonimizada)</h3>", voces_html])
        if not parts:
            return ""
        return (
            "<details class='annex-details'>"
            f"<summary class='annex-summary'>{r._icon_slot('annex')}Datos técnicos del análisis "
            "<span class='annex-hint'>(despliega para ver)</span></summary>"
            f"<div class='annex-body'>{''.join(parts)}</div>"
            "</details>"
        )
