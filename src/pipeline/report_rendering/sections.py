from __future__ import annotations

import html
from typing import Any

from .charts import ReportRenderingChartsMixin
from .generators import build_section_generators
from .section_annex_renderer import (
    render_dataset_summary_spanish,
    render_dimension_guide,
    render_voice_quotes,
)
from .section_payload_renderer import (
    render_action_items,
    render_payload,
    render_review_rows_table,
    render_score_components,
)
from .section_title_resolver import humanize_section_key


class ReportRenderingSectionsMixin(ReportRenderingChartsMixin):
    _section_generator_map: dict[str, Any] | None = None

    def _render_section_by_key(self, *, section_key: str, section_payload: Any) -> str:
        title = self._humanize_section_key(section_key)
        section_class_map = {
            "1_resumen_ejecutivo": "section section--diagnostico",
            "2_score_reputacion": "section section--puntuacion",
            "3_quien_es_tu_cliente_y_que_le_preocupa": "section section--cliente",
            "4_lectura_fuente_google_maps": "section section--fuente",
            "5_lectura_fuente_tripadvisor": "section section--fuente",
            "4_plan_de_accion": "section section--accion",
            "7_comparativa_fuentes": "section section--comparativa",
            "5_anexos_resumen": "section section--anexo",
        }
        section_class = section_class_map.get(section_key, "section")

        if not isinstance(section_payload, dict):
            content = self._render_payload(section_payload)
            if not content.strip():
                return ""
            return f"<section class='{section_class}'><h2>{html.escape(title)}</h2>{content}</section>"

        generator = self._get_section_generator_map().get(section_key)
        if generator is None:
            content = self._render_payload(section_payload)
        else:
            content = generator.render(section_payload)

        if not content.strip():
            return ""
        return f"<section class='{section_class}'><h2>{html.escape(title)}</h2>{content}</section>"

    def _get_section_generator_map(self) -> dict[str, Any]:
        if self._section_generator_map is None:
            self._section_generator_map = build_section_generators(self)
        return self._section_generator_map

    def _render_review_rows_table(self, payload: Any) -> str:
        return render_review_rows_table(self, payload)

    def _humanize_section_key(self, key: str) -> str:
        return humanize_section_key(key)

    def _render_payload(self, payload: Any, *, depth: int = 0) -> str:
        return render_payload(self, payload, depth=depth)

    def _render_score_components(self, components: Any) -> str:
        return render_score_components(self, components)

    def _render_action_items(self, payload: Any, *, is_quick_wins: bool = False) -> str:
        return render_action_items(self, payload, is_quick_wins=is_quick_wins)

    def _render_dataset_summary_spanish(self, dataset: Any) -> str:
        return render_dataset_summary_spanish(self, dataset)

    def _render_dimension_guide(self, dataset: Any) -> str:
        return render_dimension_guide(self, dataset)

    def _render_voice_quotes(self, voces: Any) -> str:
        return render_voice_quotes(self, voces)
