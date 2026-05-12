from __future__ import annotations

import html
import re
from typing import Any

from .charts import ReportRenderingChartsMixin
from .generators import build_section_generators


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
            "4_lectura_fuente_google_maps": "Lectura por fuente: Google Maps",
            "5_lectura_fuente_tripadvisor": "Lectura por fuente: Tripadvisor",
            "4_plan_de_accion": "Plan de acción",
            "7_comparativa_fuentes": "Comparativa entre fuentes",
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
