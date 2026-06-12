from __future__ import annotations

import html
from typing import Any

from .base_section_generator import _BaseSectionGenerator


class SourceNarrativeSectionGenerator(_BaseSectionGenerator):
    def __init__(self, renderer: Any, *, source_key: str) -> None:
        super().__init__(renderer)
        self._source_key = source_key

    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        if not isinstance(payload, dict):
            return ""
        availability_notice = (
            payload.get("availability_notice")
            if isinstance(payload.get("availability_notice"), dict)
            else None
        )
        if availability_notice is not None:
            flag = str(availability_notice.get("flag", "") or "").strip() or "Fuente no disponible"
            label = str(availability_notice.get("label", "") or "").strip()
            detail = r._clean_narrative_text(str(availability_notice.get("detail", "") or "").strip())
            resolution = str(availability_notice.get("resolution", "") or "").strip().lower()
            resolution_label = {
                "business_not_found": "no encontrado",
                "manual_skip": "omitido manualmente",
                "manual_close": "cerrado manualmente",
            }.get(resolution, "")
            parts = [
                "<div class='fw-card fw-weak'>",
                f"<div class='fw-icon'>{r._icon_slot('improvement')}</div>",
                "<div>",
                f"<div class='fw-title'>{html.escape(flag)}</div>",
            ]
            if label:
                parts.append(f"<div class='fw-desc'>{html.escape(label)}</div>")
            if detail:
                parts.append(f"<div class='fw-action'>{html.escape(detail)}</div>")
            if resolution_label:
                parts.append(
                    f"<div class='muted' style='margin-top:6px'><strong>Estado:</strong> {html.escape(resolution_label)}</div>"
                )
            parts.extend(["</div>", "</div>"])
            return "".join(parts)
        narrative = payload.get("narrativa") if isinstance(payload.get("narrativa"), dict) else {}
        stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
        problem_clusters = (
            payload.get("problem_clusters")
            if isinstance(payload.get("problem_clusters"), dict)
            else {}
        )
        review_metrics = payload.get("review_metrics") if isinstance(payload.get("review_metrics"), list) else []
        main_text = r._clean_narrative_text(str(narrative.get("narrativa", "") or "").strip())
        note_text = _normalize_source_note(
            r._clean_narrative_text(str(narrative.get("nota_sesgo", "") or "").strip())
        )
        strengths = narrative.get("top_fortalezas") if isinstance(narrative.get("top_fortalezas"), list) else []
        problems = _normalize_render_problem_items(
            narrative.get("top_problemas") if isinstance(narrative.get("top_problemas"), list) else []
        )

        def _negative_ratio_local(metrics: list[dict[str, Any]]) -> float:
            if not metrics:
                return 0.0
            negative_count = 0
            for item in metrics:
                if not isinstance(item, dict):
                    continue
                rating_value = r._safe_float(item.get("rating"))
                if rating_value <= 2.0 and rating_value > 0:
                    negative_count += 1
            return negative_count / max(1, len(metrics))

        def _average_dimension_local(metrics: list[dict[str, Any]], key: str) -> float:
            values: list[float] = []
            for item in metrics:
                if not isinstance(item, dict):
                    continue
                dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
                values.append(r._safe_float(dims.get(key)))
            if not values:
                return 0.0
            return sum(values) / len(values)

        avg_rating = r._safe_float(stats.get("avg_rating"))
        response_rate = r._safe_float(stats.get("response_rate"))
        negative_ratio = r._safe_float(_negative_ratio_local(review_metrics))
        sentiment_avg = r._safe_float(_average_dimension_local(review_metrics, "sentiment"))
        top_problem = ""
        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters.get("clusters"), list) else []
        if clusters:
            top_problem = r._humanize_action_text(str((clusters[0] or {}).get("problem", "") or "").strip())

        metric_cards = (
            "<div class='metric-grid'>"
            + "".join(
                [
                    "<article class='metric-card'>"
                    "<div class='metric-title'>Valoración media</div>"
                    f"<div class='metric-value'>{avg_rating:.2f} / 5</div>"
                    "</article>",
                    "<article class='metric-card'>"
                    "<div class='metric-title'>Tasa de respuesta</div>"
                    f"<div class='metric-value'>{response_rate * 100:.1f}%</div>"
                    "</article>",
                    "<article class='metric-card'>"
                    "<div class='metric-title'>Reseñas negativas</div>"
                    f"<div class='metric-value'>{negative_ratio * 100:.1f}%</div>"
                    "</article>",
                    "<article class='metric-card'>"
                    "<div class='metric-title'>Sentimiento medio</div>"
                    f"<div class='metric-value'>{sentiment_avg:.2f}</div>"
                    "</article>",
                ]
            )
            + "</div>"
        )
        strengths_html = (
            "<ul>" + "".join(f"<li>{html.escape(r._clean_narrative_text(str(item)))}</li>" for item in strengths[:3]) + "</ul>"
            if strengths
            else ""
        )
        problems_html = (
            "<ul>" + "".join(f"<li>{html.escape(r._clean_narrative_text(str(item)))}</li>" for item in problems[:3]) + "</ul>"
            if problems
            else ""
        )
        blocks = []
        if main_text:
            blocks.append(f"<p>{html.escape(main_text)}</p>")
        blocks.append(metric_cards)
        if top_problem:
            blocks.append(f"<p><strong>Tema más repetido:</strong> {html.escape(top_problem)}</p>")
        if strengths_html:
            blocks.append("<h3>Fortalezas detectadas</h3>")
            blocks.append(strengths_html)
        if problems_html:
            blocks.append("<h3>Problemas detectados</h3>")
            blocks.append(problems_html)
        if note_text:
            blocks.append(f"<p class='muted'><strong>Contexto de la fuente:</strong> {html.escape(note_text)}</p>")
        return "".join(blocks)


class SourceComparisonSectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        if not isinstance(payload, dict):
            return ""
        narrative = r._clean_narrative_text(str(payload.get("narrativa_comparacion", "") or "").strip())
        coincidences = payload.get("coincidencias") if isinstance(payload.get("coincidencias"), list) else []
        divergences = payload.get("divergencias") if isinstance(payload.get("divergencias"), list) else []
        recommendations = (
            payload.get("recomendaciones") if isinstance(payload.get("recomendaciones"), list) else []
        )
        harder_source = str(payload.get("fuente_mas_dura", "") or "").strip().lower()
        harder_label = {
            "google_maps": "Google Maps",
            "tripadvisor": "Tripadvisor",
            "similar": "Similar en dureza",
        }.get(harder_source, "Sin diferencia clara")
        explanation = r._clean_narrative_text(str(payload.get("explicacion_diferencia", "") or "").strip())

        google_data = payload.get("google_data") if isinstance(payload.get("google_data"), dict) else {}
        trip_data = payload.get("tripadvisor_data") if isinstance(payload.get("tripadvisor_data"), dict) else {}
        google_stats = google_data.get("stats") if isinstance(google_data.get("stats"), dict) else {}
        trip_stats = trip_data.get("stats") if isinstance(trip_data.get("stats"), dict) else {}
        google_metrics = google_data.get("review_metrics") if isinstance(google_data.get("review_metrics"), list) else []
        trip_metrics = trip_data.get("review_metrics") if isinstance(trip_data.get("review_metrics"), list) else []

        def _negative_ratio_local(metrics: list[dict[str, Any]]) -> float:
            if not metrics:
                return 0.0
            negative_count = 0
            for item in metrics:
                if not isinstance(item, dict):
                    continue
                rating_value = r._safe_float(item.get("rating"))
                if rating_value <= 2.0 and rating_value > 0:
                    negative_count += 1
            return negative_count / max(1, len(metrics))

        def _average_dimension_local(metrics: list[dict[str, Any]], key: str) -> float:
            values: list[float] = []
            for item in metrics:
                if not isinstance(item, dict):
                    continue
                dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
                values.append(r._safe_float(dims.get(key)))
            if not values:
                return 0.0
            return sum(values) / len(values)

        def _top_problem_text(source_payload: dict[str, Any]) -> str:
            clusters = (
                source_payload.get("problem_clusters", {}).get("clusters")
                if isinstance(source_payload.get("problem_clusters"), dict)
                else []
            )
            if not isinstance(clusters, list) or not clusters:
                return "—"
            return r._humanize_action_text(str((clusters[0] or {}).get("problem", "") or "").strip()) or "—"

        table_html = (
            "<table><thead><tr><th>Métrica</th><th>Google Maps</th><th>Tripadvisor</th></tr></thead><tbody>"
            + f"<tr><th>Valoración media</th><td>{r._safe_float(google_stats.get('avg_rating')):.2f}/5</td>"
            + f"<td>{r._safe_float(trip_stats.get('avg_rating')):.2f}/5</td></tr>"
            + f"<tr><th>Reseñas negativas</th><td>{_negative_ratio_local(google_metrics) * 100:.1f}%</td>"
            + f"<td>{_negative_ratio_local(trip_metrics) * 100:.1f}%</td></tr>"
            + f"<tr><th>Sentimiento medio</th><td>{_average_dimension_local(google_metrics, 'sentiment'):.2f}</td>"
            + f"<td>{_average_dimension_local(trip_metrics, 'sentiment'):.2f}</td></tr>"
            + f"<tr><th>Tema principal</th><td>{html.escape(_top_problem_text(google_data))}</td>"
            + f"<td>{html.escape(_top_problem_text(trip_data))}</td></tr>"
            + "</tbody></table>"
        )

        parts = []
        if narrative:
            parts.append(f"<p>{html.escape(narrative)}</p>")
        parts.append(table_html)
        parts.append(
            f"<p><strong>Fuente más dura:</strong> {html.escape(harder_label)}"
            + (f" · {html.escape(explanation)}" if explanation else "")
            + "</p>"
        )
        if coincidences:
            parts.append("<h3>Coincidencias</h3>")
            parts.append(
                "<ul>"
                + "".join(
                    f"<li>{html.escape(r._clean_narrative_text(str(item or '')))}</li>"
                    for item in coincidences[:4]
                    if str(item or "").strip()
                )
                + "</ul>"
            )
        if divergences:
            parts.append("<h3>Divergencias</h3>")
            parts.append(
                "<ul>"
                + "".join(
                    f"<li>{html.escape(r._clean_narrative_text(str(item or '')))}</li>"
                    for item in divergences[:4]
                    if str(item or "").strip()
                )
                + "</ul>"
            )
        if recommendations:
            parts.append("<h3>Recomendaciones priorizadas</h3>")
            parts.append(
                "<ol>"
                + "".join(
                    f"<li>{html.escape(r._clean_narrative_text(str(item or '')))}</li>"
                    for item in recommendations[:4]
                    if str(item or "").strip()
                )
                + "</ol>"
            )
        return "".join(parts)


def _normalize_render_problem_items(items: list[Any]) -> list[str]:
    generic_keys = {
        "experiencia general",
        "experiencia del cliente",
        "general",
    }
    cleaned_items = [str(item or "").strip(" .,:;") for item in items if str(item or "").strip(" .,:;")]
    has_specific = any(item.casefold() not in generic_keys for item in cleaned_items)

    normalized: list[str] = []
    seen: set[str] = set()
    generic_seen = False
    for cleaned in cleaned_items:
        key = cleaned.casefold()
        is_generic = key in generic_keys
        if has_specific and is_generic:
            continue
        if key in seen:
            continue
        if is_generic and generic_seen:
            continue
        seen.add(key)
        if is_generic:
            generic_seen = True
        normalized.append(cleaned)
    return normalized[:3]


def _normalize_source_note(value: str) -> str:
    cleaned = str(value or "").strip(" .,:;")
    if len(cleaned) < 12:
        return ""
    if len(cleaned.split()) < 3:
        return ""
    return cleaned
