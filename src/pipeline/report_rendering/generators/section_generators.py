from __future__ import annotations

import html
from typing import Any


class _BaseSectionGenerator:
    def __init__(self, renderer: Any) -> None:
        self.renderer = renderer


class ExecutiveSummarySectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        diagnostico = r._clean_narrative_text(str(payload.get("diagnostico", "") or "").strip())
        estado = payload.get("estado_actual") if isinstance(payload.get("estado_actual"), dict) else {}
        aciertos = payload.get("aciertos_notorios") if isinstance(payload.get("aciertos_notorios"), list) else []
        aciertos_estructurados = (
            payload.get("aciertos_estructurados")
            if isinstance(payload.get("aciertos_estructurados"), list)
            else []
        )
        score = r._safe_float(estado.get("score_reputacion"))
        score_badge = r._score_badge(score)
        pills = [
            f"<div class='pill'><strong>Puntuación:</strong> {round(score, 1)}/100</div>",
            f"<div class='pill'><strong>Nivel:</strong> {html.escape(str(estado.get('nivel_reputacion', '') or ''))}</div>",
            f"<div class='pill'><strong>Tipos de cliente detectados:</strong> {r._safe_int(estado.get('cluster_count'))}</div>",
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
                    f"<div class='fw-icon'>{r._icon_slot('strength')}</div>"
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


class ReputationScoreSectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        display = str(payload.get("score_display", "") or "").strip() or "0/100"
        score_value = r._safe_float(payload.get("score_value"))
        label = str(payload.get("nivel_reputacion", "") or "").strip()
        explicacion = r._clean_narrative_text(str(payload.get("explicacion", "") or "").strip())
        componentes = payload.get("componentes_numericos")
        evolucion = payload.get("evolucion")
        components_html = r._render_score_components(componentes)
        evolucion_html = r._render_payload(evolucion) if not r._is_empty_payload(evolucion) else ""

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


class SourceNarrativeSectionGenerator(_BaseSectionGenerator):
    def __init__(self, renderer: Any, *, source_key: str) -> None:
        super().__init__(renderer)
        self._source_key = source_key

    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        if not isinstance(payload, dict):
            return ""
        narrative = payload.get("narrativa") if isinstance(payload.get("narrativa"), dict) else {}
        stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
        problem_clusters = (
            payload.get("problem_clusters")
            if isinstance(payload.get("problem_clusters"), dict)
            else {}
        )
        review_metrics = payload.get("review_metrics") if isinstance(payload.get("review_metrics"), list) else []
        main_text = r._clean_narrative_text(str(narrative.get("narrativa", "") or "").strip())
        note_text = r._clean_narrative_text(str(narrative.get("nota_sesgo", "") or "").strip())
        strengths = narrative.get("top_fortalezas") if isinstance(narrative.get("top_fortalezas"), list) else []
        problems = narrative.get("top_problemas") if isinstance(narrative.get("top_problemas"), list) else []

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


def build_section_generators(renderer: Any) -> dict[str, _BaseSectionGenerator]:
    return {
        "1_resumen_ejecutivo": ExecutiveSummarySectionGenerator(renderer),
        "2_score_reputacion": ReputationScoreSectionGenerator(renderer),
        "3_quien_es_tu_cliente_y_que_le_preocupa": CustomerProfileSectionGenerator(renderer),
        "4_lectura_fuente_google_maps": SourceNarrativeSectionGenerator(renderer, source_key="google_maps"),
        "5_lectura_fuente_tripadvisor": SourceNarrativeSectionGenerator(renderer, source_key="tripadvisor"),
        "4_plan_de_accion": ActionPlanSectionGenerator(renderer),
        "7_comparativa_fuentes": SourceComparisonSectionGenerator(renderer),
        "5_anexos_resumen": AnnexSummarySectionGenerator(renderer),
    }
