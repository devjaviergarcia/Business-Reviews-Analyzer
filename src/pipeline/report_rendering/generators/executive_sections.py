from __future__ import annotations

import html
from typing import Any

from .base_section_generator import _BaseSectionGenerator


class ExecutiveSummarySectionGenerator(_BaseSectionGenerator):
    def render(self, payload: dict[str, Any]) -> str:
        r = self.renderer
        diagnostico = r._clean_narrative_text(str(payload.get("diagnostico", "") or "").strip())
        estado = payload.get("estado_actual") if isinstance(payload.get("estado_actual"), dict) else {}
        source_availability = (
            payload.get("source_availability")
            if isinstance(payload.get("source_availability"), dict)
            else {}
        )
        tripadvisor_availability = (
            source_availability.get("tripadvisor")
            if isinstance(source_availability.get("tripadvisor"), dict)
            else None
        )
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
        if tripadvisor_availability:
            flag_text = str(tripadvisor_availability.get("flag") or "").strip() or "Estado Tripadvisor no disponible"
            pills.append(f"<div class='pill'><strong>Tripadvisor:</strong> {html.escape(flag_text)}</div>")
        parts = [
            f"<p>{html.escape(diagnostico)}</p>" if diagnostico else "",
            f"<p>{score_badge}</p>",
            f"<div class='pill-grid'>{''.join(pills)}</div>",
        ]
        if tripadvisor_availability:
            detail = r._clean_narrative_text(str(tripadvisor_availability.get("detail", "") or "").strip())
            if detail:
                parts.append(f"<p class='muted'><strong>Fuente Tripadvisor:</strong> {html.escape(detail)}</p>")
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
        score_drivers_html = _render_score_drivers(renderer=r, components=componentes)
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
            f"{score_drivers_html}"
            "</div>"
            "</div>"
            + ("<h3>Evolución y tendencia</h3>" + evolucion_html if evolucion_html else "")
        )


def _render_score_drivers(*, renderer: Any, components: Any) -> str:
    if not isinstance(components, dict):
        return ""

    strengths: list[str] = []
    risks: list[str] = []

    avg_rating = renderer._safe_float(components.get("avg_rating"))
    if avg_rating >= 4.4:
        strengths.append(f"La valoración media está en {avg_rating:.2f}/5, un nivel competitivo.")
    elif avg_rating < 4.2:
        risks.append(f"La valoración media cae a {avg_rating:.2f}/5, por debajo del umbral de confianza.")

    response_rate = renderer._safe_float(components.get("response_rate"))
    if response_rate >= 0.45:
        strengths.append(f"Se responde aproximadamente al {response_rate * 100:.1f}% de las reseñas.")
    elif response_rate < 0.20:
        risks.append(f"La tasa de respuesta es baja ({response_rate * 100:.1f}%).")

    negative_ratio = renderer._safe_float(components.get("negative_ratio"))
    if 0.0 <= negative_ratio <= 0.12:
        strengths.append(f"El peso de reseñas negativas es bajo ({negative_ratio * 100:.1f}%).")
    elif negative_ratio >= 0.25:
        risks.append(f"Hay demasiada fricción visible en reseñas negativas ({negative_ratio * 100:.1f}%).")

    sentiment_avg = renderer._safe_float(components.get("sentiment_avg"))
    if sentiment_avg >= 0.20:
        strengths.append(f"El sentimiento medio es favorable ({sentiment_avg:.2f}).")
    elif sentiment_avg <= -0.05:
        risks.append(f"El tono medio de las reseñas empuja hacia abajo ({sentiment_avg:.2f}).")

    tranquility_avg = renderer._safe_float(components.get("tranquility_avg"))
    if tranquility_avg >= 0.15:
        strengths.append(f"El tono general se percibe calmado y estable ({tranquility_avg:.2f}).")
    elif tranquility_avg <= -0.05:
        risks.append(f"Las reseñas transmiten tensión o fricción operativa ({tranquility_avg:.2f}).")

    if not strengths and not risks:
        return ""

    strengths_html = (
        "".join(
            "<article class='fw-card fw-strong'>"
            f"<div class='fw-icon'>{renderer._icon_slot('strength')}</div>"
            "<div>"
            f"<div class='fw-title'>{html.escape(text)}</div>"
            "</div>"
            "</article>"
            for text in strengths[:4]
        )
        if strengths
        else "<p class='muted'>No se detectan palancas fuertes adicionales en este corte.</p>"
    )
    risks_html = (
        "".join(
            "<article class='fw-card fw-weak'>"
            f"<div class='fw-icon'>{renderer._icon_slot('warning')}</div>"
            "<div>"
            f"<div class='fw-title'>{html.escape(text)}</div>"
            "</div>"
            "</article>"
            for text in risks[:4]
        )
        if risks
        else "<p class='muted'>No aparece un freno claro en los componentes actuales.</p>"
    )

    return (
        "<h3>Por qué sale esta puntuación</h3>"
        "<div class='fw-grid'>"
        "<div>"
        "<div class='fw-col-title fw-col-strong'>Qué empuja el score</div>"
        f"{strengths_html}"
        "</div>"
        "<div>"
        "<div class='fw-col-title fw-col-weak'>Qué hoy lo frena</div>"
        f"{risks_html}"
        "</div>"
        "</div>"
    )
