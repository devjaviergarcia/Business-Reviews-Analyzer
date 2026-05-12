from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any


class ReportRenderingHelpersMixin:
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
