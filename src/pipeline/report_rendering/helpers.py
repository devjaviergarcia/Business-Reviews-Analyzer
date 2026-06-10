from __future__ import annotations

from typing import Any

from src.pipeline.report_rendering.metric_badge_formatter import (
    action_type_badge,
    humanize_action_type_label,
    humanize_effort,
    humanize_impact,
    metric_context_label,
    score_badge,
    severity_band,
)
from src.pipeline.report_rendering.narrative_text_formatter import (
    anonymize_person_name,
    clean_narrative_text,
    humanize_action_text,
    humanize_sentiment_value,
    humanize_trend_value,
    icon_slot,
    labelize_key_spanish,
    source_name_spanish,
)
from src.pipeline.report_rendering.rendering_value_helpers import (
    format_human_date,
    infer_action_tool_from_text,
    infer_action_type_from_text,
    is_empty_payload,
    json_default,
    normalize_text,
    safe_float,
    safe_identifier_slug,
    safe_int,
    safe_name_slug,
    slugify,
)


class ReportRenderingHelpersMixin:
    def _clean_narrative_text(self, value: str) -> str:
        return clean_narrative_text(value, humanize_action_text=self._humanize_action_text)

    def _is_empty_payload(self, payload: Any) -> bool:
        return is_empty_payload(payload)

    def _anonymize_person_name(self, name: str) -> str:
        return anonymize_person_name(name)

    def _source_name_spanish(self, source: str) -> str:
        return source_name_spanish(source)

    def _icon_slot(self, icon_name: str) -> str:
        return icon_slot(icon_name)

    def _labelize_key_spanish(self, key: str) -> str:
        return labelize_key_spanish(key)

    def _humanize_action_text(self, text: str) -> str:
        return humanize_action_text(text)

    def _humanize_sentiment_value(self, value: str) -> str:
        return humanize_sentiment_value(value, normalize_text=self._normalize_text)

    def _humanize_trend_value(self, value: str) -> str:
        return humanize_trend_value(value, normalize_text=self._normalize_text)

    def _metric_context_label(self, key: str, value: float) -> str:
        return metric_context_label(key, value, normalize_text=self._normalize_text)

    def _severity_band(self, value: float) -> str:
        return severity_band(value)

    def _humanize_effort(self, *, effort: str) -> str:
        return humanize_effort(effort=effort)

    def _humanize_impact(self, *, impact: str) -> str:
        return humanize_impact(impact=impact)

    def _humanize_role(self, role: str) -> str:
        value = str(role or "").strip()
        if not value:
            return ""
        return self._humanize_action_text(value)

    def _humanize_action_type_label(self, action_type: str) -> str:
        return humanize_action_type_label(action_type)

    def _action_type_badge(self, action_type: str) -> dict[str, str]:
        return action_type_badge(action_type)

    def _normalize_text(self, value: str) -> str:
        return normalize_text(value)

    def _format_human_date(self, value: str) -> str:
        return format_human_date(value)

    def _infer_action_type_from_text(self, text: str) -> str:
        return infer_action_type_from_text(text, normalize_text=self._normalize_text)

    def _infer_action_tool_from_text(self, text: str) -> str:
        return infer_action_tool_from_text(text, normalize_text=self._normalize_text)

    def _score_badge(self, score: float) -> str:
        return score_badge(score)

    def _safe_float(self, value: Any) -> float:
        return safe_float(value)

    def _safe_int(self, value: Any) -> int:
        return safe_int(value)

    def _slugify(self, value: str) -> str:
        return slugify(value)

    def _safe_identifier_slug(self, value: str) -> str:
        return safe_identifier_slug(value)

    def _safe_name_slug(self, value: str) -> str:
        return safe_name_slug(value)

    def _json_default(self, value: object) -> str:
        return json_default(value)
