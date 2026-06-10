from __future__ import annotations

import html
from typing import Any


def render_payload(renderer: Any, payload: Any, *, depth: int = 0) -> str:
    if payload is None:
        return ""

    if isinstance(payload, (str, int, float, bool)):
        text = renderer._clean_narrative_text(str(payload))
        return f"<p>{html.escape(text)}</p>" if text else ""

    if isinstance(payload, list):
        return _render_list_payload(renderer, payload, depth=depth)

    if isinstance(payload, dict):
        return _render_dict_payload(renderer, payload, depth=depth)

    text = renderer._clean_narrative_text(str(payload))
    return f"<p>{html.escape(text)}</p>" if text else ""


def _render_list_payload(renderer: Any, payload: list[Any], *, depth: int) -> str:
    if not payload:
        return ""
    if all(isinstance(item, (str, int, float, bool)) for item in payload):
        items = "".join(
            f"<li>{html.escape(renderer._clean_narrative_text(str(item)))}</li>"
            for item in payload
            if renderer._clean_narrative_text(str(item))
        )
        if not items:
            return ""
        return f"<ul>{items}</ul>"
    rows = []
    for item in payload:
        rendered_item = render_payload(renderer, item, depth=depth + 1)
        if rendered_item.strip():
            rows.append(f"<li>{rendered_item}</li>")
    if not rows:
        return ""
    return f"<ul>{''.join(rows)}</ul>"


def _render_dict_payload(renderer: Any, payload: dict[str, Any], *, depth: int) -> str:
    scatter_html = renderer._maybe_render_scatter_svg(payload)
    if scatter_html:
        return scatter_html

    payload_to_render = dict(payload)
    _normalize_target_rank_summary(renderer, payload_to_render)

    scalar_rows = []
    nested_rows = []
    hidden_keys = {"analysis_id", "dataset_id", "trend_slope", "sentiment_score"}
    for key, value in payload_to_render.items():
        if str(key).strip().lower() in hidden_keys:
            continue
        key_label = html.escape(renderer._labelize_key_spanish(str(key)))
        if isinstance(value, (str, int, float, bool)) or value is None:
            scalar_rows.append(
                f"<tr><th>{key_label}</th><td>{_render_scalar_value(renderer, key, value)}</td></tr>"
            )
        else:
            rendered_nested = render_payload(renderer, value, depth=depth + 1)
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


def _normalize_target_rank_summary(renderer: Any, payload_to_render: dict[str, Any]) -> None:
    rank_value = renderer._safe_int(payload_to_render.get("target_rank"))
    competitors_compared = renderer._safe_int(payload_to_render.get("total_competitors_compared"))
    total_businesses_compared = renderer._safe_int(payload_to_render.get("total_businesses_compared"))
    if rank_value > 0 and competitors_compared > 0 and total_businesses_compared <= 0:
        total_businesses_compared = competitors_compared + 1
    if rank_value > 0 and total_businesses_compared > 0:
        payload_to_render["target_rank"] = (
            f"{rank_value} de {total_businesses_compared} negocios similares analizados"
        )
        payload_to_render.pop("total_competitors_compared", None)
        payload_to_render.pop("total_businesses_compared", None)


def _render_scalar_value(renderer: Any, key: Any, value: Any) -> str:
    if isinstance(value, bool):
        return "Sí" if value else "No"
    if value is None:
        return "—"

    rendered_raw = str(value)
    lower_key = str(key).strip().lower()
    if lower_key in {"created_at", "generated_at", "report_generated_at", "preview_report_generated_at"}:
        rendered_raw = renderer._format_human_date(rendered_raw)
    elif lower_key == "target_reputation_score":
        try:
            rendered_raw = f"{float(rendered_raw):.1f}/100"
        except (TypeError, ValueError):
            rendered_raw = "—"
    elif lower_key == "overall_sentiment":
        rendered_raw = renderer._humanize_sentiment_value(rendered_raw)
    elif lower_key == "trend":
        rendered_raw = renderer._humanize_trend_value(rendered_raw)

    rendered_value = html.escape(renderer._clean_narrative_text(rendered_raw))
    return rendered_value or "—"
