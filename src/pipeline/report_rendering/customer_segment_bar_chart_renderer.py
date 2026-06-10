from __future__ import annotations

import html
from typing import Any


def render_customer_segment_bar_chart(renderer: Any, bar_chart_data: dict[str, Any]) -> str:
    rows = bar_chart_data.get("rows") if isinstance(bar_chart_data, dict) else []
    if not isinstance(rows, list) or not rows:
        return ""

    svg_w = 860
    row_h = 52
    header_h = 28
    pad_l = 10
    bar_max_w = 440
    col_sat = 560
    col_sent = 680
    col_pct = 780
    total_h = header_h + len(rows) * row_h + 10
    font = "Plus Jakarta Sans, sans-serif"

    svg_parts: list[str] = [
        f'<svg viewBox="0 0 {svg_w} {total_h}" width="100%" style="display:block;">',
        f'<text x="{pad_l}" y="20" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Segmento de cliente</text>',
        f'<text x="{col_sat}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Satisfacción</text>',
        f'<text x="{col_sent}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Sentimiento</text>',
        f'<text x="{col_pct}" y="20" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="#64748B">Peso</text>',
        f'<line x1="{pad_l}" y1="25" x2="{svg_w - 10}" y2="25" stroke="#E2DFD6" stroke-width="0.8"/>',
    ]

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        y_base = header_h + idx * row_h
        color = str(row.get("color", "#0A7567") or "#0A7567")
        label = html.escape(str(row.get("label", "") or "Segmento"))
        count = renderer._safe_int(row.get("count_reviews"))
        weight_pct = renderer._safe_float(row.get("weight_pct"))
        bar_w = max(4, round((weight_pct / 100.0) * bar_max_w))
        sat_label = html.escape(str(row.get("satisfaction_label", "") or ""))
        sat_pct = renderer._safe_float(row.get("satisfaction_pct"))
        sent_label = html.escape(str(row.get("sentiment_label", "") or ""))
        sentiment = renderer._safe_float(row.get("sentiment"))
        sent_sign = "+" if sentiment >= 0 else ""

        if idx > 0:
            svg_parts.append(
                f'<line x1="{pad_l}" y1="{y_base}" x2="{svg_w - 10}" y2="{y_base}" stroke="#E2DFD6" stroke-width="0.5"/>'
            )

        svg_parts.append(
            f'<text x="{pad_l}" y="{y_base + 18}" font-family="{font}" font-size="13" font-weight="700" fill="{color}">{label}</text>'
        )
        svg_parts.append(
            f'<text x="{pad_l}" y="{y_base + 33}" font-family="{font}" font-size="11" fill="#64748B">{count} reseñas</text>'
        )
        svg_parts.append(
            f'<rect x="{pad_l}" y="{y_base + 37}" width="{bar_w}" height="10" rx="5" fill="{color}" fill-opacity="0.85"/>'
        )
        svg_parts.append(
            f'<text x="{pad_l + bar_w + 6}" y="{y_base + 47}" font-family="{font}" font-size="10" fill="{color}" font-weight="600">{weight_pct:.1f}%</text>'
        )

        svg_parts.append(
            f'<circle cx="{col_sat}" cy="{y_base + 25}" r="21" fill="{color}" fill-opacity="0.12" stroke="{color}" stroke-width="1.5"/>'
        )
        svg_parts.append(
            f'<text x="{col_sat}" y="{y_base + 22}" text-anchor="middle" font-family="{font}" font-size="10" font-weight="700" fill="{color}">{sat_pct:.1f}%</text>'
        )
        svg_parts.append(
            f'<text x="{col_sat}" y="{y_base + 33}" text-anchor="middle" font-family="{font}" font-size="9" fill="{color}" opacity="0.8">{sat_label}</text>'
        )

        svg_parts.append(
            f'<text x="{col_sent}" y="{y_base + 23}" text-anchor="middle" font-family="{font}" font-size="14" font-weight="700" fill="{color}">{sent_sign}{sentiment:.2f}</text>'
        )
        svg_parts.append(
            f'<text x="{col_sent}" y="{y_base + 36}" text-anchor="middle" font-family="{font}" font-size="10" fill="{color}" opacity="0.8">{sent_label}</text>'
        )

        bubble_r = max(4, round(4 + (weight_pct / 100.0) * 22))
        svg_parts.append(
            f'<circle cx="{col_pct}" cy="{y_base + 25}" r="{bubble_r}" fill="{color}" fill-opacity="0.85"/>'
        )
        if weight_pct >= 5:
            svg_parts.append(
                f'<text x="{col_pct}" y="{y_base + 29}" text-anchor="middle" font-family="{font}" font-size="9" fill="#FFFFFF" font-weight="700">{weight_pct:.1f}%</text>'
            )

    svg_parts.append("</svg>")
    return "<div class='bar-chart-wrap'>" + "\n".join(svg_parts) + "</div>"


def render_customer_weight_summary_chart(renderer: Any, scatter_payload: dict[str, Any]) -> str:
    circles = scatter_payload.get("circles") if isinstance(scatter_payload, dict) else []
    if not isinstance(circles, list) or not circles:
        return ""
    total = sum(renderer._safe_int(item.get("count")) for item in circles if isinstance(item, dict))
    if total <= 0:
        return ""

    width = 640
    label_width = 180
    bar_max = width - label_width - 90
    row_h = 32
    gap = 10
    colors = ["#0A7567", "#12B08A", "#D4F0E8", "#D4950A", "#C23B18"]
    rows: list[str] = []
    visible = circles[:5]
    for idx, item in enumerate(visible):
        if not isinstance(item, dict):
            continue
        label = str(item.get("label", "") or f"Segmento {idx + 1}").strip()[:30]
        count = renderer._safe_int(item.get("count"))
        pct = count / max(1, total)
        bar_w = max(1, int(round(pct * bar_max)))
        y = idx * (row_h + gap)
        color = colors[idx % len(colors)]
        rows.append(
            f"<text x='0' y='{y + 20}' fill='#161616' font-size='12' font-family='Plus Jakarta Sans,sans-serif'>{html.escape(label)}</text>"
            f"<rect x='{label_width}' y='{y}' width='{bar_w}' height='{row_h}' rx='6' fill='{color}' opacity='0.85'/>"
            f"<text x='{label_width + bar_w + 6}' y='{y + 20}' fill='#64748B' font-size='11' font-family='Plus Jakarta Sans,sans-serif'>{int(round(pct * 100))}% ({count})</text>"
        )
    total_h = max(44, len(visible) * (row_h + gap))
    return (
        "<div class='bar-chart-wrap'>"
        f"<svg viewBox='0 0 {width} {total_h}' width='100%' height='{total_h}'>"
        f"{''.join(rows)}"
        "</svg>"
        "</div>"
    )
