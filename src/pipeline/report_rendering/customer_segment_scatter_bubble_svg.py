from __future__ import annotations

import html
import math


def render_customer_segment_bubble(
    *,
    font: str,
    bubble: dict,
    cx_raw: float,
    cy_raw: float,
    radius_raw: float,
    text_bottom_limit: float,
    safe_float,
) -> list[str]:
    color = str(bubble.get("color", "#0A7567") or "#0A7567")
    label_value = str(bubble.get("label", "") or "Segmento").strip()
    weight_pct = safe_float(bubble.get("weight_pct"))
    cx = round(cx_raw, 1)
    cy = round(cy_raw, 1)
    r = round(max(1.0, radius_raw), 1)
    meta_value = f"{weight_pct:.1f}%"
    max_text_width = max(20.0, (2.0 * r) - 14.0)

    def fit_font(
        text: str,
        base_font: float,
        min_font: float,
        factor: float,
        available_width: float,
    ) -> float:
        clean = text.strip() or "-"
        if not clean:
            return min_font
        max_font_by_width = available_width / (len(clean) * factor)
        return max(min_font, min(base_font, max_font_by_width))

    label_font = 11.0
    meta_font = 10.0
    label_min_font = 7.5
    meta_min_font = 7.0
    label_factor = 0.56
    meta_factor = 0.54

    circle_area = math.pi * (r**2)
    full_inside_area_x = 4200.0
    percent_inside_only_area_y = 1400.0
    svg_parts: list[str] = []

    if circle_area >= full_inside_area_x:
        label_candidate = label_value.strip() or "Segmento"
        label_font = fit_font(label_candidate, label_font, 6.4, label_factor, max_text_width)
        meta_font = fit_font(meta_value, meta_font, meta_min_font, meta_factor, max_text_width)

        inner_height = max(14.0, (2.0 * r) - 10.0)
        row_gap = 4.0
        total_height = label_font + meta_font + row_gap
        if total_height > inner_height:
            scale = inner_height / total_height
            label_font = max(label_min_font, label_font * scale)
            meta_font = max(meta_min_font, meta_font * scale)
            total_height = label_font + meta_font + row_gap
            if total_height > inner_height:
                row_gap = max(2.0, inner_height - label_font - meta_font)

        line1_y = cy - ((meta_font / 2.0) + (row_gap / 2.0))
        line2_y = cy + ((label_font / 2.0) + (row_gap / 2.0))
        label_text = html.escape(label_candidate)

        svg_parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>'
        )
        svg_parts.append(
            f'<text x="{cx}" y="{line1_y}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{label_font:.1f}" font-weight="700" fill="{color}">{label_text}</text>'
        )
        svg_parts.append(
            f'<text x="{cx}" y="{line2_y}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{meta_font:.1f}" fill="{color}" opacity="0.9">{html.escape(meta_value)}</text>'
        )
        return svg_parts

    if circle_area >= percent_inside_only_area_y:
        inside_text = f"{weight_pct:.1f}%"
        inside_font = fit_font(inside_text, 10.0, meta_min_font, meta_factor, max_text_width)
        outside_width = max(74.0, (2.0 * r) + 50.0, len(label_value) * 6.0)
        outside_label = label_value.strip() or "Segmento"
        outside_font = fit_font(outside_label, 10.5, 7.2, label_factor, outside_width)
        outside_y = cy + r + max(10.0, outside_font * 0.95)
        outside_y = min(text_bottom_limit, outside_y)

        svg_parts.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>'
        )
        svg_parts.append(
            f'<text x="{cx}" y="{outside_y}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{outside_font:.1f}" font-weight="700" fill="{color}">{html.escape(outside_label)}</text>'
        )
        svg_parts.append(
            f'<text x="{cx}" y="{cy}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{inside_font:.1f}" fill="{color}" opacity="0.95">{html.escape(inside_text)}</text>'
        )
        return svg_parts

    outside_width = max(82.0, (2.0 * r) + 56.0, len(label_value) * 6.6)
    outside_label = label_value.strip() or "Segmento"
    outside_font = fit_font(outside_label, 10.5, 7.2, label_factor, outside_width)
    outside_meta_font = fit_font(meta_value, 9.8, 7.5, meta_factor, outside_width)
    line_gap = max(3.0, outside_meta_font * 0.5)
    line1_y = cy + r + max(10.0, outside_font * 0.95)
    line2_y = line1_y + outside_font + line_gap
    if line2_y > text_bottom_limit:
        shift_up = line2_y - text_bottom_limit
        line1_y -= shift_up
        line2_y -= shift_up
    min_line1_from_circle = cy + r + max(6.0, outside_font * 0.55)
    if line1_y < min_line1_from_circle:
        shift_down = min_line1_from_circle - line1_y
        line1_y += shift_down
        line2_y += shift_down

    svg_parts.append(
        f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>'
    )
    svg_parts.append(
        f'<text x="{cx}" y="{line1_y}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{outside_font:.1f}" font-weight="700" fill="{color}">{html.escape(outside_label)}</text>'
    )
    svg_parts.append(
        f'<text x="{cx}" y="{line2_y}" text-anchor="middle" dominant-baseline="middle" font-family="{font}" font-size="{outside_meta_font:.1f}" fill="{color}" opacity="0.92">{html.escape(meta_value)}</text>'
    )
    return svg_parts
