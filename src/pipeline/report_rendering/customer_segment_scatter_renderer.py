from __future__ import annotations

import html
from typing import Any

from src.pipeline.report_rendering.customer_segment_scatter_bubble_svg import (
    render_customer_segment_bubble,
)
from src.pipeline.report_rendering.customer_segment_scatter_layout import (
    build_scatter_canvas_layout,
    collect_scatter_slots,
    group_scatter_bubbles,
    place_scatter_bubbles,
)


def render_customer_segment_scatter(renderer: Any, scatter_data: dict[str, Any]) -> str:
    bubbles = scatter_data.get("bubbles") if isinstance(scatter_data, dict) else []
    if not isinstance(bubbles, list) or not bubbles:
        return ""

    base_svg_h = 430
    base_pad_b = 76
    svg_w = 900
    pad_l, pad_r, pad_t = 72, 40, 28
    font = "Plus Jakarta Sans, sans-serif"
    axes = scatter_data.get("axes") if isinstance(scatter_data.get("axes"), dict) else {}
    quadrant_labels = (
        scatter_data.get("quadrant_labels")
        if isinstance(scatter_data.get("quadrant_labels"), dict)
        else {}
    )

    zone_bg: dict[str, str] = {
        "top_left": "#0A7567",
        "top_right": "#D4950A",
        "bottom_left": "#8B95A5",
        "bottom_right": "#C23B18",
    }
    zone_order = ["top_left", "top_right", "bottom_left", "bottom_right"]
    grouped = group_scatter_bubbles(renderer, bubbles=bubbles, zone_order=zone_order)
    layout = build_scatter_canvas_layout(
        grouped=grouped,
        zone_order=zone_order,
        base_svg_h=base_svg_h,
        base_pad_b=base_pad_b,
        svg_w=svg_w,
        pad_l=pad_l,
        pad_r=pad_r,
        pad_t=pad_t,
    )
    svg_h = layout["svg_h"]
    pad_b = layout["pad_b"]
    plot_h = layout["plot_h"]
    plot_w = layout["plot_w"]
    x_mid = layout["x_mid"]
    y_mid = layout["y_mid"]
    zone_rects = layout["zone_rects"]
    zone_growth_map = layout["zone_growth_map"]
    placed_slots = collect_scatter_slots(
        grouped=grouped,
        zone_rects=zone_rects,
        zone_growth_map=zone_growth_map,
        zone_order=zone_order,
    )
    placed_bubbles = place_scatter_bubbles(renderer, placed_slots=placed_slots)

    svg_parts: list[str] = [
        f'<svg viewBox="0 0 {svg_w} {svg_h}" width="100%" style="display:block;">',
    ]

    for zone in zone_order:
        rect = zone_rects[zone]
        bg_color = zone_bg.get(zone, "#64748B")
        svg_parts.append(
            f'<rect x="{rect["x"]}" y="{rect["y"]}" width="{rect["w"]}" height="{rect["h"]}" fill="{bg_color}" fill-opacity="0.045"/>'
        )
        label_text = str(quadrant_labels.get(zone, "") or "").strip()
        if label_text:
            label_center_x = rect["x"] + (rect["w"] / 2.0)
            label_y = rect["y"] + 18.0
            svg_parts.append(
                f'<text x="{label_center_x}" y="{label_y}" text-anchor="middle" font-family="{font}" font-size="11" font-weight="600" fill="{bg_color}" opacity="0.87">{html.escape(label_text)}</text>'
            )

    svg_parts.extend(
        [
            f'<line x1="{x_mid}" y1="{pad_t}" x2="{x_mid}" y2="{pad_t + plot_h}" stroke="#D9D5CA" stroke-width="1.2"/>',
            f'<line x1="{pad_l}" y1="{y_mid}" x2="{pad_l + plot_w}" y2="{y_mid}" stroke="#D9D5CA" stroke-width="1.2"/>',
        ]
    )

    for bubble, cx_raw, cy_raw, radius_raw, text_bottom_limit in placed_bubbles:
        svg_parts.extend(
            render_customer_segment_bubble(
                font=font,
                bubble=bubble,
                cx_raw=cx_raw,
                cy_raw=cy_raw,
                radius_raw=radius_raw,
                text_bottom_limit=text_bottom_limit,
                safe_float=renderer._safe_float,
            )
        )

    svg_parts.extend(
        [
            f'<line x1="{pad_l}" y1="{pad_t + plot_h}" x2="{pad_l + plot_w}" y2="{pad_t + plot_h}" stroke="#C5C1B8" stroke-width="0.8"/>',
            f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + plot_h}" stroke="#C5C1B8" stroke-width="0.8"/>',
            f'<text x="{pad_l + (plot_w / 2.0)}" y="{svg_h - 8}" text-anchor="middle" font-family="{font}" font-size="12" fill="#64748B">{html.escape(str(axes.get("x_label", "Brecha de expectativa")))}</text>',
            f'<text x="18" y="{pad_t + (plot_h / 2.0)}" text-anchor="middle" font-family="{font}" font-size="12" fill="#64748B" transform="rotate(-90,18,{pad_t + (plot_h / 2.0)})">{html.escape(str(axes.get("y_label", "Satisfacción")))}</text>',
            f'<text x="{pad_l + 4}" y="{pad_t + plot_h + 15}" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("x_low", "Expectativas cumplidas")))}</text>',
            f'<text x="{pad_l + plot_w - 4}" y="{pad_t + plot_h + 15}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("x_high", "Expectativas no cumplidas")))}</text>',
            f'<text x="{pad_l - 8}" y="{pad_t + plot_h}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("y_low", "Baja satisfacción")))}</text>',
            f'<text x="{pad_l - 8}" y="{pad_t + 8}" text-anchor="end" font-family="{font}" font-size="10" fill="#8A928E">{html.escape(str(axes.get("y_high", "Alta satisfacción")))}</text>',
            "</svg>",
        ]
    )
    return "<div class='scatter'>" + "\n".join(svg_parts) + "</div>"


def render_generic_scatter_chart(renderer: Any, payload: dict[str, Any]) -> str | None:
    if payload.get("type") == "scatter_d" or isinstance(payload.get("bubbles"), list):
        rendered = render_customer_segment_scatter(renderer, payload)
        if rendered:
            return rendered

    axes = payload.get("axes")
    circles = payload.get("circles")
    points = payload.get("points")
    if not isinstance(axes, dict):
        return None
    if not isinstance(circles, list):
        return None

    width = 920.0
    height = 400.0
    pad_left = 96.0
    pad_right = 46.0
    pad_top = 48.0
    pad_bottom = 80.0
    inner_w = width - (pad_left + pad_right)
    inner_h = height - (pad_top + pad_bottom)

    def sx(x: float) -> float:
        x = max(0.0, min(100.0, x))
        return pad_left + ((x / 100.0) * inner_w)

    def sy(y: float) -> float:
        y = max(0.0, min(100.0, y))
        return height - pad_bottom - ((y / 100.0) * inner_h)

    svg_parts = []
    svg_parts.append(
        f"<line x1='{pad_left}' y1='{height - pad_bottom}' x2='{width - pad_right}' y2='{height - pad_bottom}' stroke='#8fd6c5' stroke-width='1'/>"
    )
    svg_parts.append(
        f"<line x1='{pad_left}' y1='{height - pad_bottom}' x2='{pad_left}' y2='{pad_top}' stroke='#8fd6c5' stroke-width='1'/>"
    )

    for tick in (0, 25, 50, 75, 100):
        x = sx(float(tick))
        y = sy(float(tick))
        svg_parts.append(
            f"<line x1='{x}' y1='{height - pad_bottom}' x2='{x}' y2='{height - pad_bottom + 6}' stroke='#8fd6c5' stroke-width='1'/>"
        )
        svg_parts.append(
            f"<text x='{x - 9}' y='{height - pad_bottom + 22}' fill='#64748B' font-size='11'>{tick}</text>"
        )
        svg_parts.append(
            f"<line x1='{pad_left - 6}' y1='{y}' x2='{pad_left}' y2='{y}' stroke='#8fd6c5' stroke-width='1'/>"
        )
        svg_parts.append(f"<text x='26' y='{y + 4}' fill='#64748B' font-size='11'>{tick}</text>")

    palette = list(renderer._PALETTE)
    for index, circle in enumerate(circles):
        if not isinstance(circle, dict):
            continue
        center = circle.get("center") if isinstance(circle.get("center"), dict) else {}
        cx = sx(float(center.get("x", 0.0)))
        cy = sy(float(center.get("y", 0.0)))
        radius_raw = float(circle.get("radius", 4.0))
        radius = max(4.0, min(36.0, radius_raw))
        color = palette[index % len(palette)]
        label = html.escape(str(circle.get("label", f"cluster_{index}") or f"cluster_{index}"))
        svg_parts.append(
            f"<circle cx='{cx}' cy='{cy}' r='{radius}' fill='{color}66' stroke='{color}' stroke-width='1.5'/>"
        )
        svg_parts.append(f"<text x='{cx + 4}' y='{cy - 4}' fill='#1b3d36' font-size='10'>{label}</text>")

    if isinstance(points, list):
        for point in points[:500]:
            if not isinstance(point, dict):
                continue
            x = sx(float(point.get("x", 0.0)))
            y = sy(float(point.get("y", 0.0)))
            size = max(2.0, min(6.0, float(point.get("size", 1.0))))
            svg_parts.append(
                f"<circle cx='{x}' cy='{y}' r='{size}' fill='#1b3d36cc' stroke='#ffffff' stroke-width='0.6'/>"
            )

    x_label = html.escape(str(axes.get("x_label", axes.get("x", "X"))))
    y_label = html.escape(str(axes.get("y_label", axes.get("y", "Y"))))
    svg_parts.append(
        f"<text x='{(pad_left + inner_w / 2) - 120}' y='{height - 16}' fill='#64748B' font-size='13'>{x_label}</text>"
    )
    svg_parts.append(
        f"<text x='18' y='{(pad_top + inner_h / 2)}' transform='rotate(-90, 24, {pad_top + inner_h / 2})' fill='#64748B' font-size='13'>{y_label}</text>"
    )

    return (
        "<div class='scatter'>"
        f"<svg viewBox='0 0 {width} {height}' width='100%' height='{height}'>{''.join(svg_parts)}</svg>"
        "</div>"
    )
