from __future__ import annotations

import math
from typing import Any


def group_scatter_bubbles(
    renderer: Any,
    *,
    bubbles: list[dict[str, Any]],
    zone_order: list[str],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {zone: [] for zone in zone_order}
    for item in bubbles:
        if not isinstance(item, dict):
            continue
        zone = str(item.get("zone", "") or "").strip().lower()
        if zone not in grouped:
            zone = fallback_zone_from_xy(renderer, item)
        grouped[zone].append(item)

    for zone in zone_order:
        grouped[zone] = sorted(
            grouped[zone],
            key=lambda bubble: renderer._safe_int(bubble.get("count_reviews")),
            reverse=True,
        )
    return grouped


def fallback_zone_from_xy(renderer: Any, item: dict[str, Any]) -> str:
    x = renderer._safe_float(item.get("x"))
    y = renderer._safe_float(item.get("y"))
    if y >= 50.0 and x < 50.0:
        return "top_left"
    if y >= 50.0 and x >= 50.0:
        return "top_right"
    if y < 50.0 and x < 50.0:
        return "bottom_left"
    return "bottom_right"


def rows_needed(n: int) -> int:
    if n <= 0:
        return 0
    if n <= 2:
        return 1
    cols = int(math.ceil(math.sqrt(n)))
    return int(math.ceil(n / max(1, cols)))


def row_extra(max_rows: int, max_clusters: int) -> int:
    rows_extra = max(0, max_rows - 2)
    dense_extra = max(0, int(math.ceil((max_clusters - 4) / 3.0)))
    return max(rows_extra, dense_extra)


def build_scatter_canvas_layout(
    *,
    grouped: dict[str, list[dict[str, Any]]],
    zone_order: list[str],
    base_svg_h: int,
    base_pad_b: int,
    svg_w: int,
    pad_l: int,
    pad_r: int,
    pad_t: int,
) -> dict[str, Any]:
    top_max_rows = max(rows_needed(len(grouped["top_left"])), rows_needed(len(grouped["top_right"])))
    bottom_max_rows = max(rows_needed(len(grouped["bottom_left"])), rows_needed(len(grouped["bottom_right"])))
    top_max_clusters = max(len(grouped["top_left"]), len(grouped["top_right"]))
    bottom_max_clusters = max(len(grouped["bottom_left"]), len(grouped["bottom_right"]))

    top_extra = row_extra(top_max_rows, top_max_clusters)
    bottom_extra = row_extra(bottom_max_rows, bottom_max_clusters)

    base_plot_h = base_svg_h - pad_t - base_pad_b
    base_half_h = base_plot_h / 2.0
    top_h = base_half_h + (top_extra * 88)
    bottom_h = base_half_h + (bottom_extra * 88)
    plot_h = top_h + bottom_h
    pad_b = base_pad_b + (bottom_extra * 10)
    svg_h = int(round(pad_t + plot_h + pad_b))

    plot_w = svg_w - pad_l - pad_r
    half_w = plot_w / 2.0
    x_mid = pad_l + half_w
    y_mid = pad_t + top_h
    zone_rects: dict[str, dict[str, float]] = {
        "top_left": {"x": pad_l, "y": pad_t, "w": half_w, "h": top_h},
        "top_right": {"x": x_mid, "y": pad_t, "w": half_w, "h": top_h},
        "bottom_left": {"x": pad_l, "y": y_mid, "w": half_w, "h": bottom_h},
        "bottom_right": {"x": x_mid, "y": y_mid, "w": half_w, "h": bottom_h},
    }
    zone_growth_map = {
        "top_left": top_extra,
        "top_right": top_extra,
        "bottom_left": bottom_extra,
        "bottom_right": bottom_extra,
    }
    return {
        "svg_h": svg_h,
        "pad_b": pad_b,
        "plot_h": plot_h,
        "plot_w": plot_w,
        "x_mid": x_mid,
        "y_mid": y_mid,
        "zone_rects": zone_rects,
        "zone_growth_map": zone_growth_map,
    }


def layout_zone(
    zone_rect: dict[str, float],
    zone_bubbles: list[dict[str, Any]],
    zone_growth: int,
) -> list[tuple[dict[str, Any], float, float, float, float]]:
    n = len(zone_bubbles)
    if n <= 0:
        return []
    x0 = zone_rect["x"]
    y0 = zone_rect["y"]
    w = zone_rect["w"]
    h = zone_rect["h"]
    zone_inner_pad_x = max(14.0, w * 0.04)
    zone_inner_pad_top = max(34.0, h * 0.20)
    zone_inner_pad_bottom = max(16.0, h * 0.08)
    if zone_growth > 0 and n > 2:
        zone_inner_pad_bottom += 22.0
    content_x0 = x0 + zone_inner_pad_x
    content_y0 = y0 + zone_inner_pad_top
    content_w = max(24.0, w - (2.0 * zone_inner_pad_x))
    content_h = max(24.0, h - zone_inner_pad_top - zone_inner_pad_bottom)
    cx_center = content_x0 + (content_w / 2.0)
    cy_center = content_y0 + (content_h / 2.0)
    text_bottom_limit = y0 + h - (24.0 if zone_growth > 0 else 10.0)
    placements: list[tuple[dict[str, Any], float, float, float, float]] = []
    if n == 1:
        slot_max_r = min(content_w, content_h) * 0.44
    elif n == 2:
        slot_max_r = min(content_w * 0.34, content_h * 0.45)
    else:
        slot_max_r = min(content_w * 0.26, content_h * 0.26)

    if n == 1:
        placements.append((zone_bubbles[0], cx_center, cy_center, slot_max_r, text_bottom_limit))
        return placements

    if n == 2:
        left_x = content_x0 + (content_w * 0.26)
        right_x = content_x0 + (content_w * 0.74)
        for idx, bubble in enumerate(zone_bubbles[:2]):
            cx_b = left_x if idx == 0 else right_x
            placements.append((bubble, cx_b, cy_center, slot_max_r, text_bottom_limit))
        return placements

    cols = int(math.ceil(math.sqrt(n)))
    rows = int(math.ceil(n / cols))
    cell_w = content_w / max(1, cols)
    if rows > 1:
        growth_boost = 0.03 * min(2, zone_growth)
        base_row_gap = max(30.0, content_h * (0.22 + growth_boost))
        max_gap_fit = max(0.0, (content_h - (rows * 30.0)) / (rows - 1))
        row_gap = min(base_row_gap, max_gap_fit)
    else:
        row_gap = 0.0
    effective_h = max(30.0, content_h - (row_gap * max(0, rows - 1)))
    cell_h = effective_h / max(1, rows)
    slot_max_r = min(cell_w, cell_h) * 0.38

    for idx, bubble in enumerate(zone_bubbles):
        row = idx // cols
        col = idx % cols
        bubble_cx = content_x0 + (cell_w * (col + 0.5))
        bubble_cy = content_y0 + (cell_h * (row + 0.5)) + (row * row_gap)
        placements.append((bubble, bubble_cx, bubble_cy, slot_max_r, text_bottom_limit))
    return placements


def collect_scatter_slots(
    *,
    grouped: dict[str, list[dict[str, Any]]],
    zone_rects: dict[str, dict[str, float]],
    zone_growth_map: dict[str, int],
    zone_order: list[str],
) -> list[tuple[dict[str, Any], float, float, float, float]]:
    placed_slots: list[tuple[dict[str, Any], float, float, float, float]] = []
    for zone in zone_order:
        placed_slots.extend(layout_zone(zone_rects[zone], grouped[zone], zone_growth_map[zone]))
    return placed_slots


def place_scatter_bubbles(
    renderer: Any,
    *,
    placed_slots: list[tuple[dict[str, Any], float, float, float, float]],
) -> list[tuple[dict[str, Any], float, float, float, float]]:
    positive_weights: list[tuple[float, float]] = []
    for bubble, _cx, _cy, slot_max_r, _text_bottom_limit in placed_slots:
        weight = renderer._safe_float(bubble.get("radius"))
        if weight <= 0.0:
            weight_pct = renderer._safe_float(bubble.get("weight_pct"))
            if weight_pct > 0.0:
                weight = weight_pct / 100.0
        weight = max(0.0, weight)
        if weight > 0.0:
            positive_weights.append((weight, slot_max_r))

    if positive_weights:
        k = min(slot_max_r / math.sqrt(weight) for weight, slot_max_r in positive_weights)
        k *= 0.98
    else:
        k = 0.0

    placed_bubbles: list[tuple[dict[str, Any], float, float, float, float]] = []
    for bubble, cx, cy, slot_max_r, text_bottom_limit in placed_slots:
        weight = renderer._safe_float(bubble.get("radius"))
        if weight <= 0.0:
            weight_pct = renderer._safe_float(bubble.get("weight_pct"))
            if weight_pct > 0.0:
                weight = weight_pct / 100.0
        weight = max(0.0, weight)
        if weight <= 0.0 or k <= 0.0:
            r = 1.0
        else:
            r = math.sqrt(weight) * k
        r = min(r, slot_max_r)
        placed_bubbles.append((bubble, cx, cy, r, text_bottom_limit))
    return placed_bubbles
