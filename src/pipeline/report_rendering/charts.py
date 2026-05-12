from __future__ import annotations

import html
import math
from typing import Any

from .helpers import ReportRenderingHelpersMixin


class ReportRenderingChartsMixin(ReportRenderingHelpersMixin):
    def _render_bar_chart_vista_c(self, bar_chart_data: dict[str, Any]) -> str:
        """
        Vista C — SVG de barras horizontales por tipo de cliente.
        """
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
            count = self._safe_int(row.get("count_reviews"))
            weight_pct = self._safe_float(row.get("weight_pct"))
            bar_w = max(4, round((weight_pct / 100.0) * bar_max_w))
            sat_label = html.escape(str(row.get("satisfaction_label", "") or ""))
            sat_pct = self._safe_float(row.get("satisfaction_pct"))
            sent_label = html.escape(str(row.get("sentiment_label", "") or ""))
            sentiment = self._safe_float(row.get("sentiment"))
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

    def _render_customer_bar_chart(self, scatter_payload: dict[str, Any]) -> str:
        circles = scatter_payload.get("circles") if isinstance(scatter_payload, dict) else []
        if not isinstance(circles, list) or not circles:
            return ""
        total = sum(self._safe_int(item.get("count")) for item in circles if isinstance(item, dict))
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
            count = self._safe_int(item.get("count"))
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

    def _render_scatter_vista_d(self, scatter_data: dict[str, Any]) -> str:
        """
        Vista D — SVG por zonas semánticas (layout fijo).
        No usa coordenadas reales de scatter: cada burbuja ocupa una celda lógica.
        """
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

        def _fallback_zone_from_xy(item: dict[str, Any]) -> str:
            x = self._safe_float(item.get("x"))
            y = self._safe_float(item.get("y"))
            if y >= 50.0 and x < 50.0:
                return "top_left"
            if y >= 50.0 and x >= 50.0:
                return "top_right"
            if y < 50.0 and x < 50.0:
                return "bottom_left"
            return "bottom_right"

        grouped: dict[str, list[dict[str, Any]]] = {zone: [] for zone in zone_order}
        for item in bubbles:
            if not isinstance(item, dict):
                continue
            zone = str(item.get("zone", "") or "").strip().lower()
            if zone not in grouped:
                zone = _fallback_zone_from_xy(item)
            grouped[zone].append(item)

        for zone in zone_order:
            grouped[zone] = sorted(
                grouped[zone],
                key=lambda bubble: self._safe_int(bubble.get("count_reviews")),
                reverse=True,
            )

        def _rows_needed(n: int) -> int:
            if n <= 0:
                return 0
            if n <= 2:
                return 1
            cols = int(math.ceil(math.sqrt(n)))
            return int(math.ceil(n / max(1, cols)))

        def _row_extra(max_rows: int, max_clusters: int) -> int:
            rows_extra = max(0, max_rows - 2)
            # Dense cases may need extra room even with 2 rows.
            dense_extra = max(0, int(math.ceil((max_clusters - 4) / 3.0)))
            return max(rows_extra, dense_extra)

        top_max_rows = max(_rows_needed(len(grouped["top_left"])), _rows_needed(len(grouped["top_right"])))
        bottom_max_rows = max(_rows_needed(len(grouped["bottom_left"])), _rows_needed(len(grouped["bottom_right"])))
        top_max_clusters = max(len(grouped["top_left"]), len(grouped["top_right"]))
        bottom_max_clusters = max(len(grouped["bottom_left"]), len(grouped["bottom_right"]))

        top_extra = _row_extra(top_max_rows, top_max_clusters)
        bottom_extra = _row_extra(bottom_max_rows, bottom_max_clusters)

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

        def _layout_zone(
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
                # Stronger vertical separation for dense zones.
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

        placed_slots: list[tuple[dict[str, Any], float, float, float, float]] = []
        for zone in zone_order:
            placed_slots.extend(_layout_zone(zone_rects[zone], grouped[zone], zone_growth_map[zone]))

        # Global area scaling:
        # r = k * sqrt(weight), so circle area is proportional to real weight.
        positive_weights: list[tuple[float, float]] = []
        for bubble, _cx, _cy, slot_max_r, _text_bottom_limit in placed_slots:
            weight = self._safe_float(bubble.get("radius"))
            if weight <= 0.0:
                weight_pct = self._safe_float(bubble.get("weight_pct"))
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
            weight = self._safe_float(bubble.get("radius"))
            if weight <= 0.0:
                weight_pct = self._safe_float(bubble.get("weight_pct"))
                if weight_pct > 0.0:
                    weight = weight_pct / 100.0
            weight = max(0.0, weight)
            if weight <= 0.0 or k <= 0.0:
                r = 1.0
            else:
                r = math.sqrt(weight) * k
            r = min(r, slot_max_r)
            placed_bubbles.append((bubble, cx, cy, r, text_bottom_limit))

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
            color = str(bubble.get("color", "#0A7567") or "#0A7567")
            label_value = str(bubble.get("label", "") or "Segmento").strip()
            weight_pct = self._safe_float(bubble.get("weight_pct"))
            cx = round(cx_raw, 1)
            cy = round(cy_raw, 1)
            r = round(max(1.0, radius_raw), 1)
            meta_value = f"{weight_pct:.1f}%"
            max_text_width = max(20.0, (2.0 * r) - 14.0)

            def _fit_font(
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

            def _trim_to_width(text: str, font_size: float, factor: float, available_width: float) -> str:
                clean = (text or "").strip()
                if not clean:
                    return "-"
                est_width = len(clean) * font_size * factor
                if est_width <= available_width:
                    return clean
                allowed = int(available_width / max(0.1, font_size * factor))
                if allowed <= 1:
                    return clean[:1]
                return clean[:allowed]

            label_font = 11.0
            meta_font = 10.0
            label_min_font = 7.5
            meta_min_font = 7.0
            label_factor = 0.56
            meta_factor = 0.54

            circle_area = math.pi * (r**2)
            full_inside_area_x = 4200.0
            percent_inside_only_area_y = 1400.0

            if circle_area >= full_inside_area_x:
                label_candidate = label_value.strip() or "Segmento"
                label_font = _fit_font(
                    label_candidate,
                    label_font,
                    6.4,
                    label_factor,
                    max_text_width,
                )
                meta_font = _fit_font(
                    meta_value,
                    meta_font,
                    meta_min_font,
                    meta_factor,
                    max_text_width,
                )

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
            elif circle_area >= percent_inside_only_area_y:
                inside_text = f"{weight_pct:.1f}%"
                inside_font = _fit_font(
                    inside_text,
                    10.0,
                    meta_min_font,
                    meta_factor,
                    max_text_width,
                )
                outside_width = max(74.0, (2.0 * r) + 50.0, len(label_value) * 6.0)
                outside_label = label_value.strip() or "Segmento"
                outside_font = _fit_font(
                    outside_label,
                    10.5,
                    7.2,
                    label_factor,
                    outside_width,
                )
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
            else:
                outside_width = max(82.0, (2.0 * r) + 56.0, len(label_value) * 6.6)
                outside_label = label_value.strip() or "Segmento"
                outside_font = _fit_font(
                    outside_label,
                    10.5,
                    7.2,
                    label_factor,
                    outside_width,
                )
                outside_meta_font = _fit_font(
                    meta_value,
                    9.8,
                    7.5,
                    meta_factor,
                    outside_width,
                )
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

    def _maybe_render_scatter_svg(self, payload: dict[str, Any]) -> str | None:
        if payload.get("type") == "scatter_d" or isinstance(payload.get("bubbles"), list):
            rendered = self._render_scatter_vista_d(payload)
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

        palette = list(self._PALETTE)
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
