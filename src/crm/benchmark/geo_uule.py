from __future__ import annotations

import base64
import math
from typing import Any


def generate_uule_v2(*, lat: float, lng: float, radius_m: int = 1000) -> str:
    """Build a UULE v2 token from coordinates."""
    lat_e7 = int(float(lat) * 1e7)
    lng_e7 = int(float(lng) * 1e7)
    radius_value = max(100, int(radius_m)) * 620
    payload = (
        "role:1\n"
        "producer:12\n"
        "provenance:6\n"
        "timestamp:0\n"
        "latlng{\n"
        f"latitude_e7:{lat_e7}\n"
        f"longitude_e7:{lng_e7}\n"
        "}\n"
        f"radius:{radius_value}\n"
    )
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    return f"a+{encoded}"


def normalize_grid_size(size: int) -> int:
    parsed = max(3, min(21, int(size)))
    return parsed if parsed % 2 == 1 else parsed + 1


def build_geo_grid_points(
    *,
    center_lat: float,
    center_lng: float,
    size: int = 7,
    spacing_km: float = 0.4,
    label_prefix: str = "Grid",
) -> list[dict[str, Any]]:
    """Generate an NxN geogrid around a center coordinate."""
    normalized_size = normalize_grid_size(size)
    half = normalized_size // 2
    spacing = max(0.05, float(spacing_km))
    lat_step = spacing / 111.0
    lon_step = spacing / (111.0 * max(0.1, math.cos(math.radians(float(center_lat)))))

    points: list[dict[str, Any]] = []
    order = 0
    for row in range(-half, half + 1):
        for col in range(-half, half + 1):
            order += 1
            current_lat = float(center_lat) + (row * lat_step)
            current_lng = float(center_lng) + (col * lon_step)
            row_index = row + half + 1
            col_index = col + half + 1
            points.append(
                {
                    "order": order,
                    "label": f"{label_prefix} {row_index}-{col_index}",
                    "lat": round(current_lat, 7),
                    "lng": round(current_lng, 7),
                    "row": row_index,
                    "col": col_index,
                }
            )
    return points
