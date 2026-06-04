from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_GEO_POINTS_DIR = PROJECT_ROOT / "data" / "geo_points"


@dataclass(frozen=True)
class GeoPoint:
    order: int
    label: str
    lat: float
    lng: float

    def to_dict(self) -> dict[str, Any]:
        return {"order": self.order, "label": self.label, "lat": self.lat, "lng": self.lng}


@dataclass(frozen=True)
class CityGeoPoints:
    city: str
    center: dict[str, float]
    points: tuple[GeoPoint, ...]
    generated_at: str | None = None
    exported_at: str | None = None
    source_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "city": self.city,
            "center": dict(self.center),
            "generated_at": self.generated_at,
            "exported_at": self.exported_at,
            "points": [point.to_dict() for point in self.points],
        }


def list_supported_geo_point_cities(*, base_dir: Path | None = None) -> list[str]:
    directory = Path(base_dir or DEFAULT_GEO_POINTS_DIR)
    if not directory.exists():
        return []
    return sorted(path.stem for path in directory.glob("*.json") if path.is_file())


def load_city_geo_points(city: str, *, base_dir: Path | None = None) -> CityGeoPoints:
    slug = _slugify_city(city)
    path = Path(base_dir or DEFAULT_GEO_POINTS_DIR) / f"{slug}.json"
    if not path.exists():
        supported = ", ".join(list_supported_geo_point_cities(base_dir=base_dir)) or "ninguna"
        raise FileNotFoundError(f"No hay puntos geo para ciudad '{city}'. Ciudades soportadas: {supported}.")

    payload = json.loads(path.read_text(encoding="utf-8"))
    return parse_city_geo_points(payload, source_path=path)


def load_all_city_geo_points(*, base_dir: Path | None = None) -> list[CityGeoPoints]:
    cities: list[CityGeoPoints] = []
    for city in list_supported_geo_point_cities(base_dir=base_dir):
        cities.append(load_city_geo_points(city, base_dir=base_dir))
    return cities


def parse_city_geo_points(payload: dict[str, Any], *, source_path: Path | None = None) -> CityGeoPoints:
    if not isinstance(payload, dict):
        raise ValueError("city geo points payload must be a JSON object")

    city = str(payload.get("city") or "").strip()
    if not city:
        raise ValueError("city geo points payload requires 'city'")

    center = _parse_center(payload.get("center"))
    points = _parse_points(payload.get("points"))
    if not points:
        raise ValueError(f"city geo points payload for '{city}' does not contain valid points")

    return CityGeoPoints(
        city=city,
        center=center,
        points=tuple(points),
        generated_at=_optional_str(payload.get("generated_at")),
        exported_at=_optional_str(payload.get("exported_at")),
        source_path=source_path,
    )


def _parse_center(raw_center: Any) -> dict[str, float]:
    if not isinstance(raw_center, dict):
        raise ValueError("city geo points payload requires center object")
    lat = _coerce_coordinate(raw_center.get("lat"), name="center.lat", minimum=-90.0, maximum=90.0)
    lng = _coerce_coordinate(raw_center.get("lng"), name="center.lng", minimum=-180.0, maximum=180.0)
    return {"lat": lat, "lng": lng}


def _parse_points(raw_points: Any) -> list[GeoPoint]:
    if not isinstance(raw_points, list):
        raise ValueError("city geo points payload requires points list")

    parsed: list[GeoPoint] = []
    seen_orders: set[int] = set()
    for index, raw_point in enumerate(raw_points, start=1):
        if not isinstance(raw_point, dict):
            raise ValueError(f"point {index} must be an object")
        order = _coerce_order(raw_point.get("order"), fallback=index)
        if order in seen_orders:
            raise ValueError(f"duplicated geo point order: {order}")
        seen_orders.add(order)
        label = str(raw_point.get("label") or f"Punto {order}").strip() or f"Punto {order}"
        lat = _coerce_coordinate(raw_point.get("lat"), name=f"point {order}.lat", minimum=-90.0, maximum=90.0)
        lng = _coerce_coordinate(raw_point.get("lng"), name=f"point {order}.lng", minimum=-180.0, maximum=180.0)
        parsed.append(GeoPoint(order=order, label=label, lat=lat, lng=lng))

    return sorted(parsed, key=lambda point: point.order)


def _coerce_coordinate(value: Any, *, name: str, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not minimum <= parsed <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return parsed


def _coerce_order(value: Any, *, fallback: int) -> int:
    if value is None:
        return fallback
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("geo point order must be an integer") from exc
    if parsed <= 0:
        raise ValueError("geo point order must be positive")
    return parsed


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _slugify_city(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", ascii_text).strip("_")
