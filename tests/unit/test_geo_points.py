from __future__ import annotations

import pytest

from src.crm.benchmark import list_supported_geo_point_cities, load_city_geo_points
from src.crm.benchmark.geo_points import parse_city_geo_points


def test_load_cordoba_geo_points_from_default_data_dir() -> None:
    geo_points = load_city_geo_points("Cordoba")

    assert geo_points.city == "Cordoba"
    assert geo_points.center == {"lat": 37.8882, "lng": -4.7794}
    assert len(geo_points.points) == 73
    assert geo_points.points[0].order == 1
    assert geo_points.points[0].lat == 37.8808152
    assert geo_points.points[-1].order == 73
    assert geo_points.points[-1].lng == -4.8915768


def test_load_cordoba_geo_points_normalizes_city_accents() -> None:
    geo_points = load_city_geo_points("Córdoba")

    assert geo_points.city == "Cordoba"
    assert "cordoba" in list_supported_geo_point_cities()


def test_parse_city_geo_points_rejects_duplicate_orders() -> None:
    with pytest.raises(ValueError, match="duplicated geo point order"):
        parse_city_geo_points(
            {
                "city": "Test",
                "center": {"lat": 1.0, "lng": 2.0},
                "points": [
                    {"order": 1, "label": "A", "lat": 1.0, "lng": 2.0},
                    {"order": 1, "label": "B", "lat": 1.1, "lng": 2.1},
                ],
            }
        )
