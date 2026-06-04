from src.crm.benchmark.geo_uule import build_geo_grid_points, generate_uule_v2, normalize_grid_size


def test_normalize_grid_size_forces_odd_in_range() -> None:
    assert normalize_grid_size(1) == 3
    assert normalize_grid_size(4) == 5
    assert normalize_grid_size(21) == 21
    assert normalize_grid_size(25) == 21


def test_generate_uule_v2_returns_token() -> None:
    token = generate_uule_v2(lat=37.8882, lng=-4.7794, radius_m=1000)
    assert token.startswith("a+")
    assert len(token) > 8


def test_build_geo_grid_points_creates_nxn() -> None:
    points = build_geo_grid_points(center_lat=37.8882, center_lng=-4.7794, size=4, spacing_km=0.4)
    assert len(points) == 25
    assert points[0]["order"] == 1
    assert points[-1]["order"] == 25
    assert points[12]["row"] == 3
    assert points[12]["col"] == 3
