from __future__ import annotations

from src.crm.benchmark import select_competitors_for_business


def _business(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "benchmark_business_id": "target-1",
        "business_name": "Restaurante Centro",
        "category": "Restaurante",
        "city": "Cordoba",
        "address": "Calle Uno, 14001 Cordoba",
        "maps_url": "https://maps.example/target",
        "rating": 4.3,
        "review_count": 250,
        "discovery_rank": 8,
        "website": None,
    }
    payload.update(overrides)
    return payload


def test_select_competitors_prioritizes_same_category_city_and_quality() -> None:
    target = _business()
    candidates = [
        target,
        _business(
            benchmark_business_id="leader-1",
            business_name="Restaurante Lider Uno",
            maps_url="https://maps.example/leader-1",
            discovery_rank=1,
            rating=4.7,
            review_count=900,
            website="https://leader1.example",
        ),
        _business(
            benchmark_business_id="similar-1",
            business_name="Restaurante Similar Uno",
            maps_url="https://maps.example/similar-1",
            discovery_rank=9,
            rating=4.2,
            review_count=210,
        ),
        _business(
            benchmark_business_id="other-city",
            business_name="Restaurante Fuera",
            city="Sevilla",
            address="Calle Dos, Sevilla",
            maps_url="https://maps.example/fuera",
            discovery_rank=2,
            rating=4.9,
            review_count=2000,
        ),
        _business(
            benchmark_business_id="leader-2",
            business_name="Restaurante Lider Dos",
            maps_url="https://maps.example/leader-2",
            discovery_rank=3,
            rating=4.5,
            review_count=260,
        ),
        _business(
            benchmark_business_id="different-category",
            business_name="Hotel No Competidor",
            category="Hotel",
            maps_url="https://maps.example/hotel",
            discovery_rank=4,
            rating=5.0,
            review_count=4000,
        ),
    ]

    selected = select_competitors_for_business(target, candidates, max_competitors=3)

    assert [item["business_name"] for item in selected] == [
        "Restaurante Lider Uno",
        "Restaurante Lider Dos",
        "Restaurante Similar Uno",
    ]
    assert selected[0]["relative_position"] == "leader"
    assert selected[0]["discovery_rank"] == 1
    assert selected[0]["distance_hint"] == "same_city"
    assert "misma categoria" in selected[0]["why_selected"]
    assert selected[0]["similarity_score"] >= selected[-1]["similarity_score"]


def test_select_competitors_is_deterministic_for_same_dataset() -> None:
    target = _business()
    candidates = [
        _business(benchmark_business_id="b", business_name="B Cafe", maps_url="https://maps.example/b", rating=4.5, review_count=400),
        _business(benchmark_business_id="a", business_name="A Cafe", maps_url="https://maps.example/a", rating=4.5, review_count=400),
    ]

    first = select_competitors_for_business(target, candidates, max_competitors=2)
    second = select_competitors_for_business(target, list(reversed(candidates)), max_competitors=2)

    assert first == second
    assert [item["business_name"] for item in first] == ["A Cafe", "B Cafe"]


def test_select_competitors_uses_benchmark_position_as_visibility_signal() -> None:
    target = _business(discovery_rank=12)
    candidates = [
        _business(
            benchmark_business_id="late",
            business_name="Late Strong",
            maps_url="https://maps.example/late",
            rating=4.6,
            review_count=400,
            discovery_rank=20,
        ),
        _business(
            benchmark_business_id="early",
            business_name="Early Visible",
            maps_url="https://maps.example/early",
            rating=4.4,
            review_count=260,
            discovery_rank=2,
        ),
    ]

    selected = select_competitors_for_business(target, candidates, max_competitors=2)

    assert selected[0]["business_name"] == "Early Visible"
    assert selected[0]["discovery_rank"] == 2
    assert "aparece antes en discovery" in selected[0]["why_selected"]


def test_select_competitors_handles_few_candidates() -> None:
    target = _business()
    selected = select_competitors_for_business(target, [target], max_competitors=5)

    assert selected == []
