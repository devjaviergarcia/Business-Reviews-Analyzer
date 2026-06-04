from src.services.crm_service import CRMService


def test_geo_grid_stats_groups_businesses_and_positions() -> None:
    service = CRMService()
    run = {
        "geo_grid_run_id": "run-1",
        "keyword": "merienda cordoba",
        "city": "Cordoba",
        "city_slug": "cordoba",
        "provider_mode": "uule",
        "point_count": 2,
        "top_n": 5,
        "metrics": {
            "visibility_score": 77.5,
            "share_top3": 0.5,
            "share_top10": 1.0,
            "share_not_found": 0.0,
        },
    }
    results = [
        {
            "point_order": 1,
            "point_label": "Punto 1",
            "lat": 37.1,
            "lng": -4.1,
            "rank": 1,
            "business_key": "a",
            "business_name": "Cafe A",
            "rating": 4.8,
            "review_count": 100,
        },
        {
            "point_order": 2,
            "point_label": "Punto 2",
            "lat": 37.2,
            "lng": -4.2,
            "rank": 3,
            "business_key": "a",
            "business_name": "Cafe A",
            "rating": 4.8,
            "review_count": 100,
        },
        {
            "point_order": 1,
            "point_label": "Punto 1",
            "lat": 37.1,
            "lng": -4.1,
            "rank": 2,
            "business_key": "b",
            "business_name": "Cafe B",
        },
    ]

    stats = service._build_geo_grid_stats(run=run, results=results)

    assert stats["summary"]["unique_businesses"] == 2
    assert stats["leaders"][0]["business_key"] == "a"
    assert stats["leaders"][0]["coverage_percent"] == 100.0
    assert stats["leaders"][0]["avg_rank"] == 2.0
    assert stats["leaders"][0]["rank_stddev"] == 1.0
    assert stats["summary"]["provider_mode"] == "uule"
    assert stats["summary"]["visibility_score"] == 77.5
    assert len(stats["points"]) == 2
