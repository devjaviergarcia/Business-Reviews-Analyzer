from __future__ import annotations

from src.crm.reports.public_study import build_public_study_payload, render_public_study_html


def test_public_study_payload_uses_aggregates_and_anonymized_examples() -> None:
    benchmark = {
        "benchmark_run_id": "bench-1",
        "title": "Estudio restaurantes Cordoba",
        "query": "restaurantes cordoba",
        "city": "Cordoba",
        "status": "completed",
        "limit": 100,
    }
    businesses = [
        {
            "business_name": "Dulce Lokura",
            "category": "Pasteleria",
            "city": "Cordoba",
            "address": "C. San Alvaro, 1",
            "email": "privado@example.com",
            "phone": "617 81 09 10",
            "website": "https://dulce.example",
            "maps_url": "https://maps.example/dulce",
            "rating": 4.6,
            "review_count": 157,
            "discovery_rank": 4,
            "opportunity_score": 44,
            "listing_enriched": True,
        },
        {
            "business_name": "Cafe Secreto",
            "category": "Cafeteria",
            "city": "Cordoba",
            "phone": "600 00 00 00",
            "rating": 4.2,
            "review_count": 18,
            "discovery_rank": 14,
            "opportunity_score": 68,
            "listing_enriched": True,
        },
    ]

    payload = build_public_study_payload(benchmark_run=benchmark, businesses=businesses)

    assert payload["metrics"]["businesses"] == 2
    assert payload["metrics"]["avg_rating"] == 4.4
    assert payload["metrics"]["website_share"] == 0.5
    assert payload["top_visible_examples"][0]["label"] == "Dulce Lokura"
    assert payload["top_visible_examples"][0]["category"] == "Pasteleria"
    assert "privado@example.com" not in str(payload)
    assert "617 81 09 10" not in str(payload)
    assert "https://dulce.example" not in str(payload)
    assert "utm_source=public_study" in payload["cta"]["url"]


def test_public_study_html_is_publicable_and_hides_sensitive_business_data() -> None:
    html = render_public_study_html(
        benchmark_run={"benchmark_run_id": "bench-2", "query": "merienda cordoba", "city": "Cordoba", "status": "completed"},
        businesses=[
            {
                "business_name": "Dulce Lokura",
                "category": "Pasteleria",
                "address": "C. San Alvaro, 1",
                "email": "privado@example.com",
                "phone": "617 81 09 10",
                "website": "https://dulce.example",
                "maps_url": "https://maps.example/dulce",
                "rating": 4.6,
                "review_count": 157,
                "discovery_rank": 4,
                "opportunity_score": 44,
                "listing_enriched": True,
            }
        ],
    )

    assert "Estudio local anonimo" in html
    assert "CTA trackeable" in html
    assert "Dulce Lokura" in html
    assert "privado@example.com" not in html
    assert "617 81 09 10" not in html
    assert "C. San Alvaro" not in html
    assert "https://dulce.example" not in html
