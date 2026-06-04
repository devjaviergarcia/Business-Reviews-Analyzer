from __future__ import annotations

from src.crm.benchmark import DeepStudySnapshot, build_deep_study_snapshot


def test_build_deep_study_snapshot_returns_valid_json_contract() -> None:
    snapshot = build_deep_study_snapshot(
        business={
            "business_name": "Restaurante Centro",
            "rating": 4.6,
            "review_count": 320,
            "discovery_rank": 3,
            "website": "https://centro.example",
            "phone": "600 000 000",
        },
        reviews=[
            {"rating": 5, "text": "Comida excelente y servicio muy amable."},
            {"rating": 4, "text": "Buen ambiente, carta variada y precio correcto."},
            {"rating": 2, "text": "La espera fue lenta aunque la comida estaba bien."},
        ],
        competitors=[
            {"business_name": "Competidor Uno", "rating": 4.4, "review_count": 250, "discovery_rank": 6, "website": "https://uno.example"},
            {"business_name": "Competidor Dos", "rating": 4.7, "review_count": 500, "discovery_rank": 1, "website": "https://dos.example"},
        ],
    )

    validated = DeepStudySnapshot.model_validate(snapshot)

    assert validated.business_name == "Restaurante Centro"
    assert validated.executive_summary
    assert validated.strengths
    assert validated.monthly_actions
    assert validated.response_templates
    assert "reputation" in validated.score_breakdown
    assert validated.data_quality["listing_fields"]["discovery_rank"] is True
    assert "posicion #3" in validated.executive_summary
    assert any(topic.topic == "comida" for topic in validated.recurring_topics)


def test_build_deep_study_snapshot_degrades_without_reviews_or_competitors() -> None:
    snapshot = build_deep_study_snapshot(
        business={
            "business_name": "Dulce Local",
            "rating": 4.3,
            "review_count": 25,
            "phone": "600 000 000",
        }
    )

    validated = DeepStudySnapshot.model_validate(snapshot)

    assert "missing_reviews: snapshot degradado a listing + benchmark" in validated.warnings
    assert "missing_competitors: comparativa local limitada" in validated.warnings
    assert "missing_discovery_rank" in validated.warnings
    assert "missing_website" in validated.warnings
    assert validated.data_quality["reviews_available"] == 0
    assert validated.monthly_actions


def test_build_deep_study_snapshot_reports_competitor_gaps() -> None:
    snapshot = build_deep_study_snapshot(
        business={"business_name": "Bar Pequeno", "rating": 4.0, "review_count": 80, "discovery_rank": 12},
        competitors=[
            {"business_name": "Lider", "rating": 4.6, "review_count": 400, "discovery_rank": 2, "website": "https://lider.example"},
            {"business_name": "Similar", "rating": 4.4, "review_count": 260, "discovery_rank": 4, "website": "https://similar.example"},
        ],
    )

    validated = DeepStudySnapshot.model_validate(snapshot)

    assert any("Rating 4.0" in gap for gap in validated.competitor_gaps)
    assert any("resenas" in gap for gap in validated.competitor_gaps)
    assert any("Posicion #12" in gap for gap in validated.competitor_gaps)
    assert any("Competidores" in gap for gap in validated.competitor_gaps)
    assert validated.score_breakdown["opportunity"] > 0
