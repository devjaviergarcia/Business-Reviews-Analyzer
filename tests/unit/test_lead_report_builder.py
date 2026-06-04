from __future__ import annotations

from src.crm.benchmark import build_deep_study_snapshot
from src.crm.reports import build_lead_report_payload, render_lead_report_html


def test_lead_report_generates_html_for_complete_fixture() -> None:
    business = {
        "business_name": "Dulce Lokura",
        "category": "Pasteleria",
        "city": "Cordoba",
        "address": "C. San Alvaro, 1",
        "rating": 4.6,
        "review_count": 157,
        "discovery_rank": 4,
        "phone": "617 81 09 10",
        "website": None,
    }
    competitors = [
        {
            "business_name": "Merienda Norte",
            "discovery_rank": 1,
            "rating": 4.8,
            "review_count": 430,
            "website": "https://example.com",
        },
        {
            "business_name": "Pasteleria Centro",
            "discovery_rank": 2,
            "rating": 4.5,
            "review_count": 240,
            "website": "https://example.org",
        },
    ]
    reviews = [
        {"rating": 5, "text": "Buen postre, servicio amable y local bonito."},
        {"rating": 4, "text": "La comida esta bien, pero la espera fue larga."},
    ]
    snapshot = build_deep_study_snapshot(
        business=business,
        competitors=competitors,
        reviews=reviews,
    )

    html = render_lead_report_html(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=competitors,
    )
    payload = build_lead_report_payload(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=competitors,
    )

    assert "<html lang=\"es\">" in html
    assert "Dulce Lokura" in html
    assert "3 oportunidades principales" in html
    assert "Merienda Norte" in html
    assert "#4" in html
    assert "Quiero el informe mensual completo" in html
    assert 0 <= payload["health_score"] <= 100


def test_lead_report_generates_html_for_incomplete_fixture() -> None:
    business = {
        "business_name": "Negocio sin datos",
        "city": "Cordoba",
        "discovery_rank": None,
        "rating": None,
        "review_count": None,
        "website": None,
        "phone": None,
    }
    snapshot = build_deep_study_snapshot(business=business, competitors=[], reviews=[])

    html = render_lead_report_html(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=[],
    )

    assert "<html lang=\"es\">" in html
    assert "Negocio sin datos" in html
    assert "Sin dato" in html
    assert "Faltan resenas textuales" in html
    assert "Faltan competidores seleccionados" in html
    assert "Accion inmediata recomendada" in html
