from __future__ import annotations

from src.crm.benchmark import build_deep_study_snapshot
from src.crm.reports import build_paid_report_payload, render_paid_report_html


def test_paid_report_is_distinct_from_lead_report_with_history() -> None:
    business = {
        "business_name": "Dulce Lokura",
        "category": "Pasteleria",
        "city": "Cordoba",
        "rating": 4.6,
        "review_count": 157,
        "discovery_rank": 4,
        "website": None,
        "phone": "617 81 09 10",
    }
    competitors = [
        {"business_name": "Merienda Norte", "discovery_rank": 1, "rating": 4.8, "review_count": 430, "website": "https://example.com"},
        {"business_name": "Pasteleria Centro", "discovery_rank": 2, "rating": 4.5, "review_count": 240, "website": "https://example.org"},
    ]
    reviews = [
        {"rating": 5, "text": "Buen postre, servicio amable y local bonito."},
        {"rating": 3, "text": "La comida esta bien, pero la espera fue larga."},
    ]
    snapshot = build_deep_study_snapshot(business=business, competitors=competitors, reviews=reviews)
    history = [
        {"month": "2026-04", "health_score": 59, "discovery_rank": 7, "rating": 4.5, "review_count": 140},
        {"month": "2026-05", "health_score": 66, "discovery_rank": 4, "rating": 4.6, "review_count": 157},
    ]

    html = render_paid_report_html(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=competitors,
        history=history,
        report_month="2026-05",
    )
    payload = build_paid_report_payload(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=competitors,
        history=history,
        report_month="2026-05",
    )

    assert "Paid report mensual" in html
    assert "Plan mensual de accion" in html
    assert "Plantillas de respuesta" in html
    assert "Historico y seguimiento" in html
    assert "Merienda Norte" in html
    assert "2026-04" in html
    assert len(payload["monthly_plan"]) == 4
    assert payload["cta"]["label"] == "Agendar revision mensual"


def test_paid_report_handles_missing_history_and_data() -> None:
    business = {"business_name": "Negocio base", "city": "Cordoba"}
    snapshot = build_deep_study_snapshot(business=business, competitors=[], reviews=[])

    html = render_paid_report_html(
        business=business,
        deep_study_snapshot=snapshot,
        competitors=[],
        history=[],
        report_month="2026-05",
    )

    assert "<html lang=\"es\">" in html
    assert "Negocio base" in html
    assert "Sin historico todavia" in html
    assert "No hay competidores seleccionados" in html
    assert "Agendar revision mensual" in html
