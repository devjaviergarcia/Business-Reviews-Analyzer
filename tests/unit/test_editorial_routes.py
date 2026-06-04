from __future__ import annotations

from src.main import app
from src.routers.editorial import get_editorial_page_html


def test_editorial_routes_are_exposed() -> None:
    paths = {route.path for route in app.router.routes if hasattr(route, "path")}
    assert "/insights" in paths
    assert "/insights/{page_slug}" in paths


def test_seo_study_page_contains_seo_cta() -> None:
    html = get_editorial_page_html("estado-resenas-restaurantes-cordoba")
    assert "utm_source=seo" in html
    assert "Pedir informe individual" in html


def test_five_support_articles_exist_and_link_to_report() -> None:
    slugs = [
        "como-responder-resenas-negativas-restaurantes",
        "que-significa-tener-4-4-estrellas-y-muchas-resenas",
        "errores-frecuentes-google-maps-restaurantes",
        "como-mejorar-ficha-google-business-profile-restaurante",
        "como-comparar-tu-restaurante-con-competidores-locales",
    ]
    for slug in slugs:
        html = get_editorial_page_html(slug)
        assert "utm_source=seo" in html
        assert "/solicitud?" in html
