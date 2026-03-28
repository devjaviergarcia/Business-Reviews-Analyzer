from __future__ import annotations

from src.scraper.tripadvisor import TripadvisorScraper


def test_resolve_direct_listing_target_url_accepts_absolute_tripadvisor_listing_url() -> None:
    scraper = TripadvisorScraper()
    url = (
        "https://www.tripadvisor.es/Hotel_Review-g187430-d1432331-Reviews-"
        "Hotel_de_los_Faroles-Cordoba_Province_of_Cordoba_Andalucia.html"
    )

    resolved = scraper._resolve_direct_listing_target_url(url)

    assert resolved == url


def test_resolve_direct_listing_target_url_accepts_relative_listing_path() -> None:
    scraper = TripadvisorScraper()
    path = "/Hotel_Review-g187430-d1432331-Reviews-Hotel_de_los_Faroles-Cordoba.html"

    resolved = scraper._resolve_direct_listing_target_url(path)

    assert resolved == f"https://www.tripadvisor.es{path}"


def test_resolve_direct_listing_target_url_rejects_non_tripadvisor_domains() -> None:
    scraper = TripadvisorScraper()
    url = "https://example.com/Hotel_Review-g187430-d1432331-Reviews-Hotel_de_los_Faroles.html"

    resolved = scraper._resolve_direct_listing_target_url(url)

    assert resolved == ""


def test_pick_exact_typeahead_candidate_href_prefers_exact_title_match() -> None:
    scraper = TripadvisorScraper()
    href = scraper._pick_exact_typeahead_candidate_href(
        query="Hotel de los Faroles",
        candidates=[
            ("Casa De Los Faroles", "/Hotel_Review-g187430-d12517375-Reviews-Casa_De_Los_Faroles.html"),
            ("Hotel de los Faroles", "/Hotel_Review-g187430-d1432331-Reviews-Hotel_de_los_Faroles.html"),
        ],
    )

    assert href == "/Hotel_Review-g187430-d1432331-Reviews-Hotel_de_los_Faroles.html"


def test_pick_exact_typeahead_candidate_href_returns_empty_when_not_exact() -> None:
    scraper = TripadvisorScraper()
    href = scraper._pick_exact_typeahead_candidate_href(
        query="Hotel de los Faroles",
        candidates=[
            ("Casa De Los Faroles", "/Hotel_Review-g187430-d12517375-Reviews-Casa_De_Los_Faroles.html"),
            ("Hoteles Faroles Córdoba", "/Hotel_Review-g187430-d9999999-Reviews-Hoteles_Faroles.html"),
        ],
    )

    assert href == ""
