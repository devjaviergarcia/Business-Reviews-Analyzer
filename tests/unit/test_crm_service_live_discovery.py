from __future__ import annotations

import asyncio
from typing import Any

from bson import ObjectId

import src.services.crm_service as crm_service_module
from src.services.business_service import BusinessService
from src.services.crm_service import CRMService
from src.workers.contracts import CRMLeadDiscoveryTaskPayload


class _Dummy:
    pass


class _FakePage:
    def __init__(self) -> None:
        self.url = "https://www.google.com/maps/search/restaurantes+cordoba"
        self.keyboard = _FakeKeyboard()

    async def wait_for_timeout(self, _ms: int) -> None:
        return None

    async def evaluate(self, _script: str) -> dict[str, Any]:
        return {"found": True, "items": []}


class _FakeKeyboard:
    def __init__(self) -> None:
        self.pressed: list[str] = []

    async def press(self, key: str) -> None:
        self.pressed.append(str(key))


class _FakeScraper:
    def __init__(self) -> None:
        self.page = _FakePage()

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeDetailPage:
    def __init__(self) -> None:
        self.url = "https://www.google.com/maps/place/fake"

    async def goto(self, url: str, wait_until: str = "domcontentloaded") -> None:
        _ = wait_until
        self.url = url


class _FakeDetailScraper:
    def __init__(self) -> None:
        self.page = _FakeDetailPage()

    async def _dismiss_google_consent_if_present(self) -> None:
        return None

    async def _wait_for_listing_ready(self, timeout_ms: int = 0) -> None:
        _ = timeout_ms
        return None

    async def extract_listing(self) -> dict[str, Any]:
        return {
            "business_name": "Restaurante El Sella",
            "address": "C. Pureza, 4, 41010 Sevilla",
            "phone": "636 47 45 61",
            "website": "https://www.facebook.com/elsellatriana/",
            "overall_rating": 4.7,
            "total_reviews": 5124,
            "categories": ["Restaurante", "Asturiano"],
        }


class _InsertOneResult:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _FakeLeadsCollection:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self.docs:
            if self._matches(doc, query):
                return dict(doc)
        return None

    async def insert_one(self, doc: dict[str, Any]) -> _InsertOneResult:
        payload = dict(doc)
        if "_id" not in payload:
            payload["_id"] = ObjectId()
        self.docs.append(payload)
        return _InsertOneResult(payload["_id"])

    async def update_one(self, filter_doc: dict[str, Any], update_doc: dict[str, Any]) -> None:
        for index, doc in enumerate(self.docs):
            if not self._matches(doc, filter_doc):
                continue
            updated = dict(doc)
            for key, value in dict(update_doc.get("$set") or {}).items():
                updated[key] = value
            self.docs[index] = updated
            return None

    def _matches(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        if "$and" in query:
            clauses = query.get("$and")
            if not isinstance(clauses, list):
                return False
            return all(self._matches(doc, clause) for clause in clauses if isinstance(clause, dict))
        for key, value in query.items():
            if key == "$and":
                continue
            if doc.get(key) != value:
                return False
        return True


class _FakeDatabase:
    def __init__(self, leads: _FakeLeadsCollection) -> None:
        self._leads = leads

    def __getitem__(self, name: str) -> Any:
        if name == CRMService._LEADS_COLLECTION:
            return self._leads
        raise KeyError(name)


def _service() -> CRMService:
    service = CRMService(job_service=_Dummy(), business_service=_Dummy())
    service._use_repo_v2 = False
    service._use_discovery_v2 = False
    return service


def test_live_google_maps_discovery_auto_scroll_collects_candidates(monkeypatch: Any) -> None:
    service = _service()
    fake_scraper = _FakeScraper()
    calls = {"collect": 0, "scroll": 0}
    enrich_calls = {"count": 0}

    monkeypatch.setattr(BusinessService, "build_default_scraper", classmethod(lambda cls: fake_scraper))

    async def _fake_search(**_kwargs: Any) -> None:
        return None

    async def _fake_wait_feed(**_kwargs: Any) -> bool:
        return True

    async def _fake_collect(**_kwargs: Any) -> list[dict[str, Any]]:
        calls["collect"] += 1
        if calls["collect"] == 1:
            return [{"name": "Restaurante A", "maps_url": "https://maps.google.com/maps/place/a", "source_card_label": "Restaurante"}]
        if calls["collect"] == 2:
            return [
                {"name": "Restaurante A", "maps_url": "https://maps.google.com/maps/place/a", "source_card_label": "Restaurante"},
                {"name": "Restaurante B", "maps_url": "https://maps.google.com/maps/place/b", "source_card_label": "Restaurante"},
            ]
        return [
            {"name": "Restaurante A", "maps_url": "https://maps.google.com/maps/place/a", "source_card_label": "Restaurante"},
            {"name": "Restaurante B", "maps_url": "https://maps.google.com/maps/place/b", "source_card_label": "Restaurante"},
        ]

    async def _fake_scroll(**_kwargs: Any) -> None:
        calls["scroll"] += 1
        return None

    service._search_google_maps_query = _fake_search  # type: ignore[method-assign]
    service._wait_for_results_feed = _fake_wait_feed  # type: ignore[method-assign]
    service._collect_visible_google_maps_results = _fake_collect  # type: ignore[method-assign]
    service._scroll_google_maps_results = _fake_scroll  # type: ignore[method-assign]

    async def _fake_enrich(**kwargs: Any) -> list[dict[str, Any]]:
        enrich_calls["count"] += 1
        candidates = list(kwargs.get("candidates") or [])
        for item in candidates:
            source_ref = dict(item.get("source_ref") or {})
            source_ref["listing_enriched"] = True
            item["source_ref"] = source_ref
        return candidates

    service._enrich_live_google_maps_candidates = _fake_enrich  # type: ignore[method-assign]

    payload = CRMLeadDiscoveryTaskPayload(
        query="restaurantes cordoba",
        city=None,
        category=None,
        limit=10,
        source="live_google_maps",
    )

    candidates = asyncio.run(service._discover_candidates(task_payload=payload))
    assert len(candidates) == 2
    assert calls["scroll"] >= 1
    assert enrich_calls["count"] == 1
    assert all(str(item.get("source")) == "google_maps_live_discovery" for item in candidates)
    assert all(bool((item.get("source_ref") or {}).get("listing_enriched")) for item in candidates)


def test_search_google_maps_query_runs_maps_home_and_consent_first() -> None:
    service = _service()
    call_order: list[str] = []

    class _LocalScraper:
        def __init__(self) -> None:
            self.page = _FakePage()

        async def _go_to_maps_home(self) -> None:
            call_order.append("go_to_maps_home")

        async def _dismiss_google_consent_if_present(self) -> None:
            call_order.append("dismiss_consent")

        async def _human_click(self, _locator: Any) -> None:
            call_order.append("human_click")

        async def _human_type(self, _locator: Any, _text: str) -> None:
            call_order.append("human_type")

    scraper = _LocalScraper()

    async def _fake_first_visible_from_patterns(
        *,
        scraper: Any,
        key: str,
        timeout_ms: int = 0,
    ) -> Any:
        _ = scraper, timeout_ms
        if key == "SEARCH_INPUT":
            return object()
        if key == "SEARCH_BUTTON":
            return object()
        return None

    service._first_visible_from_patterns = _fake_first_visible_from_patterns  # type: ignore[method-assign]

    asyncio.run(service._search_google_maps_query(scraper=scraper, query="restaurantes sevilla"))
    assert call_order[:2] == ["go_to_maps_home", "dismiss_consent"]
    assert "human_type" in call_order
    assert "Control+A" in scraper.page.keyboard.pressed
    assert "Backspace" in scraper.page.keyboard.pressed


def test_collect_visible_google_maps_results_parses_rating_and_reviews() -> None:
    service = _service()

    class _EvalPage(_FakePage):
        async def evaluate(self, _script: str) -> dict[str, Any]:
            return {
                "found": True,
                "items": [
                    {
                        "name": "Restaurante Demo",
                        "maps_url": "https://www.google.com/maps/place/demo",
                        "source_card_label": "Restaurante Demo",
                        "rating_label": "4,6 estrellas 7518 reseñas",
                        "reviews_label": "(7518)",
                    }
                ],
            }

    class _EvalScraper:
        def __init__(self) -> None:
            self.page = _EvalPage()

    items = asyncio.run(service._collect_visible_google_maps_results(scraper=_EvalScraper()))  # type: ignore[arg-type]
    assert len(items) == 1
    assert items[0]["name"] == "Restaurante Demo"
    assert items[0]["rating"] == 4.6
    assert items[0]["review_count"] == 7518


def test_live_google_maps_candidate_enrichment_merges_listing_data() -> None:
    service = _service()
    detail_scraper = _FakeDetailScraper()
    candidate = {
        "business_name": "El Sella",
        "category": None,
        "address": None,
        "city": None,
        "phone": None,
        "email": None,
        "website": None,
        "source": "google_maps_live_discovery",
        "source_ref": {
            "maps_url": "https://www.google.com/maps/place/El+Sella?hl=es",
            "discovery_mode": "live_google_maps_auto_scroll",
        },
        "rating": None,
        "review_count": None,
    }

    enriched = asyncio.run(
        service._enrich_live_google_maps_candidate(  # type: ignore[arg-type]
            detail_scraper=detail_scraper,
            candidate=candidate,
        )
    )

    assert enriched["business_name"] == "Restaurante El Sella"
    assert enriched["address"] == "C. Pureza, 4, 41010 Sevilla"
    assert enriched["phone"] == "636 47 45 61"
    assert enriched["website"] == "https://www.facebook.com/elsellatriana/"
    assert enriched["rating"] == 4.7
    assert enriched["review_count"] == 5124
    assert enriched["category"] == "Restaurante, Asturiano"
    assert enriched["city"] == "41010 Sevilla"
    assert bool((enriched.get("source_ref") or {}).get("listing_enriched"))


def test_process_discovery_task_auto_live_persists_into_crm_leads(monkeypatch: Any) -> None:
    service = _service()
    fake_leads = _FakeLeadsCollection()
    fake_db = _FakeDatabase(fake_leads)

    monkeypatch.setattr(crm_service_module, "get_database", lambda: fake_db)

    async def _fake_indexes() -> None:
        return None

    async def _fake_record_event(**_kwargs: Any) -> None:
        return None

    async def _fake_live_discovery(**_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "business_name": "Restaurante A",
                "category": "restaurante",
                "address": "Calle A, Cordoba",
                "city": "Cordoba",
                "phone": None,
                "email": None,
                "website": "https://restaurante-a.example",
                "source": "google_maps_live_discovery",
                "source_ref": {"maps_url": "https://maps.google.com/maps/place/a"},
                "rating": 4.2,
                "review_count": 100,
            },
            {
                "business_name": "Restaurante B",
                "category": "restaurante",
                "address": "Calle B, Cordoba",
                "city": "Cordoba",
                "phone": None,
                "email": None,
                "website": "https://restaurante-b.example",
                "source": "google_maps_live_discovery",
                "source_ref": {"maps_url": "https://maps.google.com/maps/place/b"},
                "rating": 4.0,
                "review_count": 60,
            },
        ]

    service.ensure_indexes = _fake_indexes  # type: ignore[method-assign]
    service._record_event = _fake_record_event  # type: ignore[method-assign]
    service._discover_candidates_live_google_maps = _fake_live_discovery  # type: ignore[method-assign]

    payload = CRMLeadDiscoveryTaskPayload(
        query="restaurantes cordoba",
        city=None,
        category=None,
        limit=100,
        source="live_google_maps",
    )
    result = asyncio.run(service.process_discovery_task(task_payload=payload, job_id="job-live-1"))

    assert result["source"] == "live_google_maps"
    assert int(result["candidates"]) == 2
    assert int(result["inserted"]) == 2
    assert len(fake_leads.docs) == 2
    assert all(str(doc.get("source")) == "google_maps_live_discovery" for doc in fake_leads.docs)
    assert all(isinstance(doc.get("rating"), (int, float)) for doc in fake_leads.docs)
    assert all(isinstance(doc.get("review_count"), int) for doc in fake_leads.docs)


def test_discover_candidates_auto_alias_uses_live_flow(monkeypatch: Any) -> None:
    service = _service()
    calls = {"live": 0, "stored": 0}

    async def _fake_live(**_kwargs: Any) -> list[dict[str, Any]]:
        calls["live"] += 1
        return [
            {
                "business_name": "Cafe Demo",
                "source": "google_maps_live_discovery",
                "source_ref": {"listing_enriched": True},
            }
        ]

    async def _fake_stored(**_kwargs: Any) -> list[dict[str, Any]]:
        calls["stored"] += 1
        return []

    service._discover_candidates_live_google_maps = _fake_live  # type: ignore[method-assign]
    service._discover_candidates_from_stored_sources = _fake_stored  # type: ignore[method-assign]

    payload = CRMLeadDiscoveryTaskPayload(
        query="cafeterias madrid",
        city=None,
        category=None,
        limit=1,
        source="auto",
    )
    result = asyncio.run(service._discover_candidates(task_payload=payload))

    assert calls["live"] == 1
    assert calls["stored"] == 0
    assert len(result) == 1
    assert result[0]["business_name"] == "Cafe Demo"
