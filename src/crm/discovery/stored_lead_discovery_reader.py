from __future__ import annotations

import re
from typing import Any, Callable

from src.workers.contracts import CRMLeadDiscoveryTaskPayload


DatabaseFactory = Callable[[], Any]
NormalizeTextFn = Callable[[Any], str]
ExtractCityFromAddressFn = Callable[[str | None], str | None]


class StoredLeadDiscoveryReader:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        research_leads_collection_name: str,
        businesses_collection_name: str,
        normalize_text: NormalizeTextFn,
        extract_city_from_address: ExtractCityFromAddressFn,
    ) -> None:
        self._database_factory = database_factory
        self._research_leads_collection_name = research_leads_collection_name
        self._businesses_collection_name = businesses_collection_name
        self._normalize_text = normalize_text
        self._extract_city_from_address = extract_city_from_address

    async def discover_candidates_from_stored_sources(
        self,
        *,
        task_payload: CRMLeadDiscoveryTaskPayload,
        normalized_query: str,
        normalized_city: str | None,
        normalized_category: str | None,
        safe_limit: int,
    ) -> list[dict[str, Any]]:
        database = self._database_factory()
        candidates: list[dict[str, Any]] = []
        sources_to_scan = self._resolve_sources_to_scan(task_payload=task_payload)

        if "research" in sources_to_scan:
            research = database[self._research_leads_collection_name]
            research_query: dict[str, Any] = {}
            if normalized_category:
                research_query["category"] = {"$regex": re.escape(task_payload.category or ""), "$options": "i"}
            if normalized_city:
                research_query["$or"] = [
                    {"address": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                    {"term_key": {"$regex": re.escape(task_payload.city or ""), "$options": "i"}},
                ]

            raw_docs = (
                await research.find(research_query)
                .sort([("processed_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in raw_docs:
                candidate = self._build_research_candidate(
                    doc=doc,
                    normalized_query=normalized_query,
                )
                if candidate is None:
                    continue
                candidates.append(candidate)
                if len(candidates) >= safe_limit:
                    return candidates

        if "businesses" in sources_to_scan and len(candidates) < safe_limit:
            businesses = database[self._businesses_collection_name]
            business_query: dict[str, Any] = {}
            if normalized_category:
                business_query["listing.categories"] = {
                    "$regex": re.escape(task_payload.category or ""),
                    "$options": "i",
                }

            business_docs = (
                await businesses.find(business_query)
                .sort([("updated_at", -1), ("_id", -1)])
                .limit(safe_limit * 2)
                .to_list(length=safe_limit * 2)
            )
            for doc in business_docs:
                candidate = self._build_existing_business_candidate(
                    doc=doc,
                    normalized_query=normalized_query,
                    normalized_city=normalized_city,
                )
                if candidate is None:
                    continue
                candidates.append(candidate)
                if len(candidates) >= safe_limit:
                    return candidates

        return candidates[:safe_limit]

    def _resolve_sources_to_scan(self, *, task_payload: CRMLeadDiscoveryTaskPayload) -> list[str]:
        normalized_source = str(task_payload.source or "").strip().lower()
        if normalized_source in {"research_google_maps", "research"}:
            return ["research"]
        if normalized_source in {"businesses", "existing_businesses"}:
            return ["businesses"]
        return ["research", "businesses"]

    def _build_research_candidate(
        self,
        *,
        doc: dict[str, Any],
        normalized_query: str,
    ) -> dict[str, Any] | None:
        name = str(doc.get("name") or "").strip()
        if not name:
            return None

        searchable = self._normalize_text(
            " ".join(
                [
                    name,
                    str(doc.get("address") or ""),
                    str(doc.get("category") or ""),
                    str(doc.get("term_key") or ""),
                ]
            )
        )
        if normalized_query and normalized_query not in searchable:
            return None

        address = str(doc.get("address") or "").strip() or None
        return {
            "business_name": name,
            "category": str(doc.get("category") or "").strip() or None,
            "address": address,
            "city": self._extract_city_from_address(address),
            "phone": str(doc.get("phone") or "").strip() or None,
            "email": str(doc.get("email") or "").strip() or None,
            "website": str(doc.get("website") or "").strip() or None,
            "source": "research_google_maps",
            "source_ref": {
                "listing_id": str(doc.get("listing_id") or "").strip() or None,
                "term_id": str(doc.get("term_id") or "").strip() or None,
                "term_key": str(doc.get("term_key") or "").strip() or None,
                "maps_url": str(doc.get("maps_url") or "").strip() or None,
            },
            "rating": doc.get("rating"),
            "review_count": doc.get("review_count"),
        }

    def _build_existing_business_candidate(
        self,
        *,
        doc: dict[str, Any],
        normalized_query: str,
        normalized_city: str | None,
    ) -> dict[str, Any] | None:
        name = str(doc.get("name") or "").strip()
        if not name:
            return None

        listing = doc.get("listing") if isinstance(doc.get("listing"), dict) else {}
        address = str(listing.get("address") or "").strip() or None
        searchable = self._normalize_text(
            " ".join(
                [
                    name,
                    str(address or ""),
                    " ".join(
                        [str(item) for item in (listing.get("categories") or []) if str(item).strip()]
                    ),
                ]
            )
        )
        if normalized_query and normalized_query not in searchable:
            return None
        if normalized_city and normalized_city not in self._normalize_text(address):
            return None

        return {
            "business_name": name,
            "category": ", ".join(
                [str(item) for item in (listing.get("categories") or []) if str(item).strip()]
            )
            or None,
            "address": address,
            "city": self._extract_city_from_address(address),
            "phone": str(listing.get("phone") or "").strip() or None,
            "email": None,
            "website": str(listing.get("website") or "").strip() or None,
            "source": str(doc.get("source") or "google_maps"),
            "source_ref": {
                "business_id": str(doc.get("_id")),
                "name_normalized": str(doc.get("name_normalized") or "").strip() or None,
            },
            "rating": listing.get("overall_rating"),
            "review_count": listing.get("total_reviews"),
        }
