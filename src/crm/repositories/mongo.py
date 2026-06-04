from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.crm.benchmark import CityGeoPoints, load_all_city_geo_points
from src.database import get_database
from src.models.benchmark import (
    BenchmarkBusiness,
    BenchmarkRun,
    BenchmarkRunStatus,
    CompetitorCandidate,
    CompetitorSet,
    LeadReport,
    PaidReport,
)
from src.models.crm import CRMDiscoveryRun, CRMDiscoveryRunStatus
from src.models.geo_grid import GeoCity, GeoGridResult, GeoGridRun, GeoGridRunStatus
from src.services.pagination import build_pagination_payload, coerce_pagination


class MongoLeadRepository:
    COLLECTION = "crm_leads"

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
        sort_by: str,
        sort_dir: str,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        collection = get_database()[self.COLLECTION]
        query = self._build_query(
            status_filter=status_filter,
            consent_filter=consent_filter,
            source_filter=source_filter,
            q=q,
        )
        sort_spec = self._resolve_sort(sort_by=sort_by, sort_dir=sort_dir)

        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_doc(doc, id_key="lead_id") for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    async def get_by_id(self, *, lead_id: str) -> dict[str, Any] | None:
        collection = get_database()[self.COLLECTION]
        parsed = self._parse_object_id(lead_id, field_name="lead_id")
        return await collection.find_one({"_id": parsed})

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        return await get_database()[self.COLLECTION].find_one(query)

    async def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> Any:
        return await get_database()[self.COLLECTION].update_one(query, update)

    async def find_one_and_update(self, query: dict[str, Any], update: dict[str, Any]) -> dict[str, Any] | None:
        return await get_database()[self.COLLECTION].find_one_and_update(
            query,
            update,
            return_document=ReturnDocument.AFTER,
        )

    async def bulk_delete(
        self,
        *,
        lead_ids: list[str] | None,
        delete_all_matching: bool,
        exclude_lead_ids: list[str] | None,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        collection = get_database()[self.COLLECTION]

        normalized_ids: list[ObjectId] = []
        seen_ids: set[str] = set()
        for raw_id in list(lead_ids or []):
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_ids:
                continue
            seen_ids.add(normalized)
            normalized_ids.append(self._parse_object_id(normalized, field_name="lead_id"))

        excluded_ids: list[ObjectId] = []
        seen_excluded_ids: set[str] = set()
        for raw_id in list(exclude_lead_ids or []):
            normalized = str(raw_id or "").strip()
            if not normalized or normalized in seen_excluded_ids:
                continue
            seen_excluded_ids.add(normalized)
            excluded_ids.append(self._parse_object_id(normalized, field_name="exclude_lead_id"))

        if not normalized_ids and not bool(delete_all_matching):
            raise ValueError("Specify lead_ids or set delete_all_matching=true.")

        if normalized_ids:
            query: dict[str, Any] = {"_id": {"$in": normalized_ids}}
        else:
            query = self._build_query(
                status_filter=status_filter,
                consent_filter=consent_filter,
                source_filter=source_filter,
                q=q,
            )
            if excluded_ids:
                query["_id"] = {"$nin": excluded_ids}

        matched_count = await collection.count_documents(query)
        deleted_result = await collection.delete_many(query)

        return {
            "deleted_count": int(deleted_result.deleted_count),
            "matched_count": int(matched_count),
            "delete_all_matching": bool(delete_all_matching),
            "requested_ids": len(normalized_ids),
            "excluded_ids": len(excluded_ids),
            "filters": {
                "status": str(status_filter or "").strip() or None,
                "consent_status": str(consent_filter or "").strip() or None,
                "source": str(source_filter or "").strip() or None,
                "q": str(q or "").strip() or None,
            },
        }

    def _build_query(
        self,
        *,
        status_filter: str | None,
        consent_filter: str | None,
        source_filter: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        normalized_status = str(status_filter or "").strip().lower()
        if normalized_status:
            query["status"] = normalized_status

        normalized_consent = str(consent_filter or "").strip().lower()
        if normalized_consent:
            query["legal.consent_status"] = normalized_consent

        normalized_source = str(source_filter or "").strip().lower()
        if normalized_source:
            query["source"] = normalized_source

        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"email": {"$regex": escaped, "$options": "i"}},
                {"website": {"$regex": escaped, "$options": "i"}},
                {"city": {"$regex": escaped, "$options": "i"}},
            ]
        return query

    def _resolve_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        normalized_sort_by = str(sort_by or "updated_at").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("Invalid sort_dir. Use 'asc' or 'desc'.")

        field_map = {
            "updated_at": "updated_at",
            "business_name": "business_name_normalized",
            "score": "score",
            "status": "status",
            "consent_status": "legal.consent_status",
            "source": "source",
        }
        field_name = field_map.get(normalized_sort_by)
        if field_name is None:
            raise ValueError(
                "Invalid sort_by. Use 'updated_at', 'business_name', 'score', 'status', 'consent_status' or 'source'."
            )

        direction = -1 if normalized_sort_dir == "desc" else 1
        return [(field_name, direction), ("_id", direction)]

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def _serialize_doc(self, doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop("_id"))
        return payload


class MongoEventRepository:
    COLLECTION = "crm_events"

    async def insert(self, event_doc: dict[str, Any]) -> Any:
        return await get_database()[self.COLLECTION].insert_one(event_doc)

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        lead_id: str | None,
        campaign_id: str | None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        collection = get_database()[self.COLLECTION]

        query: dict[str, Any] = {}
        if lead_id:
            query["lead_id"] = str(lead_id).strip()
        if campaign_id:
            query["campaign_id"] = str(campaign_id).strip()

        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )

        items: list[dict[str, Any]] = []
        for doc in docs:
            payload = dict(doc)
            payload["event_id"] = str(payload.pop("_id"))
            items.append(payload)
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)


class MongoCampaignRepository:
    COLLECTION = "crm_campaigns"

    async def collection(self) -> Any:
        return get_database()[self.COLLECTION]


class MongoMessageRepository:
    COLLECTION = "crm_messages"

    async def collection(self) -> Any:
        return get_database()[self.COLLECTION]


class MongoSuppressionRepository:
    COLLECTION = "crm_suppressions"

    async def collection(self) -> Any:
        return get_database()[self.COLLECTION]


class MongoDiscoveryRunRepository:
    COLLECTION = "crm_discovery_runs"

    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        run_model = CRMDiscoveryRun(
            job_id=str(payload.get("job_id") or "").strip() or None,
            query=str(payload.get("query") or "").strip(),
            city=str(payload.get("city") or "").strip() or None,
            category=str(payload.get("category") or "").strip() or None,
            source=str(payload.get("source") or "auto_live_google_maps").strip() or "auto_live_google_maps",
            limit=int(payload.get("limit") or 100),
            status=CRMDiscoveryRunStatus.QUEUED,
            metrics=dict(payload.get("metrics") or {}),
            steps=list(payload.get("steps") or []),
            failure_reason=None,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        doc = run_model.model_dump(mode="python")
        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return self._serialize_run(doc)

    async def mark_running(self, *, run_id: str) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": self._parse_object_id(run_id)},
            {
                "$set": {
                    "status": CRMDiscoveryRunStatus.RUNNING.value,
                    "started_at": now,
                    "updated_at": now,
                    "failure_reason": None,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def append_step(
        self,
        *,
        run_id: str,
        step: str,
        ok: bool,
        duration_ms: int,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        step_payload = {
            "step": str(step or "step").strip(),
            "ok": bool(ok),
            "duration_ms": max(0, int(duration_ms)),
            "data": dict(data or {}),
            "error": str(error).strip() if error else None,
            "created_at": now,
        }
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": self._parse_object_id(run_id)},
            {
                "$push": {"steps": step_payload},
                "$set": {"updated_at": now},
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def finalize(
        self,
        *,
        run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        normalized_status = str(status or CRMDiscoveryRunStatus.FAILED.value).strip().lower()
        if normalized_status not in {
            CRMDiscoveryRunStatus.PARTIAL.value,
            CRMDiscoveryRunStatus.COMPLETED.value,
            CRMDiscoveryRunStatus.FAILED.value,
        }:
            normalized_status = CRMDiscoveryRunStatus.FAILED.value

        set_fields: dict[str, Any] = {
            "status": normalized_status,
            "updated_at": now,
            "finished_at": now,
        }
        if metrics is not None:
            set_fields["metrics"] = dict(metrics)
        set_fields["failure_reason"] = str(failure_reason).strip() if failure_reason else None

        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": self._parse_object_id(run_id)},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def list_runs(self, *, page: int, page_size: int) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        collection = get_database()[self.COLLECTION]
        total = await collection.count_documents({})
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find({})
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_run(doc) for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    async def get_run(self, *, run_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one({"_id": self._parse_object_id(run_id)})
        return self._serialize_run(doc) if doc else None

    def _serialize_run(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["discovery_run_id"] = str(payload.pop("_id"))
        return payload

    def _parse_object_id(self, value: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError("Invalid discovery_run_id. Expected a Mongo ObjectId string.") from exc


class MongoBenchmarkRunRepository:
    COLLECTION = "benchmark_runs"

    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        run_model = BenchmarkRun(
            title=str(payload.get("title") or "").strip() or None,
            query=str(payload.get("query") or "").strip(),
            city=str(payload.get("city") or "").strip() or None,
            category=str(payload.get("category") or "").strip() or None,
            source=str(payload.get("source") or "auto_live_google_maps").strip() or "auto_live_google_maps",
            limit=int(payload.get("limit") or 100),
            status=BenchmarkRunStatus.QUEUED,
            metrics=dict(payload.get("metrics") or {}),
            failure_reason=None,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        doc = run_model.model_dump(mode="python")
        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return self._serialize_run(doc)

    async def mark_running(self, *, benchmark_run_id: str) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(benchmark_run_id, field_name="benchmark_run_id")},
            {
                "$set": {
                    "status": BenchmarkRunStatus.RUNNING.value,
                    "started_at": now,
                    "updated_at": now,
                    "failure_reason": None,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def finalize(
        self,
        *,
        benchmark_run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        normalized_status = str(status or BenchmarkRunStatus.FAILED.value).strip().lower()
        if normalized_status not in {
            BenchmarkRunStatus.PARTIAL.value,
            BenchmarkRunStatus.COMPLETED.value,
            BenchmarkRunStatus.FAILED.value,
        }:
            normalized_status = BenchmarkRunStatus.FAILED.value

        set_fields: dict[str, Any] = {
            "status": normalized_status,
            "updated_at": now,
            "finished_at": now,
            "failure_reason": str(failure_reason).strip() if failure_reason else None,
        }
        if metrics is not None:
            set_fields["metrics"] = dict(metrics)

        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(benchmark_run_id, field_name="benchmark_run_id")},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        status_filter: str | None = None,
        city: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query: dict[str, Any] = {}
        if str(status_filter or "").strip():
            query["status"] = str(status_filter).strip().lower()
        if str(city or "").strip():
            query["city"] = str(city).strip()
        if str(category or "").strip():
            query["category"] = str(category).strip()

        collection = get_database()[self.COLLECTION]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_run(doc) for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    async def get_run(self, *, benchmark_run_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {"_id": _parse_object_id(benchmark_run_id, field_name="benchmark_run_id")}
        )
        return self._serialize_run(doc) if doc else None

    def _serialize_run(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["benchmark_run_id"] = str(payload.pop("_id"))
        return payload


class MongoBenchmarkBusinessRepository:
    COLLECTION = "benchmark_businesses"

    async def upsert_business(self, *, benchmark_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        business_name = str(payload.get("business_name") or payload.get("name") or "").strip()
        if not business_name:
            return {"action": "skipped", "business": None}

        business_name_normalized = str(payload.get("business_name_normalized") or "").strip()
        if not business_name_normalized:
            business_name_normalized = _normalize_text(business_name)

        maps_url = str(payload.get("maps_url") or "").strip() or None
        source_ref = dict(payload.get("source_ref") or {})
        maps_url_canonical = (
            str(payload.get("maps_url_canonical") or source_ref.get("maps_url_canonical") or "").strip()
            or _canonicalize_maps_url(maps_url or "")
            or None
        )
        address = str(payload.get("address") or "").strip() or None
        discovery_rank = _coerce_positive_int(
            payload.get("discovery_rank")
            or source_ref.get("discovery_rank")
            or payload.get("search_position")
            or source_ref.get("search_position")
        )
        rank_visibility_score = _score_from_discovery_rank(discovery_rank)
        explicit_visibility_score = _coerce_score(payload.get("visibility_score"))
        visibility_score = _blend_score(explicit_visibility_score, rank_visibility_score, explicit_weight=0.60)
        explicit_opportunity_score = _coerce_score(payload.get("opportunity_score"))
        rank_opportunity_score = 100.0 - rank_visibility_score if rank_visibility_score is not None else None
        opportunity_score = _blend_score(explicit_opportunity_score, rank_opportunity_score, explicit_weight=0.85)

        model = BenchmarkBusiness(
            benchmark_id=str(benchmark_id).strip(),
            lead_id=str(payload.get("lead_id") or "").strip() or None,
            business_name=business_name,
            business_name_normalized=business_name_normalized,
            category=str(payload.get("category") or "").strip() or None,
            city=str(payload.get("city") or "").strip() or None,
            address=address,
            maps_url=maps_url,
            maps_url_canonical=maps_url_canonical,
            phone=str(payload.get("phone") or "").strip() or None,
            website=str(payload.get("website") or "").strip() or None,
            source=str(payload.get("source") or "google_maps_live_discovery").strip() or "google_maps_live_discovery",
            source_ref=source_ref,
            discovery_rank=discovery_rank,
            rating=_coerce_float(payload.get("rating")),
            review_count=_coerce_int(payload.get("review_count")),
            opportunity_score=opportunity_score,
            reputation_score=_coerce_score(payload.get("reputation_score")),
            visibility_score=visibility_score,
            conversion_risk_score=_coerce_score(payload.get("conversion_risk_score")),
            listing_enriched=bool(payload.get("listing_enriched") or source_ref.get("listing_enriched")),
            raw_snapshot=dict(payload.get("raw_snapshot") or payload),
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")

        query = self._build_upsert_query(
            benchmark_id=str(benchmark_id).strip(),
            maps_url_canonical=maps_url_canonical,
            business_name_normalized=business_name_normalized,
            address=address,
        )
        existing = await get_database()[self.COLLECTION].find_one(query)
        if existing:
            set_fields = dict(doc)
            set_fields.pop("created_at", None)
            set_fields["updated_at"] = now
            updated = await get_database()[self.COLLECTION].find_one_and_update(
                {"_id": existing["_id"]},
                {"$set": set_fields},
                return_document=ReturnDocument.AFTER,
            )
            return {"action": "updated", "business": self._serialize_business(updated)}

        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return {"action": "inserted", "business": self._serialize_business(doc)}

    async def get_business(self, *, benchmark_business_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {"_id": _parse_object_id(benchmark_business_id, field_name="benchmark_business_id")}
        )
        return self._serialize_business(doc) if doc else None

    async def list_businesses(
        self,
        *,
        benchmark_id: str | None,
        page: int,
        page_size: int,
        city: str | None = None,
        category: str | None = None,
        q: str | None = None,
        sort_by: str = "opportunity_score",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query = self._build_query(benchmark_id=benchmark_id, city=city, category=category, q=q)
        sort_spec = self._resolve_sort(sort_by=sort_by, sort_dir=sort_dir)
        collection = get_database()[self.COLLECTION]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_business(doc) for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    def _build_upsert_query(
        self,
        *,
        benchmark_id: str,
        maps_url_canonical: str | None,
        business_name_normalized: str,
        address: str | None,
    ) -> dict[str, Any]:
        if maps_url_canonical:
            return {"benchmark_id": benchmark_id, "maps_url_canonical": maps_url_canonical}
        clauses: list[dict[str, Any]] = [
            {"benchmark_id": benchmark_id},
            {"business_name_normalized": business_name_normalized},
        ]
        if address:
            clauses.append({"address": address})
        return {"$and": clauses}

    def _build_query(
        self,
        *,
        benchmark_id: str | None,
        city: str | None,
        category: str | None,
        q: str | None,
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if str(benchmark_id or "").strip():
            query["benchmark_id"] = str(benchmark_id).strip()
        if str(city or "").strip():
            query["city"] = str(city).strip()
        if str(category or "").strip():
            query["category"] = str(category).strip()
        normalized_q = str(q or "").strip()
        if normalized_q:
            escaped = re.escape(normalized_q)
            query["$or"] = [
                {"business_name": {"$regex": escaped, "$options": "i"}},
                {"category": {"$regex": escaped, "$options": "i"}},
                {"address": {"$regex": escaped, "$options": "i"}},
            ]
        return query

    def _resolve_sort(self, *, sort_by: str, sort_dir: str) -> list[tuple[str, int]]:
        normalized_sort_by = str(sort_by or "opportunity_score").strip().lower()
        normalized_sort_dir = str(sort_dir or "desc").strip().lower()
        if normalized_sort_dir not in {"asc", "desc"}:
            raise ValueError("Invalid sort_dir. Use 'asc' or 'desc'.")
        field_map = {
            "updated_at": "updated_at",
            "business_name": "business_name_normalized",
            "discovery_rank": "discovery_rank",
            "search_position": "discovery_rank",
            "rating": "rating",
            "review_count": "review_count",
            "opportunity_score": "opportunity_score",
            "reputation_score": "reputation_score",
            "visibility_score": "visibility_score",
            "conversion_risk_score": "conversion_risk_score",
        }
        field_name = field_map.get(normalized_sort_by)
        if field_name is None:
            raise ValueError(
                "Invalid sort_by. Use 'updated_at', 'business_name', 'rating', 'review_count', "
                "'discovery_rank', 'opportunity_score', 'reputation_score', 'visibility_score' "
                "or 'conversion_risk_score'."
            )
        direction = -1 if normalized_sort_dir == "desc" else 1
        return [(field_name, direction), ("_id", direction)]

    def _serialize_business(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["benchmark_business_id"] = str(payload.pop("_id"))
        return payload


class MongoCompetitorSetRepository:
    COLLECTION = "competitor_sets"

    async def upsert_set(
        self,
        *,
        benchmark_id: str,
        target_business_id: str,
        competitors: list[dict[str, Any]],
        selection_version: str = "v1",
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        candidate_models = [CompetitorCandidate(**dict(item)) for item in competitors]
        existing = await get_database()[self.COLLECTION].find_one(
            {"benchmark_id": str(benchmark_id).strip(), "target_business_id": str(target_business_id).strip()}
        )
        if existing:
            updated = await get_database()[self.COLLECTION].find_one_and_update(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "competitors": [item.model_dump(mode="python") for item in candidate_models],
                        "selection_version": str(selection_version or "v1").strip() or "v1",
                        "updated_at": now,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
            return {"action": "updated", "competitor_set": self._serialize_set(updated)}

        model = CompetitorSet(
            benchmark_id=str(benchmark_id).strip(),
            target_business_id=str(target_business_id).strip(),
            competitors=candidate_models,
            selection_version=str(selection_version or "v1").strip() or "v1",
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")
        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return {"action": "inserted", "competitor_set": self._serialize_set(doc)}

    async def get_for_business(self, *, target_business_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one({"target_business_id": str(target_business_id).strip()})
        return self._serialize_set(doc) if doc else None

    async def list_by_benchmark(self, *, benchmark_id: str, page: int, page_size: int) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query = {"benchmark_id": str(benchmark_id).strip()}
        collection = get_database()[self.COLLECTION]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_set(doc) for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    def _serialize_set(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["competitor_set_id"] = str(payload.pop("_id"))
        return payload


class MongoLeadReportRepository:
    COLLECTION = "lead_reports"

    async def upsert_for_business(
        self,
        *,
        benchmark_business_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        report_type = "lead"
        model = LeadReport(
            benchmark_id=str(payload.get("benchmark_id") or "").strip() or None,
            benchmark_business_id=str(benchmark_business_id or payload.get("benchmark_business_id") or "").strip(),
            report_type="lead",
            business_name=str(payload.get("business_name") or "").strip(),
            html=str(payload.get("html") or "").strip(),
            deep_study_snapshot=dict(payload.get("deep_study_snapshot") or {}),
            source_payload=dict(payload.get("source_payload") or {}),
            cta=dict(payload.get("cta") or {}),
            status="generated",
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")
        existing = await get_database()[self.COLLECTION].find_one(
            {
                "benchmark_business_id": model.benchmark_business_id,
                "report_type": report_type,
            }
        )
        if existing:
            set_fields = dict(doc)
            set_fields.pop("created_at", None)
            set_fields["updated_at"] = now
            updated = await get_database()[self.COLLECTION].find_one_and_update(
                {"_id": existing["_id"]},
                {"$set": set_fields},
                return_document=ReturnDocument.AFTER,
            )
            return {"action": "updated", "lead_report": self._serialize_report(updated)}

        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return {"action": "inserted", "lead_report": self._serialize_report(doc)}

    async def get_for_business(
        self,
        *,
        benchmark_business_id: str,
        report_type: str = "lead",
    ) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {
                "benchmark_business_id": str(benchmark_business_id).strip(),
                "report_type": str(report_type or "lead").strip() or "lead",
            }
        )
        return self._serialize_report(doc) if doc else None

    async def get_report(self, *, lead_report_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {"_id": _parse_object_id(lead_report_id, field_name="lead_report_id")}
        )
        return self._serialize_report(doc) if doc else None

    def _serialize_report(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["lead_report_id"] = str(payload.pop("_id"))
        return payload


class MongoPaidReportRepository:
    COLLECTION = "paid_reports"

    async def upsert_for_business_month(
        self,
        *,
        benchmark_business_id: str,
        report_month: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        normalized_month = str(report_month or now.strftime("%Y-%m")).strip() or now.strftime("%Y-%m")
        model = PaidReport(
            benchmark_id=str(payload.get("benchmark_id") or "").strip() or None,
            benchmark_business_id=str(benchmark_business_id or payload.get("benchmark_business_id") or "").strip(),
            report_month=normalized_month,
            business_name=str(payload.get("business_name") or "").strip(),
            html=str(payload.get("html") or "").strip(),
            deep_study_snapshot=dict(payload.get("deep_study_snapshot") or {}),
            source_payload=dict(payload.get("source_payload") or {}),
            history=[dict(item) for item in payload.get("history") or [] if isinstance(item, dict)],
            cta=dict(payload.get("cta") or {}),
            status="generated",
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")
        existing = await get_database()[self.COLLECTION].find_one(
            {
                "benchmark_business_id": model.benchmark_business_id,
                "report_month": model.report_month,
            }
        )
        if existing:
            set_fields = dict(doc)
            set_fields.pop("created_at", None)
            set_fields["updated_at"] = now
            updated = await get_database()[self.COLLECTION].find_one_and_update(
                {"_id": existing["_id"]},
                {"$set": set_fields},
                return_document=ReturnDocument.AFTER,
            )
            return {"action": "updated", "paid_report": self._serialize_report(updated)}

        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return {"action": "inserted", "paid_report": self._serialize_report(doc)}

    async def get_for_business_month(
        self,
        *,
        benchmark_business_id: str,
        report_month: str,
    ) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {
                "benchmark_business_id": str(benchmark_business_id).strip(),
                "report_month": str(report_month).strip(),
            }
        )
        return self._serialize_report(doc) if doc else None

    async def get_report(self, *, paid_report_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {"_id": _parse_object_id(paid_report_id, field_name="paid_report_id")}
        )
        return self._serialize_report(doc) if doc else None

    def _serialize_report(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["paid_report_id"] = str(payload.pop("_id"))
        return payload


class MongoGeoCityRepository:
    COLLECTION = "geo_cities"

    async def seed_default_cities(self) -> dict[str, Any]:
        seeded: list[dict[str, Any]] = []
        for city_points in load_all_city_geo_points():
            seeded.append(await self.upsert_from_geo_points(city_points=city_points))
        return {"seeded": seeded, "count": len(seeded)}

    async def upsert_from_geo_points(self, *, city_points: CityGeoPoints) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        city_slug = _slugify_geo_city(city_points.city)
        model = GeoCity(
            city=city_points.city,
            city_slug=city_slug,
            center=dict(city_points.center),
            points=[point.to_dict() for point in city_points.points],
            point_count=len(city_points.points),
            enabled=True,
            source="data/geo_points",
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")
        result = await get_database()[self.COLLECTION].update_one(
            {"city_slug": city_slug},
            {
                "$set": {
                    "city": doc["city"],
                    "center": doc["center"],
                    "points": doc["points"],
                    "point_count": doc["point_count"],
                    "enabled": doc["enabled"],
                    "source": doc["source"],
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "city_slug": city_slug,
                    "created_at": now,
                },
            },
            upsert=True,
        )
        return {
            "city": city_points.city,
            "city_slug": city_slug,
            "point_count": len(city_points.points),
            "inserted": bool(result.upserted_id),
            "matched": int(result.matched_count),
            "modified": int(result.modified_count),
        }

    async def list_enabled(self) -> list[dict[str, Any]]:
        docs = (
            await get_database()[self.COLLECTION]
            .find({"enabled": True})
            .sort([("city", 1), ("_id", 1)])
            .to_list(length=200)
        )
        return [self._serialize_city(doc) for doc in docs]

    async def get_by_slug(self, *, city_slug: str) -> dict[str, Any] | None:
        normalized_slug = _slugify_geo_city(city_slug)
        doc = await get_database()[self.COLLECTION].find_one({"city_slug": normalized_slug, "enabled": True})
        return self._serialize_city(doc) if doc else None

    def _serialize_city(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["geo_city_id"] = str(payload.pop("_id"))
        return payload


class MongoGeoGridRunRepository:
    COLLECTION = "geo_grid_runs"

    async def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        top_n = max(1, min(100, int(payload.get("top_n") or 10)))
        points = payload.get("points") if isinstance(payload.get("points"), list) else []
        point_count = int(payload.get("point_count") or len(points))
        total_units = max(0, point_count * top_n)
        provider_mode = str(payload.get("provider_mode") or "maps_live").strip().lower() or "maps_live"
        grid_size = _coerce_positive_int(payload.get("grid_size"))
        grid_spacing_km = _coerce_float(payload.get("grid_spacing_km"))
        uule_radius_m = _coerce_positive_int(payload.get("uule_radius_m"))
        throttle_ms = _coerce_positive_int(payload.get("throttle_ms"))
        model = GeoGridRun(
            keyword=str(payload.get("keyword") or "").strip(),
            city=str(payload.get("city") or "").strip(),
            city_slug=_slugify_geo_city(payload.get("city_slug") or payload.get("city") or ""),
            center=dict(payload.get("center") or {}),
            provider_mode=provider_mode,
            grid_size=grid_size,
            grid_spacing_km=grid_spacing_km,
            uule_radius_m=uule_radius_m,
            throttle_ms=throttle_ms,
            top_n=top_n,
            point_count=point_count,
            total_units=total_units,
            completed_units=0,
            completed_points=0,
            status=GeoGridRunStatus.QUEUED,
            metrics={
                "point_count": point_count,
                "top_n": top_n,
                "total_units": total_units,
                "points_completed": 0,
                "results_found": 0,
                "points_failed": 0,
                "provider_mode": provider_mode,
                "grid_size": grid_size,
                "grid_spacing_km": grid_spacing_km,
                "uule_radius_m": uule_radius_m,
                "throttle_ms": throttle_ms,
            },
            created_at=now,
            updated_at=now,
        )
        doc = model.model_dump(mode="python")
        result = await get_database()[self.COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        return self._serialize_run(doc)

    async def set_job_id(self, *, geo_grid_run_id: str, job_id: str | None) -> dict[str, Any] | None:
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")},
            {"$set": {"job_id": str(job_id).strip() if job_id else None, "updated_at": datetime.now(timezone.utc)}},
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def mark_running(self, *, geo_grid_run_id: str) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")},
            {
                "$set": {
                    "status": GeoGridRunStatus.RUNNING.value,
                    "started_at": now,
                    "updated_at": now,
                    "failure_reason": None,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def update_progress(
        self,
        *,
        geo_grid_run_id: str,
        completed_points: int,
        completed_units: int,
        metrics: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        set_fields: dict[str, Any] = {
            "status": GeoGridRunStatus.RUNNING.value,
            "completed_points": max(0, int(completed_points)),
            "completed_units": max(0, int(completed_units)),
            "updated_at": now,
        }
        if metrics is not None:
            set_fields["metrics"] = dict(metrics)
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def finalize(
        self,
        *,
        geo_grid_run_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        normalized_status = str(status or GeoGridRunStatus.FAILED.value).strip().lower()
        if normalized_status not in {
            GeoGridRunStatus.PARTIAL.value,
            GeoGridRunStatus.COMPLETED.value,
            GeoGridRunStatus.FAILED.value,
        }:
            normalized_status = GeoGridRunStatus.FAILED.value
        set_fields: dict[str, Any] = {
            "status": normalized_status,
            "updated_at": now,
            "finished_at": now,
            "failure_reason": str(failure_reason).strip() if failure_reason else None,
        }
        if metrics is not None:
            set_fields["metrics"] = dict(metrics)
        updated = await get_database()[self.COLLECTION].find_one_and_update(
            {"_id": _parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")},
            {"$set": set_fields},
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize_run(updated) if updated else None

    async def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        city_slug: str | None = None,
        status_filter: str | None = None,
    ) -> dict[str, Any]:
        page_value, page_size_value = coerce_pagination(page=page, page_size=page_size, max_page_size=200)
        query: dict[str, Any] = {}
        if str(city_slug or "").strip():
            query["city_slug"] = _slugify_geo_city(city_slug)
        if str(status_filter or "").strip():
            query["status"] = str(status_filter).strip().lower()
        collection = get_database()[self.COLLECTION]
        total = await collection.count_documents(query)
        skip = (page_value - 1) * page_size_value
        docs = (
            await collection.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip(skip)
            .limit(page_size_value)
            .to_list(length=page_size_value)
        )
        items = [self._serialize_run(doc) for doc in docs]
        return build_pagination_payload(items=items, page=page_value, page_size=page_size_value, total=total)

    async def get_run(self, *, geo_grid_run_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self.COLLECTION].find_one(
            {"_id": _parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")}
        )
        return self._serialize_run(doc) if doc else None

    def _serialize_run(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["geo_grid_run_id"] = str(payload.pop("_id"))
        return payload


class MongoGeoGridResultRepository:
    COLLECTION = "geo_grid_results"

    async def replace_point_results(
        self,
        *,
        geo_grid_run_id: str,
        city_slug: str,
        keyword: str,
        point: dict[str, Any],
        results: list[dict[str, Any]],
    ) -> int:
        collection = get_database()[self.COLLECTION]
        point_order = int(point.get("order") or 0)
        await collection.delete_many({"geo_grid_run_id": str(geo_grid_run_id), "point_order": point_order})
        docs: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for item in results:
            if not isinstance(item, dict):
                continue
            business_name = str(item.get("business_name") or item.get("name") or "").strip()
            if not business_name:
                continue
            rank = _coerce_positive_int(item.get("rank"))
            if rank is None:
                continue
            maps_url = str(item.get("maps_url") or "").strip() or None
            maps_url_canonical = (
                str(item.get("maps_url_canonical") or "").strip()
                or _canonicalize_maps_url(maps_url or "")
                or None
            )
            business_name_normalized = _normalize_text(business_name)
            business_key = str(item.get("business_key") or maps_url_canonical or business_name_normalized).strip()
            source_ref = dict(item.get("source_ref") or {})
            model = GeoGridResult(
                geo_grid_run_id=str(geo_grid_run_id),
                city_slug=_slugify_geo_city(city_slug),
                keyword=str(keyword).strip(),
                point_order=point_order,
                point_label=str(point.get("label") or f"Punto {point_order}").strip(),
                grid_row=_coerce_positive_int(point.get("row")),
                grid_col=_coerce_positive_int(point.get("col")),
                lat=float(point.get("lat")),
                lng=float(point.get("lng")),
                rank=rank,
                visible_top10=bool(item.get("visible_top10", rank <= 10)),
                provider_mode=str(item.get("provider_mode") or "").strip() or None,
                business_key=business_key,
                business_name=business_name,
                business_name_normalized=business_name_normalized,
                maps_url=maps_url,
                maps_url_canonical=maps_url_canonical,
                rating=_coerce_float(item.get("rating")),
                review_count=_coerce_int(item.get("review_count")),
                category=str(item.get("category") or "").strip() or None,
                source_ref=source_ref,
                raw_snapshot=dict(item),
                captured_at=now,
                created_at=now,
            )
            docs.append(model.model_dump(mode="python"))
        if not docs:
            return 0
        result = await collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)

    async def list_results(self, *, geo_grid_run_id: str) -> list[dict[str, Any]]:
        docs = (
            await get_database()[self.COLLECTION]
            .find({"geo_grid_run_id": str(geo_grid_run_id).strip()})
            .sort([("point_order", 1), ("rank", 1), ("_id", 1)])
            .to_list(length=100_000)
        )
        return [self._serialize_result(doc) for doc in docs]

    def _serialize_result(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["geo_grid_result_id"] = str(payload.pop("_id"))
        return payload


def _parse_object_id(value: str, *, field_name: str) -> ObjectId:
    try:
        return ObjectId(str(value))
    except (InvalidId, TypeError) as exc:
        raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_value)
    return re.sub(r"\s+", " ", collapsed).strip()


def _slugify_geo_city(value: Any) -> str:
    normalized = _normalize_text(value)
    return normalized.replace(" ", "-")


def _canonicalize_maps_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    path = str(parsed.path or "").strip()
    if not path:
        return raw
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None
    return parsed


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).replace(".", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _coerce_positive_int(value: Any) -> int | None:
    parsed = _coerce_int(value)
    if parsed is None or parsed < 1:
        return None
    return parsed


def _coerce_score(value: Any) -> float:
    parsed = _coerce_float(value)
    return float(parsed) if parsed is not None and parsed >= 0 else 0.0


def _score_from_discovery_rank(rank: int | None) -> float | None:
    if rank is None:
        return None
    # Rank 1 means first visible result. Keep the curve simple and bounded so
    # position influences scoring without overriding rating/reviews/website.
    return max(20.0, min(100.0, 105.0 - (float(rank) * 5.0)))


def _blend_score(explicit_score: float, inferred_score: float | None, *, explicit_weight: float) -> float:
    if inferred_score is None:
        return explicit_score
    if explicit_score <= 0:
        return inferred_score
    inferred_weight = 1.0 - explicit_weight
    return round((explicit_score * explicit_weight) + (inferred_score * inferred_weight), 2)
