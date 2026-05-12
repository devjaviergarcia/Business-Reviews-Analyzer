from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.database import get_database
from src.models.crm import CRMDiscoveryRun, CRMDiscoveryRunStatus
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
