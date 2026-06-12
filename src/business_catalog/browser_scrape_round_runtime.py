from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Iterable
from uuid import uuid4

from pymongo import ReturnDocument

from src.workers.contracts import AnalysisGenerateTaskPayload


DatabaseFactory = Callable[[], Any]


class BrowserScrapeRoundRuntime:
    _COLLECTION = "browser_scrape_rounds"
    _SUPPORTED_SOURCES = {"google_maps", "tripadvisor"}
    _RESOLVED_SOURCE_STATUSES = frozenset({"done", "omitted", "not_found"})

    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        job_service: Any,
        collection_name: str | None = None,
    ) -> None:
        self._database_factory = database_factory
        self._job_service = job_service
        self._collection_name = str(collection_name or self._COLLECTION).strip() or self._COLLECTION

    async def open_round(
        self,
        *,
        canonical_name: str,
        canonical_name_normalized: str,
        root_business_id: str | None,
        requested_sources: Iterable[str],
        requested_by: str | None,
    ) -> dict[str, Any]:
        normalized_sources = self._normalize_sources(requested_sources)
        if not normalized_sources:
            raise ValueError("requested_sources cannot be empty when opening a browser scrape round.")

        now = datetime.now(timezone.utc)
        round_id = uuid4().hex
        doc = {
            "_id": round_id,
            "canonical_name": str(canonical_name).strip(),
            "canonical_name_normalized": str(canonical_name_normalized).strip(),
            "root_business_id": str(root_business_id or "").strip() or None,
            "requested_sources": list(normalized_sources),
            "requested_by": str(requested_by or "").strip().lower() or None,
            "source_jobs": {},
            "analysis_handoff": {
                "status": "pending",
                "analysis_job_id": None,
                "queue_name": None,
                "job_type": None,
                "claimed_at": None,
                "claimed_by_source": None,
                "claimed_by_job_id": None,
                "enqueued_at": None,
                "error": None,
            },
            "created_at": now,
            "updated_at": now,
        }
        await self._collection.insert_one(doc)
        return self._serialize_round_doc(doc)

    async def register_source_job(
        self,
        *,
        scrape_round_id: str,
        source: str,
        source_job_id: str,
        queue_name: str,
        execution_mode: str,
        source_name: str | None,
    ) -> dict[str, Any]:
        normalized_round_id = self._normalize_round_id(scrape_round_id)
        normalized_source = self._normalize_source(source)
        now = datetime.now(timezone.utc)
        updated_doc = await self._collection.find_one_and_update(
            {"_id": normalized_round_id},
            {
                "$set": {
                    f"source_jobs.{normalized_source}.source": normalized_source,
                    f"source_jobs.{normalized_source}.job_id": str(source_job_id).strip(),
                    f"source_jobs.{normalized_source}.queue_name": str(queue_name).strip().lower(),
                    f"source_jobs.{normalized_source}.execution_mode": str(execution_mode).strip().lower() or None,
                    f"source_jobs.{normalized_source}.source_name": str(source_name or "").strip() or None,
                    f"source_jobs.{normalized_source}.registered_at": now,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise LookupError(f"Browser scrape round '{normalized_round_id}' not found.")
        return self._serialize_round_doc(updated_doc)

    async def complete_source_job_and_maybe_enqueue_analysis(
        self,
        *,
        scrape_round_id: str,
        source: str,
        source_job_id: str,
        business_id: str | None,
        dataset_id: str | None,
        source_profile_id: str | None,
        scrape_run_id: str | None,
        completion_mode: str | None = None,
        resolution: str | None = None,
        resolution_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_round_id = self._normalize_round_id(scrape_round_id)
        normalized_source = self._normalize_source(source)
        normalized_business_id = str(business_id or "").strip() or None

        normalized_source_job_id = str(source_job_id or "").strip()
        if not normalized_source_job_id:
            raise ValueError("source_job_id cannot be empty when completing a browser scrape round source.")

        now = datetime.now(timezone.utc)
        resolved_status = self._resolve_source_status(
            completion_mode=completion_mode,
            resolution=resolution,
        )
        updated_doc = await self._collection.find_one_and_update(
            {"_id": normalized_round_id},
            {
                "$set": {
                    f"source_jobs.{normalized_source}.source": normalized_source,
                    f"source_jobs.{normalized_source}.job_id": normalized_source_job_id,
                    f"source_jobs.{normalized_source}.status": resolved_status,
                    f"source_jobs.{normalized_source}.completed_at": now,
                    f"source_jobs.{normalized_source}.business_id": normalized_business_id,
                    f"source_jobs.{normalized_source}.dataset_id": str(dataset_id or "").strip() or None,
                    f"source_jobs.{normalized_source}.source_profile_id": (
                        str(source_profile_id or "").strip() or None
                    ),
                    f"source_jobs.{normalized_source}.scrape_run_id": str(scrape_run_id or "").strip() or None,
                    f"source_jobs.{normalized_source}.completion_mode": (
                        str(completion_mode or "").strip().lower() or "captured"
                    ),
                    f"source_jobs.{normalized_source}.resolution": str(resolution or "").strip().lower() or None,
                    f"source_jobs.{normalized_source}.resolution_metadata": (
                        dict(resolution_metadata) if isinstance(resolution_metadata, dict) else None
                    ),
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise LookupError(f"Browser scrape round '{normalized_round_id}' not found.")

        resolved_business_id = normalized_business_id or self._resolve_business_id_for_analysis(round_doc=updated_doc)
        if not resolved_business_id:
            raise ValueError(
                "business_id could not be resolved when completing a browser scrape round source."
            )

        expected_sources = self._normalize_sources(updated_doc.get("requested_sources") or [])
        completed_sources = self._completed_sources(updated_doc, expected_sources)
        pending_sources = [item for item in expected_sources if item not in completed_sources]
        if pending_sources:
            return {
                "mode": "scrape_round",
                "scrape_round_id": normalized_round_id,
                "analysis_enqueued": False,
                "waiting_for_sources": True,
                "claim_in_progress": False,
                "completed_sources": completed_sources,
                "pending_sources": pending_sources,
                "resolved_business_id": resolved_business_id,
                "source_status": resolved_status,
                "analysis_job_id": None,
                "analysis_queue_name": None,
                "analysis_job_type": None,
                "analysis_payload": None,
            }

        claim_query: dict[str, Any] = {
            "_id": normalized_round_id,
            "$or": [
                {"analysis_handoff.status": {"$exists": False}},
                {"analysis_handoff.status": None},
                {"analysis_handoff.status": {"$in": ["pending", "failed"]}},
            ],
        }
        for expected_source in expected_sources:
            claim_query[f"source_jobs.{expected_source}.status"] = {
                "$in": list(self._RESOLVED_SOURCE_STATUSES)
            }

        claimed_doc = await self._collection.find_one_and_update(
            claim_query,
            {
                "$set": {
                    "analysis_handoff.status": "claiming",
                    "analysis_handoff.claimed_at": now,
                    "analysis_handoff.claimed_by_source": normalized_source,
                    "analysis_handoff.claimed_by_job_id": normalized_source_job_id,
                    "analysis_handoff.error": None,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if claimed_doc is None:
            current_doc = await self._collection.find_one({"_id": normalized_round_id}) or updated_doc
            analysis_handoff = (
                dict(current_doc.get("analysis_handoff"))
                if isinstance(current_doc.get("analysis_handoff"), dict)
                else {}
            )
            return {
                "mode": "scrape_round",
                "scrape_round_id": normalized_round_id,
                "analysis_enqueued": bool(str(analysis_handoff.get("analysis_job_id") or "").strip()),
                "waiting_for_sources": False,
                "claim_in_progress": str(analysis_handoff.get("status") or "").strip().lower() == "claiming",
                "completed_sources": completed_sources,
                "pending_sources": [],
                "source_status": resolved_status,
                "analysis_job_id": str(analysis_handoff.get("analysis_job_id") or "").strip() or None,
                "analysis_queue_name": str(analysis_handoff.get("queue_name") or "").strip() or None,
                "analysis_job_type": str(analysis_handoff.get("job_type") or "").strip() or None,
                "analysis_payload": None,
            }

        payload = self._build_analysis_payload(
            round_doc=claimed_doc,
            source=normalized_source,
            source_job_id=normalized_source_job_id,
            business_id=resolved_business_id,
            dataset_id=str(dataset_id or "").strip() or None,
            source_profile_id=str(source_profile_id or "").strip() or None,
            scrape_run_id=str(scrape_run_id or "").strip() or None,
        )
        try:
            analysis_enqueue_result = await self._job_service.enqueue_analysis_generate_job(
                task_payload=payload,
            )
        except Exception as exc:
            await self._collection.update_one(
                {"_id": normalized_round_id},
                {
                    "$set": {
                        "analysis_handoff.status": "failed",
                        "analysis_handoff.error": str(exc),
                        "updated_at": datetime.now(timezone.utc),
                    },
                    "$unset": {
                        "analysis_handoff.claimed_at": "",
                        "analysis_handoff.claimed_by_source": "",
                        "analysis_handoff.claimed_by_job_id": "",
                        "analysis_handoff.enqueued_at": "",
                        "analysis_handoff.analysis_job_id": "",
                        "analysis_handoff.queue_name": "",
                        "analysis_handoff.job_type": "",
                    },
                },
            )
            raise

        analysis_job_id = str(analysis_enqueue_result.get("job_id") or "").strip() or None
        finished_at = datetime.now(timezone.utc)
        await self._collection.update_one(
            {"_id": normalized_round_id},
            {
                "$set": {
                    "analysis_handoff.status": "queued",
                    "analysis_handoff.analysis_job_id": analysis_job_id,
                    "analysis_handoff.queue_name": str(analysis_enqueue_result.get("queue_name") or "").strip() or None,
                    "analysis_handoff.job_type": str(analysis_enqueue_result.get("job_type") or "").strip() or None,
                    "analysis_handoff.enqueued_at": finished_at,
                    "analysis_handoff.error": None,
                    "updated_at": finished_at,
                }
            },
        )
        return {
            "mode": "scrape_round",
            "scrape_round_id": normalized_round_id,
            "analysis_enqueued": True,
            "waiting_for_sources": False,
            "claim_in_progress": False,
            "completed_sources": completed_sources,
            "pending_sources": [],
            "resolved_business_id": resolved_business_id,
            "source_status": resolved_status,
            "analysis_job_id": analysis_job_id,
            "analysis_queue_name": analysis_enqueue_result.get("queue_name"),
            "analysis_job_type": analysis_enqueue_result.get("job_type"),
            "analysis_payload": payload.model_dump(mode="python"),
        }

    @property
    def _collection(self) -> Any:
        return self._database_factory()[self._collection_name]

    def _build_analysis_payload(
        self,
        *,
        round_doc: dict[str, Any],
        source: str,
        source_job_id: str,
        business_id: str,
        dataset_id: str | None,
        source_profile_id: str | None,
        scrape_run_id: str | None,
    ) -> AnalysisGenerateTaskPayload:
        requested_sources = self._normalize_sources(round_doc.get("requested_sources") or [])
        single_source = requested_sources[0] if len(requested_sources) == 1 else None
        can_scope_single_source = bool(dataset_id or source_profile_id or scrape_run_id)
        source_mode = (
            "single"
            if single_source in self._SUPPORTED_SOURCES and can_scope_single_source
            else "auto"
        )
        selected_source = single_source if source_mode == "single" else None
        payload_dataset_id = dataset_id if source_mode == "single" else None
        payload_source_profile_id = source_profile_id if source_mode == "single" else None
        payload_scrape_run_id = scrape_run_id if source_mode == "single" else None
        return AnalysisGenerateTaskPayload(
            business_id=business_id,
            dataset_id=payload_dataset_id,
            source_profile_id=payload_source_profile_id,
            scrape_run_id=payload_scrape_run_id,
            source_job_id=source_job_id,
            source_mode=source_mode,
            selected_source=selected_source,
            scrape_round_id=str(round_doc.get("_id") or "").strip() or None,
        )

    def _completed_sources(
        self,
        round_doc: dict[str, Any],
        expected_sources: tuple[str, ...],
    ) -> list[str]:
        source_jobs = round_doc.get("source_jobs") if isinstance(round_doc.get("source_jobs"), dict) else {}
        completed: list[str] = []
        for source in expected_sources:
            source_state = source_jobs.get(source) if isinstance(source_jobs.get(source), dict) else {}
            if self._is_resolved_source_status(source_state.get("status")):
                completed.append(source)
        return completed

    def _is_resolved_source_status(self, value: Any) -> bool:
        return str(value or "").strip().lower() in self._RESOLVED_SOURCE_STATUSES

    def _resolve_source_status(
        self,
        *,
        completion_mode: str | None,
        resolution: str | None,
    ) -> str:
        normalized_resolution = str(resolution or "").strip().lower()
        normalized_completion_mode = str(completion_mode or "").strip().lower()
        if normalized_resolution == "business_not_found":
            return "not_found"
        if normalized_resolution in {"manual_skip", "manual_close"}:
            return "omitted"
        if normalized_completion_mode == "resolved_without_capture":
            return "omitted"
        return "done"

    def _resolve_business_id_for_analysis(self, *, round_doc: dict[str, Any]) -> str | None:
        source_jobs = round_doc.get("source_jobs") if isinstance(round_doc.get("source_jobs"), dict) else {}
        requested_sources = self._normalize_sources(round_doc.get("requested_sources") or [])
        for source in requested_sources:
            source_state = source_jobs.get(source) if isinstance(source_jobs.get(source), dict) else {}
            business_id = str(source_state.get("business_id") or "").strip()
            if business_id:
                return business_id

        for source_state in source_jobs.values():
            if not isinstance(source_state, dict):
                continue
            business_id = str(source_state.get("business_id") or "").strip()
            if business_id:
                return business_id

        root_business_id = str(round_doc.get("root_business_id") or "").strip()
        return root_business_id or None

    def _normalize_sources(self, requested_sources: Iterable[str]) -> tuple[str, ...]:
        normalized: list[str] = []
        for raw_source in requested_sources:
            source = str(raw_source or "").strip().lower()
            if not source or source not in self._SUPPORTED_SOURCES or source in normalized:
                continue
            normalized.append(source)
        return tuple(normalized)

    def _normalize_round_id(self, scrape_round_id: str) -> str:
        normalized_round_id = str(scrape_round_id or "").strip()
        if not normalized_round_id:
            raise ValueError("scrape_round_id cannot be empty.")
        return normalized_round_id

    def _normalize_source(self, source: str) -> str:
        normalized_source = str(source or "").strip().lower()
        if normalized_source not in self._SUPPORTED_SOURCES:
            allowed = ", ".join(sorted(self._SUPPORTED_SOURCES))
            raise ValueError(f"Unsupported scrape round source '{source}'. Allowed values: {allowed}.")
        return normalized_source

    def _serialize_round_doc(self, round_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(round_doc)
        payload["round_id"] = str(payload.pop("_id"))
        return payload
