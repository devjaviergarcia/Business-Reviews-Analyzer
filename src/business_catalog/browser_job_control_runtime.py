from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from pymongo import ReturnDocument

from src.config import settings
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_FALLBACK_POLICY,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
    default_source_display_name,
)
from src.workers.contracts import AnalysisGenerateTaskPayload, AnalyzeBusinessTaskPayload


DatabaseFactory = Callable[[], Any]


class BrowserJobControlRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        job_service: Any,
        tripadvisor_local_worker_control_service: Any,
        validate_business_name: Callable[[str], str],
        normalize_text: Callable[[str], str],
        resolve_reviews_strategy: Callable[[str | None], str],
        resolve_force_mode: Callable[[str | None], str],
        resolve_scrape_sources: Callable[[tuple[str, ...] | list[str] | None], tuple[str, ...]],
        parse_object_id: Callable[..., Any],
        sanitize_response_payload: Callable[[Any], Any],
        ensure_job_is_scrape: Callable[[dict[str, Any]], None],
        businesses_collection_name: str,
        active_job_statuses: set[str],
    ) -> None:
        self._database_factory = database_factory
        self._job_service = job_service
        self._tripadvisor_local_worker_control_service = tripadvisor_local_worker_control_service
        self._validate_business_name = validate_business_name
        self._normalize_text = normalize_text
        self._resolve_reviews_strategy = resolve_reviews_strategy
        self._resolve_force_mode = resolve_force_mode
        self._resolve_scrape_sources = resolve_scrape_sources
        self._parse_object_id = parse_object_id
        self._sanitize_response_payload = sanitize_response_payload
        self._ensure_job_is_scrape = ensure_job_is_scrape
        self._businesses_collection_name = businesses_collection_name
        self._active_job_statuses = active_job_statuses

    async def ensure_root_business_on_enqueue(
        self,
        *,
        canonical_name: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        try:
            businesses = self._database_factory()[self._businesses_collection_name]
        except RuntimeError:
            return {
                "_id": None,
                "name": canonical_name,
                "name_normalized": canonical_name_normalized,
                "source": "multi_source",
                "created_at": now,
                "updated_at": now,
            }
        business_doc = await businesses.find_one_and_update(
            {"name_normalized": canonical_name_normalized},
            {
                "$set": {
                    "name": canonical_name,
                    "name_normalized": canonical_name_normalized,
                    "source": "multi_source",
                    "updated_at": now,
                },
                "$setOnInsert": {
                    "listing": {},
                    "stats": {},
                    "review_count": 0,
                    "scraped_review_count": 0,
                    "processed_review_count": 0,
                    "last_scraped_at": None,
                    "active_dataset_id": None,
                    "created_at": now,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if business_doc is None:
            raise RuntimeError("Failed to create or fetch root business while enqueueing scrape jobs.")
        return business_doc

    async def ensure_tripadvisor_worker_started_on_enqueue(self, *, selected_sources: tuple[str, ...]) -> None:
        if "tripadvisor" not in selected_sources:
            return
        if not settings.tripadvisor_local_worker_autostart_on_enqueue:
            return
        if not settings.tripadvisor_local_worker_bridge_enabled:
            raise RuntimeError(
                "Tripadvisor local worker autostart is enabled, but bridge is disabled. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_ENABLED=true."
            )
        bridge_result = await self._tripadvisor_local_worker_control_service.ensure_started(
            use_xvfb=True,
            reason="business_scrape_jobs_enqueue",
        )
        worker_payload = bridge_result.get("worker")
        if isinstance(worker_payload, dict) and worker_payload.get("running") is True:
            return
        raise RuntimeError(
            "Tripadvisor local worker bridge did not confirm a running worker. "
            f"Bridge response: {bridge_result}"
        )

    async def enqueue_business_scrape_jobs(  # noqa: PLR0913
        self,
        *,
        name: str,
        force: bool = False,
        strategy: str | None = None,
        force_mode: str | None = None,
        interactive_max_rounds: int | None = None,
        html_scroll_max_rounds: int | None = None,
        html_stable_rounds: int | None = None,
        tripadvisor_max_pages: int | None = None,
        tripadvisor_pages_percent: float | None = None,
        sources: tuple[str, ...] | list[str] | None = None,
        google_maps_name: str | None = None,
        tripadvisor_name: str | None = None,
        execution_mode: str | None = None,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        business_name = self._validate_business_name(name)
        canonical_name_normalized = self._normalize_text(business_name)
        selected_strategy = self._resolve_reviews_strategy(strategy)
        selected_force_mode = self._resolve_force_mode(force_mode)
        selected_sources = self._resolve_scrape_sources(sources)
        await self.ensure_tripadvisor_worker_started_on_enqueue(selected_sources=selected_sources)
        root_business_doc = await self.ensure_root_business_on_enqueue(
            canonical_name=business_name,
            canonical_name_normalized=canonical_name_normalized,
        )
        root_business_id = str(root_business_doc.get("_id") or "").strip() or None

        source_names: dict[str, str] = {}
        for source in selected_sources:
            raw_source_name = google_maps_name if source == "google_maps" else tripadvisor_name if source == "tripadvisor" else None
            resolved_name = (
                self._validate_business_name(raw_source_name)
                if isinstance(raw_source_name, str) and raw_source_name.strip()
                else business_name
            )
            source_names[source] = resolved_name

        queue_by_source = {
            "google_maps": "scrape_google_maps",
            "tripadvisor": "scrape_tripadvisor",
        }
        normalized_execution_mode = (
            str(execution_mode or DEFAULT_BROWSER_EXECUTION_MODE).strip().lower()
            or DEFAULT_BROWSER_EXECUTION_MODE
        )
        normalized_requested_by = str(requested_by or "").strip().lower().replace(" ", "_") or "business_api"
        jobs_by_source: dict[str, dict[str, Any]] = {}
        for source in selected_sources:
            source_business_name = source_names[source]
            source_name_normalized = self._normalize_text(source_business_name)
            task_payload = AnalyzeBusinessTaskPayload(
                name=source_business_name,
                source=source,
                canonical_name=business_name,
                canonical_name_normalized=canonical_name_normalized,
                source_name=source_business_name,
                source_name_normalized=source_name_normalized,
                root_business_id=root_business_id,
                execution_mode=normalized_execution_mode,
                runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                requested_by=normalized_requested_by,
                fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
                source_display_name=default_source_display_name(source),
                force=bool(force),
                strategy=selected_strategy,
                force_mode=selected_force_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                tripadvisor_max_pages=tripadvisor_max_pages,
                tripadvisor_pages_percent=tripadvisor_pages_percent,
            )
            queued_job = await self._job_service.enqueue_job(
                task_payload=task_payload,
                name_normalized=source_name_normalized,
                queue_name=queue_by_source[source],
                job_type="business_analyze",
                source=source,
                runtime_target=DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                execution_mode=normalized_execution_mode,
                requested_by=normalized_requested_by,
                fallback_policy=DEFAULT_BROWSER_FALLBACK_POLICY,
                source_display_name=default_source_display_name(source),
            )
            jobs_by_source[source] = queued_job

        primary_source = selected_sources[0]
        primary_job_id = str((jobs_by_source.get(primary_source) or {}).get("job_id", "")).strip()
        return self._sanitize_response_payload(
            {
                "job_id": primary_job_id,
                "primary_job_id": primary_job_id,
                "primary_source": primary_source,
                "status": "queued",
                "name": business_name,
                "canonical_name": business_name,
                "canonical_name_normalized": canonical_name_normalized,
                "business_id": root_business_id,
                "execution_mode": normalized_execution_mode,
                "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                "sources_requested": list(selected_sources),
                "source_names": source_names,
                "jobs_by_source": jobs_by_source,
            }
        )

    async def resolve_business_id_for_scrape_job(self, job_payload: dict[str, Any]) -> str | None:
        result_payload = job_payload.get("result")
        if isinstance(result_payload, dict):
            result_business_id = str(result_payload.get("business_id") or "").strip()
            if result_business_id:
                return result_business_id

        payload_data = job_payload.get("payload")
        payload_name = ""
        payload_canonical_name = ""
        payload_root_business_id = ""
        if isinstance(payload_data, dict):
            payload_name = str(payload_data.get("name") or "").strip()
            payload_canonical_name = str(payload_data.get("canonical_name") or "").strip()
            payload_root_business_id = str(payload_data.get("root_business_id") or "").strip()
        if not payload_root_business_id:
            payload_root_business_id = str(job_payload.get("root_business_id") or "").strip()
        if payload_root_business_id:
            try:
                return str(self._parse_object_id(payload_root_business_id, field_name="root_business_id"))
            except ValueError:
                pass
        lookup_name = payload_canonical_name or payload_name or str(job_payload.get("name") or "").strip()
        if not lookup_name:
            return None
        name_normalized = self._normalize_text(lookup_name)
        businesses = self._database_factory()[self._businesses_collection_name]
        business_doc = await businesses.find_one({"name_normalized": name_normalized}, projection={"_id": 1})
        if business_doc is None:
            return None
        return str(business_doc.get("_id") or "").strip() or None

    async def relaunch_scrape_job(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        reason: str | None,
        force: bool,
        restart_from_zero: bool,
        google_maps_name: str | None,
        tripadvisor_name: str | None,
        execution_mode: str | None,
        interactive_max_rounds: int | None,
        html_scroll_max_rounds: int | None,
        html_stable_rounds: int | None,
        tripadvisor_max_pages: int | None,
        tripadvisor_pages_percent: float | None,
        ensure_tripadvisor_session_available_for_relaunch: Callable[..., Awaitable[None]],
    ) -> dict[str, Any]:
        existing = await self._job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing)
        queue_name = str(existing.get("queue_name") or "").strip().lower()
        normalized_execution_mode = str(execution_mode or "").strip().lower() or None
        if queue_name == "scrape_tripadvisor" and normalized_execution_mode != "live":
            await ensure_tripadvisor_session_available_for_relaunch(
                operation="relaunch_tripadvisor_job",
                job_id=job_id,
            )
        payload_override: dict[str, Any] = {}
        if normalized_execution_mode:
            payload_override["execution_mode"] = normalized_execution_mode
            payload_override["runtime_target"] = DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET
            payload_override["requested_by"] = (
                "manual_live_relaunch" if normalized_execution_mode == "live" else "manual_relaunch"
            )
        if interactive_max_rounds is not None:
            payload_override["interactive_max_rounds"] = int(interactive_max_rounds)
        if html_scroll_max_rounds is not None:
            payload_override["html_scroll_max_rounds"] = int(html_scroll_max_rounds)
        if html_stable_rounds is not None:
            payload_override["html_stable_rounds"] = int(html_stable_rounds)
        if tripadvisor_max_pages is not None:
            payload_override["tripadvisor_max_pages"] = int(tripadvisor_max_pages)
        if tripadvisor_pages_percent is not None:
            payload_override["tripadvisor_pages_percent"] = float(tripadvisor_pages_percent)

        override_source_name: str | None = None
        if queue_name == "scrape_google_maps" and isinstance(google_maps_name, str) and google_maps_name.strip():
            override_source_name = self._validate_business_name(google_maps_name)
        elif queue_name == "scrape_tripadvisor" and isinstance(tripadvisor_name, str) and tripadvisor_name.strip():
            override_source_name = self._validate_business_name(tripadvisor_name)
        if override_source_name:
            payload_override["name"] = override_source_name
            payload_override["source_name"] = override_source_name
            payload_override["source_name_normalized"] = self._normalize_text(override_source_name)

        return await self._job_service.relaunch_job(
            job_id=job_id,
            reason=reason or "Job relaunched via API.",
            force=bool(force) or bool(restart_from_zero),
            restart_from_zero=bool(restart_from_zero),
            payload_override=payload_override or None,
        )

    async def stop_business_scrape_job(
        self,
        *,
        job_id: str,
        continue_analysis_if_google: bool,
        wait_active_stop_seconds: float,
        poll_seconds: float,
    ) -> dict[str, Any]:
        job_payload = await self._job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(job_payload)
        queue_name = str(job_payload.get("queue_name") or "").strip().lower()

        cancel_result = await self._job_service.request_job_cancellation(
            job_id=job_id,
            reason="Manual scrape stop requested via API.",
        )
        safe_wait_seconds = max(0.5, float(wait_active_stop_seconds))
        safe_poll_seconds = max(0.1, float(poll_seconds))
        started_wait_at = time.monotonic()
        timed_out_waiting_stop = False
        final_job_payload = cancel_result

        while True:
            try:
                current = await self._job_service.get_job(job_id=job_id)
            except LookupError:
                break
            final_job_payload = current
            current_status = str(current.get("status") or "").strip().lower()
            if current_status not in self._active_job_statuses:
                break
            if (time.monotonic() - started_wait_at) >= safe_wait_seconds:
                timed_out_waiting_stop = True
                break
            await asyncio.sleep(safe_poll_seconds)

        is_google_scrape_queue = queue_name in {"scrape", "scrape_google_maps"}
        result_payload = final_job_payload.get("result") if isinstance(final_job_payload.get("result"), dict) else {}
        analysis_handoff_payload = result_payload.get("analysis_handoff") if isinstance(result_payload.get("analysis_handoff"), dict) else {}
        analysis_handoff_job_id = str(analysis_handoff_payload.get("analysis_job_id") or "").strip()
        handoff_event_present = any(
            isinstance(event, dict) and str(event.get("stage") or "").strip().lower() == "handoff_analysis_queued"
            for event in (final_job_payload.get("events") or [])
        )
        analysis_already_handed_off = (
            str(final_job_payload.get("queue_name") or "").strip().lower() == "analysis"
            or str(final_job_payload.get("job_type") or "").strip().lower() == "analysis_generate"
            or bool(analysis_handoff_job_id)
            or handoff_event_present
        )
        continue_analysis_requested = bool(continue_analysis_if_google and is_google_scrape_queue)

        analysis_enqueue_result: dict[str, Any] | None = None
        continue_analysis_note: str | None = None
        if continue_analysis_requested and not analysis_already_handed_off:
            business_id = await self.resolve_business_id_for_scrape_job(final_job_payload)
            if business_id:
                analysis_enqueue_result = await self._job_service.enqueue_analysis_generate_job(
                    task_payload=AnalysisGenerateTaskPayload(
                        business_id=business_id,
                        source_job_id=str(job_id),
                    )
                )
                continue_analysis_note = "Analysis job was enqueued after stopping scrape."
            else:
                continue_analysis_note = (
                    "Scrape stop requested, but analysis could not be enqueued yet "
                    "because no business_id was resolved from current data."
                )
        elif continue_analysis_requested and analysis_already_handed_off:
            continue_analysis_note = "Analysis flow was already handed off before stop completion."
        elif not continue_analysis_requested:
            continue_analysis_note = "No analysis continuation was requested for this source."

        return self._sanitize_response_payload(
            {
                "job_id": str(job_id),
                "queue_name": queue_name,
                "cancel_requested": True,
                "status": final_job_payload.get("status"),
                "timed_out_waiting_stop": timed_out_waiting_stop,
                "continue_analysis_requested": continue_analysis_requested,
                "analysis_already_handed_off": analysis_already_handed_off,
                "analysis_handoff_job_id": analysis_handoff_job_id or None,
                "analysis_enqueue_result": analysis_enqueue_result,
                "note": continue_analysis_note,
            }
        )

    def build_related_business_jobs_query(
        self,
        *,
        business_id: str,
        canonical_name_normalized: str,
    ) -> dict[str, Any]:
        clauses: list[dict[str, Any]] = [
            {"payload.business_id": business_id},
            {"business_id": business_id},
            {"root_business_id": business_id},
            {"payload.root_business_id": business_id},
        ]
        if canonical_name_normalized:
            clauses.extend(
                [
                    {"canonical_name_normalized": canonical_name_normalized},
                    {"payload.canonical_name_normalized": canonical_name_normalized},
                    {"name_normalized": canonical_name_normalized},
                    {"payload.name_normalized": canonical_name_normalized},
                ]
            )
        return {"$or": clauses}
