from __future__ import annotations

import re
from typing import Any, Awaitable, Callable


DatabaseFactory = Callable[[], Any]


class TripadvisorAntibotJobRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        jobs_collection_name: str,
        active_job_statuses: set[str],
        sanitize_response_payload: Callable[[Any], Any],
        ensure_tripadvisor_session_available_for_relaunch: Callable[..., Awaitable[None]],
        job_service: Any,
    ) -> None:
        self._database_factory = database_factory
        self._jobs_collection_name = jobs_collection_name
        self._active_job_statuses = active_job_statuses
        self._sanitize_response_payload = sanitize_response_payload
        self._ensure_tripadvisor_session_available_for_relaunch = ensure_tripadvisor_session_available_for_relaunch
        self._job_service = job_service

    async def list_jobs(
        self,
        *,
        limit: int,
        status_filter: str,
    ) -> dict[str, Any]:
        normalized_status_filter = str(status_filter or "failed_or_needs_human").strip().lower()
        status_filter_map: dict[str, set[str] | None] = {
            "failed_or_needs_human": {"failed", "needs_human"},
            "failed": {"failed"},
            "needs_human": {"needs_human"},
            "all": None,
        }
        if normalized_status_filter not in status_filter_map:
            allowed_values = ", ".join(sorted(status_filter_map.keys()))
            raise ValueError(f"Invalid status_filter={status_filter!r}. Allowed values: {allowed_values}.")

        safe_limit = max(1, min(int(limit), 200))
        statuses = status_filter_map[normalized_status_filter]
        query: dict[str, Any] = {
            "queue_name": "scrape_tripadvisor",
            "job_type": "business_analyze",
        }
        if statuses is not None:
            query["status"] = {"$in": sorted(statuses)}
        jobs_collection = self._database_factory()[self._jobs_collection_name]
        scan_limit = min(2000, max(safe_limit * 8, safe_limit))
        docs = (
            await jobs_collection.find(query)
            .sort([("updated_at", -1), ("_id", -1)])
            .limit(scan_limit)
            .to_list(length=scan_limit)
        )

        items: list[dict[str, Any]] = []
        for doc in docs:
            summary = self.summarize_job(doc)
            if summary is None:
                continue
            items.append(summary)
            if len(items) >= safe_limit:
                break

        return self._sanitize_response_payload(
            {
                "limit": safe_limit,
                "status_filter": normalized_status_filter,
                "scanned_jobs": len(docs),
                "matched_jobs": len(items),
                "items": items,
            }
        )

    async def relaunch_jobs(
        self,
        *,
        limit: int,
        reason: str | None,
        status_filter: str,
    ) -> dict[str, Any]:
        await self._ensure_tripadvisor_session_available_for_relaunch(
            operation="relaunch_tripadvisor_antibot_jobs",
        )
        list_result = await self.list_jobs(limit=limit, status_filter=status_filter)
        items = list_result.get("items") if isinstance(list_result, dict) else []
        if not isinstance(items, list):
            items = []
        safe_limit = max(1, min(int(limit), 200))
        relaunched: list[str] = []
        errors: list[dict[str, str]] = []
        for item in items[:safe_limit]:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("job_id") or "").strip()
            if not job_id:
                continue
            try:
                await self._job_service.relaunch_job(
                    job_id=job_id,
                    reason=reason or "Relaunched latest TripAdvisor anti-bot failed jobs via API.",
                )
                relaunched.append(job_id)
            except Exception as exc:  # noqa: BLE001
                errors.append({"job_id": job_id, "error": str(exc)})
        return self._sanitize_response_payload(
            {
                "requested_limit": safe_limit,
                "status_filter": str(status_filter or "failed_or_needs_human").strip().lower(),
                "matched_jobs": len(items),
                "relaunched_jobs": relaunched,
                "errors": errors,
            }
        )

    def summarize_job(self, job_doc: dict[str, Any]) -> dict[str, Any] | None:
        events = job_doc.get("events") if isinstance(job_doc.get("events"), list) else []
        antibot_events: list[dict[str, Any]] = []
        for index, event in enumerate(events, start=1):
            if not isinstance(event, dict):
                continue
            summary = self.extract_event_summary(event, index=index)
            if summary is None:
                continue
            antibot_events.append(summary)
        if not antibot_events:
            progress = job_doc.get("progress") if isinstance(job_doc.get("progress"), dict) else {}
            progress_message = str(progress.get("message") or "")
            error_message = str(job_doc.get("error") or "")
            if not (self.looks_like_antibot_text(progress_message) or self.looks_like_antibot_text(error_message)):
                return None
            fallback_diagnostic_id = self.extract_diagnostic_id_from_text(progress_message) or self.extract_diagnostic_id_from_text(error_message)
            antibot_events.append(
                {
                    "index": None,
                    "status": str(job_doc.get("status") or "").strip().lower(),
                    "stage": str(progress.get("stage") or "").strip().lower() or "failed",
                    "message": progress_message or error_message or "Anti-bot related failure detected.",
                    "created_at": progress.get("updated_at") or job_doc.get("updated_at"),
                    "reason_code": "tripadvisor_antibot_detected",
                    "diagnostic_id": fallback_diagnostic_id,
                }
            )

        attempts = int(job_doc.get("attempts") or 0)
        status_value = str(job_doc.get("status") or "").strip().lower()
        return {
            "job_id": str(job_doc.get("_id")),
            "queue_name": str(job_doc.get("queue_name") or "").strip().lower(),
            "job_type": str(job_doc.get("job_type") or "").strip().lower(),
            "name": str(job_doc.get("name") or "").strip(),
            "name_normalized": str(job_doc.get("name_normalized") or "").strip(),
            "status": status_value,
            "attempts": attempts,
            "updated_at": job_doc.get("updated_at"),
            "first_antibot_event": antibot_events[0],
            "latest_antibot_event": antibot_events[-1],
            "antibot_event_count": len(antibot_events),
            "relaunch_eligible": status_value not in self._active_job_statuses,
        }

    def extract_event_summary(self, event: dict[str, Any], *, index: int) -> dict[str, Any] | None:
        stage = str(event.get("stage") or "").strip().lower()
        status = str(event.get("status") or "").strip().lower()
        message = str(event.get("message") or "")
        created_at = event.get("created_at")
        data = event.get("data") if isinstance(event.get("data"), dict) else {}
        reason_code = str(data.get("reason_code") or "").strip().lower()
        diagnostic_id = str(data.get("diagnostic_id") or "").strip() or None
        message_error = str(data.get("error") or "")
        message_reason = str(data.get("reason") or "")
        anti_bot_detected = bool(data.get("anti_bot_detected"))
        anti_bot_flag = bool(data.get("anti_bot"))
        failure_like_stages = {"failed", "needs_human", "scraper_stage_error", "scraper_stage_timeout", "scrape_source_failed"}
        message_indicates_antibot = (
            self.looks_like_antibot_text(message)
            or self.looks_like_antibot_text(message_error)
            or self.looks_like_antibot_text(message_reason)
        )
        event_matches_antibot = (
            reason_code == "tripadvisor_antibot_detected"
            or anti_bot_detected
            or anti_bot_flag
            or (stage in failure_like_stages and message_indicates_antibot)
        )
        if not event_matches_antibot:
            return None
        extracted_diagnostic_id = diagnostic_id or self.extract_diagnostic_id_from_text(message) or self.extract_diagnostic_id_from_text(message_error)
        return {
            "index": index,
            "status": status or None,
            "stage": stage or None,
            "message": message or None,
            "created_at": created_at,
            "reason_code": reason_code or None,
            "diagnostic_id": extracted_diagnostic_id,
        }

    def looks_like_antibot_text(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        text = value.strip().lower()
        if not text:
            return False
        strong_markers = (
            "anti-bot",
            "antibot",
            "captcha",
            "verify you are human",
            "verifica que eres humano",
            "tráfico inusual",
            "unusual traffic",
            "automated access",
            "security check",
            "challenge detected",
        )
        if any(marker in text for marker in strong_markers):
            return True
        return bool(re.search(r"\\bbot\\b", text))

    def extract_diagnostic_id_from_text(self, value: str) -> str | None:
        text = str(value or "")
        if not text:
            return None
        match = re.search(r"diagnostic_id=([A-Za-z0-9_-]+)", text)
        if not match:
            return None
        return str(match.group(1)).strip() or None
