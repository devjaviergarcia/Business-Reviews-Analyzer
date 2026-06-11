from __future__ import annotations

from typing import Any, Awaitable, Callable

from src.workers.contracts import AnalysisJobStatus, parse_analyze_business_payload


class TripadvisorLiveCaptureRuntime:
    def __init__(
        self,
        *,
        job_service: Any,
        parse_object_id: Callable[..., Any],
        validate_business_name: Callable[[str], str],
        sanitize_response_payload: Callable[[Any], Any],
        ensure_job_is_scrape: Callable[[dict[str, Any]], None],
        scrape_business_for_analysis_pipeline: Callable[..., Awaitable[dict[str, Any]]],
        handoff_completed_scrape_to_analysis: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        self._job_service = job_service
        self._parse_object_id = parse_object_id
        self._validate_business_name = validate_business_name
        self._sanitize_response_payload = sanitize_response_payload
        self._ensure_job_is_scrape = ensure_job_is_scrape
        self._scrape_business_for_analysis_pipeline = scrape_business_for_analysis_pipeline
        self._handoff_completed_scrape_to_analysis = handoff_completed_scrape_to_analysis

    async def commit_live_capture(
        self,
        *,
        job_id: str,
        listing: dict[str, Any],
        reviews: list[dict[str, Any]],
        commit_reason: str | None,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not isinstance(listing, dict):
            raise ValueError("listing must be an object.")
        if not isinstance(reviews, list):
            raise ValueError("reviews must be an array.")

        existing_job = await self._job_service.get_job(job_id=job_id)
        self._ensure_job_is_scrape(existing_job)
        queue_name = str(existing_job.get("queue_name") or "").strip().lower()
        if queue_name != "scrape_tripadvisor":
            raise ValueError("live commit is supported only for scrape_tripadvisor jobs.")

        status_value = str(existing_job.get("status") or "").strip().lower()
        if status_value == "done":
            return self._sanitize_response_payload(
                {
                    "job_id": str(job_id),
                    "status": "done",
                    "already_done": True,
                    "result": existing_job.get("result"),
                }
            )

        task_payload = parse_analyze_business_payload(existing_job)
        source_business_name = self._validate_business_name(task_payload.source_name or task_payload.name)
        canonical_business_name = self._validate_business_name(task_payload.canonical_name or task_payload.name)
        normalized_metadata = dict(metadata) if isinstance(metadata, dict) else {}
        normalized_reviews = [dict(item) for item in reviews if isinstance(item, dict)]

        job_object_id = self._parse_object_id(job_id, field_name="job_id")
        await self._job_service.append_event(
            job_id=job_object_id,
            stage="live_commit_started",
            message="Live TripAdvisor capture commit started.",
            status=AnalysisJobStatus.RUNNING,
            data={
                "source": "tripadvisor",
                "review_count_received": len(normalized_reviews),
                "commit_reason": str(commit_reason or "").strip() or "live_session_capture",
                "metadata": normalized_metadata,
            },
        )

        async def _job_progress(event: dict[str, Any]) -> None:
            stage_value = str(event.get("stage", "") or "live_commit_progress")
            message_value = str(event.get("message", "") or "Live commit in progress.")
            raw_data = event.get("data")
            data_value = raw_data if isinstance(raw_data, dict) else {}
            await self._job_service.append_event(
                job_id=job_object_id,
                stage=f"live_{stage_value}",
                message=message_value,
                status=AnalysisJobStatus.RUNNING,
                data={"source": "tripadvisor", "live_commit": True, **data_value},
            )

        try:
            scrape_result = await self._scrape_business_for_analysis_pipeline(
                name=source_business_name,
                canonical_name=canonical_business_name,
                source_name=source_business_name,
                root_business_id=task_payload.root_business_id,
                force=True,
                strategy=task_payload.strategy,
                force_mode=task_payload.force_mode,
                interactive_max_rounds=task_payload.interactive_max_rounds,
                html_scroll_max_rounds=task_payload.html_scroll_max_rounds,
                html_stable_rounds=task_payload.html_stable_rounds,
                tripadvisor_max_pages=task_payload.tripadvisor_max_pages,
                tripadvisor_pages_percent=task_payload.tripadvisor_pages_percent,
                sources=("tripadvisor",),
                preloaded_source_payloads={
                    "tripadvisor": {
                        "listing": dict(listing),
                        "reviews": normalized_reviews,
                    }
                },
                source_job_id=str(job_id),
                progress_callback=_job_progress,
            )
        except Exception as exc:
            await self._job_service.append_event(
                job_id=job_object_id,
                stage="live_commit_failed",
                message="Live TripAdvisor capture commit failed.",
                status=AnalysisJobStatus.NEEDS_HUMAN,
                data={"source": "tripadvisor", "error": str(exc)},
            )
            raise

        result_payload = dict(scrape_result)
        result_payload["pipeline"] = {
            "worker": "live_commit",
            "source": "tripadvisor",
            "queue_name": "scrape_tripadvisor",
            "mode": "live_commit",
        }
        result_payload["live_commit"] = {
            "committed": True,
            "review_count_received": len(normalized_reviews),
            "commit_reason": str(commit_reason or "").strip() or "live_session_capture",
            "metadata": normalized_metadata,
        }

        handoff_result = await self._handoff_completed_scrape_to_analysis(
            scrape_round_id=task_payload.scrape_round_id,
            source="tripadvisor",
            source_job_id=str(job_id),
            business_id=str(scrape_result.get("business_id") or "").strip(),
            dataset_id=str(scrape_result.get("analysis_dataset_id") or "").strip() or None,
            source_profile_id=str(scrape_result.get("source_profile_id") or "").strip() or None,
            scrape_run_id=str(scrape_result.get("scrape_run_id") or "").strip() or None,
        )
        if handoff_result.get("analysis_enqueued"):
            await self._job_service.append_event(
                job_id=job_object_id,
                stage="handoff_analysis_queued",
                message="Live TripAdvisor capture completed. Analysis job queued.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "source": "tripadvisor",
                    "analysis_job_id": handoff_result.get("analysis_job_id"),
                    "analysis_queue_name": handoff_result.get("analysis_queue_name"),
                    "analysis_job_type": handoff_result.get("analysis_job_type"),
                    "analysis_payload": handoff_result.get("analysis_payload"),
                    "scrape_round_id": handoff_result.get("scrape_round_id"),
                },
            )
        else:
            await self._job_service.append_event(
                job_id=job_object_id,
                stage="handoff_analysis_waiting_round",
                message="Live TripAdvisor capture completed. Waiting for remaining scrape sources before analysis.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "source": "tripadvisor",
                    "pending_sources": handoff_result.get("pending_sources") or [],
                    "completed_sources": handoff_result.get("completed_sources") or [],
                    "scrape_round_id": handoff_result.get("scrape_round_id"),
                    "claim_in_progress": bool(handoff_result.get("claim_in_progress")),
                },
            )
        result_payload["analysis_handoff"] = {
            "mode": handoff_result.get("mode"),
            "scrape_round_id": handoff_result.get("scrape_round_id"),
            "analysis_job_id": handoff_result.get("analysis_job_id"),
            "queue_name": handoff_result.get("analysis_queue_name"),
            "job_type": handoff_result.get("analysis_job_type"),
            "waiting_for_sources": bool(handoff_result.get("waiting_for_sources")),
            "pending_sources": handoff_result.get("pending_sources") or [],
            "completed_sources": handoff_result.get("completed_sources") or [],
            "claim_in_progress": bool(handoff_result.get("claim_in_progress")),
        }
        await self._job_service.mark_done(job_id=job_object_id, result=result_payload)
        return self._sanitize_response_payload(
            {
                "job_id": str(job_id),
                "status": "done",
                "already_done": False,
                "result": result_payload,
            }
        )
