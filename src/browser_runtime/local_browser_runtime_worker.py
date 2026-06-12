from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
from collections import Counter
from typing import Any

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_LIVE_DISPLAY_MODE,
    DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
    infer_browser_source,
    normalize_browser_source,
)
from src.job_runtime.local_browser_job_coordinator import LocalBrowserJobCoordinator
from src.scraping_google_maps.google_maps_browser_adapter import GoogleMapsBrowserAdapter
from src.scraping_shared.browser_scrape_errors import (
    ScrapeBotDetectedError,
    ScrapeNeedsHumanInterventionError,
)
from src.scraping_tripadvisor.tripadvisor_browser_adapter import TripadvisorBrowserAdapter
from src.services.analysis_job_service import AnalysisJobService
from src.services.business_service import BusinessService
from src.services.crm_service import CRMService
from src.workers.contracts import (
    AnalysisJobStatus,
    parse_analyze_business_payload,
    parse_crm_lead_discovery_payload,
    parse_geo_grid_study_payload,
)

from .browser_job_live_display_runtime import BrowserJobLiveDisplayRuntime
from .local_browser_worker_registry import LocalBrowserWorkerRegistry

LOGGER = logging.getLogger("local_browser_runtime_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

_CANCELLED_BY_USER_ERROR = "Cancelled by user."


class LocalBrowserRuntimeWorker:
    def __init__(
        self,
        *,
        job_service: AnalysisJobService,
        business_service: BusinessService,
        crm_service: CRMService,
        local_browser_jobs: LocalBrowserJobCoordinator,
        local_browser_registry: LocalBrowserWorkerRegistry,
        google_maps_adapter: GoogleMapsBrowserAdapter,
        tripadvisor_adapter: TripadvisorBrowserAdapter,
        worker_id: str | None = None,
        supported_sources: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        configured_sources = supported_sources or settings.local_browser_worker_supported_sources
        normalized_sources = [
            source
            for source in (
                normalize_browser_source(item) for item in (configured_sources or [])
            )
            if source is not None
        ]
        if not normalized_sources:
            normalized_sources = ["google_maps", "tripadvisor"]

        resolved_worker_id = str(worker_id or settings.local_browser_worker_id or "").strip()
        if not resolved_worker_id:
            resolved_worker_id = f"local-browser:{socket.gethostname()}:{os.getpid()}"

        self._job_service = job_service
        self._business_service = business_service
        self._crm_service = crm_service
        self._local_browser_jobs = local_browser_jobs
        self._local_browser_registry = local_browser_registry
        self._google_maps_adapter = google_maps_adapter
        self._tripadvisor_adapter = tripadvisor_adapter
        self._worker_id = resolved_worker_id
        self._host_name = socket.gethostname()
        self._supported_sources = tuple(normalized_sources)
        self._poll_seconds = max(1, int(settings.worker_poll_seconds))
        self._idle_log_seconds = max(5, int(settings.worker_idle_log_seconds))
        self._heartbeat_seconds = max(3, int(settings.local_browser_worker_heartbeat_seconds))
        self._idle_log_every_ticks = max(1, self._idle_log_seconds // self._poll_seconds)
        self._live_display_runtime = BrowserJobLiveDisplayRuntime()
        self._current_job_state: dict[str, Any] = {
            "state": "idle",
            "job_id": None,
            "source": None,
            "execution_mode": None,
        }

    async def run_forever(self) -> None:
        await connect_to_mongo()
        await self._crm_service.ensure_indexes()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            LOGGER.info(
                "LocalBrowserRuntimeWorker started worker_id=%s host=%s supported_sources=%s poll_interval=%ss heartbeat=%ss",
                self._worker_id,
                self._host_name,
                list(self._supported_sources),
                self._poll_seconds,
                self._heartbeat_seconds,
            )
            idle_ticks = 0
            while True:
                job = await self._local_browser_jobs.claim_next_job(
                    worker_id=self._worker_id,
                    host_name=self._host_name,
                    supported_sources=self._supported_sources,
                )
                if not job:
                    idle_ticks += 1
                    if idle_ticks % self._idle_log_every_ticks == 0:
                        LOGGER.info(
                            "Local browser runtime idle worker_id=%s no_jobs_for=%ss",
                            self._worker_id,
                            idle_ticks * self._poll_seconds,
                        )
                    await asyncio.sleep(self._poll_seconds)
                    continue

                idle_ticks = 0
                await self._process_job(job)
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._local_browser_registry.heartbeat(
                worker_id=self._worker_id,
                state="stopped",
                supported_sources=self._supported_sources,
            )
            await close_mongo_connection()

    async def _heartbeat_loop(self) -> None:
        while True:
            await self._local_browser_registry.heartbeat(
                worker_id=self._worker_id,
                state=str(self._current_job_state.get("state") or "idle"),
                supported_sources=self._supported_sources,
                current_job_id=self._current_job_state.get("job_id"),
                current_source=self._current_job_state.get("source"),
                current_execution_mode=self._current_job_state.get("execution_mode"),
            )
            await asyncio.sleep(self._heartbeat_seconds)

    async def _process_job(self, job: dict[str, Any]) -> None:
        job_type = str(job.get("job_type") or "").strip().lower() or "unknown"
        if job_type == "business_analyze":
            await self._process_business_scrape_job(job)
            return
        if job_type == "crm_lead_discovery":
            await self._process_crm_discovery_job(job)
            return
        if job_type == "geo_grid_study":
            await self._process_geo_grid_study_job(job)
            return

        job_id = job.get("_id")
        error = f"Unsupported local browser job type '{job_type}'."
        await self._job_service.mark_failed(job_id=job_id, error=error)
        LOGGER.error("Unsupported local browser job id=%s job_type=%s", job_id, job_type)

    async def _process_business_scrape_job(self, job: dict[str, Any]) -> None:
        job_id = job.get("_id")
        task_payload = parse_analyze_business_payload(job)
        source = infer_browser_source(
            queue_name=job.get("queue_name"),
            payload=job.get("payload") if isinstance(job.get("payload"), dict) else None,
            explicit_source=job.get("source"),
        )
        if source is None:
            await self._job_service.mark_failed(job_id=job_id, error="Could not infer browser scrape source.")
            return
        if source == "tripadvisor":
            reason = (
                "TripAdvisor scrape jobs are locked to the replay-headfull live-session flow. "
                "Launch them through 'Abrir Needs Human TA' / live replay instead of the local browser worker."
            )
            await self._job_service.mark_needs_human(
                job_id=job_id,
                reason=reason,
                data={
                    "source": "tripadvisor",
                    "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                    "reason_code": "tripadvisor_replay_headfull_required",
                    "required_flow": "replay-headfull",
                    "automatic_relaunch_disabled": True,
                },
            )
            LOGGER.warning(
                "Blocked TripAdvisor local browser execution outside replay-headfull job=%s queue=%s",
                job_id,
                job.get("queue_name"),
            )
            return

        execution_mode = str(job.get("execution_mode") or task_payload.execution_mode or DEFAULT_BROWSER_EXECUTION_MODE)
        execution_mode = execution_mode.strip().lower() or DEFAULT_BROWSER_EXECUTION_MODE
        live_display_mode = (
            str(
                job.get("live_display_mode")
                or task_payload.live_display_mode
                or DEFAULT_BROWSER_LIVE_DISPLAY_MODE
            )
            .strip()
            .lower()
            or DEFAULT_BROWSER_LIVE_DISPLAY_MODE
        )
        adapter = self._google_maps_adapter if source == "google_maps" else self._tripadvisor_adapter
        self._configure_browser_mode(execution_mode=execution_mode)
        self._current_job_state.update(
            {
                "state": "running",
                "job_id": str(job_id),
                "source": source,
                "execution_mode": execution_mode,
            }
        )

        started_at = time.monotonic()
        stage_counts: Counter[str] = Counter()
        heartbeat_seconds = max(5, int(settings.worker_job_heartbeat_seconds))
        stall_warning_seconds = max(heartbeat_seconds, int(settings.worker_progress_stall_warning_seconds))
        progress_state: dict[str, Any] = {
            "stage": "local_browser_worker_started",
            "message": "Local browser runtime worker claimed job.",
            "last_progress_monotonic": started_at,
        }

        async def on_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "running")
            message = str(event.get("message", "") or "In progress.")
            raw_data = event.get("data")
            data = dict(raw_data) if isinstance(raw_data, dict) else {}
            data.setdefault("source", source)
            data.setdefault("runtime_target", DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET)
            data.setdefault("execution_mode", execution_mode)
            data.setdefault("live_display_mode", live_display_mode)
            stage_counts[stage] += 1
            progress_state["stage"] = stage
            progress_state["message"] = message
            progress_state["last_progress_monotonic"] = time.monotonic()
            LOGGER.info(
                "Local browser progress job=%s source=%s mode=%s display=%s stage=%s count=%s data=%s",
                job_id,
                source,
                execution_mode,
                live_display_mode,
                stage,
                stage_counts[stage],
                self._summarize_progress_data(data),
            )
            await self._job_service.append_event(
                job_id=job_id,
                stage=stage,
                message=message,
                data=data,
                status=AnalysisJobStatus.RUNNING,
            )

        async def heartbeat_loop() -> None:
            while True:
                await asyncio.sleep(heartbeat_seconds)
                elapsed = round(time.monotonic() - started_at, 2)
                seconds_without_progress = round(
                    time.monotonic() - float(progress_state.get("last_progress_monotonic", started_at)),
                    2,
                )
                current_stage = str(progress_state.get("stage", "unknown"))
                current_message = str(progress_state.get("message", "") or "")
                current_stage_count = int(stage_counts.get(current_stage, 0))
                log_method = LOGGER.warning if seconds_without_progress >= stall_warning_seconds else LOGGER.info
                log_method(
                    "Local browser heartbeat job=%s source=%s mode=%s display=%s elapsed=%ss current_stage=%s stage_count=%s seconds_without_progress=%ss last_message=%s",
                    job_id,
                    source,
                    execution_mode,
                    live_display_mode,
                    elapsed,
                    current_stage,
                    current_stage_count,
                    seconds_without_progress,
                    current_message,
                )

        async def cancellation_watch_loop() -> None:
            while True:
                should_cancel = await self._job_service.is_job_cancel_requested(job_id=job_id)
                if should_cancel:
                    LOGGER.warning("Cancellation requested for local browser job=%s", job_id)
                    return
                await asyncio.sleep(1.0)

        heartbeat_task = asyncio.create_task(heartbeat_loop())
        cancellation_task = asyncio.create_task(cancellation_watch_loop())
        job_outcome = "failed"

        try:
            async def run_scrape_with_selected_display() -> dict[str, Any]:
                with self._live_display_runtime.activate_for_job(
                    execution_mode=execution_mode,
                    live_display_mode=live_display_mode,
                ):
                    return await adapter.run_scrape(
                        task_payload=task_payload,
                        job_id=str(job_id),
                        progress_callback=on_progress,
                    )

            scrape_task = asyncio.create_task(run_scrape_with_selected_display())
            done, _ = await asyncio.wait(
                {scrape_task, cancellation_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_task in done:
                scrape_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await scrape_task
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

            scrape_result = await scrape_task
            cancellation_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_task

            if await self._job_service.is_job_cancel_requested(job_id=job_id):
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

            business_id = str(scrape_result.get("business_id") or "").strip()
            if not business_id:
                raise RuntimeError("Scrape stage did not return a valid business_id.")

            handoff_result = await self._business_service.handoff_completed_scrape_to_analysis(
                scrape_round_id=task_payload.scrape_round_id,
                source=source,
                source_job_id=str(job_id),
                business_id=business_id,
                dataset_id=str(scrape_result.get("analysis_dataset_id") or "").strip() or None,
                source_profile_id=str(scrape_result.get("source_profile_id") or "").strip() or None,
                scrape_run_id=str(scrape_result.get("scrape_run_id") or "").strip() or None,
            )
            analysis_job_id = str(handoff_result.get("analysis_job_id") or "").strip() or None
            if handoff_result.get("analysis_enqueued"):
                await self._job_service.append_event(
                    job_id=job_id,
                    stage="handoff_analysis_queued",
                    message="Local browser scrape completed. Analysis job queued.",
                    status=AnalysisJobStatus.RUNNING,
                    data={
                        "source": source,
                        "execution_mode": execution_mode,
                        "live_display_mode": live_display_mode,
                        "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                        "analysis_job_id": analysis_job_id,
                        "analysis_queue_name": handoff_result.get("analysis_queue_name"),
                        "analysis_job_type": handoff_result.get("analysis_job_type"),
                        "analysis_payload": handoff_result.get("analysis_payload"),
                        "scrape_round_id": handoff_result.get("scrape_round_id"),
                    },
                )
            else:
                await self._job_service.append_event(
                    job_id=job_id,
                    stage="handoff_analysis_waiting_round",
                    message="Local browser scrape completed. Waiting for remaining scrape sources before analysis.",
                    status=AnalysisJobStatus.RUNNING,
                    data={
                        "source": source,
                        "execution_mode": execution_mode,
                        "live_display_mode": live_display_mode,
                        "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                        "scrape_round_id": handoff_result.get("scrape_round_id"),
                        "pending_sources": handoff_result.get("pending_sources") or [],
                        "completed_sources": handoff_result.get("completed_sources") or [],
                        "claim_in_progress": bool(handoff_result.get("claim_in_progress")),
                    },
                )
            scrape_result = dict(scrape_result)
            scrape_result["pipeline"] = {
                "worker": "local_browser_runtime",
                "source": source,
                "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                "execution_mode": execution_mode,
                "live_display_mode": live_display_mode,
            }
            scrape_result["analysis_handoff"] = {
                "mode": handoff_result.get("mode"),
                "scrape_round_id": handoff_result.get("scrape_round_id"),
                "source_status": handoff_result.get("source_status"),
                "analysis_job_id": analysis_job_id,
                "queue_name": handoff_result.get("analysis_queue_name"),
                "job_type": handoff_result.get("analysis_job_type"),
                "waiting_for_sources": bool(handoff_result.get("waiting_for_sources")),
                "pending_sources": handoff_result.get("pending_sources") or [],
                "completed_sources": handoff_result.get("completed_sources") or [],
                "claim_in_progress": bool(handoff_result.get("claim_in_progress")),
            }
            await self._job_service.mark_done(job_id=job_id, result=scrape_result)
            job_outcome = "done"
        except ScrapeBotDetectedError as exc:
            await self._job_service.mark_needs_human(
                job_id=job_id,
                reason=str(exc),
                data={
                    "source": source,
                    "execution_mode": execution_mode,
                    "live_display_mode": live_display_mode,
                    "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                    "reason_code": "scrape_antibot_detected",
                    "suggested_execution_mode": "live",
                    **(exc.context if isinstance(exc.context, dict) else {}),
                },
            )
            job_outcome = "needs_human"
        except ScrapeNeedsHumanInterventionError as exc:
            await self._job_service.mark_needs_human(
                job_id=job_id,
                reason=str(exc),
                data={
                    "source": source,
                    "execution_mode": execution_mode,
                    "live_display_mode": live_display_mode,
                    "runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET,
                    "reason_code": "scrape_needs_human",
                    "suggested_execution_mode": "live",
                    **(exc.context if isinstance(exc.context, dict) else {}),
                },
            )
            job_outcome = "needs_human"
        except RuntimeError as exc:
            if str(exc).strip() == _CANCELLED_BY_USER_ERROR:
                await self._job_service.mark_failed(job_id=job_id, error=_CANCELLED_BY_USER_ERROR)
                job_outcome = "cancelled"
            else:
                await self._job_service.mark_failed(job_id=job_id, error=str(exc))
                job_outcome = "failed"
                LOGGER.exception(
                    "Local browser scrape failed job=%s source=%s mode=%s display=%s error=%s",
                    job_id,
                    source,
                    execution_mode,
                    live_display_mode,
                    exc,
                )
        except Exception as exc:  # noqa: BLE001
            await self._job_service.mark_failed(job_id=job_id, error=str(exc))
            job_outcome = "failed"
            LOGGER.exception("Local browser scrape failed job=%s source=%s mode=%s display=%s error=%s", job_id, source, execution_mode, live_display_mode, exc)
        finally:
            cancellation_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_task
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            self._current_job_state.update(
                {
                    "state": "idle",
                    "job_id": None,
                    "source": None,
                    "execution_mode": None,
                }
            )
            LOGGER.info(
                "Local browser scrape job finished job=%s source=%s mode=%s outcome=%s stage_counts=%s",
                job_id,
                source,
                f"{execution_mode}:{live_display_mode}",
                job_outcome,
                dict(stage_counts),
            )

    async def _process_crm_discovery_job(self, job: dict[str, Any]) -> None:
        job_id = job.get("_id")
        self._current_job_state.update(
            {
                "state": "running",
                "job_id": str(job_id),
                "source": "google_maps",
                "execution_mode": "automatic",
            }
        )
        self._configure_browser_mode(execution_mode="automatic")
        try:
            await self._job_service.append_event(
                job_id=job_id,
                stage="crm_discovery_local_browser_started",
                message="CRM discovery started on local browser runtime worker.",
                status=AnalysisJobStatus.RUNNING,
                data={"runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET, "source": "google_maps"},
            )
            payload = parse_crm_lead_discovery_payload(job)
            result = await self._crm_service.process_discovery_task(task_payload=payload, job_id=job_id)
            await self._job_service.mark_done(job_id=job_id, result=result)
        except Exception as exc:  # noqa: BLE001
            await self._job_service.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception("CRM discovery job failed on local browser runtime id=%s error=%s", job_id, exc)
        finally:
            self._current_job_state.update({"state": "idle", "job_id": None, "source": None, "execution_mode": None})

    async def _process_geo_grid_study_job(self, job: dict[str, Any]) -> None:
        job_id = job.get("_id")
        self._current_job_state.update(
            {
                "state": "running",
                "job_id": str(job_id),
                "source": "google_maps",
                "execution_mode": "automatic",
            }
        )
        self._configure_browser_mode(execution_mode="automatic")
        try:
            await self._job_service.append_event(
                job_id=job_id,
                stage="geo_grid_local_browser_started",
                message="Geo grid study started on local browser runtime worker.",
                status=AnalysisJobStatus.RUNNING,
                data={"runtime_target": DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET, "source": "google_maps"},
            )
            payload = parse_geo_grid_study_payload(job)
            result = await self._crm_service.process_geo_grid_study_task(task_payload=payload, job_id=job_id)
            await self._job_service.mark_done(job_id=job_id, result=result)
        except Exception as exc:  # noqa: BLE001
            await self._job_service.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception("Geo grid job failed on local browser runtime id=%s error=%s", job_id, exc)
        finally:
            self._current_job_state.update({"state": "idle", "job_id": None, "source": None, "execution_mode": None})

    def _configure_browser_mode(self, *, execution_mode: str) -> None:
        is_live = str(execution_mode or DEFAULT_BROWSER_EXECUTION_MODE).strip().lower() == "live"
        headless = not is_live
        for service in (self._business_service, self._crm_service.business_service):
            try:
                service.scraper.headless = headless
            except Exception:
                pass
            try:
                service.scraper._headless = headless  # noqa: SLF001
            except Exception:
                pass
            try:
                service.tripadvisor_scraper.headless = headless
            except Exception:
                pass
            try:
                service.tripadvisor_scraper._headless = headless  # noqa: SLF001
            except Exception:
                pass

    def _summarize_progress_data(self, data: dict[str, Any]) -> dict[str, Any]:
        prioritized_keys = [
            "source",
            "execution_mode",
            "live_display_mode",
            "event",
            "round",
            "reviews_loaded",
            "scraped_review_count",
            "processed_review_count",
            "review_count",
            "business_id",
            "query",
            "dataset_id",
            "analysis_dataset_id",
            "source_profile_id",
            "scrape_run_id",
            "tripadvisor_max_pages",
            "tripadvisor_pages_percent",
        ]
        summary: dict[str, Any] = {}
        for key in prioritized_keys:
            if key in data:
                summary[key] = data.get(key)
        return summary
