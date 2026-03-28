from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
import uuid
from collections import Counter
from typing import Any

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.dependencies import create_business_service, create_worker_job_broker
from src.services.business_service import (
    BusinessService,
    ScrapeBotDetectedError,
    ScrapeNeedsHumanInterventionError,
)
from src.services.tripadvisor_session_service import TripadvisorSessionService
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisGenerateTaskPayload, AnalysisJobStatus, parse_analyze_business_payload

LOGGER = logging.getLogger("scraper_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

_CANCELLED_BY_USER_ERROR = "Cancelled by user."


class ScraperWorker(QueuedJobWorkerBase):
    queue_name = settings.worker_scrape_queue
    logger_name = "scraper_worker"

    def __init__(
        self,
        service: BusinessService | None = None,
        job_broker: WorkerJobBroker | None = None,
    ) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())
        self._service = service or create_business_service()
        self._tripadvisor_session_service = TripadvisorSessionService()
        self.queue_name = self._resolve_queue_name(settings.worker_scrape_queue)
        self._scrape_source = self._resolve_scrape_source(settings.worker_scrape_source)
        self._selected_sources = None if self._scrape_source == "all" else (self._scrape_source,)
        self._singleton_lock_lost = asyncio.Event()
        self._singleton_token: str | None = None
        self._singleton_pid = os.getpid()
        self._singleton_host = socket.gethostname()

    async def run_forever(self) -> None:
        if not self._should_use_tripadvisor_singleton():
            await super().run_forever()
            return

        await connect_to_mongo()
        singleton_token = await self._acquire_tripadvisor_singleton_or_none()
        if not singleton_token:
            await close_mongo_connection()
            return

        heartbeat_task = asyncio.create_task(self._tripadvisor_singleton_heartbeat_loop(singleton_token))
        try:
            self._logger.info(
                "%s started. queue=%s poll_interval=%ss idle_log_every=%ss singleton_pid=%s singleton_host=%s singleton_token=%s",
                type(self).__name__,
                self.queue_name,
                self._poll_seconds,
                self._idle_log_seconds,
                self._singleton_pid,
                self._singleton_host,
                singleton_token,
            )
            idle_ticks = 0
            while True:
                if self._singleton_lock_lost.is_set():
                    self._logger.error(
                        "Tripadvisor worker singleton lock lost. queue=%s token=%s. Stopping worker.",
                        self.queue_name,
                        singleton_token,
                    )
                    break
                job = await self._job_broker.claim_next_job(queue_name=self.queue_name)
                if not job:
                    idle_ticks += 1
                    if idle_ticks % self._idle_log_every_ticks == 0:
                        self._logger.info(
                            "%s idle. queue=%s no_jobs_for=%ss",
                            type(self).__name__,
                            self.queue_name,
                            idle_ticks * self._poll_seconds,
                        )
                    await asyncio.sleep(self._poll_seconds)
                    continue
                idle_ticks = 0
                self._logger.info(
                    "%s claimed job queue=%s job_id=%s job_type=%s attempts=%s status=%s",
                    type(self).__name__,
                    self.queue_name,
                    job.get("_id"),
                    job.get("job_type"),
                    job.get("attempts"),
                    job.get("status"),
                )
                try:
                    await self._process_job(job)
                except Exception:  # noqa: BLE001
                    self._logger.exception(
                        "%s unexpected error while processing claimed job queue=%s job_id=%s",
                        type(self).__name__,
                        self.queue_name,
                        job.get("_id"),
                    )
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._release_tripadvisor_singleton(
                token=singleton_token,
                reason="worker_shutdown",
            )
            await close_mongo_connection()

    def _resolve_queue_name(self, queue_name: str) -> str:
        normalized_queue = str(queue_name or "").strip().lower()
        allowed = {"scrape", "scrape_google_maps", "scrape_tripadvisor"}
        if normalized_queue not in allowed:
            allowed_values = ", ".join(sorted(allowed))
            raise ValueError(
                f"Unsupported scrape queue '{queue_name}'. Allowed values: {allowed_values}."
            )
        return normalized_queue

    def _resolve_scrape_source(self, source: str) -> str:
        normalized_source = str(source or "").strip().lower()
        allowed = {"all", "google_maps", "tripadvisor"}
        if normalized_source not in allowed:
            allowed_values = ", ".join(sorted(allowed))
            raise ValueError(
                f"Unsupported scrape source '{source}'. Allowed values: {allowed_values}."
            )
        if self.queue_name == "scrape_google_maps":
            return "google_maps"
        if self.queue_name == "scrape_tripadvisor":
            return "tripadvisor"
        return normalized_source

    def _should_use_tripadvisor_singleton(self) -> bool:
        if not bool(settings.tripadvisor_worker_singleton_enabled):
            return False
        return self.queue_name == "scrape_tripadvisor" or self._scrape_source == "tripadvisor"

    def _singleton_metadata(self) -> dict[str, Any]:
        return {
            "env": str(settings.app_env),
            "user": str(os.environ.get("USER") or os.environ.get("USERNAME") or "").strip() or None,
            "process_start_monotonic": time.monotonic(),
        }

    async def _acquire_tripadvisor_singleton_or_none(self) -> str | None:
        token = uuid.uuid4().hex
        self._singleton_token = token
        try:
            result = await self._tripadvisor_session_service.try_acquire_worker_singleton(
                token=token,
                pid=self._singleton_pid,
                queue_name=self.queue_name,
                worker_source=self._scrape_source,
                host=self._singleton_host,
                metadata=self._singleton_metadata(),
                stale_after_seconds=int(settings.tripadvisor_worker_singleton_stale_seconds),
            )
        except Exception:  # noqa: BLE001
            self._logger.exception(
                "Failed acquiring TripAdvisor worker singleton lock. queue=%s pid=%s host=%s",
                self.queue_name,
                self._singleton_pid,
                self._singleton_host,
            )
            return None

        if bool(result.get("acquired")):
            self._logger.info(
                "Tripadvisor worker singleton lock acquired. queue=%s token=%s pid=%s host=%s",
                self.queue_name,
                token,
                self._singleton_pid,
                self._singleton_host,
            )
            return token

        holder = result.get("worker_singleton") if isinstance(result, dict) else None
        holder_pid = holder.get("pid") if isinstance(holder, dict) else None
        holder_host = holder.get("host") if isinstance(holder, dict) else None
        holder_heartbeat = holder.get("heartbeat_at") if isinstance(holder, dict) else None
        holder_token = holder.get("token") if isinstance(holder, dict) else None
        self._logger.error(
            "Another TripAdvisor worker is already active. queue=%s current_pid=%s current_host=%s holder_pid=%s holder_host=%s holder_heartbeat=%s holder_token=%s. Exiting.",
            self.queue_name,
            self._singleton_pid,
            self._singleton_host,
            holder_pid,
            holder_host,
            holder_heartbeat,
            holder_token,
        )
        return None

    async def _tripadvisor_singleton_heartbeat_loop(self, token: str) -> None:
        interval_seconds = max(3, int(settings.tripadvisor_worker_singleton_heartbeat_seconds))
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                still_owned = await self._tripadvisor_session_service.touch_worker_singleton(
                    token=token,
                    pid=self._singleton_pid,
                    metadata=self._singleton_metadata(),
                )
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "Tripadvisor singleton heartbeat failed unexpectedly. token=%s pid=%s",
                    token,
                    self._singleton_pid,
                )
                self._singleton_lock_lost.set()
                return
            if not still_owned:
                self._logger.error(
                    "Tripadvisor singleton heartbeat lost ownership. token=%s pid=%s",
                    token,
                    self._singleton_pid,
                )
                self._singleton_lock_lost.set()
                return

    async def _release_tripadvisor_singleton(self, *, token: str, reason: str) -> None:
        try:
            released = await self._tripadvisor_session_service.release_worker_singleton(
                token=token,
                reason=reason,
            )
            if released:
                self._logger.info(
                    "Tripadvisor worker singleton lock released. token=%s pid=%s reason=%s",
                    token,
                    self._singleton_pid,
                    reason,
                )
            else:
                self._logger.warning(
                    "Tripadvisor worker singleton lock release skipped (token not owner). token=%s pid=%s reason=%s",
                    token,
                    self._singleton_pid,
                    reason,
                )
        except Exception:  # noqa: BLE001
            self._logger.exception(
                "Failed releasing TripAdvisor worker singleton lock. token=%s pid=%s reason=%s",
                token,
                self._singleton_pid,
                reason,
            )

    def _should_handoff_to_analysis(self) -> bool:
        return self._scrape_source in {"all", "google_maps"}

    def _with_worker_source(self, data: Any) -> dict[str, Any]:
        payload = dict(data) if isinstance(data, dict) else {}
        if self._scrape_source != "all":
            payload.setdefault("source", self._scrape_source)
        return payload

    def _summarize_progress_data(self, data: Any) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}

        prioritized_keys = [
            "source",
            "event",
            "round",
            "reviews_loaded",
            "at_bottom",
            "unchanged_rounds",
            "effective_max_rounds",
            "scraped_review_count",
            "processed_review_count",
            "dataset_review_count",
            "review_count",
            "business_id",
            "query",
            "total_reviews",
            "dataset_id",
            "analysis_dataset_id",
            "legacy_dataset_id",
            "source_profile_id",
            "scrape_run_id",
            "tripadvisor_max_pages",
            "tripadvisor_pages_percent",
            "total_pages",
            "current_page",
            "remaining_pages",
        ]
        summary: dict[str, Any] = {}
        for key in prioritized_keys:
            if key not in data:
                continue
            value = data.get(key)
            if isinstance(value, (str, int, float, bool)) or value is None:
                summary[key] = value
            elif isinstance(value, (list, tuple, set)):
                summary[key] = f"<{type(value).__name__} len={len(value)}>"
            elif isinstance(value, dict):
                summary[key] = f"<dict keys={len(value)}>"
            else:
                summary[key] = str(value)

        if summary:
            return summary

        for key, value in data.items():
            if len(summary) >= 8:
                break
            if isinstance(value, (str, int, float, bool)) or value is None:
                summary[key] = value
            elif isinstance(value, (list, tuple, set)):
                summary[key] = f"<{type(value).__name__} len={len(value)}>"
            elif isinstance(value, dict):
                summary[key] = f"<dict keys={len(value)}>"
            else:
                summary[key] = str(value)
        return summary

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        started_at = time.monotonic()
        stage_counts: Counter[str] = Counter()
        heartbeat_seconds = max(5, int(settings.worker_job_heartbeat_seconds))
        stall_warning_seconds = max(heartbeat_seconds, int(settings.worker_progress_stall_warning_seconds))
        cancel_poll_seconds = 1.0
        progress_state: dict[str, Any] = {
            "stage": "worker_started",
            "message": "Worker claimed job.",
            "last_progress_monotonic": started_at,
        }
        task_payload = parse_analyze_business_payload(job)
        job_name = task_payload.name
        force = bool(task_payload.force)
        strategy = task_payload.strategy
        force_mode = task_payload.force_mode
        canonical_name = task_payload.canonical_name
        source_name = task_payload.source_name
        root_business_id = task_payload.root_business_id
        interactive_max_rounds = task_payload.interactive_max_rounds
        html_scroll_max_rounds = task_payload.html_scroll_max_rounds
        html_stable_rounds = task_payload.html_stable_rounds
        tripadvisor_max_pages = task_payload.tripadvisor_max_pages
        tripadvisor_pages_percent = task_payload.tripadvisor_pages_percent
        LOGGER.info(
            "Processing scrape job id=%s name=%r force=%s force_mode=%s strategy=%s interactive_max_rounds=%s html_scroll_max_rounds=%s html_stable_rounds=%s tripadvisor_max_pages=%s tripadvisor_pages_percent=%s queue=%s worker_source=%s job_type=%s",
            job_id,
            job_name,
            force,
            force_mode,
            strategy,
            interactive_max_rounds,
            html_scroll_max_rounds,
            html_stable_rounds,
            tripadvisor_max_pages,
            tripadvisor_pages_percent,
            job.get("queue_name"),
            self._scrape_source,
            job.get("job_type"),
        )
        job_outcome = "failed"
        singleton_job_tracking_enabled = bool(
            self._should_use_tripadvisor_singleton() and self._singleton_token and job_id
        )
        if singleton_job_tracking_enabled:
            try:
                await self._tripadvisor_session_service.set_worker_singleton_active_job(
                    token=str(self._singleton_token),
                    job_id=str(job_id),
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "Failed setting active TripAdvisor singleton job. token=%s job_id=%s",
                    self._singleton_token,
                    job_id,
                )

        async def on_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "running")
            message = str(event.get("message", "") or "In progress.")
            data = self._with_worker_source(event.get("data", {}))
            stage_counts[stage] += 1
            elapsed_s = round(time.monotonic() - started_at, 2)
            progress_state["stage"] = stage
            progress_state["message"] = message
            progress_state["last_progress_monotonic"] = time.monotonic()
            summarized_data = self._summarize_progress_data(data)
            LOGGER.info(
                "Scrape progress job=%s elapsed=%ss stage=%s count=%s message=%s data=%s",
                job_id,
                elapsed_s,
                stage,
                stage_counts[stage],
                message,
                summarized_data,
            )
            await self._job_broker.append_event(
                job_id=job_id,
                stage=stage,
                message=message,
                data=data if isinstance(data, dict) else {},
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
                    "Scrape heartbeat job=%s elapsed=%ss current_stage=%s stage_count=%s seconds_without_progress=%ss last_message=%s",
                    job_id,
                    elapsed,
                    current_stage,
                    current_stage_count,
                    seconds_without_progress,
                    current_message,
                )

        async def cancellation_watch_loop() -> None:
            while True:
                should_cancel = await self._job_broker.is_cancel_requested(job_id=job_id)
                if should_cancel:
                    LOGGER.warning("Cancellation requested for scrape job=%s", job_id)
                    return
                await asyncio.sleep(cancel_poll_seconds)

        heartbeat_task = asyncio.create_task(heartbeat_loop())
        cancellation_watch_task = asyncio.create_task(cancellation_watch_loop())

        try:
            scrape_task = asyncio.create_task(
                self._service.scrape_business_for_analysis_pipeline(
                    name=job_name,
                    canonical_name=canonical_name,
                    source_name=source_name,
                    root_business_id=root_business_id,
                    force=force,
                    strategy=strategy,
                    force_mode=force_mode,
                    interactive_max_rounds=interactive_max_rounds,
                    html_scroll_max_rounds=html_scroll_max_rounds,
                    html_stable_rounds=html_stable_rounds,
                    tripadvisor_max_pages=tripadvisor_max_pages,
                    tripadvisor_pages_percent=tripadvisor_pages_percent,
                    sources=self._selected_sources,
                    source_job_id=str(job_id),
                    progress_callback=on_progress,
                )
            )
            done, _ = await asyncio.wait(
                {scrape_task, cancellation_watch_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if cancellation_watch_task in done:
                scrape_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await scrape_task
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

            scrape_result = await scrape_task
            cancellation_watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_watch_task

            elapsed_s = round(time.monotonic() - started_at, 2)
            business_id = str(scrape_result.get("business_id") or "").strip()
            if not business_id:
                raise RuntimeError("Scrape stage did not return a valid business_id.")
            LOGGER.info(
                "Scrape result job=%s elapsed=%ss business_id=%s review_count=%s scraped=%s processed=%s stored_before=%s stored_after=%s new_reviews=%s stage_counts=%s",
                job_id,
                elapsed_s,
                business_id,
                scrape_result.get("review_count"),
                scrape_result.get("scraped_review_count"),
                scrape_result.get("processed_review_count"),
                scrape_result.get("stored_review_count_before"),
                scrape_result.get("stored_review_count_after"),
                scrape_result.get("scrape_produced_new_reviews"),
                dict(stage_counts),
            )
            if not bool(scrape_result.get("scrape_produced_new_reviews")):
                LOGGER.warning(
                    "No new reviews scraped for job=%s business_id=%s. Analysis stage will use stored reviews.",
                    job_id,
                    business_id,
                )

            if await self._job_broker.is_cancel_requested(job_id=job_id):
                raise RuntimeError(_CANCELLED_BY_USER_ERROR)

            if self._should_handoff_to_analysis():
                next_payload = AnalysisGenerateTaskPayload(
                    business_id=business_id,
                    dataset_id=str(scrape_result.get("analysis_dataset_id") or "").strip() or None,
                    source_profile_id=str(scrape_result.get("source_profile_id") or "").strip() or None,
                    scrape_run_id=str(scrape_result.get("scrape_run_id") or "").strip() or None,
                    source_job_id=str(job_id),
                )
                LOGGER.info(
                    "Handing off job=%s to queue=analysis job_type=analysis_generate payload=%s",
                    job_id,
                    next_payload.model_dump(mode="python"),
                )
                analysis_enqueue_result = await self._service.job_service.enqueue_analysis_generate_job(
                    task_payload=next_payload
                )
                analysis_job_id = str(analysis_enqueue_result.get("job_id") or "").strip() or None
                await self._job_broker.append_event(
                    job_id=job_id,
                    stage="handoff_analysis_queued",
                    message="Scrape stage completed. Analysis job queued.",
                    status=AnalysisJobStatus.RUNNING,
                    data=self._with_worker_source(
                        {
                            "analysis_job_id": analysis_job_id,
                            "analysis_queue_name": analysis_enqueue_result.get("queue_name"),
                            "analysis_job_type": analysis_enqueue_result.get("job_type"),
                            "analysis_payload": analysis_enqueue_result.get("payload"),
                            "scrape_result": {
                                "business_id": business_id,
                                "review_count": scrape_result.get("review_count"),
                                "scraped_review_count": scrape_result.get("scraped_review_count"),
                                "processed_review_count": scrape_result.get("processed_review_count"),
                                "cached_scrape": scrape_result.get("cached_scrape"),
                                "stored_review_count_before": scrape_result.get("stored_review_count_before"),
                                "stored_review_count_after": scrape_result.get("stored_review_count_after"),
                                "scrape_produced_new_reviews": scrape_result.get("scrape_produced_new_reviews"),
                                "dataset_id": scrape_result.get("dataset_id"),
                                "analysis_dataset_id": scrape_result.get("analysis_dataset_id"),
                                "legacy_dataset_id": scrape_result.get("legacy_dataset_id"),
                                "source_profile_id": scrape_result.get("source_profile_id"),
                                "scrape_run_id": scrape_result.get("scrape_run_id"),
                            },
                        }
                    ),
                )
                scrape_result = dict(scrape_result)
                scrape_result["pipeline"] = {
                    "worker": "scraper",
                    "source": self._scrape_source,
                    "queue_name": self.queue_name,
                }
                scrape_result["analysis_handoff"] = {
                    "analysis_job_id": analysis_job_id,
                    "queue_name": analysis_enqueue_result.get("queue_name"),
                    "job_type": analysis_enqueue_result.get("job_type"),
                }
                await self._job_broker.mark_done(job_id=job_id, result=scrape_result)
                job_outcome = "done"
                LOGGER.info(
                    "Scrape job done and handed off to analysis: scrape_job=%s analysis_job=%s business_id=%s",
                    job_id,
                    analysis_job_id,
                    business_id,
                )
            else:
                scrape_result = dict(scrape_result)
                scrape_result["pipeline"] = {
                    "worker": "scraper",
                    "source": self._scrape_source,
                    "queue_name": self.queue_name,
                }
                await self._job_broker.mark_done(job_id=job_id, result=scrape_result)
                job_outcome = "done"
                LOGGER.info(
                    "Scrape source job done without analysis handoff job=%s business_id=%s source=%s",
                    job_id,
                    business_id,
                    self._scrape_source,
                )
        except ScrapeBotDetectedError as exc:
            bot_context = exc.context if isinstance(getattr(exc, "context", None), dict) else {}
            await self._job_broker.mark_needs_human(
                job_id=job_id,
                reason=str(exc),
                data=self._with_worker_source(
                    {
                        "reason_code": "tripadvisor_antibot_detected",
                        **bot_context,
                    }
                ),
            )
            job_outcome = "needs_human"
            elapsed_s = round(time.monotonic() - started_at, 2)
            LOGGER.warning(
                "Scrape job moved to needs_human due to anti-bot detection id=%s elapsed=%ss name=%r strategy=%s stage_counts=%s error=%s",
                job_id,
                elapsed_s,
                job_name,
                strategy,
                dict(stage_counts),
                exc,
            )
        except ScrapeNeedsHumanInterventionError as exc:
            await self._job_broker.mark_needs_human(
                job_id=job_id,
                reason=str(exc),
                data=self._with_worker_source(
                    {
                        "reason_code": "tripadvisor_session_unavailable",
                        **(exc.context if isinstance(exc.context, dict) else {}),
                    }
                ),
            )
            job_outcome = "needs_human"
            elapsed_s = round(time.monotonic() - started_at, 2)
            LOGGER.warning(
                "Scrape job moved to needs_human id=%s elapsed=%ss name=%r strategy=%s stage_counts=%s error=%s",
                job_id,
                elapsed_s,
                job_name,
                strategy,
                dict(stage_counts),
                exc,
            )
        except RuntimeError as exc:
            if str(exc).strip() == _CANCELLED_BY_USER_ERROR:
                await self._job_broker.mark_failed(job_id=job_id, error=_CANCELLED_BY_USER_ERROR)
                job_outcome = "cancelled"
                elapsed_s = round(time.monotonic() - started_at, 2)
                LOGGER.warning(
                    "Scrape job cancelled id=%s elapsed=%ss name=%r strategy=%s stage_counts=%s",
                    job_id,
                    elapsed_s,
                    job_name,
                    strategy,
                    dict(stage_counts),
                )
            else:
                await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
                job_outcome = "failed"
                elapsed_s = round(time.monotonic() - started_at, 2)
                LOGGER.exception(
                    "Scrape job failed id=%s elapsed=%ss name=%r force=%s force_mode=%s strategy=%s stage_counts=%s error=%s",
                    job_id,
                    elapsed_s,
                    job_name,
                    force,
                    force_mode,
                    strategy,
                    dict(stage_counts),
                    exc,
                )
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            job_outcome = "failed"
            elapsed_s = round(time.monotonic() - started_at, 2)
            LOGGER.exception(
                "Scrape job failed id=%s elapsed=%ss name=%r force=%s force_mode=%s strategy=%s stage_counts=%s error=%s",
                job_id,
                elapsed_s,
                job_name,
                force,
                force_mode,
                strategy,
                dict(stage_counts),
                exc,
            )
        finally:
            cancellation_watch_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancellation_watch_task
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            if singleton_job_tracking_enabled:
                try:
                    await self._tripadvisor_session_service.clear_worker_singleton_active_job(
                        token=str(self._singleton_token),
                        job_id=str(job_id),
                        job_status=job_outcome,
                    )
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "Failed clearing active TripAdvisor singleton job. token=%s job_id=%s outcome=%s",
                        self._singleton_token,
                        job_id,
                        job_outcome,
                    )


async def _main() -> None:
    worker = ScraperWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
