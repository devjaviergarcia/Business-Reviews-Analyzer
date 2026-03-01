from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter
from typing import Any

from src.config import settings
from src.dependencies import create_business_service, create_worker_job_broker
from src.services.business_service import BusinessService
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisGenerateTaskPayload, AnalysisJobStatus, parse_analyze_business_payload

LOGGER = logging.getLogger("scraper_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class ScraperWorker(QueuedJobWorkerBase):
    queue_name = "scrape"
    logger_name = "scraper_worker"

    def __init__(
        self,
        service: BusinessService | None = None,
        job_broker: WorkerJobBroker | None = None,
    ) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())
        self._service = service or create_business_service()

    def _summarize_progress_data(self, data: Any) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}

        prioritized_keys = [
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
        interactive_max_rounds = task_payload.interactive_max_rounds
        html_scroll_max_rounds = task_payload.html_scroll_max_rounds
        html_stable_rounds = task_payload.html_stable_rounds
        LOGGER.info(
            "Processing scrape job id=%s name=%r force=%s force_mode=%s strategy=%s interactive_max_rounds=%s html_scroll_max_rounds=%s html_stable_rounds=%s queue=%s job_type=%s",
            job_id,
            job_name,
            force,
            force_mode,
            strategy,
            interactive_max_rounds,
            html_scroll_max_rounds,
            html_stable_rounds,
            job.get("queue_name"),
            job.get("job_type"),
        )

        async def on_progress(event: dict[str, Any]) -> None:
            stage = str(event.get("stage", "") or "running")
            message = str(event.get("message", "") or "In progress.")
            data = event.get("data", {})
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

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        try:
            scrape_result = await self._service.scrape_business_for_analysis_pipeline(
                name=job_name,
                force=force,
                strategy=strategy,
                force_mode=force_mode,
                interactive_max_rounds=interactive_max_rounds,
                html_scroll_max_rounds=html_scroll_max_rounds,
                html_stable_rounds=html_stable_rounds,
                progress_callback=on_progress,
            )
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
            await self._job_broker.handoff_job(
                job_id=job_id,
                queue_name="analysis",
                job_type="analysis_generate",
                task_payload=next_payload,
                stage="handoff_analysis_queued",
                message="Scrape stage completed. Job handed off to analysis worker.",
                data={
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
                    }
                },
            )
            LOGGER.info("Job handed off to analysis queue: job=%s business_id=%s", job_id, business_id)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
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
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task


async def _main() -> None:
    worker = ScraperWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
