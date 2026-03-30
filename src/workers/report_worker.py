from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId

from src.config import settings
from src.database import get_database
from src.dependencies import create_worker_job_broker
from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.pipeline.report_renderer import StructuredReportRenderer
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import AnalysisJobStatus, parse_report_generate_payload

LOGGER = logging.getLogger("report_worker")
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class ReportWorker(QueuedJobWorkerBase):
    queue_name = "report"
    logger_name = "report_worker"

    _BUSINESSES_COLLECTION = "businesses"
    _REVIEWS_COLLECTION = "reviews"
    _ANALYSES_COLLECTION = "analyses"

    def __init__(
        self,
        *,
        job_broker: WorkerJobBroker | None = None,
        report_builder: AdvancedBusinessReportBuilder | None = None,
        report_renderer: StructuredReportRenderer | None = None,
    ) -> None:
        super().__init__(job_broker=job_broker or create_worker_job_broker())
        self._report_builder = report_builder or AdvancedBusinessReportBuilder()
        self._report_renderer = report_renderer or StructuredReportRenderer()

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip() or "unknown"
        try:
            task_payload = parse_report_generate_payload(job)
            await self._job_broker.append_event(
                job_id=job_id,
                stage="report_worker_started",
                message="Report worker started.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                    "payload": task_payload.model_dump(mode="python"),
                },
            )
            LOGGER.info(
                "Report job id=%s business_id=%s analysis_id=%s format=%s template_id=%s",
                job_id,
                task_payload.business_id,
                task_payload.analysis_id,
                task_payload.output_format,
                task_payload.template_id,
            )

            database = get_database()
            analyses = database[self._ANALYSES_COLLECTION]
            businesses = database[self._BUSINESSES_COLLECTION]
            reviews = database[self._REVIEWS_COLLECTION]

            analysis_id = self._parse_object_id(task_payload.analysis_id, field_name="analysis_id")
            analysis_doc = await analyses.find_one({"_id": analysis_id})
            if analysis_doc is None:
                raise LookupError(f"Analysis '{task_payload.analysis_id}' not found.")

            business_id = str(task_payload.business_id or "").strip()
            business_doc = await businesses.find_one({"_id": self._parse_object_id(business_id, field_name="business_id")})
            if business_doc is None:
                raise LookupError(f"Business '{business_id}' not found.")

            dataset_id = str(analysis_doc.get("dataset_id") or "").strip() or None
            reviews_query: dict[str, Any] = {"business_id": business_id}
            if dataset_id:
                reviews_query["dataset_id"] = dataset_id

            review_docs = (
                await reviews.find(reviews_query)
                .sort([("scraped_at", -1), ("_id", -1)])
                .limit(800)
                .to_list(length=800)
            )
            normalized_reviews = [self._normalize_review_doc(doc) for doc in review_docs]

            analysis_payload = dict(analysis_doc)
            analysis_payload.pop("_id", None)
            advanced_report = analysis_doc.get("advanced_report")
            if not isinstance(advanced_report, dict):
                advanced_report = await self._report_builder.build(
                    business_id=business_id,
                    business_name=str(business_doc.get("name", "") or "").strip(),
                    listing=business_doc.get("listing") if isinstance(business_doc.get("listing"), dict) else {},
                    stats=business_doc.get("stats") if isinstance(business_doc.get("stats"), dict) else {},
                    reviews=normalized_reviews,
                    analysis_payload=analysis_payload,
                    businesses_collection=businesses,
                    analyses_collection=analyses,
                )

            intro_context = self._build_intro_context_text(
                business_name=str(business_doc.get("name", "") or "").strip(),
                analysis_doc=analysis_doc,
                review_docs=review_docs,
            )
            artifacts = await self._report_renderer.render(
                report_payload=advanced_report,
                intro_context_text=intro_context,
                business_id=business_id,
                analysis_id=str(task_payload.analysis_id),
                output_format=str(task_payload.output_format or "pdf"),
            )
            preview_report = self._report_builder.build_preview_report(
                advanced_report=advanced_report,
                business_name=str(business_doc.get("name", "") or "").strip(),
                max_comments=3,
            )
            preview_artifacts = await self._report_renderer.render_preview(
                preview_payload=preview_report,
                business_id=business_id,
                analysis_id=str(task_payload.analysis_id),
                output_format=str(task_payload.output_format or "pdf"),
            )

            now = datetime.now(timezone.utc)
            await analyses.update_one(
                {"_id": analysis_id},
                {
                    "$set": {
                        "advanced_report": advanced_report,
                        "preview_report": preview_report,
                        "report_intro_context": intro_context,
                        "report_artifacts": artifacts,
                        "preview_report_artifacts": preview_artifacts,
                        "report_generated_at": now,
                        "preview_report_generated_at": now,
                        "updated_at": now,
                    }
                },
            )

            await self._job_broker.append_event(
                job_id=job_id,
                stage="report_worker_completed",
                message="Structured report generated and attached to analysis.",
                status=AnalysisJobStatus.RUNNING,
                data={
                    "queue_name": self.queue_name,
                    "job_type": job_type,
                    "payload": task_payload.model_dump(mode="python"),
                    "analysis_id": task_payload.analysis_id,
                    "business_id": business_id,
                    "dataset_id": dataset_id,
                    "report_sections": list((advanced_report.get("sections") or {}).keys()),
                    "report_artifacts": artifacts,
                    "preview_report_sections": list((preview_report.get("sections") or {}).keys()),
                    "preview_report_artifacts": preview_artifacts,
                },
            )
            await self._job_broker.mark_done(
                job_id=job_id,
                result={
                    "analysis_id": task_payload.analysis_id,
                    "business_id": business_id,
                    "dataset_id": dataset_id,
                    "output_format": task_payload.output_format,
                    "report_version": advanced_report.get("report_version"),
                    "section_count": len((advanced_report.get("sections") or {})),
                    "stored_in_analysis": True,
                    "artifacts": artifacts,
                    "preview_report_version": preview_report.get("preview_version"),
                    "preview_section_count": len((preview_report.get("sections") or {})),
                    "preview_artifacts": preview_artifacts,
                },
            )
            LOGGER.info("Report job done id=%s analysis_id=%s", job_id, task_payload.analysis_id)
        except Exception as exc:  # noqa: BLE001
            await self._job_broker.mark_failed(job_id=job_id, error=str(exc))
            LOGGER.exception(
                "Report job failed id=%s job_type=%s error=%s",
                job_id,
                job_type,
                exc,
            )

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected Mongo ObjectId string.") from exc

    def _normalize_review_doc(self, review_doc: dict[str, Any]) -> dict[str, Any]:
        owner_reply_value = review_doc.get("owner_reply")
        owner_reply_text = ""
        if isinstance(owner_reply_value, dict):
            owner_reply_text = str(owner_reply_value.get("text", "") or "").strip()
        elif isinstance(owner_reply_value, str):
            owner_reply_text = owner_reply_value.strip()

        return {
            "review_id": str(review_doc.get("review_id") or review_doc.get("_id") or "").strip() or None,
            "source": str(review_doc.get("source", "") or "").strip() or "unknown",
            "author_name": str(review_doc.get("author_name", "") or "").strip(),
            "rating": review_doc.get("rating"),
            "relative_time": str(review_doc.get("relative_time", "") or "").strip(),
            "relative_time_bucket": str(review_doc.get("relative_time_bucket", "unknown") or "unknown"),
            "text": str(review_doc.get("text", "") or "").strip(),
            "owner_reply": owner_reply_text,
            "has_owner_reply": bool(review_doc.get("has_owner_reply") or owner_reply_text),
        }

    def _build_intro_context_text(
        self,
        *,
        business_name: str,
        analysis_doc: dict[str, Any],
        review_docs: list[dict[str, Any]],
    ) -> str:
        source_counter: dict[str, int] = {}
        for review in review_docs:
            source = str(review.get("source", "unknown") or "unknown").strip().lower() or "unknown"
            source_counter[source] = int(source_counter.get(source, 0)) + 1
        source_label_map = {
            "google_maps": "Google Maps",
            "tripadvisor": "Tripadvisor",
            "trustpilot": "Trustpilot",
            "booking": "Booking",
            "reddit": "Reddit",
            "unknown": "fuente no identificada",
        }
        sources_summary = ", ".join(
            f"{source_label_map.get(source, source.replace('_', ' '))}: {count}"
            for source, count in sorted(source_counter.items(), key=lambda item: item[0])
        )
        if not sources_summary:
            sources_summary = "sin fuente identificada"

        analysis_created_at = analysis_doc.get("created_at")
        analysis_created_at_text = self._format_date_human(analysis_created_at)
        return (
            f"Este reporte de '{business_name or 'negocio'}' resume {len(review_docs)} opiniones reales "
            f"recogidas en {sources_summary}. "
            f"Última actualización del análisis: {analysis_created_at_text}."
        )

    def _format_date_human(self, value: Any) -> str:
        months = [
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        ]
        dt: datetime | None = None
        if isinstance(value, datetime):
            dt = value
        else:
            raw = str(value or "").strip()
            if raw:
                try:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except Exception:
                    dt = None
        if dt is None:
            return "fecha no disponible"
        return f"{dt.day} de {months[dt.month - 1]} de {dt.year}"


async def _main() -> None:
    worker = ReportWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
