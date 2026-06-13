from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId

from src.config import settings
from src.database import get_database
from src.dependencies import create_crm_service, create_worker_job_broker
from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.pipeline.client_audit import ClientAuditPreparationRuntime
from src.pipeline.client_audit.report_payload_builder import build_client_audit_report_payload
from src.pipeline.report_renderer import StructuredReportRenderer
from src.services.analysis_job_service import AnalysisJobService
from src.workers.base_queue_worker import QueuedJobWorkerBase
from src.workers.broker import WorkerJobBroker
from src.workers.contracts import (
    AnalysisJobStatus,
    parse_report_generate_payload,
    parse_report_prepare_payload,
)

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
    _JOBS_COLLECTION = "analysis_jobs"
    _SCRAPE_ROUNDS_COLLECTION = "browser_scrape_rounds"
    _REPORT_REVIEWS_LIMIT = 800
    _REPORT_BALANCED_SOURCES = ("google_maps", "tripadvisor")

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
        self._client_audit_preparation_runtime = ClientAuditPreparationRuntime(
            job_service=AnalysisJobService(),
            crm_service=create_crm_service(),
        )

    async def _process_job(self, job: dict) -> None:
        job_id = job.get("_id")
        job_type = str(job.get("job_type") or "").strip() or "unknown"
        try:
            if job_type == "report_prepare":
                preparation_payload = parse_report_prepare_payload(job)
                await self._job_broker.append_event(
                    job_id=job_id,
                    stage="study_hydration_started",
                    message="Client audit hydration preparation started.",
                    status=AnalysisJobStatus.RUNNING,
                    data={
                        "queue_name": self.queue_name,
                        "job_type": job_type,
                        "payload": preparation_payload.model_dump(mode="python"),
                    },
                )
                hydration_result = await self._client_audit_preparation_runtime.process_preparation_task(
                    task_payload=preparation_payload,
                    job_id=job_id,
                )
                await self._job_broker.append_event(
                    job_id=job_id,
                    stage="study_hydration_completed",
                    message="Client audit hydration preparation completed.",
                    status=AnalysisJobStatus.RUNNING,
                    data=hydration_result,
                )
                await self._job_broker.mark_done(job_id=job_id, result=hydration_result)
                LOGGER.info(
                    "Report preparation job done id=%s preparation_id=%s hydration_status=%s",
                    job_id,
                    preparation_payload.preparation_id,
                    hydration_result.get("hydration_status"),
                )
                return

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
            jobs = database[self._JOBS_COLLECTION]
            scrape_rounds = database[self._SCRAPE_ROUNDS_COLLECTION]

            analysis_id = self._parse_object_id(task_payload.analysis_id, field_name="analysis_id")
            analysis_doc = await analyses.find_one({"_id": analysis_id})
            if analysis_doc is None:
                raise LookupError(f"Analysis '{task_payload.analysis_id}' not found.")

            business_id = str(task_payload.business_id or "").strip()
            business_doc = await businesses.find_one({"_id": self._parse_object_id(business_id, field_name="business_id")})
            if business_doc is None:
                raise LookupError(f"Business '{business_id}' not found.")

            dataset_id = str(analysis_doc.get("dataset_id") or "").strip() or None
            source_availability = await self._load_source_availability_metadata(
                analysis_doc=analysis_doc,
                report_source_job_id=str(task_payload.source_job_id or "").strip() or None,
                jobs_collection=jobs,
                scrape_rounds_collection=scrape_rounds,
            )
            (
                review_docs,
                report_source_mode,
                report_sources_included,
                report_source_counts,
            ) = await self._load_report_review_docs(
                reviews_collection=reviews,
                business_id=business_id,
                source_mode=task_payload.source_mode,
                selected_source=task_payload.selected_source,
                limit=self._REPORT_REVIEWS_LIMIT,
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
            report_metadata = (
                advanced_report.get("report_metadata")
                if isinstance(advanced_report.get("report_metadata"), dict)
                else {}
            )
            report_metadata = {
                **report_metadata,
                "report_source_mode": report_source_mode,
                "report_sources_included": list(report_sources_included),
                "source_counts": dict(report_source_counts),
                "dataset_id": dataset_id,
                "source_availability": source_availability,
            }
            advanced_report = {
                **advanced_report,
                "report_metadata": report_metadata,
            }
            final_report_payload = await self._build_render_report_payload(
                task_payload=task_payload,
                business_doc=business_doc,
                analysis_doc=analysis_doc,
                advanced_report=advanced_report,
                source_availability=source_availability,
                report_source_mode=report_source_mode,
                report_sources_included=report_sources_included,
                report_source_counts=report_source_counts,
            )

            intro_context = self._build_intro_context_text(
                business_name=str(business_doc.get("name", "") or "").strip(),
                analysis_doc=analysis_doc,
                review_docs=review_docs,
            )
            artifacts = await self._report_renderer.render(
                report_payload=final_report_payload,
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
                        "final_report_payload": final_report_payload,
                        "preview_report": preview_report,
                        "report_intro_context": intro_context,
                        "report_artifacts": artifacts,
                        "preview_report_artifacts": preview_artifacts,
                        "report_generated_at": now,
                        "preview_report_generated_at": now,
                        "report_profile": final_report_payload.get("report_profile"),
                        "report_complexity": final_report_payload.get("report_complexity"),
                        "report_cadence": final_report_payload.get("report_cadence"),
                        "report_preparation_id": task_payload.preparation_id,
                        "report_source_mode": report_source_mode,
                        "report_sources_included": report_sources_included,
                        "report_source_counts": report_source_counts,
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
                    "report_source_mode": report_source_mode,
                    "report_sources_included": report_sources_included,
                    "source_counts": report_source_counts,
                    "report_sections": list((advanced_report.get("sections") or {}).keys()),
                    "report_profile": final_report_payload.get("report_profile"),
                    "report_complexity": final_report_payload.get("report_complexity"),
                    "report_cadence": final_report_payload.get("report_cadence"),
                    "report_preparation_id": task_payload.preparation_id,
                    "study_hydration": final_report_payload.get("study_hydration"),
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
                    "report_source_mode": report_source_mode,
                    "report_sources_included": report_sources_included,
                    "source_counts": report_source_counts,
                    "report_profile": final_report_payload.get("report_profile"),
                    "report_complexity": final_report_payload.get("report_complexity"),
                    "report_cadence": final_report_payload.get("report_cadence"),
                    "report_preparation_id": task_payload.preparation_id,
                    "study_hydration": final_report_payload.get("study_hydration"),
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

    async def _build_render_report_payload(
        self,
        *,
        task_payload: Any,
        business_doc: dict[str, Any],
        analysis_doc: dict[str, Any],
        advanced_report: dict[str, Any],
        source_availability: dict[str, Any],
        report_source_mode: str,
        report_sources_included: list[str],
        report_source_counts: dict[str, int],
    ) -> dict[str, Any]:
        generated_at = datetime.now(timezone.utc).isoformat()
        report_profile = str(task_payload.report_profile or "classic").strip().lower() or "classic"
        if report_profile == "client_audit":
            return await self._build_client_audit_render_payload(
                task_payload=task_payload,
                business_doc=business_doc,
                analysis_doc=analysis_doc,
                advanced_report=advanced_report,
                source_availability=source_availability,
                report_source_mode=report_source_mode,
                report_sources_included=report_sources_included,
                report_source_counts=report_source_counts,
                generated_at=generated_at,
            )
        return {
            **advanced_report,
            "generated_at": generated_at,
            "report_profile": "classic",
            "report_complexity": "basic",
            "report_cadence": str(task_payload.report_cadence or "one_off").strip().lower() or "one_off",
        }

    async def _build_client_audit_render_payload(
        self,
        *,
        task_payload: Any,
        business_doc: dict[str, Any],
        analysis_doc: dict[str, Any],
        advanced_report: dict[str, Any],
        source_availability: dict[str, Any],
        report_source_mode: str,
        report_sources_included: list[str],
        report_source_counts: dict[str, int],
        generated_at: str,
    ) -> dict[str, Any]:
        preparation: dict[str, Any] | None = None
        preparation_id = str(task_payload.preparation_id or "").strip()
        if preparation_id:
            try:
                preparation = await self._client_audit_preparation_runtime.get_preparation(
                    preparation_id=preparation_id
                )
            except Exception:
                preparation = None
        render_payload = build_client_audit_report_payload(
            business_doc=business_doc,
            analysis_doc=analysis_doc,
            advanced_report=advanced_report,
            source_availability=source_availability,
            source_mode=report_source_mode,
            sources_included=report_sources_included,
            source_counts=report_source_counts,
            report_profile="client_audit",
            report_complexity=str(task_payload.report_complexity or "basic").strip().lower() or "basic",
            report_cadence=str(task_payload.report_cadence or "one_off").strip().lower() or "one_off",
            include_competitors=bool(task_payload.include_competitors),
            include_geogrid=bool(task_payload.include_geogrid),
            preparation=preparation,
        )
        render_payload["generated_at"] = generated_at
        return render_payload

    async def _load_report_review_docs(
        self,
        *,
        reviews_collection: Any,
        business_id: str,
        source_mode: str,
        selected_source: str | None,
        limit: int,
    ) -> tuple[list[dict[str, Any]], str, list[str], dict[str, int]]:
        safe_limit = max(1, int(limit))
        normalized_mode = str(source_mode or "").strip().lower()
        if normalized_mode not in {"auto", "combined", "single"}:
            normalized_mode = "auto"
        normalized_selected_source = str(selected_source or "").strip().lower() or None

        # Single-source mode (explicit source) falls back to auto if source is missing/invalid.
        if normalized_mode == "single" and normalized_selected_source in self._REPORT_BALANCED_SOURCES:
            docs = await (
                reviews_collection.find(
                    {
                        "business_id": business_id,
                        "source": normalized_selected_source,
                    }
                )
                .sort([("scraped_at", -1), ("_id", -1)])
                .limit(safe_limit)
                .to_list(length=safe_limit)
            )
            source_counts = self._count_sources(docs)
            included_sources = [source for source in self._REPORT_BALANCED_SOURCES if source_counts.get(source, 0) > 0]
            return docs, "single", included_sources, source_counts

        normalized_mode = "auto" if normalized_mode == "single" else normalized_mode
        source_doc_counts = {
            source: int(
                await reviews_collection.count_documents(
                    {"business_id": business_id, "source": source}
                )
            )
            for source in self._REPORT_BALANCED_SOURCES
        }
        has_balanced_sources = all(source_doc_counts.get(source, 0) > 0 for source in self._REPORT_BALANCED_SOURCES)
        if not has_balanced_sources:
            docs = await (
                reviews_collection.find({"business_id": business_id})
                .sort([("scraped_at", -1), ("_id", -1)])
                .limit(safe_limit)
                .to_list(length=safe_limit)
            )
            source_counts = self._count_sources(docs)
            included_sources = [source for source, count in source_counts.items() if count > 0]
            return docs, normalized_mode, included_sources, source_counts

        half_limit = safe_limit // 2
        limits = {source: min(source_doc_counts[source], half_limit) for source in self._REPORT_BALANCED_SOURCES}
        remaining = safe_limit - sum(limits.values())
        if remaining > 0:
            expandable_sources = sorted(
                self._REPORT_BALANCED_SOURCES,
                key=lambda source: source_doc_counts[source] - limits[source],
                reverse=True,
            )
            for source in expandable_sources:
                if remaining <= 0:
                    break
                available_extra = max(0, source_doc_counts[source] - limits[source])
                take_extra = min(remaining, available_extra)
                limits[source] += take_extra
                remaining -= take_extra

        balanced_docs: list[dict[str, Any]] = []
        for source in self._REPORT_BALANCED_SOURCES:
            source_limit = max(0, int(limits.get(source, 0)))
            if source_limit <= 0:
                continue
            source_docs = await (
                reviews_collection.find({"business_id": business_id, "source": source})
                .sort([("scraped_at", -1), ("_id", -1)])
                .limit(source_limit)
                .to_list(length=source_limit)
            )
            balanced_docs.extend(source_docs)

        balanced_docs.sort(key=self._review_sort_key, reverse=True)
        docs = balanced_docs[:safe_limit]
        source_counts = self._count_sources(docs)
        included_sources = [source for source in self._REPORT_BALANCED_SOURCES if source_counts.get(source, 0) > 0]
        return docs, normalized_mode, included_sources, source_counts

    def _count_sources(self, review_docs: list[dict[str, Any]]) -> dict[str, int]:
        source_counter = Counter(
            str((doc or {}).get("source") or "unknown").strip().lower() or "unknown"
            for doc in review_docs
            if isinstance(doc, dict)
        )
        return {source: int(count) for source, count in sorted(source_counter.items(), key=lambda item: item[0])}

    async def _load_source_availability_metadata(
        self,
        *,
        analysis_doc: dict[str, Any],
        report_source_job_id: str | None,
        jobs_collection: Any,
        scrape_rounds_collection: Any,
    ) -> dict[str, Any]:
        scrape_round_id = str(analysis_doc.get("scrape_round_id") or "").strip() or None
        if not scrape_round_id and report_source_job_id:
            scrape_round_id = await self._load_scrape_round_id_from_analysis_job(
                analysis_job_id=report_source_job_id,
                jobs_collection=jobs_collection,
            )
        if not scrape_round_id:
            return {}

        round_doc = await scrape_rounds_collection.find_one({"_id": scrape_round_id})
        if not isinstance(round_doc, dict):
            return {}

        source_jobs = round_doc.get("source_jobs") if isinstance(round_doc.get("source_jobs"), dict) else {}
        tripadvisor_state = (
            source_jobs.get("tripadvisor")
            if isinstance(source_jobs.get("tripadvisor"), dict)
            else {}
        )
        if not tripadvisor_state:
            return {}

        tripadvisor_status = str(tripadvisor_state.get("status") or "").strip().lower()
        tripadvisor_resolution = str(tripadvisor_state.get("resolution") or "").strip().lower()
        if tripadvisor_status not in {"omitted", "not_found"} and tripadvisor_resolution not in {
            "business_not_found",
            "manual_skip",
            "manual_close",
        }:
            return {}

        normalized_status = tripadvisor_status or (
            "not_found" if tripadvisor_resolution == "business_not_found" else "omitted"
        )
        detail = self._tripadvisor_unavailable_detail(
            status=normalized_status,
            resolution=tripadvisor_resolution,
        )
        return {
            "tripadvisor": {
                "source": "tripadvisor",
                "status": normalized_status,
                "flag": "NO TIENE TRIPADVISOR",
                "label": "Tripadvisor no disponible para este informe",
                "detail": detail,
                "resolution": tripadvisor_resolution or None,
                "completion_mode": str(tripadvisor_state.get("completion_mode") or "").strip().lower() or None,
            }
        }

    async def _load_scrape_round_id_from_analysis_job(
        self,
        *,
        analysis_job_id: str,
        jobs_collection: Any,
    ) -> str | None:
        try:
            job_object_id = self._parse_object_id(analysis_job_id, field_name="analysis_job_id")
        except ValueError:
            return None
        job_doc = await jobs_collection.find_one({"_id": job_object_id}, projection={"scrape_round_id": 1})
        if not isinstance(job_doc, dict):
            return None
        return str(job_doc.get("scrape_round_id") or "").strip() or None

    def _tripadvisor_unavailable_detail(self, *, status: str, resolution: str) -> str:
        if status == "not_found" or resolution == "business_not_found":
            return (
                "Durante la captura no se encontró una ficha válida del negocio en Tripadvisor, "
                "así que el análisis se ha generado sin esa fuente."
            )
        if resolution == "manual_close":
            return (
                "La sesión de Tripadvisor se cerró sin captura final y la pipeline continuó sin esa fuente."
            )
        return (
            "La fuente de Tripadvisor se omitió durante la captura y el informe se ha generado solo con las fuentes disponibles."
        )

    def _review_sort_key(self, review_doc: dict[str, Any]) -> tuple[float, str]:
        raw_scraped_at = review_doc.get("scraped_at") if isinstance(review_doc, dict) else None
        if isinstance(raw_scraped_at, datetime):
            try:
                scraped_ts = float(raw_scraped_at.timestamp())
            except (OverflowError, OSError, ValueError):
                scraped_ts = 0.0
        else:
            scraped_ts = 0.0
        review_identifier = str(review_doc.get("_id") or review_doc.get("review_id") or "")
        return (scraped_ts, review_identifier)

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
