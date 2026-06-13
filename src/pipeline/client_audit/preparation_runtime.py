from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument

from src.crm.benchmark.deep_study import build_deep_study_snapshot
from src.database import get_database
from src.workers.contracts import ReportGenerateTaskPayload, ReportPreparationTaskPayload


class ClientAuditPreparationRuntime:
    _PREPARATIONS_COLLECTION = "client_audit_preparations"
    _BUSINESSES_COLLECTION = "businesses"
    _ANALYSES_COLLECTION = "analyses"
    _BENCHMARK_RUNS_COLLECTION = "benchmark_runs"
    _BENCHMARK_BUSINESSES_COLLECTION = "benchmark_businesses"
    _COMPETITOR_SETS_COLLECTION = "competitor_sets"
    _GEO_GRID_RUNS_COLLECTION = "geo_grid_runs"

    _BENCHMARK_TTL_DAYS = 30
    _GEO_GRID_TTL_DAYS = 30
    _FINAL_STATUSES = {
        "ready_reused",
        "ready_refreshed",
        "ready_partial",
        "failed_scope_resolution",
        "failed_hydration",
    }

    def __init__(self, *, job_service: Any, crm_service: Any) -> None:
        self._job_service = job_service
        self._crm_service = crm_service

    async def create_preparation(
        self,
        *,
        business_id: str,
        analysis_id: str,
        output_format: str,
        locale: str | None,
        template_id: str | None,
        source_job_id: str | None,
        source_mode: str,
        selected_source: str | None,
        report_profile: str,
        report_complexity: str,
        report_cadence: str,
        study_resolution_mode: str,
        include_competitors: bool,
        include_geogrid: bool,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        doc = {
            "business_id": business_id,
            "analysis_id": analysis_id,
            "output_format": output_format,
            "locale": locale,
            "template_id": template_id,
            "source_job_id": source_job_id,
            "source_mode": source_mode,
            "selected_source": selected_source,
            "report_profile": report_profile,
            "report_complexity": report_complexity,
            "report_cadence": report_cadence,
            "study_resolution_mode": study_resolution_mode,
            "include_competitors": bool(include_competitors),
            "include_geogrid": bool(include_geogrid),
            "hydration_status": "queued",
            "business_presence_state": "study_scope_unresolved",
            "scope": {},
            "hydration_context": {},
            "notes": [],
            "dependencies": {
                "benchmark": {"status": "skipped"},
                "geogrid": {"status": "skipped"},
            },
            "latest_prepare_job_id": None,
            "final_report_job_id": None,
            "created_at": now,
            "updated_at": now,
            "ready_at": None,
        }
        result = await get_database()[self._PREPARATIONS_COLLECTION].insert_one(doc)
        doc["_id"] = result.inserted_id
        doc["report_preparation_id"] = str(result.inserted_id)
        return self._serialize_preparation(doc)

    async def get_preparation(self, *, preparation_id: str) -> dict[str, Any] | None:
        doc = await get_database()[self._PREPARATIONS_COLLECTION].find_one(
            {"_id": self._parse_object_id(preparation_id, field_name="preparation_id")}
        )
        return self._serialize_preparation(doc) if doc else None

    async def relaunch_dependency(
        self,
        *,
        preparation_id: str,
        dependency_name: str,
        execution_mode: str = "automatic",
        live_display_mode: str = "native",
    ) -> dict[str, Any]:
        preparation = await self.get_preparation(preparation_id=preparation_id)
        if preparation is None:
            raise LookupError(f"Report preparation '{preparation_id}' not found.")

        normalized_dependency = str(dependency_name or "").strip().lower()
        if normalized_dependency not in {"benchmark", "geogrid"}:
            raise ValueError("dependency_name must be 'benchmark' or 'geogrid'.")

        scope = preparation.get("scope") if isinstance(preparation.get("scope"), dict) else {}
        if not scope.get("benchmark_query") or not scope.get("city") or not scope.get("category"):
            business_doc = await self._load_business_doc(str(preparation.get("business_id") or ""))
            scope = self._derive_scope_from_business(business_doc)
        if not scope.get("benchmark_query") or not scope.get("city") or not scope.get("category"):
            raise ValueError("Preparation scope is unresolved; cannot relaunch hydration dependency.")

        normalized_execution_mode = str(execution_mode or "automatic").strip().lower() or "automatic"
        normalized_live_display_mode = str(live_display_mode or "native").strip().lower() or "native"
        dependencies = dict(preparation.get("dependencies") or {})

        if normalized_dependency == "benchmark":
            queued = await self._crm_service.enqueue_benchmark_study_job(
                query=str(scope.get("benchmark_query") or ""),
                city=str(scope.get("city") or "") or None,
                category=str(scope.get("category") or "") or None,
                limit=100,
                source="auto_live_google_maps",
                title=f"Client audit · {scope.get('benchmark_query')}",
                execution_mode=normalized_execution_mode,
                live_display_mode=normalized_live_display_mode,
                requested_by="client_audit_dependency_relaunch",
            )
            dependencies["benchmark"] = {
                "status": "queued",
                "job_id": str(queued.get("job_id") or "").strip() or None,
                "benchmark_run_id": str(queued.get("benchmark_run_id") or "").strip() or None,
                "execution_mode": normalized_execution_mode,
                "live_display_mode": normalized_live_display_mode,
                "manual_relaunch": True,
            }
            hydration_status = "waiting_benchmark"
        else:
            if not bool(preparation.get("include_geogrid")):
                raise ValueError("This preparation does not include geogrid hydration.")
            city_slug = str(scope.get("city_slug") or "").strip()
            if not city_slug:
                raise ValueError("Preparation scope does not contain a valid city_slug for geogrid.")
            previous_geogrid = (
                dependencies.get("geogrid") if isinstance(dependencies.get("geogrid"), dict) else {}
            )
            queued = await self._crm_service.enqueue_geo_grid_study_job(
                keyword=str(scope.get("benchmark_query") or ""),
                city_slug=city_slug,
                top_n=10,
                execution_mode=normalized_execution_mode,
                live_display_mode=normalized_live_display_mode,
                requested_by="client_audit_dependency_relaunch",
            )
            dependencies["geogrid"] = {
                "status": "queued",
                "job_id": str(queued.get("job_id") or "").strip() or None,
                "geo_grid_run_id": str(queued.get("geo_grid_run_id") or "").strip() or None,
                "refresh_attempts": self._parse_refresh_attempts(previous_geogrid.get("refresh_attempts")) + 1,
                "execution_mode": normalized_execution_mode,
                "live_display_mode": normalized_live_display_mode,
                "manual_relaunch": True,
            }
            hydration_status = "waiting_geogrid"

        notes = self._merge_notes(
            list(preparation.get("notes") or []),
            [f"{normalized_dependency}_manual_relaunch_queued"],
        )
        updated = await self._update_preparation(
            preparation_id=preparation_id,
            set_fields={
                "scope": scope,
                "dependencies": dependencies,
                "hydration_status": hydration_status,
                "notes": notes,
                "final_report_job_id": None,
                "ready_at": None,
                "updated_at": datetime.now(timezone.utc),
            },
        )
        updated["dependency_relaunch"] = {
            "dependency": normalized_dependency,
            "execution_mode": normalized_execution_mode,
            "live_display_mode": normalized_live_display_mode,
            "queued_job_id": (
                dependencies.get(normalized_dependency, {}).get("job_id")
                if isinstance(dependencies.get(normalized_dependency), dict)
                else None
            ),
        }
        return updated

    async def process_preparation_task(
        self,
        *,
        task_payload: ReportPreparationTaskPayload,
        job_id: Any | None = None,
    ) -> dict[str, Any]:
        preparation = await self.get_preparation(preparation_id=task_payload.preparation_id)
        if preparation is None:
            raise LookupError(f"Report preparation '{task_payload.preparation_id}' not found.")

        if job_id is not None:
            await self._update_preparation(
                preparation_id=task_payload.preparation_id,
                set_fields={
                    "latest_prepare_job_id": str(job_id),
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            preparation = await self.get_preparation(preparation_id=task_payload.preparation_id) or preparation

        if preparation.get("final_report_job_id"):
            return {
                "report_preparation_id": task_payload.preparation_id,
                "hydration_status": preparation.get("hydration_status"),
                "final_report_job_id": preparation.get("final_report_job_id"),
                "already_ready": True,
            }

        business_doc = await self._load_business_doc(task_payload.business_id)
        analysis_doc = await self._load_analysis_doc(task_payload.analysis_id)
        scope = self._derive_scope_from_business(business_doc)
        notes: list[str] = []
        if not scope.get("city") or not scope.get("category") or not scope.get("benchmark_query"):
            notes.append("study_scope_unresolved")
            updated = await self._finalize_preparation(
                preparation_id=task_payload.preparation_id,
                hydration_status="failed_scope_resolution",
                business_presence_state="study_scope_unresolved",
                scope=scope,
                hydration_context={"scope": scope, "notes": notes},
                notes=notes,
                report_payload=task_payload,
            )
            return updated

        benchmark_dependency = dict((preparation.get("dependencies") or {}).get("benchmark") or {})
        geogrid_dependency = dict((preparation.get("dependencies") or {}).get("geogrid") or {})

        benchmark_context = await self._resolve_benchmark_context(
            business_doc=business_doc,
            scope=scope,
            dependency=benchmark_dependency,
            study_resolution_mode=task_payload.study_resolution_mode,
            include_competitors=bool(task_payload.include_competitors),
        )
        geogrid_context = await self._resolve_geogrid_context(
            business_doc=business_doc,
            scope=scope,
            dependency=geogrid_dependency,
            study_resolution_mode=task_payload.study_resolution_mode,
            include_geogrid=bool(task_payload.include_geogrid),
        )

        dependencies = {
            "benchmark": benchmark_context["dependency"],
            "geogrid": geogrid_context["dependency"],
        }

        if benchmark_context["waiting"] or geogrid_context["waiting"]:
            hydration_status = "waiting_geogrid" if geogrid_context["waiting"] else "waiting_benchmark"
            updated = await self._update_preparation(
                preparation_id=task_payload.preparation_id,
                set_fields={
                    "scope": scope,
                    "hydration_status": hydration_status,
                    "dependencies": dependencies,
                    "hydration_context": {
                        "scope": scope,
                        "benchmark": {
                            **benchmark_context["context"],
                            "presence_state": benchmark_context["presence_state"],
                        },
                        "geogrid": {
                            **geogrid_context["context"],
                            "presence_state": geogrid_context["presence_state"],
                        },
                    },
                    "notes": self._merge_notes(notes, benchmark_context["notes"], geogrid_context["notes"]),
                },
            )
            return updated

        notes = self._merge_notes(notes, benchmark_context["notes"], geogrid_context["notes"])
        overall_presence_state = benchmark_context["presence_state"] or geogrid_context["presence_state"]
        hydration_status = self._choose_final_hydration_status(
            study_resolution_mode=task_payload.study_resolution_mode,
            benchmark_context=benchmark_context,
            geogrid_context=geogrid_context,
        )
        if hydration_status == "ready_partial" and overall_presence_state == "present_in_study":
            overall_presence_state = "study_scope_unresolved"

        updated = await self._finalize_preparation(
            preparation_id=task_payload.preparation_id,
            hydration_status=hydration_status,
            business_presence_state=overall_presence_state or "study_scope_unresolved",
            scope=scope,
            hydration_context={
                "scope": scope,
                "benchmark": {
                    **benchmark_context["context"],
                    "presence_state": benchmark_context["presence_state"],
                },
                "geogrid": {
                    **geogrid_context["context"],
                    "presence_state": geogrid_context["presence_state"],
                },
                "tripadvisor_policy": "soft_non_blocking",
                "notes": notes,
            },
            dependencies=dependencies,
            notes=notes,
            report_payload=task_payload,
        )
        return updated

    async def resume_preparations_for_benchmark_run(
        self,
        *,
        benchmark_run_id: str,
        dependency_status: str | None = None,
    ) -> dict[str, Any]:
        return await self._resume_preparations_for_dependency(
            dependency_path="dependencies.benchmark.benchmark_run_id",
            dependency_id=benchmark_run_id,
            dependency_status=dependency_status,
        )

    async def resume_preparations_for_geo_grid_run(
        self,
        *,
        geo_grid_run_id: str,
        dependency_status: str | None = None,
    ) -> dict[str, Any]:
        return await self._resume_preparations_for_dependency(
            dependency_path="dependencies.geogrid.geo_grid_run_id",
            dependency_id=geo_grid_run_id,
            dependency_status=dependency_status,
        )

    async def _resume_preparations_for_dependency(
        self,
        *,
        dependency_path: str,
        dependency_id: str,
        dependency_status: str | None = None,
    ) -> dict[str, Any]:
        collection = get_database()[self._PREPARATIONS_COLLECTION]
        docs = await collection.find(
            {
                dependency_path: str(dependency_id or "").strip(),
                "final_report_job_id": None,
                "hydration_status": {"$in": ["waiting_benchmark", "waiting_geogrid", "queued"]},
            }
        ).to_list(length=None)
        queued_jobs: list[str] = []
        for doc in docs:
            preparation = self._serialize_preparation(doc)
            payload = ReportPreparationTaskPayload(
                preparation_id=str(preparation.get("report_preparation_id") or ""),
                business_id=str(preparation.get("business_id") or ""),
                analysis_id=str(preparation.get("analysis_id") or ""),
                output_format=str(preparation.get("output_format") or "pdf"),
                locale=preparation.get("locale"),
                template_id=preparation.get("template_id"),
                source_job_id=preparation.get("source_job_id"),
                source_mode=str(preparation.get("source_mode") or "auto"),
                selected_source=preparation.get("selected_source"),
                report_profile=str(preparation.get("report_profile") or "client_audit"),
                report_complexity=str(preparation.get("report_complexity") or "hydrated"),
                report_cadence=str(preparation.get("report_cadence") or "one_off"),
                study_resolution_mode=str(preparation.get("study_resolution_mode") or "auto_ttl"),
                include_competitors=bool(preparation.get("include_competitors", True)),
                include_geogrid=bool(preparation.get("include_geogrid", False)),
            )
            queued = await self._job_service.enqueue_report_prepare_job(task_payload=payload)
            queued_jobs.append(str(queued.get("job_id") or "").strip())
            note = (
                f"dependency_resumed:{dependency_status}"
                if str(dependency_status or "").strip()
                else "dependency_resumed"
            )
            await self._update_preparation(
                preparation_id=payload.preparation_id,
                set_fields={
                    "updated_at": datetime.now(timezone.utc),
                    "last_resume_note": note,
                    "latest_prepare_job_id": str(queued.get("job_id") or "").strip() or None,
                },
            )
        return {
            "dependency_id": dependency_id,
            "dependency_status": dependency_status,
            "resumed_preparations": len(queued_jobs),
            "queued_prepare_job_ids": [job_id for job_id in queued_jobs if job_id],
        }

    async def _resolve_benchmark_context(
        self,
        *,
        business_doc: dict[str, Any],
        scope: dict[str, Any],
        dependency: dict[str, Any],
        study_resolution_mode: str,
        include_competitors: bool,
    ) -> dict[str, Any]:
        notes: list[str] = []
        dependency = {"status": "skipped", **dependency}
        latest_run = await self._find_latest_compatible_benchmark(scope=scope)
        business_presence_state = "study_scope_unresolved"
        matched_business = None
        selected_run = None
        should_refresh = False
        waiting = False

        if dependency.get("benchmark_run_id"):
            run = await self._load_benchmark_run(str(dependency.get("benchmark_run_id")))
            if run is None:
                dependency["status"] = "failed"
                notes.append("benchmark_dependency_missing")
            else:
                run_status = str(run.get("status") or "").strip().lower()
                dependency["benchmark_status"] = run_status
                selected_run = run
                if run_status in {"queued", "running"}:
                    dependency["status"] = "queued"
                    waiting = True
                elif run_status in {"completed", "partial"}:
                    matched_business = await self._match_benchmark_business(
                        benchmark_id=str(run.get("benchmark_run_id") or ""),
                        business_doc=business_doc,
                    )
                    if matched_business is None:
                        business_presence_state = "not_in_fresh_study"
                        notes.append("benchmark_business_absent_after_refresh")
                    else:
                        business_presence_state = "present_in_study"
                    dependency["status"] = "ready"
                else:
                    dependency["status"] = "failed"
                    notes.append("benchmark_dependency_failed")
        elif study_resolution_mode == "refresh_now":
            should_refresh = True
        elif study_resolution_mode == "reuse_latest":
            selected_run = latest_run
            if latest_run is None:
                notes.append("benchmark_reuse_latest_missing")
            else:
                matched_business = await self._match_benchmark_business(
                    benchmark_id=str(latest_run.get("benchmark_run_id") or ""),
                    business_doc=business_doc,
                )
                if matched_business is None:
                    business_presence_state = "not_in_latest_study"
                    notes.append("benchmark_business_absent_in_latest")
                else:
                    business_presence_state = "present_in_study"
                dependency["status"] = "reused"
        else:
            if latest_run is not None and self._is_fresh(latest_run, ttl_days=self._BENCHMARK_TTL_DAYS):
                matched_business = await self._match_benchmark_business(
                    benchmark_id=str(latest_run.get("benchmark_run_id") or ""),
                    business_doc=business_doc,
                )
                if matched_business is not None:
                    selected_run = latest_run
                    business_presence_state = "present_in_study"
                    dependency["status"] = "reused"
                else:
                    should_refresh = True
                    notes.append("benchmark_auto_ttl_target_missing")
            else:
                should_refresh = True
                notes.append("benchmark_auto_ttl_refresh")

        if should_refresh:
            queued = await self._crm_service.enqueue_benchmark_study_job(
                query=str(scope.get("benchmark_query") or ""),
                city=str(scope.get("city") or "") or None,
                category=str(scope.get("category") or "") or None,
                limit=100,
                source="auto_live_google_maps",
                title=f"Client audit · {scope.get('benchmark_query')}",
                execution_mode="automatic",
                live_display_mode="native",
                requested_by="client_audit_hydration",
            )
            dependency = {
                "status": "queued",
                "job_id": str(queued.get("job_id") or "").strip() or None,
                "benchmark_run_id": str(queued.get("benchmark_run_id") or "").strip() or None,
                "execution_mode": "automatic",
                "live_display_mode": "native",
            }
            waiting = True
            notes.append("benchmark_refresh_queued")

        competitors: list[dict[str, Any]] = []
        deep_study_snapshot: dict[str, Any] | None = None
        if selected_run is not None and matched_business is not None and include_competitors:
            competitors = await self._resolve_competitors_for_business(
                benchmark_id=str(selected_run.get("benchmark_run_id") or ""),
                benchmark_business=matched_business,
            )
            deep_study_snapshot = build_deep_study_snapshot(
                business=matched_business,
                listing=matched_business,
                reviews=[],
                competitors=competitors,
                benchmark=selected_run,
            )

        context = {
            "benchmark_run": selected_run,
            "benchmark_business": matched_business,
            "competitors": competitors,
            "deep_study_snapshot": deep_study_snapshot,
        }
        return {
            "context": context,
            "dependency": dependency,
            "waiting": waiting,
            "notes": notes,
            "presence_state": business_presence_state,
        }

    async def _resolve_geogrid_context(
        self,
        *,
        business_doc: dict[str, Any],
        scope: dict[str, Any],
        dependency: dict[str, Any],
        study_resolution_mode: str,
        include_geogrid: bool,
    ) -> dict[str, Any]:
        if not include_geogrid:
            return {
                "context": {"geo_grid_run": None, "geo_grid_stats": None, "geo_grid_business": None},
                "dependency": {"status": "skipped"},
                "waiting": False,
                "notes": [],
                "presence_state": "study_scope_unresolved",
            }

        notes: list[str] = []
        dependency = {"status": "skipped", **dependency}
        dependency_refresh_attempts = self._parse_refresh_attempts(
            dependency.get("refresh_attempts")
        )
        latest_run = await self._find_latest_compatible_geo_grid(scope=scope)
        business_presence_state = "study_scope_unresolved"
        selected_run = None
        selected_stats = None
        selected_business = None
        should_refresh = False
        waiting = False

        if dependency.get("geo_grid_run_id"):
            run = await self._load_geo_grid_run(str(dependency.get("geo_grid_run_id")))
            if run is None:
                dependency["status"] = "failed"
                notes.append("geogrid_dependency_missing")
                if self._should_retry_geogrid_dependency(
                    study_resolution_mode=study_resolution_mode,
                    refresh_attempts=dependency_refresh_attempts,
                ):
                    should_refresh = True
                    notes.append("geogrid_dependency_retry_queued")
            else:
                run_status = str(run.get("status") or "").strip().lower()
                dependency["geo_grid_status"] = run_status
                selected_run = run
                if run_status in {"queued", "running"}:
                    dependency["status"] = "queued"
                    waiting = True
                elif run_status in {"completed", "partial"}:
                    selected_stats = await self._safe_geo_grid_stats(str(run.get("geo_grid_run_id") or ""))
                    selected_business = self._match_geo_grid_business(
                        stats=selected_stats,
                        business_doc=business_doc,
                    )
                    if selected_business is None:
                        business_presence_state = "not_in_fresh_study"
                        notes.append("geogrid_business_absent_after_refresh")
                    else:
                        business_presence_state = "present_in_study"
                    dependency["status"] = "ready"
                else:
                    dependency["status"] = "failed"
                    notes.append("geogrid_dependency_failed")
                    if self._should_retry_geogrid_dependency(
                        study_resolution_mode=study_resolution_mode,
                        refresh_attempts=dependency_refresh_attempts,
                    ):
                        should_refresh = True
                        notes.append("geogrid_dependency_retry_queued")
        elif study_resolution_mode == "refresh_now":
            should_refresh = True
        elif study_resolution_mode == "reuse_latest":
            selected_run = latest_run
            if latest_run is None:
                notes.append("geogrid_reuse_latest_missing")
            else:
                selected_stats = await self._safe_geo_grid_stats(
                    str(latest_run.get("geo_grid_run_id") or "")
                )
                selected_business = self._match_geo_grid_business(
                    stats=selected_stats,
                    business_doc=business_doc,
                )
                if selected_business is None:
                    business_presence_state = "not_in_latest_study"
                    notes.append("geogrid_business_absent_in_latest")
                else:
                    business_presence_state = "present_in_study"
                dependency["status"] = "reused"
        else:
            if latest_run is not None and self._is_fresh(latest_run, ttl_days=self._GEO_GRID_TTL_DAYS):
                selected_stats = await self._safe_geo_grid_stats(
                    str(latest_run.get("geo_grid_run_id") or "")
                )
                selected_business = self._match_geo_grid_business(
                    stats=selected_stats,
                    business_doc=business_doc,
                )
                if selected_business is not None:
                    selected_run = latest_run
                    business_presence_state = "present_in_study"
                    dependency["status"] = "reused"
                else:
                    should_refresh = True
                    notes.append("geogrid_auto_ttl_target_missing")
            else:
                should_refresh = True
                notes.append("geogrid_auto_ttl_refresh")

        if should_refresh:
            queued = await self._crm_service.enqueue_geo_grid_study_job(
                keyword=str(scope.get("benchmark_query") or ""),
                city_slug=str(scope.get("city_slug") or ""),
                top_n=10,
                execution_mode="automatic",
                live_display_mode="native",
                requested_by="client_audit_hydration",
            )
            dependency = {
                "status": "queued",
                "job_id": str(queued.get("job_id") or "").strip() or None,
                "geo_grid_run_id": str(queued.get("geo_grid_run_id") or "").strip() or None,
                "refresh_attempts": dependency_refresh_attempts + 1,
                "execution_mode": "automatic",
                "live_display_mode": "native",
            }
            waiting = True
            notes.append("geogrid_refresh_queued")

        context = {
            "geo_grid_run": selected_run,
            "geo_grid_stats": selected_stats,
            "geo_grid_business": selected_business,
        }
        return {
            "context": context,
            "dependency": dependency,
            "waiting": waiting,
            "notes": notes,
            "presence_state": business_presence_state,
        }

    def _parse_refresh_attempts(self, value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    def _should_retry_geogrid_dependency(
        self,
        *,
        study_resolution_mode: str,
        refresh_attempts: int,
    ) -> bool:
        normalized_mode = str(study_resolution_mode or "").strip().lower()
        if normalized_mode not in {"auto_ttl", "refresh_now"}:
            return False
        return refresh_attempts < 1

    async def _finalize_preparation(
        self,
        *,
        preparation_id: str,
        hydration_status: str,
        business_presence_state: str,
        scope: dict[str, Any],
        hydration_context: dict[str, Any],
        notes: list[str],
        report_payload: ReportPreparationTaskPayload,
        dependencies: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        report_job = await self._job_service.enqueue_report_generate_job(
            task_payload=ReportGenerateTaskPayload(
                business_id=report_payload.business_id,
                analysis_id=report_payload.analysis_id,
                output_format=report_payload.output_format,
                locale=report_payload.locale,
                template_id=report_payload.template_id,
                source_job_id=report_payload.source_job_id,
                source_mode=report_payload.source_mode,
                selected_source=report_payload.selected_source,
                report_profile=report_payload.report_profile,
                report_complexity=report_payload.report_complexity,
                report_cadence=report_payload.report_cadence,
                study_resolution_mode=report_payload.study_resolution_mode,
                include_competitors=report_payload.include_competitors,
                include_geogrid=report_payload.include_geogrid,
                preparation_id=preparation_id,
            )
        )
        return await self._update_preparation(
            preparation_id=preparation_id,
            set_fields={
                "scope": scope,
                "hydration_status": hydration_status,
                "business_presence_state": business_presence_state,
                "hydration_context": hydration_context,
                "notes": notes,
                "dependencies": dependencies or {"benchmark": {"status": "skipped"}, "geogrid": {"status": "skipped"}},
                "final_report_job_id": str(report_job.get("job_id") or "").strip() or None,
                "ready_at": datetime.now(timezone.utc),
            },
        )

    async def _update_preparation(
        self,
        *,
        preparation_id: str,
        set_fields: dict[str, Any],
    ) -> dict[str, Any]:
        updated = await get_database()[self._PREPARATIONS_COLLECTION].find_one_and_update(
            {"_id": self._parse_object_id(preparation_id, field_name="preparation_id")},
            {"$set": {**set_fields, "updated_at": datetime.now(timezone.utc)}},
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise LookupError(f"Report preparation '{preparation_id}' not found.")
        return self._serialize_preparation(updated)

    def _choose_final_hydration_status(
        self,
        *,
        study_resolution_mode: str,
        benchmark_context: dict[str, Any],
        geogrid_context: dict[str, Any],
    ) -> str:
        benchmark_ready = benchmark_context["dependency"].get("status") in {"ready", "reused", "skipped"}
        geogrid_ready = geogrid_context["dependency"].get("status") in {"ready", "reused", "skipped"}
        if benchmark_ready and geogrid_ready:
            active_presence_states = []
            if benchmark_context["dependency"].get("status") != "skipped":
                active_presence_states.append(str(benchmark_context.get("presence_state") or "").strip().lower())
            if geogrid_context["dependency"].get("status") != "skipped":
                active_presence_states.append(str(geogrid_context.get("presence_state") or "").strip().lower())
            if any(
                state in {"not_in_latest_study", "not_in_fresh_study", "study_scope_unresolved"}
                for state in active_presence_states
            ):
                return "ready_partial"
            if benchmark_context["dependency"].get("status") == "reused" and geogrid_context["dependency"].get("status") in {"reused", "skipped"}:
                return "ready_reused"
            if study_resolution_mode == "refresh_now":
                return "ready_refreshed"
            if any(
                marker in set(benchmark_context["notes"] + geogrid_context["notes"])
                for marker in {
                    "benchmark_refresh_queued",
                    "geogrid_refresh_queued",
                    "benchmark_business_absent_after_refresh",
                    "geogrid_business_absent_after_refresh",
                }
            ):
                if benchmark_context["context"].get("benchmark_run") or geogrid_context["context"].get("geo_grid_run"):
                    return "ready_refreshed"
            return "ready_partial" if benchmark_context["notes"] or geogrid_context["notes"] else "ready_reused"
        return "failed_hydration"

    async def _load_business_doc(self, business_id: str) -> dict[str, Any]:
        doc = await get_database()[self._BUSINESSES_COLLECTION].find_one(
            {"_id": self._parse_object_id(business_id, field_name="business_id")}
        )
        if doc is None:
            raise LookupError(f"Business '{business_id}' not found.")
        return doc

    async def _load_analysis_doc(self, analysis_id: str) -> dict[str, Any]:
        doc = await get_database()[self._ANALYSES_COLLECTION].find_one(
            {"_id": self._parse_object_id(analysis_id, field_name="analysis_id")}
        )
        if doc is None:
            raise LookupError(f"Analysis '{analysis_id}' not found.")
        return doc

    async def _find_latest_compatible_benchmark(self, *, scope: dict[str, Any]) -> dict[str, Any] | None:
        cursor = get_database()[self._BENCHMARK_RUNS_COLLECTION].find(
            {"status": {"$in": ["completed", "partial"]}}
        ).sort([("created_at", -1), ("_id", -1)])
        docs = await cursor.to_list(length=80)
        for doc in docs:
            city = self._normalize_text(str(doc.get("city") or ""))
            category = self._normalize_text(str(doc.get("category") or ""))
            query = self._normalize_text(str(doc.get("query") or ""))
            if (
                city == str(scope.get("city_normalized") or "")
                and category == str(scope.get("category_normalized") or "")
                and query == str(scope.get("query_normalized") or "")
            ):
                return self._serialize_id_doc(doc, "benchmark_run_id")
        return None

    async def _find_latest_compatible_geo_grid(self, *, scope: dict[str, Any]) -> dict[str, Any] | None:
        cursor = get_database()[self._GEO_GRID_RUNS_COLLECTION].find(
            {"status": {"$in": ["completed", "partial"]}, "city_slug": str(scope.get("city_slug") or "")}
        ).sort([("created_at", -1), ("_id", -1)])
        docs = await cursor.to_list(length=80)
        for doc in docs:
            keyword = self._normalize_text(str(doc.get("keyword") or ""))
            if keyword == str(scope.get("query_normalized") or ""):
                return self._serialize_id_doc(doc, "geo_grid_run_id")
        return None

    async def _load_benchmark_run(self, benchmark_run_id: str) -> dict[str, Any] | None:
        try:
            doc = await get_database()[self._BENCHMARK_RUNS_COLLECTION].find_one(
                {"_id": self._parse_object_id(benchmark_run_id, field_name="benchmark_run_id")}
            )
        except LookupError:
            return None
        return self._serialize_id_doc(doc, "benchmark_run_id") if doc else None

    async def _load_geo_grid_run(self, geo_grid_run_id: str) -> dict[str, Any] | None:
        try:
            doc = await get_database()[self._GEO_GRID_RUNS_COLLECTION].find_one(
                {"_id": self._parse_object_id(geo_grid_run_id, field_name="geo_grid_run_id")}
            )
        except LookupError:
            return None
        return self._serialize_id_doc(doc, "geo_grid_run_id") if doc else None

    async def _match_benchmark_business(
        self,
        *,
        benchmark_id: str,
        business_doc: dict[str, Any],
    ) -> dict[str, Any] | None:
        listing = business_doc.get("listing") if isinstance(business_doc.get("listing"), dict) else {}
        maps_url = (
            str(listing.get("maps_url_canonical") or listing.get("maps_url") or "").strip() or None
        )
        name = str(listing.get("business_name") or business_doc.get("name") or "").strip()
        address = str(listing.get("address") or "").strip()
        name_normalized = self._normalize_text(name)
        address_normalized = self._normalize_text(address)

        collection = get_database()[self._BENCHMARK_BUSINESSES_COLLECTION]
        if maps_url:
            doc = await collection.find_one(
                {"benchmark_id": benchmark_id, "maps_url_canonical": maps_url}
            )
            if doc is not None:
                return self._serialize_id_doc(doc, "benchmark_business_id")

        docs = await collection.find(
            {"benchmark_id": benchmark_id, "business_name_normalized": name_normalized}
        ).to_list(length=10)
        if not docs:
            return None
        if address_normalized:
            for doc in docs:
                candidate_address = self._normalize_text(str(doc.get("address") or ""))
                if candidate_address and candidate_address == address_normalized:
                    return self._serialize_id_doc(doc, "benchmark_business_id")
        return self._serialize_id_doc(docs[0], "benchmark_business_id")

    async def _resolve_competitors_for_business(
        self,
        *,
        benchmark_id: str,
        benchmark_business: dict[str, Any],
    ) -> list[dict[str, Any]]:
        target_id = str(benchmark_business.get("benchmark_business_id") or "")
        collection = get_database()[self._COMPETITOR_SETS_COLLECTION]
        existing = await collection.find_one(
            {"benchmark_id": benchmark_id, "target_business_id": target_id}
        )
        competitor_set = None
        if existing is not None:
            competitor_set = existing
        else:
            selected = await self._crm_service.select_competitors_for_benchmark_business(
                benchmark_business_id=target_id,
                max_competitors=5,
            )
            competitor_set = selected.get("competitor_set") if isinstance(selected, dict) else None
        competitors = (
            competitor_set.get("competitors")
            if isinstance(competitor_set, dict) and isinstance(competitor_set.get("competitors"), list)
            else []
        )
        return [dict(item) for item in competitors if isinstance(item, dict)]

    async def _safe_geo_grid_stats(self, geo_grid_run_id: str) -> dict[str, Any] | None:
        try:
            return await self._crm_service.get_geo_grid_stats(geo_grid_run_id=geo_grid_run_id)
        except Exception:  # noqa: BLE001
            return None

    def _match_geo_grid_business(
        self,
        *,
        stats: dict[str, Any] | None,
        business_doc: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not isinstance(stats, dict):
            return None
        businesses = stats.get("businesses") if isinstance(stats.get("businesses"), list) else []
        listing = business_doc.get("listing") if isinstance(business_doc.get("listing"), dict) else {}
        maps_url = str(listing.get("maps_url_canonical") or listing.get("maps_url") or "").strip()
        name = str(listing.get("business_name") or business_doc.get("name") or "").strip()
        name_normalized = self._normalize_text(name)
        if maps_url:
            for item in businesses:
                if not isinstance(item, dict):
                    continue
                if str(item.get("maps_url_canonical") or item.get("maps_url") or "").strip() == maps_url:
                    return dict(item)
        for item in businesses:
            if not isinstance(item, dict):
                continue
            if self._normalize_text(str(item.get("business_name") or "")) == name_normalized:
                return dict(item)
        return None

    def _derive_scope_from_business(self, business_doc: dict[str, Any]) -> dict[str, Any]:
        listing = business_doc.get("listing") if isinstance(business_doc.get("listing"), dict) else {}
        categories = listing.get("categories") if isinstance(listing.get("categories"), list) else []
        category = str(listing.get("category") or (categories[0] if categories else "") or "").strip()
        address = str(listing.get("address") or "").strip()
        city = self._derive_city_from_address(address)
        city_normalized = self._normalize_text(city)
        category_normalized = self._normalize_text(category)
        query = " ".join(part for part in [category, city] if part).strip()
        query_normalized = self._normalize_text(query)
        return {
            "category": category or None,
            "category_normalized": category_normalized or None,
            "city": city or None,
            "city_normalized": city_normalized or None,
            "city_slug": city_normalized.replace(" ", "-") if city_normalized else None,
            "benchmark_query": query or None,
            "query_normalized": query_normalized or None,
        }

    def _derive_city_from_address(self, address: str) -> str:
        cleaned = str(address or "").strip()
        if not cleaned:
            return ""
        segment = cleaned.split(",")[-1].strip()
        segment = re.sub(r"^\d{4,6}\s*", "", segment).strip()
        segment = re.sub(r"\s{2,}", " ", segment).strip()
        return segment

    def _is_fresh(self, run: dict[str, Any], *, ttl_days: int) -> bool:
        candidate = run.get("finished_at") or run.get("updated_at") or run.get("created_at")
        normalized_candidate = self._normalize_datetime(candidate)
        if normalized_candidate is None:
            return False
        return normalized_candidate >= datetime.now(timezone.utc) - timedelta(days=ttl_days)

    def _normalize_datetime(self, value: Any) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _merge_notes(self, *note_groups: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for notes in note_groups:
            for note in notes:
                normalized = str(note or "").strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                merged.append(normalized)
        return merged

    def _serialize_preparation(self, doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(doc)
        payload["report_preparation_id"] = str(payload.pop("_id"))
        return payload

    def _serialize_id_doc(self, doc: dict[str, Any], id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop("_id"))
        return payload

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value or "").strip())
        except (InvalidId, TypeError) as exc:
            raise LookupError(f"Invalid {field_name}: '{value}'.") from exc
