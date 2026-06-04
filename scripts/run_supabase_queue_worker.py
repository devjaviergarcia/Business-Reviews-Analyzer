#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback
    load_dotenv = None

from src.database import close_mongo_connection, connect_to_mongo, get_database
from src.services.crm_service import CRMService

if load_dotenv is not None:
    load_dotenv(REPO_ROOT / ".env", override=False)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def env_text(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def env_int(name: str, default: int) -> int:
    raw = env_text(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class WorkerConfig:
    supabase_url: str
    supabase_service_key: str
    worker_id: str
    worker_name: str
    poll_seconds: int
    control_poll_seconds: int
    max_jobs_per_pull: int
    retry_backoff_seconds: int
    lease_seconds: int
    once: bool
    manual_pull: bool
    verbose: bool


class SupabaseQueueClient:
    def __init__(self, *, base_url: str, service_key: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.service_key = service_key

    def _request(
        self,
        *,
        method: str,
        path: str,
        body: dict[str, Any] | list[dict[str, Any]] | None = None,
        prefer: str | None = None,
    ) -> Any:
        full_url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        payload_bytes = None
        if body is not None:
            payload_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers = {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        request = Request(full_url, data=payload_bytes, headers=headers, method=method.upper())
        try:
            with urlopen(request, timeout=20) as response:
                text = response.read().decode("utf-8", errors="replace")
                if not text:
                    return None
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return text
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Supabase HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"Supabase network error: {exc}") from exc

    def list_due_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        now_str = isoformat_utc(now_utc())
        query = (
            "/rest/v1/job_queue"
            "?select=*"
            "&or=(status.eq.pending,and(status.eq.retry_wait,next_retry_at.lte."
            + quote(now_str)
            + "))"
            "&order=priority.asc,created_at.asc"
            f"&limit={max(1, int(limit))}"
        )
        payload = self._request(method="GET", path=query)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    def claim_job(
        self,
        *,
        job: dict[str, Any],
        worker_id: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        job_id = str(job.get("id") or "").strip()
        status = str(job.get("status") or "").strip().lower()
        if not job_id or status not in {"pending", "retry_wait"}:
            return None
        claim_token = str(uuid.uuid4())
        attempt_count = int(job.get("attempt_count") or 0) + 1
        now_value = now_utc()
        lease_until = now_value + timedelta(seconds=max(30, int(lease_seconds)))
        patch = {
            "status": "running",
            "attempt_count": attempt_count,
            "claim_token": claim_token,
            "claimed_by": worker_id,
            "claimed_at": isoformat_utc(now_value),
            "lease_until": isoformat_utc(lease_until),
            "last_heartbeat_at": isoformat_utc(now_value),
            "updated_at": isoformat_utc(now_value),
            "error_last": None,
        }
        path = f"/rest/v1/job_queue?id=eq.{quote(job_id)}&status=eq.{quote(status)}"
        claimed = self._request(method="PATCH", path=path, body=patch, prefer="return=representation")
        if isinstance(claimed, list) and claimed:
            row = claimed[0]
            if isinstance(row, dict):
                return row
        return None

    def heartbeat_job(self, *, job_id: str, claim_token: str, lease_seconds: int) -> None:
        now_value = now_utc()
        lease_until = now_value + timedelta(seconds=max(30, int(lease_seconds)))
        patch = {
            "lease_until": isoformat_utc(lease_until),
            "last_heartbeat_at": isoformat_utc(now_value),
            "updated_at": isoformat_utc(now_value),
        }
        path = (
            f"/rest/v1/job_queue?id=eq.{quote(job_id)}"
            f"&claim_token=eq.{quote(claim_token)}"
            "&status=eq.running"
        )
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def complete_job(self, *, job_id: str, claim_token: str, result_ref: dict[str, Any]) -> None:
        patch = {
            "status": "completed",
            "result_ref": result_ref,
            "error_last": None,
            "claim_token": None,
            "lease_until": None,
            "updated_at": isoformat_utc(now_utc()),
        }
        path = (
            f"/rest/v1/job_queue?id=eq.{quote(job_id)}"
            f"&claim_token=eq.{quote(claim_token)}"
            "&status=eq.running"
        )
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def fail_job(
        self,
        *,
        job: dict[str, Any],
        claim_token: str,
        error_message: str,
        retry_backoff_seconds: int,
    ) -> None:
        job_id = str(job.get("id") or "").strip()
        attempt_count = int(job.get("attempt_count") or 0)
        max_attempts = int(job.get("max_attempts") or 5)
        terminal = attempt_count >= max_attempts
        patch: dict[str, Any] = {
            "status": "dead_letter" if terminal else "retry_wait",
            "error_last": error_message[:4000],
            "updated_at": isoformat_utc(now_utc()),
            "claim_token": None,
            "lease_until": None,
        }
        if terminal:
            patch["next_retry_at"] = None
        else:
            patch["next_retry_at"] = isoformat_utc(now_utc() + timedelta(seconds=max(60, retry_backoff_seconds)))
        path = (
            f"/rest/v1/job_queue?id=eq.{quote(job_id)}"
            f"&claim_token=eq.{quote(claim_token)}"
            "&status=eq.running"
        )
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def insert_job_event(
        self,
        *,
        job_id: str,
        event_type: str,
        message: str | None = None,
        meta: dict[str, Any] | None = None,
        progress_pct: float | None = None,
    ) -> None:
        row: dict[str, Any] = {
            "job_id": job_id,
            "event_type": event_type,
            "message": message,
            "meta": meta or {},
        }
        if progress_pct is not None:
            row["progress_pct"] = float(progress_pct)
        self._request(method="POST", path="/rest/v1/job_events", body=row, prefer="return=minimal")

    def get_intake_request(self, *, request_id: str) -> dict[str, Any] | None:
        path = f"/rest/v1/intake_requests?select=*&id=eq.{quote(request_id)}&limit=1"
        rows = self._request(method="GET", path=path)
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[0]
        return None

    def mark_intake_processed(self, *, request_id: str, technical: dict[str, Any]) -> None:
        patch = {
            "status": "processed",
            "technical": technical,
            "error_last": None,
        }
        path = f"/rest/v1/intake_requests?id=eq.{quote(request_id)}"
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def mark_intake_error(self, *, request_id: str, error_message: str) -> None:
        patch = {"status": "error", "error_last": error_message[:4000]}
        path = f"/rest/v1/intake_requests?id=eq.{quote(request_id)}"
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def get_feedback_submission(self, *, feedback_id: str) -> dict[str, Any] | None:
        path = f"/rest/v1/report_feedback_submissions?select=*&id=eq.{quote(feedback_id)}&limit=1"
        rows = self._request(method="GET", path=path)
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[0]
        return None

    def mark_feedback_processed(self, *, feedback_id: str) -> None:
        patch = {"status": "processed", "error_last": None}
        path = f"/rest/v1/report_feedback_submissions?id=eq.{quote(feedback_id)}"
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def mark_feedback_error(self, *, feedback_id: str, error_message: str) -> None:
        patch = {"status": "error", "error_last": error_message[:4000]}
        path = f"/rest/v1/report_feedback_submissions?id=eq.{quote(feedback_id)}"
        self._request(method="PATCH", path=path, body=patch, prefer="return=minimal")

    def get_worker_control(self, *, worker_id: str) -> dict[str, Any] | None:
        path = f"/rest/v1/worker_control?select=*&worker_id=eq.{quote(worker_id)}&limit=1"
        rows = self._request(method="GET", path=path)
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[0]
        return None

    def upsert_worker_control(self, *, worker_id: str, nonce: int, requested_at: str) -> None:
        row = {
            "worker_id": worker_id,
            "manual_pull_nonce": int(nonce),
            "manual_pull_requested_at": requested_at,
            "updated_at": requested_at,
        }
        self._request(
            method="POST",
            path="/rest/v1/worker_control",
            body=row,
            prefer="resolution=merge-duplicates,return=minimal",
        )

    def upsert_worker_runtime(
        self,
        *,
        worker_id: str,
        worker_name: str,
        status: str,
        mode: str,
        heartbeat: dict[str, Any],
        last_pull_at: str | None = None,
        last_manual_pull_at: str | None = None,
        next_pull_at: str | None = None,
        claimed_job_id: str | None = None,
    ) -> None:
        row = {
            "worker_id": worker_id,
            "worker_name": worker_name,
            "status": status,
            "mode": mode,
            "last_seen_at": isoformat_utc(now_utc()),
            "next_auto_pull_at": next_pull_at,
            "last_auto_pull_at": last_pull_at,
            "last_manual_pull_at": last_manual_pull_at,
            "claimed_job_id": claimed_job_id,
            "heartbeat": heartbeat,
        }
        self._request(
            method="POST",
            path="/rest/v1/worker_runtime",
            body=row,
            prefer="resolution=merge-duplicates,return=minimal",
        )


class RepiqSupabaseWorker:
    def __init__(self, *, config: WorkerConfig, supabase: SupabaseQueueClient, crm_service: CRMService) -> None:
        self.config = config
        self.supabase = supabase
        self.crm_service = crm_service
        self.stop_event = asyncio.Event()
        self.last_control_nonce = -1
        self.next_auto_pull_at = now_utc()
        self.hostname = socket.gethostname()

    def _log(self, message: str, payload: dict[str, Any] | None = None) -> None:
        base = {
            "ts": isoformat_utc(now_utc()),
            "worker_id": self.config.worker_id,
            "message": message,
        }
        if payload:
            base["data"] = payload
        print(json.dumps(base, ensure_ascii=False), flush=True)

    async def run(self) -> int:
        self._install_signal_handlers()
        self._log("worker_started", {"mode": "once" if self.config.once else "daemon"})

        initial_control = self.supabase.get_worker_control(worker_id=self.config.worker_id)
        if initial_control is not None:
            try:
                self.last_control_nonce = int(initial_control.get("manual_pull_nonce") or 0)
            except (TypeError, ValueError):
                self.last_control_nonce = 0

        if self.config.manual_pull:
            await self.run_pull_cycle(mode="manual")
            return 0

        if self.config.once:
            await self.run_pull_cycle(mode="auto")
            return 0

        while not self.stop_event.is_set():
            now_value = now_utc()
            run_reason: str | None = None

            control = self.supabase.get_worker_control(worker_id=self.config.worker_id)
            if control is not None:
                try:
                    control_nonce = int(control.get("manual_pull_nonce") or 0)
                except (TypeError, ValueError):
                    control_nonce = 0
                if control_nonce > self.last_control_nonce:
                    self.last_control_nonce = control_nonce
                    run_reason = "manual"

            if run_reason is None and now_value >= self.next_auto_pull_at:
                run_reason = "auto"

            if run_reason is not None:
                await self.run_pull_cycle(mode=run_reason)
                wait_seconds = max(1, int(self.config.control_poll_seconds))
            else:
                self._heartbeat(status="idle", mode="watch", last_pull=False)
                wait_seconds = max(1, int(self.config.control_poll_seconds))

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                continue

        self._heartbeat(status="stopped", mode="stop", last_pull=False)
        self._log("worker_stopped")
        return 0

    async def run_pull_cycle(self, *, mode: str) -> None:
        started_at = now_utc()
        mode_label = "manual" if mode == "manual" else "auto"
        self._log("pull_cycle_started", {"mode": mode_label})

        claimed = 0
        completed = 0
        failed = 0

        jobs = self.supabase.list_due_jobs(limit=self.config.max_jobs_per_pull)
        for job in jobs:
            claimed_job = self.supabase.claim_job(
                job=job,
                worker_id=self.config.worker_id,
                lease_seconds=self.config.lease_seconds,
            )
            if claimed_job is None:
                continue
            claimed += 1
            job_id = str(claimed_job.get("id") or "").strip()
            claim_token = str(claimed_job.get("claim_token") or "").strip()
            if not job_id or not claim_token:
                continue

            self._heartbeat(
                status="running",
                mode=mode_label,
                claimed_job_id=job_id,
                last_pull=False,
            )
            self.supabase.insert_job_event(
                job_id=job_id,
                event_type="claimed",
                message=f"claimed by {self.config.worker_id}",
                meta={"hostname": self.hostname, "mode": mode_label},
            )
            try:
                result = await self._process_claimed_job(job=claimed_job)
                self.supabase.complete_job(job_id=job_id, claim_token=claim_token, result_ref=result)
                self.supabase.insert_job_event(
                    job_id=job_id,
                    event_type="completed",
                    message="job completed",
                    meta=result,
                    progress_pct=100.0,
                )
                completed += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                error_text = str(exc)
                self.supabase.fail_job(
                    job=claimed_job,
                    claim_token=claim_token,
                    error_message=error_text,
                    retry_backoff_seconds=self.config.retry_backoff_seconds,
                )
                self.supabase.insert_job_event(
                    job_id=job_id,
                    event_type="failed",
                    message=error_text[:500],
                    meta={"mode": mode_label},
                )
                job_type = str(claimed_job.get("type") or "").strip().lower()
                payload = claimed_job.get("payload") if isinstance(claimed_job.get("payload"), dict) else {}
                if job_type == "lead_intake":
                    request_id = str(payload.get("intake_request_id") or "").strip()
                    if request_id:
                        self.supabase.mark_intake_error(request_id=request_id, error_message=error_text)
                elif job_type == "report_feedback":
                    feedback_id = str(payload.get("report_feedback_id") or "").strip()
                    if feedback_id:
                        self.supabase.mark_feedback_error(feedback_id=feedback_id, error_message=error_text)
                self._log("job_failed", {"job_id": job_id, "error": error_text})

        now_iso = isoformat_utc(now_utc())
        if mode_label == "auto":
            self.next_auto_pull_at = now_utc() + timedelta(seconds=max(60, self.config.poll_seconds))
            self._heartbeat(
                status="idle",
                mode="auto",
                last_pull=True,
                last_pull_at=now_iso,
                next_pull_at=isoformat_utc(self.next_auto_pull_at),
            )
        else:
            self._heartbeat(
                status="idle",
                mode="manual",
                last_pull=True,
                last_pull_at=now_iso,
                last_manual_pull_at=now_iso,
                next_pull_at=isoformat_utc(self.next_auto_pull_at),
            )

        elapsed = max(0.0, (now_utc() - started_at).total_seconds())
        self._log(
            "pull_cycle_finished",
            {
                "mode": mode_label,
                "claimed": claimed,
                "completed": completed,
                "failed": failed,
                "elapsed_seconds": round(elapsed, 3),
            },
        )

    async def _process_claimed_job(self, *, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job.get("id") or "").strip()
        claim_token = str(job.get("claim_token") or "").strip()
        if job_id and claim_token:
            self.supabase.heartbeat_job(
                job_id=job_id,
                claim_token=claim_token,
                lease_seconds=self.config.lease_seconds,
            )

        job_type = str(job.get("type") or "").strip().lower()
        payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}

        if job_type == "lead_intake":
            return await self._handle_lead_intake(job=job, payload=payload)
        if job_type == "report_feedback":
            return await self._handle_report_feedback(job=job, payload=payload)
        if job_type == "manual_pipeline_launch":
            return await self._handle_manual_pipeline_launch(job=job, payload=payload)
        raise ValueError(f"Unsupported job type: {job_type}")

    async def _handle_lead_intake(self, *, job: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        request_id = str(payload.get("intake_request_id") or "").strip()
        if not request_id:
            raise ValueError("lead_intake payload missing intake_request_id.")

        intake = self.supabase.get_intake_request(request_id=request_id)
        if intake is None:
            raise LookupError(f"intake_request '{request_id}' not found.")

        if str(intake.get("status") or "").strip().lower() == "processed":
            technical = intake.get("technical") if isinstance(intake.get("technical"), dict) else {}
            return {
                "type": "lead_intake",
                "intake_request_id": request_id,
                "status": "already_processed",
                "crm_lead_id": technical.get("crm_lead_id"),
            }

        technical = intake.get("technical") if isinstance(intake.get("technical"), dict) else {}
        legal = intake.get("legal") if isinstance(intake.get("legal"), dict) else {}
        page_context = intake.get("page_context") if isinstance(intake.get("page_context"), dict) else {}
        utm = intake.get("utm") if isinstance(intake.get("utm"), dict) else {}

        existing = await get_database()["crm_leads"].find_one(
            {"source_ref.intake_request_id": request_id},
            {"_id": 1},
        )
        if existing is not None:
            existing_lead_id = str(existing.get("_id") or "").strip() or None
            updated_technical = dict(technical)
            updated_technical["crm_lead_id"] = existing_lead_id
            updated_technical["worker_processed_at"] = isoformat_utc(now_utc())
            updated_technical["worker_id"] = self.config.worker_id
            self.supabase.mark_intake_processed(request_id=request_id, technical=updated_technical)
            return {
                "type": "lead_intake",
                "intake_request_id": request_id,
                "status": "deduplicated_existing_lead",
                "crm_lead_id": existing_lead_id,
            }

        message = str(intake.get("message") or "").strip()
        notes = []
        if message:
            notes.append(f"Mensaje formulario: {message}")
        source_page = str(page_context.get("source_page") or "").strip()
        if source_page:
            notes.append(f"source_page={source_page}")
        if utm:
            compact_utm = json.dumps(utm, ensure_ascii=False)
            notes.append(f"utm={compact_utm}")

        consent_proof = {
            "source": "landing_form_pre",
            "legal_version": legal.get("legal_version"),
            "consent_timestamp": legal.get("consent_timestamp"),
            "consent_origin": legal.get("consent_origin"),
            "consent_contact": bool(legal.get("consent_contact")),
            "consent_privacy": bool(legal.get("consent_privacy")),
            "consent_terms": bool(legal.get("consent_terms")),
            "consent_marketing": bool(legal.get("consent_marketing")),
            "intake_request_id": request_id,
        }

        lead = await self.crm_service.create_lead(
            business_name=str(intake.get("business_name") or "").strip(),
            contact_name=str(intake.get("contact_name") or "").strip() or None,
            email=str(intake.get("email") or "").strip() or None,
            phone=str(intake.get("phone") or "").strip() or None,
            city=str(intake.get("city") or "").strip() or None,
            source=str(intake.get("source") or "landing").strip() or "landing",
            status="prospecto",
            notes=notes,
            consent_status="granted",
            consent_proof=consent_proof,
            source_ref={
                "created_from": "supabase_queue_worker",
                "intake_request_id": request_id,
                "delivery_channel": str(intake.get("delivery_channel") or "").strip() or None,
                "source_page": source_page or None,
            },
        )

        lead_id = str(lead.get("lead_id") or "").strip() or None
        updated_technical = dict(technical)
        updated_technical["crm_lead_id"] = lead_id
        updated_technical["worker_processed_at"] = isoformat_utc(now_utc())
        updated_technical["worker_id"] = self.config.worker_id
        self.supabase.mark_intake_processed(request_id=request_id, technical=updated_technical)

        return {
            "type": "lead_intake",
            "intake_request_id": request_id,
            "crm_lead_id": lead_id,
            "business_name": str(intake.get("business_name") or "").strip(),
        }

    async def _handle_report_feedback(self, *, job: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        feedback_id = str(payload.get("report_feedback_id") or "").strip()
        if not feedback_id:
            raise ValueError("report_feedback payload missing report_feedback_id.")
        feedback = self.supabase.get_feedback_submission(feedback_id=feedback_id)
        if feedback is None:
            raise LookupError(f"report_feedback_submissions '{feedback_id}' not found.")

        if str(feedback.get("status") or "").strip().lower() == "processed":
            return {
                "type": "report_feedback",
                "report_feedback_id": feedback_id,
                "status": "already_processed",
            }

        created = await self.crm_service.create_report_feedback(
            branch=str(feedback.get("branch") or "").strip().upper(),
            answers=feedback.get("answers") if isinstance(feedback.get("answers"), dict) else {},
            lead_id=str(feedback.get("lead_id") or "").strip() or None,
            report_request_id=str(feedback.get("report_request_id") or "").strip() or None,
            lead_report_id=str(feedback.get("lead_report_id") or "").strip() or None,
            benchmark_business_id=str(feedback.get("benchmark_business_id") or "").strip() or None,
            report_kind=str(feedback.get("report_kind") or "").strip() or None,
            source_page=str(feedback.get("source_page") or "").strip() or None,
            referrer=str(feedback.get("referrer") or "").strip() or None,
            user_agent=str(feedback.get("user_agent") or "").strip() or None,
            ip_hash=str(feedback.get("ip_hash") or "").strip() or None,
        )
        self.supabase.mark_feedback_processed(feedback_id=feedback_id)
        return {
            "type": "report_feedback",
            "report_feedback_id": feedback_id,
            "crm_report_feedback_id": created.get("report_feedback_id"),
            "label": created.get("label"),
        }

    async def _handle_manual_pipeline_launch(self, *, job: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        lead_id = str(payload.get("lead_id") or "").strip()
        if not lead_id:
            raise ValueError("manual_pipeline_launch payload missing lead_id.")
        sources = payload.get("sources") if isinstance(payload.get("sources"), list) else None
        normalized_sources = [str(item).strip().lower() for item in (sources or []) if str(item).strip()]
        force = bool(payload.get("force"))
        google_maps_name = str(payload.get("google_maps_name") or "").strip() or None
        tripadvisor_name = str(payload.get("tripadvisor_name") or "").strip() or None

        queued = await self.crm_service.enqueue_lead_pipeline_job(
            lead_id=lead_id,
            force=force,
            sources=normalized_sources or None,
            google_maps_name=google_maps_name,
            tripadvisor_name=tripadvisor_name,
        )
        return {
            "type": "manual_pipeline_launch",
            "lead_id": lead_id,
            "crm_job_id": queued.get("job_id"),
            "queue_name": queued.get("queue_name"),
        }

    def _heartbeat(
        self,
        *,
        status: str,
        mode: str,
        last_pull: bool,
        last_pull_at: str | None = None,
        last_manual_pull_at: str | None = None,
        next_pull_at: str | None = None,
        claimed_job_id: str | None = None,
    ) -> None:
        heartbeat = {
            "hostname": self.hostname,
            "pid": os.getpid(),
            "last_pull": bool(last_pull),
            "status": status,
            "mode": mode,
        }
        self.supabase.upsert_worker_runtime(
            worker_id=self.config.worker_id,
            worker_name=self.config.worker_name,
            status=status,
            mode=mode,
            heartbeat=heartbeat,
            last_pull_at=last_pull_at,
            last_manual_pull_at=last_manual_pull_at,
            next_pull_at=next_pull_at,
            claimed_job_id=claimed_job_id,
        )

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def stop() -> None:
            self.stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop)
            except NotImplementedError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Worker local: pull de jobs en Supabase y sync al CRM local de Repiq."
    )
    parser.add_argument("--once", action="store_true", help="Ejecuta un pull y termina.")
    parser.add_argument("--manual-pull", action="store_true", help="Fuerza una pasada manual inmediata y termina.")
    parser.add_argument("--poll-seconds", type=int, default=None, help="Intervalo de auto pull (default env o 900).")
    parser.add_argument(
        "--control-poll-seconds",
        type=int,
        default=None,
        help="Frecuencia para escuchar trigger manual (default env o 30).",
    )
    parser.add_argument("--max-jobs", type=int, default=None, help="Maximo de jobs por ciclo pull.")
    parser.add_argument("--verbose", action="store_true", help="Salida detallada.")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> WorkerConfig:
    supabase_url = env_text("SUPABASE_URL")
    supabase_service_key = env_text("SUPABASE_SERVICE_ROLE_KEY") or env_text("SUPABASE_SECRET_KEY")
    if not supabase_url:
        raise RuntimeError("SUPABASE_URL is required.")
    if not supabase_service_key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_SECRET_KEY) is required.")

    worker_id = env_text("SUPABASE_WORKER_ID", "repiq-local-worker")
    worker_name = env_text("SUPABASE_WORKER_NAME", "Repiq Local Worker")

    poll_seconds = int(args.poll_seconds or env_int("SUPABASE_WORKER_POLL_SECONDS", 900))
    control_poll_seconds = int(args.control_poll_seconds or env_int("SUPABASE_WORKER_CONTROL_POLL_SECONDS", 30))
    max_jobs_per_pull = int(args.max_jobs or env_int("SUPABASE_WORKER_MAX_JOBS_PER_PULL", 20))
    retry_backoff_seconds = int(env_int("SUPABASE_WORKER_RETRY_BACKOFF_SECONDS", 300))
    lease_seconds = int(env_int("SUPABASE_WORKER_LEASE_SECONDS", 1200))

    return WorkerConfig(
        supabase_url=supabase_url,
        supabase_service_key=supabase_service_key,
        worker_id=worker_id,
        worker_name=worker_name,
        poll_seconds=max(60, poll_seconds),
        control_poll_seconds=max(5, control_poll_seconds),
        max_jobs_per_pull=max(1, min(200, max_jobs_per_pull)),
        retry_backoff_seconds=max(60, retry_backoff_seconds),
        lease_seconds=max(60, lease_seconds),
        once=bool(args.once),
        manual_pull=bool(args.manual_pull),
        verbose=bool(args.verbose),
    )


async def main_async() -> int:
    args = parse_args()
    config = build_config(args)
    await connect_to_mongo()
    try:
        crm_service = CRMService()
        supabase = SupabaseQueueClient(base_url=config.supabase_url, service_key=config.supabase_service_key)
        worker = RepiqSupabaseWorker(config=config, supabase=supabase, crm_service=crm_service)
        return await worker.run()
    finally:
        await close_mongo_connection()


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
