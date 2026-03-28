from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pymongo import ReturnDocument

from src.config import settings
from src.database import get_database


class TripadvisorSessionService:
    _COLLECTION = "tripadvisor_session_state"
    _DOC_ID = "global_tripadvisor_session"

    async def get_state(self) -> dict[str, Any]:
        collection = get_database()[self._COLLECTION]
        now = datetime.now(timezone.utc)
        doc = await collection.find_one({"_id": self._DOC_ID})
        if doc is None:
            doc = {
                "_id": self._DOC_ID,
                "session_state": "invalid",
                "availability_now": False,
                "last_human_intervention_at": None,
                "session_cookie_expires_at": None,
                "playwright_profile_path": self._resolve_profile_dir_path().as_posix(),
                "playwright_storage_state_path": self._resolve_storage_state_path().as_posix(),
                "last_validation_attempt_at": None,
                "last_validation_result": "not_initialized",
                "last_error": "Session state not initialized.",
                "bot_detected_count": 0,
                "created_at": now,
                "updated_at": now,
            }
            await collection.insert_one(doc)
            return self._sanitize_payload(doc)

        return self._sanitize_payload(await self._normalize_availability(doc=doc, now=now))

    async def refresh_from_storage_state(
        self,
        *,
        storage_state_path: str | None = None,
        profile_dir: str | None = None,
        mark_human_intervention: bool = False,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        resolved_storage_state_path = self._resolve_storage_state_path(storage_state_path, profile_dir)
        payload = self._read_storage_state_file(resolved_storage_state_path)
        expires_at = self.extract_tripadvisor_cookie_expiration(payload)
        session_state, availability_now, validation_result, error = self.classify_session(
            expires_at=expires_at,
            now=now,
        )

        collection = get_database()[self._COLLECTION]
        update_fields: dict[str, Any] = {
            "session_state": session_state,
            "availability_now": availability_now,
            "session_cookie_expires_at": expires_at,
            "playwright_profile_path": (
                self._resolve_profile_dir_path(profile_dir).as_posix()
            ),
            "playwright_storage_state_path": resolved_storage_state_path.as_posix(),
            "last_validation_attempt_at": now,
            "last_validation_result": validation_result,
            "last_error": error,
            "updated_at": now,
        }
        if mark_human_intervention:
            update_fields["last_human_intervention_at"] = now

        updated_doc = await collection.find_one_and_update(
            {"_id": self._DOC_ID},
            {
                "$set": update_fields,
                "$setOnInsert": {"created_at": now, "bot_detected_count": 0},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise RuntimeError("Failed to update TripAdvisor session state.")
        normalized = await self._normalize_availability(doc=updated_doc, now=now)
        return self._sanitize_payload(normalized)

    async def mark_invalid(
        self,
        *,
        reason: str,
        increment_bot_detected: bool = False,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        collection = get_database()[self._COLLECTION]
        update_fields: dict[str, Any] = {
            "session_state": "invalid",
            "availability_now": False,
            "last_validation_attempt_at": now,
            "last_validation_result": "invalidated",
            "last_error": str(reason or "Session invalidated."),
            "updated_at": now,
        }
        set_on_insert: dict[str, Any] = {
            "created_at": now,
            "playwright_profile_path": self._resolve_profile_dir_path().as_posix(),
            "playwright_storage_state_path": self._resolve_storage_state_path().as_posix(),
            "last_human_intervention_at": None,
            "session_cookie_expires_at": None,
        }
        if not increment_bot_detected:
            set_on_insert["bot_detected_count"] = 0

        update_ops: dict[str, Any] = {
            "$set": update_fields,
            "$setOnInsert": set_on_insert,
        }
        if increment_bot_detected:
            update_ops["$inc"] = {"bot_detected_count": 1}
        updated_doc = await collection.find_one_and_update(
            {"_id": self._DOC_ID},
            update_ops,
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if updated_doc is None:
            raise RuntimeError("Failed to invalidate TripAdvisor session state.")
        return self._sanitize_payload(updated_doc)

    async def ensure_available(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        state = await self.get_state()
        raw_state = str(state.get("session_state") or "invalid").strip().lower()
        expires_at = state.get("session_cookie_expires_at")
        if isinstance(expires_at, str):
            try:
                expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            except Exception:
                expires_at = None

        if raw_state == "valid" and isinstance(expires_at, datetime) and expires_at > now:
            return state
        return self._sanitize_payload(
            await self._normalize_availability(
                doc={
                    **state,
                    "session_state": raw_state,
                    "session_cookie_expires_at": expires_at,
                },
                now=now,
                persist=True,
            )
        )

    async def try_acquire_worker_singleton(
        self,
        *,
        token: str,
        pid: int,
        queue_name: str,
        worker_source: str,
        host: str,
        metadata: dict[str, Any] | None = None,
        stale_after_seconds: int,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        safe_token = str(token or "").strip()
        if not safe_token:
            raise ValueError("worker singleton token is required.")

        safe_stale_seconds = max(10, int(stale_after_seconds))
        stale_before = now - timedelta(seconds=safe_stale_seconds)

        collection = get_database()[self._COLLECTION]
        # Ensure the singleton document exists before lock acquisition.
        # Do not use upsert during acquisition to avoid duplicate-key races.
        await self.get_state()
        singleton_payload: dict[str, Any] = {
            "active": True,
            "token": safe_token,
            "pid": int(pid),
            "host": str(host or "").strip() or "unknown",
            "queue_name": str(queue_name or "").strip(),
            "worker_source": str(worker_source or "").strip(),
            "metadata": dict(metadata or {}),
            "started_at": now,
            "heartbeat_at": now,
            "released_at": None,
            "release_reason": None,
        }
        updated_doc = await collection.find_one_and_update(
            {
                "_id": self._DOC_ID,
                "$or": [
                    {"worker_singleton": {"$exists": False}},
                    {"worker_singleton.active": {"$ne": True}},
                    {"worker_singleton.heartbeat_at": {"$lt": stale_before}},
                    {"worker_singleton.token": safe_token},
                ],
            },
            {
                "$set": {
                    "worker_singleton": singleton_payload,
                    "updated_at": now,
                },
            },
            return_document=ReturnDocument.AFTER,
        )

        if updated_doc is not None:
            current = updated_doc.get("worker_singleton")
            if isinstance(current, dict) and str(current.get("token") or "").strip() == safe_token:
                return {
                    "acquired": True,
                    "worker_singleton": self._sanitize_payload(current),
                }

        holder_doc = await collection.find_one({"_id": self._DOC_ID}, projection={"worker_singleton": 1})
        holder = holder_doc.get("worker_singleton") if isinstance(holder_doc, dict) else None
        return {
            "acquired": False,
            "worker_singleton": self._sanitize_payload(holder) if isinstance(holder, dict) else None,
        }

    async def touch_worker_singleton(
        self,
        *,
        token: str,
        pid: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        now = datetime.now(timezone.utc)
        safe_token = str(token or "").strip()
        if not safe_token:
            return False
        collection = get_database()[self._COLLECTION]
        updated_doc = await collection.find_one_and_update(
            {
                "_id": self._DOC_ID,
                "worker_singleton.token": safe_token,
                "worker_singleton.active": True,
            },
            {
                "$set": {
                    "worker_singleton.pid": int(pid),
                    "worker_singleton.heartbeat_at": now,
                    "worker_singleton.metadata": dict(metadata or {}),
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return updated_doc is not None

    async def release_worker_singleton(self, *, token: str, reason: str | None = None) -> bool:
        now = datetime.now(timezone.utc)
        safe_token = str(token or "").strip()
        if not safe_token:
            return False
        collection = get_database()[self._COLLECTION]
        updated_doc = await collection.find_one_and_update(
            {
                "_id": self._DOC_ID,
                "worker_singleton.token": safe_token,
            },
            {
                "$set": {
                    "worker_singleton.active": False,
                    "worker_singleton.heartbeat_at": now,
                    "worker_singleton.released_at": now,
                    "worker_singleton.release_reason": str(reason or "worker_exit"),
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return updated_doc is not None

    async def set_worker_singleton_active_job(self, *, token: str, job_id: str) -> bool:
        now = datetime.now(timezone.utc)
        safe_token = str(token or "").strip()
        safe_job_id = str(job_id or "").strip()
        if not safe_token or not safe_job_id:
            return False
        collection = get_database()[self._COLLECTION]
        updated_doc = await collection.find_one_and_update(
            {
                "_id": self._DOC_ID,
                "worker_singleton.token": safe_token,
                "worker_singleton.active": True,
            },
            {
                "$set": {
                    "worker_singleton.active_job_id": safe_job_id,
                    "worker_singleton.active_job_started_at": now,
                    "worker_singleton.active_job_finished_at": None,
                    "worker_singleton.active_job_status": "running",
                    "worker_singleton.heartbeat_at": now,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return updated_doc is not None

    async def clear_worker_singleton_active_job(
        self,
        *,
        token: str,
        job_id: str,
        job_status: str,
    ) -> bool:
        now = datetime.now(timezone.utc)
        safe_token = str(token or "").strip()
        safe_job_id = str(job_id or "").strip()
        if not safe_token or not safe_job_id:
            return False
        collection = get_database()[self._COLLECTION]
        updated_doc = await collection.find_one_and_update(
            {
                "_id": self._DOC_ID,
                "worker_singleton.token": safe_token,
                "worker_singleton.active": True,
                "worker_singleton.active_job_id": safe_job_id,
            },
            {
                "$set": {
                    "worker_singleton.active_job_status": str(job_status or "unknown"),
                    "worker_singleton.active_job_finished_at": now,
                    "worker_singleton.heartbeat_at": now,
                    "updated_at": now,
                },
                "$unset": {
                    "worker_singleton.active_job_id": "",
                    "worker_singleton.active_job_started_at": "",
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        return updated_doc is not None

    async def _normalize_availability(
        self,
        *,
        doc: dict[str, Any],
        now: datetime,
        persist: bool = False,
    ) -> dict[str, Any]:
        normalized_doc = dict(doc)
        raw_state = str(normalized_doc.get("session_state") or "invalid").strip().lower()
        expires_at = normalized_doc.get("session_cookie_expires_at")
        if isinstance(expires_at, str):
            try:
                expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            except Exception:
                expires_at = None
        if isinstance(expires_at, datetime) and expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if raw_state == "valid" and isinstance(expires_at, datetime) and expires_at <= now:
            raw_state = "expired"
        availability_now = raw_state == "valid" and isinstance(expires_at, datetime) and expires_at > now
        normalized_doc["session_state"] = raw_state if raw_state in {"valid", "invalid", "expired"} else "invalid"
        normalized_doc["availability_now"] = bool(availability_now)
        normalized_doc["session_cookie_expires_at"] = expires_at
        normalized_doc["updated_at"] = now

        if persist:
            collection = get_database()[self._COLLECTION]
            updated = await collection.find_one_and_update(
                {"_id": self._DOC_ID},
                {
                    "$set": {
                        "session_state": normalized_doc["session_state"],
                        "availability_now": normalized_doc["availability_now"],
                        "session_cookie_expires_at": normalized_doc["session_cookie_expires_at"],
                        "updated_at": now,
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            if updated is not None:
                normalized_doc = updated
        return normalized_doc

    @classmethod
    def extract_tripadvisor_cookie_expiration(cls, storage_state_payload: dict[str, Any]) -> datetime | None:
        cookies = storage_state_payload.get("cookies")
        if not isinstance(cookies, list):
            return None
        latest_expiration: datetime | None = None
        for item in cookies:
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain") or "").strip().lower()
            if "tripadvisor." not in domain:
                continue
            expires_raw = item.get("expires")
            try:
                expires_unix = float(expires_raw)
            except (TypeError, ValueError):
                continue
            if expires_unix <= 0:
                continue
            candidate = datetime.fromtimestamp(expires_unix, tz=timezone.utc)
            if latest_expiration is None or candidate > latest_expiration:
                latest_expiration = candidate
        return latest_expiration

    @classmethod
    def classify_session(
        cls,
        *,
        expires_at: datetime | None,
        now: datetime,
    ) -> tuple[str, bool, str, str | None]:
        if expires_at is None:
            return "invalid", False, "missing_cookie_expiration", "TripAdvisor cookies are missing."
        if expires_at <= now:
            return "expired", False, "cookie_expired", "TripAdvisor session cookie has expired."
        return "valid", True, "ok", None

    def _resolve_profile_dir_path(self, profile_dir: str | None = None) -> Path:
        raw_path = str(profile_dir or settings.scraper_tripadvisor_user_data_dir).strip()
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        return path.resolve()

    def _resolve_storage_state_path(
        self,
        storage_state_path: str | None = None,
        profile_dir: str | None = None,
    ) -> Path:
        if storage_state_path:
            path = Path(str(storage_state_path).strip()).expanduser()
            if not path.is_absolute():
                path = Path.cwd() / path
            return path.resolve()
        return (self._resolve_profile_dir_path(profile_dir) / "storage_state.json").resolve()

    def _read_storage_state_file(self, storage_state_path: Path) -> dict[str, Any]:
        if not storage_state_path.exists():
            raise FileNotFoundError(f"Storage state file not found: {storage_state_path}")
        with storage_state_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
        if not isinstance(payload, dict):
            raise ValueError("Invalid Playwright storage_state payload.")
        return payload

    def _sanitize_payload(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self._sanitize_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._sanitize_payload(item) for item in value]
        return value
