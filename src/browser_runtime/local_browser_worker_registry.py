from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from typing import Any, Iterable

from src.database import get_database


class LocalBrowserWorkerRegistry:
    _COLLECTION = "local_browser_runtime_workers"

    def __init__(self, *, collection_name: str | None = None) -> None:
        self._collection_name = str(collection_name or self._COLLECTION).strip() or self._COLLECTION

    async def heartbeat(
        self,
        *,
        worker_id: str,
        state: str,
        supported_sources: Iterable[str],
        current_job_id: str | None = None,
        current_source: str | None = None,
        current_execution_mode: str | None = None,
    ) -> dict[str, Any]:
        database = get_database()
        collection = database[self._collection_name]
        now = datetime.now(timezone.utc)
        payload = {
            "worker_id": str(worker_id),
            "state": str(state or "idle").strip().lower() or "idle",
            "supported_sources": list(dict.fromkeys(str(item).strip().lower() for item in supported_sources if str(item).strip())),
            "current_job_id": str(current_job_id or "").strip() or None,
            "current_source": str(current_source or "").strip().lower() or None,
            "current_execution_mode": str(current_execution_mode or "").strip().lower() or None,
            "host_name": socket.gethostname(),
            "pid": int(os.getpid()),
            "last_seen_at": now,
        }
        await collection.update_one(
            {"worker_id": str(worker_id)},
            {
                "$set": payload,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return payload

    async def list_workers(self) -> list[dict[str, Any]]:
        database = get_database()
        collection = database[self._collection_name]
        docs = await collection.find({}).sort([("last_seen_at", -1), ("worker_id", 1)]).to_list(length=100)
        items: list[dict[str, Any]] = []
        for doc in docs:
            payload = dict(doc)
            payload["worker_id"] = str(payload.get("worker_id") or payload.get("_id") or "")
            payload.pop("_id", None)
            items.append(payload)
        return items

