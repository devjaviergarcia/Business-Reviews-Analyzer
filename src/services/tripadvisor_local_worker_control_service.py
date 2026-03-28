from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib import error, request

from src.config import settings


class TripadvisorLocalWorkerControlService:
    """Calls a host-local bridge to control TripAdvisor worker lifecycle."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        bridge_url: str | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self._enabled = bool(
            settings.tripadvisor_local_worker_bridge_enabled
            if enabled is None
            else enabled
        )
        raw_url = (
            settings.tripadvisor_local_worker_bridge_url
            if bridge_url is None
            else bridge_url
        )
        self._bridge_url = str(raw_url or "").strip().rstrip("/")
        configured_timeout = (
            settings.tripadvisor_local_worker_bridge_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        self._timeout_seconds = max(0.5, float(configured_timeout))

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def ensure_started(
        self,
        *,
        use_xvfb: bool = True,
        reason: str = "enqueue_tripadvisor_scrape_job",
    ) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        payload = {
            "use_xvfb": bool(use_xvfb),
            "reason": str(reason or "enqueue_tripadvisor_scrape_job"),
        }
        return await asyncio.to_thread(
            self._request_json,
            method="POST",
            path="/worker/ensure-started",
            payload=payload,
        )

    async def status(self) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        return await asyncio.to_thread(
            self._request_json,
            method="GET",
            path="/worker/status",
            payload=None,
        )

    async def live_session_status(self) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        return await asyncio.to_thread(
            self._request_json,
            method="GET",
            path="/live-session/status",
            payload=None,
        )

    async def launch_live_session(
        self,
        *,
        reason: str = "needs_human_live",
        display: str | None = None,
        profile_dir: str | None = None,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        payload: dict[str, Any] = {
            "reason": str(reason or "needs_human_live"),
        }
        if isinstance(display, str) and display.strip():
            payload["display"] = display.strip()
        if isinstance(profile_dir, str) and profile_dir.strip():
            payload["profile_dir"] = profile_dir.strip()
        if isinstance(job_id, str) and job_id.strip():
            payload["job_id"] = job_id.strip()
        return await asyncio.to_thread(
            self._request_json,
            method="POST",
            path="/live-session/launch",
            payload=payload,
        )

    async def live_session_log_tail(self, *, max_chars: int = 6000) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        safe_max = max(200, min(int(max_chars), 50000))
        return await asyncio.to_thread(
            self._request_json,
            method="GET",
            path=f"/live-session/log-tail?max_chars={safe_max}",
            payload=None,
        )

    async def stop_live_session(self) -> dict[str, Any]:
        if not self._enabled:
            return {
                "ok": False,
                "skipped": True,
                "reason": "bridge_disabled",
            }
        if not self._bridge_url:
            raise RuntimeError(
                "Tripadvisor local worker bridge URL is empty. "
                "Set TRIPADVISOR_LOCAL_WORKER_BRIDGE_URL."
            )
        return await asyncio.to_thread(
            self._request_json,
            method="POST",
            path="/live-session/stop",
            payload={},
        )

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        url = f"{self._bridge_url}{path}"
        body: bytes | None = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            url=url,
            method=method.upper(),
            data=body,
            headers=headers,
        )
        try:
            with request.urlopen(req, timeout=self._timeout_seconds) as response:  # noqa: S310
                content = response.read().decode("utf-8", errors="replace").strip()
                if not content:
                    return {"ok": True}
                parsed = json.loads(content)
                if not isinstance(parsed, dict):
                    raise RuntimeError(
                        f"Tripadvisor bridge response is not a JSON object: {parsed!r}"
                    )
                return parsed
        except error.HTTPError as exc:
            response_body = ""
            try:
                response_body = exc.read().decode("utf-8", errors="replace").strip()
            except Exception:
                response_body = ""
            detail = response_body or str(exc.reason or exc)
            raise RuntimeError(
                f"Tripadvisor local worker bridge HTTP {exc.code}: {detail}"
            ) from exc
        except error.URLError as exc:
            raise RuntimeError(
                "Tripadvisor local worker bridge is unreachable at "
                f"{self._bridge_url}. Detail: {exc.reason!r}"
            ) from exc
        except TimeoutError as exc:
            raise RuntimeError(
                "Tripadvisor local worker bridge timed out after "
                f"{self._timeout_seconds:.1f}s."
            ) from exc
