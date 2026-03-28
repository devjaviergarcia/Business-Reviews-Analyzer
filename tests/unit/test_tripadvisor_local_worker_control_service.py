from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib import error

import pytest

from src.services import (
    tripadvisor_local_worker_control_service as local_worker_control_module,
)
from src.services.tripadvisor_local_worker_control_service import (
    TripadvisorLocalWorkerControlService,
)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> bool:  # noqa: ANN001
        return False


def test_ensure_started_returns_skipped_when_bridge_disabled() -> None:
    service = TripadvisorLocalWorkerControlService(
        enabled=False,
        bridge_url="http://127.0.0.1:8765",
        timeout_seconds=1.0,
    )
    result = asyncio.run(service.ensure_started())
    assert result["skipped"] is True
    assert result["reason"] == "bridge_disabled"


def test_ensure_started_calls_bridge_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_urlopen(req, timeout: float = 0):  # noqa: ANN001
        captured["url"] = req.full_url
        captured["method"] = req.get_method()
        captured["timeout"] = timeout
        captured["headers"] = dict(req.headers)
        captured["body"] = json.loads((req.data or b"{}").decode("utf-8"))
        return _FakeResponse({"ok": True, "worker": {"running": True}})

    monkeypatch.setattr(local_worker_control_module.request, "urlopen", _fake_urlopen)

    service = TripadvisorLocalWorkerControlService(
        enabled=True,
        bridge_url="http://bridge.local:8765",
        timeout_seconds=7.5,
    )
    result = asyncio.run(
        service.ensure_started(use_xvfb=False, reason="unit_test_enqueue")
    )

    assert result["ok"] is True
    assert result["worker"]["running"] is True
    assert captured["url"] == "http://bridge.local:8765/worker/ensure-started"
    assert captured["method"] == "POST"
    assert captured["timeout"] == 7.5
    assert captured["body"]["use_xvfb"] is False
    assert captured["body"]["reason"] == "unit_test_enqueue"


def test_ensure_started_raises_runtime_error_on_unreachable_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_urlopen(_req, timeout: float = 0):  # noqa: ANN001
        del timeout
        raise error.URLError("connection refused")

    monkeypatch.setattr(local_worker_control_module.request, "urlopen", _fake_urlopen)

    service = TripadvisorLocalWorkerControlService(
        enabled=True,
        bridge_url="http://127.0.0.1:8765",
        timeout_seconds=2.0,
    )
    with pytest.raises(RuntimeError) as exc_info:
        asyncio.run(service.ensure_started())

    assert "unreachable" in str(exc_info.value).lower()

