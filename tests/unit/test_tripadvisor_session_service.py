from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from src.services import tripadvisor_session_service as tripadvisor_session_module
from src.services.tripadvisor_session_service import TripadvisorSessionService


def test_extract_tripadvisor_cookie_expiration_returns_latest_cookie_expiration() -> None:
    now = datetime.now(timezone.utc)
    payload = {
        "cookies": [
            {
                "name": "sessionid",
                "domain": ".tripadvisor.es",
                "expires": (now + timedelta(hours=2)).timestamp(),
            },
            {
                "name": "auth",
                "domain": ".tripadvisor.com",
                "expires": (now + timedelta(hours=6)).timestamp(),
            },
            {
                "name": "other",
                "domain": ".google.com",
                "expires": (now + timedelta(hours=10)).timestamp(),
            },
        ]
    }

    expiration = TripadvisorSessionService.extract_tripadvisor_cookie_expiration(payload)

    assert expiration is not None
    assert expiration > now + timedelta(hours=5)
    assert expiration < now + timedelta(hours=7)


def test_extract_tripadvisor_cookie_expiration_returns_none_when_missing() -> None:
    payload = {
        "cookies": [
            {
                "name": "other",
                "domain": ".google.com",
                "expires": 9999999999,
            }
        ]
    }

    expiration = TripadvisorSessionService.extract_tripadvisor_cookie_expiration(payload)

    assert expiration is None


def test_classify_session_valid_expired_and_invalid() -> None:
    now = datetime.now(timezone.utc)
    valid_expiration = now + timedelta(minutes=30)
    expired_expiration = now - timedelta(minutes=1)

    valid = TripadvisorSessionService.classify_session(expires_at=valid_expiration, now=now)
    expired = TripadvisorSessionService.classify_session(expires_at=expired_expiration, now=now)
    invalid = TripadvisorSessionService.classify_session(expires_at=None, now=now)

    assert valid[0] == "valid"
    assert valid[1] is True
    assert valid[2] == "ok"
    assert valid[3] is None

    assert expired[0] == "expired"
    assert expired[1] is False
    assert expired[2] == "cookie_expired"
    assert isinstance(expired[3], str)

    assert invalid[0] == "invalid"
    assert invalid[1] is False
    assert invalid[2] == "missing_cookie_expiration"
    assert isinstance(invalid[3], str)


class _FakeCollection:
    def __init__(self) -> None:
        self.last_filter: dict[str, Any] | None = None
        self.last_update: dict[str, Any] | None = None
        self.last_kwargs: dict[str, Any] | None = None

    async def find_one_and_update(self, filter_doc: dict[str, Any], update_doc: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        self.last_filter = filter_doc
        self.last_update = update_doc
        self.last_kwargs = kwargs
        result: dict[str, Any] = {"_id": "global_tripadvisor_session"}
        set_doc = update_doc.get("$set", {})
        if isinstance(set_doc, dict):
            result.update(set_doc)
        return result


class _FakeDatabase:
    def __init__(self, collection: _FakeCollection) -> None:
        self._collection = collection

    def __getitem__(self, name: str) -> _FakeCollection:
        assert name == "tripadvisor_session_state"
        return self._collection


def test_mark_invalid_with_increment_avoids_conflicting_update_paths(monkeypatch) -> None:
    fake_collection = _FakeCollection()
    monkeypatch.setattr(tripadvisor_session_module, "get_database", lambda: _FakeDatabase(fake_collection))
    service = TripadvisorSessionService()

    asyncio.run(service.mark_invalid(reason="Bot detected", increment_bot_detected=True))

    assert fake_collection.last_update is not None
    update_doc = fake_collection.last_update
    assert update_doc.get("$inc") == {"bot_detected_count": 1}
    assert "bot_detected_count" not in (update_doc.get("$setOnInsert") or {})


def test_mark_invalid_without_increment_initializes_bot_counter(monkeypatch) -> None:
    fake_collection = _FakeCollection()
    monkeypatch.setattr(tripadvisor_session_module, "get_database", lambda: _FakeDatabase(fake_collection))
    service = TripadvisorSessionService()

    asyncio.run(service.mark_invalid(reason="Manual invalidation", increment_bot_detected=False))

    assert fake_collection.last_update is not None
    update_doc = fake_collection.last_update
    assert "$inc" not in update_doc
    assert (update_doc.get("$setOnInsert") or {}).get("bot_detected_count") == 0
