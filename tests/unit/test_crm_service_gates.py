from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from src.models.crm import CRMCadenceStep
from src.services.crm_service import CRMService


class _Dummy:
    pass


def _service() -> CRMService:
    return CRMService(job_service=_Dummy(), business_service=_Dummy())


def test_can_send_requires_consent_granted() -> None:
    service = _service()

    async def _fake_suppressed(_value: str) -> bool:
        return False

    service._is_email_suppressed = _fake_suppressed  # type: ignore[method-assign]
    lead_doc = {
        "email": "owner@example.com",
        "legal": {
            "consent_status": "missing",
            "consent_proof": None,
            "do_not_contact": False,
            "unsubscribed_at": None,
        },
    }

    allowed, reason = asyncio.run(service._can_send_to_lead(lead_doc=lead_doc))
    assert allowed is False
    assert reason == "consent_not_granted"


def test_can_send_requires_consent_proof() -> None:
    service = _service()

    async def _fake_suppressed(_value: str) -> bool:
        return False

    service._is_email_suppressed = _fake_suppressed  # type: ignore[method-assign]
    lead_doc = {
        "email": "owner@example.com",
        "legal": {
            "consent_status": "granted",
            "consent_proof": None,
            "do_not_contact": False,
            "unsubscribed_at": None,
        },
    }

    allowed, reason = asyncio.run(service._can_send_to_lead(lead_doc=lead_doc))
    assert allowed is False
    assert reason == "consent_proof_missing"


def test_can_send_blocked_when_suppressed() -> None:
    service = _service()

    async def _fake_suppressed(_value: str) -> bool:
        return True

    service._is_email_suppressed = _fake_suppressed  # type: ignore[method-assign]
    lead_doc = {
        "email": "owner@example.com",
        "legal": {
            "consent_status": "granted",
            "consent_proof": {
                "granted_at": datetime.now(timezone.utc),
                "source": "manual",
                "legal_text_version": "v1",
                "evidence": "checkbox",
            },
            "do_not_contact": False,
            "unsubscribed_at": None,
        },
    }

    allowed, reason = asyncio.run(service._can_send_to_lead(lead_doc=lead_doc))
    assert allowed is False
    assert reason == "suppressed"


def test_can_send_passes_with_valid_optin() -> None:
    service = _service()

    async def _fake_suppressed(_value: str) -> bool:
        return False

    service._is_email_suppressed = _fake_suppressed  # type: ignore[method-assign]
    lead_doc = {
        "email": "owner@example.com",
        "legal": {
            "consent_status": "granted",
            "consent_proof": {
                "granted_at": datetime.now(timezone.utc),
                "source": "manual",
                "legal_text_version": "v1",
                "evidence": "checkbox",
            },
            "do_not_contact": False,
            "unsubscribed_at": None,
        },
    }

    allowed, reason = asyncio.run(service._can_send_to_lead(lead_doc=lead_doc))
    assert allowed is True
    assert reason == "ok"


def test_render_cadence_step_includes_unsubscribe_link() -> None:
    service = _service()
    step = CRMCadenceStep(
        step_order=1,
        step_key="d0_intro",
        delay_days=0,
        subject_template="{business_name}: hola",
        body_template="Resumen: {mini_report}\nBaja: {unsubscribe_url}",
    )
    subject, html = service._render_cadence_step(
        step=step,
        lead_doc={"_id": "lead-1", "business_name": "Bar Demo", "email": "owner@example.com"},
        mini_report="Todo correcto",
    )

    assert "Bar Demo" in subject
    assert "unsubscribe" in html.lower() or "baja" in html.lower()
