from __future__ import annotations

from unittest.mock import Mock

from src.services.business_service import BusinessService


def _build_service() -> BusinessService:
    return BusinessService(
        scraper=Mock(),
        tripadvisor_scraper=Mock(),
        preprocessor=Mock(),
        llm_analyzer=Mock(),
        job_service=Mock(),
        query_service=Mock(),
        analyze_use_case=Mock(),
        reanalyze_use_case=Mock(),
        tripadvisor_local_worker_control_service=Mock(),
    )


def test_detect_tripadvisor_antibot_from_captcha_provider_markers() -> None:
    service = _build_service()
    detected, rule = service._detect_tripadvisor_antibot(
        html_text="<html><script src='https://ct.captcha-delivery.com/c.js'></script></html>",
        keyword_matches={"captcha": ["https://geo.captcha-delivery.com/captcha/?initialCid=abc"]},
    )
    assert detected is True
    assert rule.startswith("captcha_provider_markers:")


def test_detect_tripadvisor_antibot_keeps_non_provider_captcha_as_non_blocking() -> None:
    service = _build_service()
    detected, rule = service._detect_tripadvisor_antibot(
        html_text="<html>captcha helper text only</html>",
        keyword_matches={"captcha": ["some random captcha label"]},
    )
    assert detected is False
    assert rule == "captcha_without_companion"
