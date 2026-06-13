from __future__ import annotations

from src.business_catalog.browser_scrape_round_runtime import BrowserScrapeRoundRuntime


def _build_runtime() -> BrowserScrapeRoundRuntime:
    return BrowserScrapeRoundRuntime(
        database_factory=lambda: {},
        job_service=object(),
    )


def test_build_analysis_payload_uses_round_analysis_request_for_hydrated_client_audit() -> None:
    runtime = _build_runtime()

    payload = runtime._build_analysis_payload(  # noqa: SLF001
        round_doc={
            "_id": "round-1",
            "requested_sources": ["tripadvisor"],
            "analysis_request": {
                "report_profile": "client_audit",
                "report_complexity": "hydrated",
                "report_cadence": "quarterly",
                "study_resolution_mode": "refresh_now",
                "include_competitors": True,
                "include_geogrid": True,
            },
        },
        source="tripadvisor",
        source_job_id="job-1",
        business_id="biz-1",
        dataset_id="dataset-1",
        source_profile_id="source-profile-1",
        scrape_run_id="scrape-run-1",
    )

    dumped = payload.model_dump(mode="python")
    assert dumped["report_profile"] == "client_audit"
    assert dumped["report_complexity"] == "hydrated"
    assert dumped["report_cadence"] == "quarterly"
    assert dumped["study_resolution_mode"] == "refresh_now"
    assert dumped["include_competitors"] is True
    assert dumped["include_geogrid"] is True
    assert dumped["source_mode"] == "single"
    assert dumped["selected_source"] == "tripadvisor"


def test_build_analysis_payload_normalizes_classic_report_shape_from_round_analysis_request() -> None:
    runtime = _build_runtime()

    payload = runtime._build_analysis_payload(  # noqa: SLF001
        round_doc={
            "_id": "round-2",
            "requested_sources": ["google_maps", "tripadvisor"],
            "analysis_request": {
                "report_profile": "classic",
                "report_complexity": "hydrated",
                "study_resolution_mode": "refresh_now",
                "include_competitors": True,
                "include_geogrid": True,
            },
        },
        source="google_maps",
        source_job_id="job-2",
        business_id="biz-2",
        dataset_id="dataset-2",
        source_profile_id="source-profile-2",
        scrape_run_id="scrape-run-2",
    )

    dumped = payload.model_dump(mode="python")
    assert dumped["report_profile"] == "classic"
    assert dumped["report_complexity"] == "basic"
    assert dumped["study_resolution_mode"] == "auto_ttl"
    assert dumped["include_competitors"] is False
    assert dumped["include_geogrid"] is False
    assert dumped["source_mode"] == "auto"
    assert dumped["selected_source"] is None
