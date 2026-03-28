from __future__ import annotations

from src.main import app


def test_scrape_and_analyze_job_routes_are_exposed_without_legacy_queue_path() -> None:
    paths = {route.path for route in app.router.routes if hasattr(route, "path")}

    assert "/business/analyze/queue" not in paths
    assert "/business/analyze/queue/{job_id}" not in paths
    assert "/business/analyze/queue/{job_id}/events" not in paths

    assert "/business/scrape/jobs" in paths
    assert "/business/scrape/jobs/tripadvisor/antibot" in paths
    assert "/business/scrape/jobs/tripadvisor/antibot/relaunch" in paths
    assert "/business/scrape/jobs/{job_id}" in paths
    assert "/business/scrape/jobs/{job_id}/comments" in paths
    assert "/business/scrape/jobs/{job_id}/events" in paths
    assert "/business/scrape/jobs/{job_id}/stop" in paths

    assert "/business/analyze/jobs" in paths
    assert "/business/analyze/jobs/{job_id}" in paths
    assert "/business/analyze/jobs/{job_id}/events" in paths
    assert "/business/{business_id}" in paths
    assert "/business/{business_id}/sources" in paths
    assert "/business/{business_id}/comments" in paths
    assert "/tripadvisor/live-session/status" in paths
    assert "/tripadvisor/live-session/launch" in paths
    assert "/tripadvisor/live-session/stop" in paths
