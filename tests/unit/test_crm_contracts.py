from __future__ import annotations

from src.workers.contracts import (
    CRMCampaignDispatchTaskPayload,
    CRMLeadDiscoveryTaskPayload,
    CRMLeadPipelineTaskPayload,
    build_worker_job_envelope,
    parse_crm_campaign_dispatch_payload,
    parse_crm_lead_discovery_payload,
    parse_crm_lead_pipeline_payload,
)


def test_parse_crm_lead_discovery_payload_legacy_defaults() -> None:
    payload = parse_crm_lead_discovery_payload(
        {
            "queue_name": "crm",
            "job_type": "crm_lead_discovery",
            "query": "restaurante sevilla",
        }
    )

    assert payload.query == "restaurante sevilla"
    assert payload.limit == 100
    assert payload.source == "auto_live_google_maps"
    assert payload.discovery_run_id is None


def test_parse_crm_lead_discovery_payload_from_scrape_google_maps_queue() -> None:
    payload = parse_crm_lead_discovery_payload(
        {
            "queue_name": "scrape_google_maps",
            "job_type": "crm_lead_discovery",
            "payload": {
                "query": "merienda cordoba",
                "limit": 100,
                "source": "auto_live_google_maps",
            },
        }
    )
    assert payload.query == "merienda cordoba"
    assert payload.limit == 100
    assert payload.source == "auto_live_google_maps"


def test_parse_crm_lead_pipeline_payload_sources_normalized() -> None:
    payload = parse_crm_lead_pipeline_payload(
        {
            "queue_name": "crm",
            "job_type": "crm_lead_pipeline",
            "payload": {
                "lead_id": "abc123",
                "sources": ["google_maps", "tripadvisor", "google_maps"],
            },
        }
    )

    assert payload.lead_id == "abc123"
    assert payload.sources == ["google_maps", "tripadvisor"]


def test_parse_crm_campaign_dispatch_payload() -> None:
    payload = parse_crm_campaign_dispatch_payload(
        {
            "queue_name": "crm",
            "job_type": "crm_campaign_dispatch",
            "payload": {
                "campaign_id": "c1",
                "message_id": "m1",
            },
        }
    )

    assert payload.campaign_id == "c1"
    assert payload.message_id == "m1"


def test_build_worker_job_envelope_crm_types() -> None:
    discovery_env = build_worker_job_envelope(
        queue_name="crm",
        job_type="crm_lead_discovery",
        task_payload=CRMLeadDiscoveryTaskPayload(query="hotel cordoba"),
    )
    assert discovery_env.queue_name == "crm"
    assert discovery_env.job_type == "crm_lead_discovery"

    discovery_scrape_env = build_worker_job_envelope(
        queue_name="scrape_google_maps",
        job_type="crm_lead_discovery",
        task_payload=CRMLeadDiscoveryTaskPayload(query="hotel cordoba"),
    )
    assert discovery_scrape_env.queue_name == "scrape_google_maps"
    assert discovery_scrape_env.job_type == "crm_lead_discovery"

    pipeline_env = build_worker_job_envelope(
        queue_name="crm",
        job_type="crm_lead_pipeline",
        task_payload=CRMLeadPipelineTaskPayload(lead_id="l1", sources=["google_maps"]),
    )
    assert pipeline_env.queue_name == "crm"
    assert pipeline_env.job_type == "crm_lead_pipeline"

    dispatch_env = build_worker_job_envelope(
        queue_name="crm",
        job_type="crm_campaign_dispatch",
        task_payload=CRMCampaignDispatchTaskPayload(campaign_id="c1", message_id="m1"),
    )
    assert dispatch_env.queue_name == "crm"
    assert dispatch_env.job_type == "crm_campaign_dispatch"
