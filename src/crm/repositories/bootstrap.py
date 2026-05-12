from __future__ import annotations

import asyncio

from src.database import get_database


class CRMRepositoryBootstrap:
    """Ensures CRM Mongo indexes in a single place."""

    def __init__(self) -> None:
        self._ensured = False
        self._lock = asyncio.Lock()

    async def ensure_indexes(self) -> None:
        if self._ensured:
            return
        async with self._lock:
            if self._ensured:
                return
            database = get_database()
            leads = database["crm_leads"]
            campaigns = database["crm_campaigns"]
            cadence = database["crm_cadence_templates"]
            messages = database["crm_messages"]
            events = database["crm_events"]
            suppressions = database["crm_suppressions"]
            discovery_runs = database["crm_discovery_runs"]

            await leads.create_index(
                [("email_normalized", 1)],
                name="idx_crm_leads_email_partial_unique",
                unique=True,
                partialFilterExpression={"email_normalized": {"$type": "string"}},
            )
            await leads.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_leads_status_updated",
            )
            await leads.create_index(
                [("business_name_normalized", 1), ("address", 1)],
                name="idx_crm_leads_name_address",
            )
            await leads.create_index(
                [("legal.consent_status", 1), ("legal.do_not_contact", 1)],
                name="idx_crm_leads_legal",
            )

            await campaigns.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_campaign_status_updated",
            )

            await cadence.create_index(
                [("key", 1)],
                name="idx_crm_cadence_key_unique",
                unique=True,
            )

            await messages.create_index(
                [("campaign_id", 1), ("scheduled_at", 1), ("status", 1)],
                name="idx_crm_messages_campaign_schedule_status",
            )
            await messages.create_index(
                [("provider_message_id", 1)],
                name="idx_crm_messages_provider_id",
                sparse=True,
            )
            await messages.create_index(
                [("lead_id", 1), ("status", 1)],
                name="idx_crm_messages_lead_status",
            )

            await events.create_index(
                [("lead_id", 1), ("created_at", -1)],
                name="idx_crm_events_lead_created",
            )
            await events.create_index(
                [("campaign_id", 1), ("created_at", -1)],
                name="idx_crm_events_campaign_created",
            )

            await suppressions.create_index(
                [("email_normalized", 1)],
                name="idx_crm_suppressions_email_unique",
                unique=True,
            )

            await discovery_runs.create_index(
                [("created_at", -1), ("_id", -1)],
                name="idx_crm_discovery_runs_created",
            )
            await discovery_runs.create_index(
                [("job_id", 1)],
                name="idx_crm_discovery_runs_job_id",
                sparse=True,
            )

            self._ensured = True
